# ================================================================================
# KeralaCaptain Bot — Pure Streaming Engine V4.5
# ================================================================================

# ── SECTION 1: IMPORTS & LOGGING ─────────────────────────────────────────────

import os
import time
import hmac
import base64
import hashlib
import random
import signal
import asyncio
import logging
import aiohttp
import sys
import psutil
import aiohttp_jinja2
import jinja2
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web, ClientTimeout
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.errors import (
    FloodWait, AuthBytesInvalid,
    FileReferenceExpired
)
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId, FileType
from pyrogram import raw
from pyrogram.raw.types import InputPhotoFileLocation, InputDocumentFileLocation

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s - %(levelname)s] - %(message)s'
)
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

start_time = time.time()


# ── SECTION 2: CONFIGURATION ──────────────────────────────────────────────────

class Config:
    API_ID           = int(os.environ.get("API_ID", 0))
    API_HASH         = os.environ.get("API_HASH", "")
    BOT_TOKEN        = os.environ.get("BOT_TOKEN", "")
    ADMIN_IDS        = list(int(x) for x in os.environ.get("ADMIN_IDS", "6644681404").split())
    PROTECTED_DOMAIN = os.environ.get("PROTECTED_DOMAIN", "https://www.keralacaptain.shop/").rstrip('/') + '/'
    MONGO_URI        = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID   = int(os.environ.get("LOG_CHANNEL_ID", 0))
    STREAM_URL       = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT             = int(os.environ.get("PORT", 8080))
    PING_INTERVAL    = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU        = 'DYNO' in os.environ

    # ── Bandwidth Thresholds (fallback defaults only) ─────────────────────────
    # These are used ONLY when no DB config exists AND DEFAULT_LIMIT_MODE=True.
    BANDWIDTH_WARNING_BYTES = 85 * 1024 * 1024 * 1024   # 85 GB
    BANDWIDTH_KILL_BYTES    = 90 * 1024 * 1024 * 1024   # 90 GB
    BANDWIDTH_FLUSH_EVERY   = 500 * 1024 * 1024          # Flush to DB every 500 MB

    # ── [NEW] VPS-Ready Default Limit Mode ────────────────────────────────────
    # True  = limit is ON by default (Render/Heroku free-plan mode).
    # False = limit is OFF by default (VPS with unlimited bandwidth).
    # Set DEFAULT_LIMIT_MODE=false in .env when moving to VPS.
    DEFAULT_LIMIT_MODE = os.environ.get("DEFAULT_LIMIT_MODE", "true").lower() == "true"

    # ── Streaming Throttle ────────────────────────────────────────────────────
    BURST_DURATION_SECS      = 10
    THROTTLE_SLEEP_SECS      = 0.5
    THROTTLE_JITTER_SECS     = 0.15
    DATA_ESCALATION_MB       = 80
    DATA_ESCALATION_SLEEP    = 1.5

    # ── V4.5 Connection Lifespan (THE MAIN FIX) ──────────────────────────────
    MAX_CONNECTION_LIFESPAN_SECS = 50

    # ── V4.5 Per-IP Concurrent Connection Limit ───────────────────────────────
    MAX_CONNECTIONS_PER_IP = 4


required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
    Config.ADMIN_IDS
]
if not all(required_vars) or Config.ADMIN_IDS == [0]:
    LOGGER.critical(
        "FATAL: One or more required env vars are missing: "
        "API_ID, API_HASH, BOT_TOKEN, MONGO_URI, LOG_CHANNEL_ID, STREAM_URL, ADMIN_IDS"
    )
    exit(1)

CURRENT_PROTECTED_DOMAIN = Config.PROTECTED_DOMAIN


# ── SECTION 3: HELPER FUNCTIONS ───────────────────────────────────────────────

async def encode(string: str) -> str:
    """Base64-encodes a string for use in stream URLs."""
    string_bytes = string.encode("ascii")
    base64_bytes = base64.urlsafe_b64encode(string_bytes)
    return (base64_bytes.decode("ascii")).strip("=")


async def decode(base64_string: str) -> str:
    """Decodes a base64-encoded stream URL string."""
    base64_string = base64_string.strip("=")
    base64_bytes  = (base64_string + "=" * (-len(base64_string) % 4)).encode("ascii")
    string_bytes  = base64.urlsafe_b64decode(base64_bytes)
    return string_bytes.decode("ascii")


def humanbytes(size) -> str:
    """Converts a byte count into a human-readable string (KB, MB, GB, TB)."""
    if not size:
        return "0 B"
    power        = 1024
    n            = 0
    power_labels = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n    += 1
    return f"{round(size, 2)} {power_labels[n]}B"


def get_readable_time(seconds: int) -> str:
    """Converts seconds into a human-readable duration string (e.g. 1d 2h 3m 4s)."""
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days:
        result += f"{days}d "
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours:
        result += f"{hours}h "
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes:
        result += f"{minutes}m "
    result += f"{int(seconds)}s"
    return result


# ── SECTION 4: TOKEN SYSTEM — HMAC-SHA256, 60-second validity ────────────────
# Token format (after base64url decoding): "<video_id>:<unix_timestamp>:<hmac_hex>"
# Secret key = BOT_TOKEN (never sent to clients). Completely unchanged from V4.4.

TOKEN_MAX_AGE_SECS = 60


def generate_stream_token(video_id: str) -> str:
    """Generates a fresh HMAC-SHA256 token for video_id, valid for TOKEN_MAX_AGE_SECS."""
    timestamp = int(time.time())
    message   = f"{video_id}:{timestamp}"
    sig       = hmac.new(
        Config.BOT_TOKEN.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()
    raw_token = f"{message}:{sig}"
    return base64.urlsafe_b64encode(raw_token.encode()).decode().rstrip("=")


def verify_stream_token(token: str) -> bool:
    """Verifies HMAC, timestamp freshness. Returns True only if ALL checks pass."""
    try:
        padded  = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode()).decode()
        parts = decoded.rsplit(":", 2)
        if len(parts) != 3:
            return False
        video_id, timestamp_str, received_sig = parts
        message      = f"{video_id}:{timestamp_str}"
        expected_sig = hmac.new(
            Config.BOT_TOKEN.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected_sig, received_sig):
            return False
        token_age = time.time() - int(timestamp_str)
        if token_age > TOKEN_MAX_AGE_SECS or token_age < 0:
            return False
        return True
    except Exception:
        return False


# ── SECTION 5: V4.5 SECURITY HELPERS ─────────────────────────────────────────

_BLOCKED_UA_FRAGMENTS: frozenset = frozenset([
    # Dedicated download managers
    "1dm",
    "idm/",
    "internet download manager",
    "fdm",
    "free download manager",
    "jdownloader",
    "getright",
    "flashget",
    "xunlei",
    "thunder/",
    "bitcomet",
    "bittorrent",
    "utorrent",
    "aria2",
    "axel",
    # Generic HTTP tools (not browsers)
    "wget/",
    "curl/",
    "libcurl",
    "httpget",
    "lwp-",
    "libwww-perl",
    "python-requests",
    "python-urllib",
    "python/",
    "java/",
    "apachehttpclient",
    "apache-httpclient",
    "go-http-client",
    "ruby",
    "perl/",
    # Android automation / download
    "dalvik/",
    "okhttp/",
    "downloadmanager",
    "android download",
    "uiautomator",
    # Bots & scrapers
    "bot",
    "spider",
    "crawler",
    "scraper",
    "headless",
    "phantomjs",
    "selenium",
    "puppeteer",
    "playwright",
])


def _is_download_manager_ua(ua: str) -> bool:
    """Returns True if the UA belongs to a known DM or non-browser HTTP client."""
    if not ua or not ua.strip():
        return True
    ua_lower = ua.lower()
    return any(fragment in ua_lower for fragment in _BLOCKED_UA_FRAGMENTS)


def _passes_header_integrity_check(request: web.Request) -> bool:
    """Validates that the request carries headers consistent with a real browser."""
    has_accept_encoding = bool(request.headers.get("Accept-Encoding", "").strip())
    has_accept_language = bool(request.headers.get("Accept-Language", "").strip())
    return has_accept_encoding or has_accept_language


# ── SECTION 6: V4.5 PER-IP CONNECTION TRACKING ───────────────────────────────

_ip_connection_counts: dict = {}


async def _cleanup_ip_counters_task():
    """Background task: removes stale zero-count entries from _ip_connection_counts."""
    while True:
        await asyncio.sleep(300)
        stale_ips = [ip for ip, count in list(_ip_connection_counts.items()) if count <= 0]
        for ip in stale_ips:
            _ip_connection_counts.pop(ip, None)
        if stale_ips:
            LOGGER.debug(f"[CLEANUP] Removed {len(stale_ips)} stale IP counter entries.")


# ── SECTION 7: DATABASE SETUP ─────────────────────────────────────────────────

db_client = AsyncIOMotorClient(Config.MONGO_URI)
db        = db_client['KeralaCaptainBotDB']

# Original collections — STRICTLY DO NOT MODIFY OR REMOVE
media_collection        = db['media']
media_backup_collection = db['media_backup']
user_conversations_col  = db['conversations']
settings_collection     = db['settings']

# Per-bot bandwidth tracking
bandwidth_collection    = db['bandwidth']

# Permanent global lifetime stats. ONE document ever: { "_id": "global_stats" }
lifetime_stats_collection = db['lifetime_stats']

# Live viewer tracking via heartbeat
heartbeat_collection    = db['heartbeats']

# [NEW] Dynamic bandwidth limit configuration (global & per-bot)
bw_limits_collection    = db['bw_limits']


# ── SECTION 8: DATABASE FUNCTIONS ─────────────────────────────────────────────

async def check_duplicate(tmdb_id):
    """Checks for duplicates only in the main collection."""
    return await media_collection.find_one({"tmdb_id": tmdb_id})


async def add_media_to_db(data):
    """Inserts new media data into both the main and backup collections."""
    await media_collection.insert_one(data)
    await media_backup_collection.insert_one(data)


async def get_media_by_post_id(post_id: int):
    """Reads media data from the main collection."""
    return await media_collection.find_one({"wp_post_id": post_id})


async def update_media_links_in_db(post_id: int, new_message_ids: dict, new_stream_link: str):
    """Updates links in both the main and backup collections."""
    update_query = {"$set": {"message_ids": new_message_ids, "stream_link": new_stream_link}}
    await media_collection.update_one({"wp_post_id": post_id}, update_query)
    await media_backup_collection.update_one({"wp_post_id": post_id}, update_query)


async def delete_media_from_db(post_id: int):
    """Deletes media data from both the main and backup collections."""
    result_main = await media_collection.delete_one({"wp_post_id": post_id})
    await media_backup_collection.delete_one({"wp_post_id": post_id})
    return result_main


async def get_stats():
    """Calculates stats based only on the main collection."""
    movies_count = await media_collection.count_documents({"type": "movie"})
    series_count = await media_collection.count_documents({"type": "series"})
    return movies_count, series_count


async def get_all_media_for_library(page: int = 0, limit: int = 10):
    """Fetches the library list from the main collection."""
    cursor = media_collection.find().sort("added_at", -1).skip(page * limit).limit(limit)
    return await cursor.to_list(length=limit)


async def get_user_conversation(chat_id):
    """Gets user conversation state from DB."""
    return await user_conversations_col.find_one({"_id": chat_id})


async def update_user_conversation(chat_id, data):
    """Sets or clears user conversation state in DB."""
    if data:
        await user_conversations_col.update_one({"_id": chat_id}, {"$set": data}, upsert=True)
    else:
        await user_conversations_col.delete_one({"_id": chat_id})


async def get_post_id_from_msg_id(msg_id: int):
    """Helper for stream refresh — finds which post_id owns a given message_id."""
    doc = await media_collection.find_one({"message_ids": {"$in": [msg_id]}})
    return doc['wp_post_id'] if doc else None


async def get_protected_domain() -> str:
    """Fetches the protected domain from DB settings; falls back to Config default."""
    try:
        doc = await settings_collection.find_one({"_id": "bot_settings"})
        if doc and "protected_domain" in doc:
            return doc["protected_domain"]
    except Exception as e:
        LOGGER.error(f"Could not fetch domain from DB: {e}. Using default.")
    return Config.PROTECTED_DOMAIN


async def set_protected_domain(new_domain: str) -> str:
    """Saves a new protected domain to the database and updates the global variable."""
    global CURRENT_PROTECTED_DOMAIN
    if not (new_domain.startswith("https://") or new_domain.startswith("http://")):
        new_domain = "https://" + new_domain
    if not new_domain.endswith('/'):
        new_domain += '/'
    await settings_collection.update_one(
        {"_id": "bot_settings"},
        {"$set": {"protected_domain": new_domain}},
        upsert=True
    )
    CURRENT_PROTECTED_DOMAIN = new_domain
    LOGGER.info(f"Protected domain updated in DB: {new_domain}")
    return new_domain


# ── SECTION 9: BANDWIDTH TRACKING & DEAD MODE ────────────────────────────────
# Core bandwidth accounting. Unchanged from V4.4 except:
#  • Lifetime stats increment BEFORE the IS_DEAD check (Golden Rule 2 fix).
#  • Warning & kill thresholds now use the dynamic in-memory limit cache
#    (_effective_limit_enabled / _effective_limit_bytes) set in Section 10.

BOT_USERNAME = ""

_bandwidth_in_memory   = 0
_bandwidth_since_flush = 0
IS_DEAD                = False
_warning_85gb_sent     = False


async def load_bandwidth_state():
    """
    Loads this bot's bandwidth counter and Dead Mode state from MongoDB.
    Uses the STREAM_URL as the unique DB ID. Also loads the effective limit.
    """
    global _bandwidth_in_memory, IS_DEAD, _warning_85gb_sent

    if not Config.STREAM_URL:
        LOGGER.error("[BANDWIDTH] STREAM_URL not set. Cannot load bandwidth state.")
        return

    doc = await bandwidth_collection.find_one({"_id": Config.STREAM_URL})
    if doc:
        _bandwidth_in_memory = doc.get("bandwidth_used", 0)
        IS_DEAD              = doc.get("is_dead", False)
        _warning_85gb_sent   = doc.get("warning_sent", False)
        LOGGER.info(
            f"[BANDWIDTH] Loaded state for URL {Config.STREAM_URL}: "
            f"Used={humanbytes(_bandwidth_in_memory)}, Dead={IS_DEAD}"
        )
    else:
        _bandwidth_in_memory = 0
        IS_DEAD              = False
        _warning_85gb_sent   = False
        await bandwidth_collection.insert_one({
            "_id":            Config.STREAM_URL,
            "bot_username":   BOT_USERNAME,
            "bandwidth_used": 0,
            "is_dead":        False,
            "warning_sent":   False,
            "created_at":     datetime.utcnow()
        })
        LOGGER.info(f"[BANDWIDTH] New URL detected ({Config.STREAM_URL}) - counter starts at 0.")

    # Load effective dynamic limit on startup
    await refresh_effective_limit()


async def flush_bandwidth_to_db():
    """Persists the current in-memory bandwidth counter to MongoDB using STREAM_URL."""
    if not Config.STREAM_URL:
        return
    await bandwidth_collection.update_one(
        {"_id": Config.STREAM_URL},
        {"$set": {
            "bot_username":   BOT_USERNAME,
            "bandwidth_used": _bandwidth_in_memory,
            "is_dead":        IS_DEAD,
            "warning_sent":   _warning_85gb_sent,
            "last_updated":   datetime.utcnow()
        }},
        upsert=True
    )


async def add_bandwidth(bytes_sent: int):
    """
    Adds bytes_sent to the in-memory counter and checks dynamic kill thresholds.
    Called from inside the stream_handler loop so even partial streams are counted.

    GOLDEN RULE: Lifetime stats ALWAYS increment, even when IS_DEAD is True.
    """
    global _bandwidth_in_memory, _bandwidth_since_flush, IS_DEAD, _warning_85gb_sent

    # Lifetime stats increment UNCONDITIONALLY (regardless of dead/limit state)
    asyncio.create_task(_increment_lifetime_bandwidth_db(bytes_sent))

    if IS_DEAD:
        return

    _bandwidth_in_memory   += bytes_sent
    _bandwidth_since_flush += bytes_sent

    if _bandwidth_since_flush >= Config.BANDWIDTH_FLUSH_EVERY:
        _bandwidth_since_flush = 0
        await flush_bandwidth_to_db()
        LOGGER.info(f"[BANDWIDTH] Flushed to DB. Total used: {humanbytes(_bandwidth_in_memory)}")

    # ── Dynamic Warning & Kill Checks ─────────────────────────────────────────
    # Uses the in-memory cache updated by refresh_effective_limit() every 60 s.
    # If the limit is disabled (unlimited mode), neither warning nor kill fires.
    if _effective_limit_enabled:
        # Warning fires 5 GB before the kill limit (scales with whatever limit is set)
        warning_threshold = max(0, _effective_limit_bytes - (5 * 1024 * 1024 * 1024))
        if not _warning_85gb_sent and _bandwidth_in_memory >= warning_threshold:
            _warning_85gb_sent = True
            await flush_bandwidth_to_db()
            LOGGER.warning(f"[BANDWIDTH] WARNING threshold reached: {humanbytes(_bandwidth_in_memory)}")
            try:
                for admin_id in Config.ADMIN_IDS:
                    await main_bot.send_message(
                        admin_id,
                        f"⚠️ **BANDWIDTH WARNING!**\n\n"
                        f"**Bot:** @{BOT_USERNAME}\n"
                        f"**Used:** `{humanbytes(_bandwidth_in_memory)}`\n\n"
                        f"Approaching the **{humanbytes(_effective_limit_bytes)} auto-kill limit.**\n\n"
                        f"👉 Go to **Admin Panel → 🚦 Bandwidth Limits** to increase the "
                        f"limit or turn it OFF before the bot goes to sleep!"
                    )
            except Exception as e:
                LOGGER.error(f"Could not send bandwidth warning to admin: {e}")

        if _bandwidth_in_memory >= _effective_limit_bytes:
            await trigger_dead_mode(reason="auto")


async def trigger_dead_mode(reason: str = "auto"):
    """Puts the bot into Dead Mode. Can be revived instantly via admin panel."""
    global IS_DEAD

    if IS_DEAD:
        return

    IS_DEAD = True
    LOGGER.critical(
        f"[DEAD MODE] URL {Config.STREAM_URL} is now DEAD. "
        f"Reason: {reason}. Bandwidth used: {humanbytes(_bandwidth_in_memory)}"
    )

    await bandwidth_collection.update_one(
        {"_id": Config.STREAM_URL},
        {"$set": {
            "bot_username":   BOT_USERNAME,
            "bandwidth_used": _bandwidth_in_memory,
            "is_dead":        True,
            "warning_sent":   _warning_85gb_sent,
            "dead_reason":    reason,
            "dead_at":        datetime.utcnow()
        }},
        upsert=True
    )

    try:
        limit_str   = humanbytes(_effective_limit_bytes) if _effective_limit_enabled else "N/A"
        reason_text = (
            f"automatically (**{limit_str}** bandwidth limit reached)"
            if reason == "auto" else "**manually** by admin"
        )
        for admin_id in Config.ADMIN_IDS:
            await main_bot.send_message(
                admin_id,
                f"🔴 **BOT IS NOW IN SLEEP (DEAD) MODE**\n\n"
                f"**Bot:** @{BOT_USERNAME}\n"
                f"**URL:** `{Config.STREAM_URL}`\n"
                f"**Killed:** {reason_text}\n"
                f"**Total bandwidth used:** `{humanbytes(_bandwidth_in_memory)}`\n\n"
                f"The bot will no longer serve any video streams.\n\n"
                f"**✨ To Revive Without Restart:**\n"
                f"Go to **Admin Panel → 🚦 Bandwidth Limits** and either increase "
                f"this bot's limit or turn it OFF. The bot wakes up instantly!"
            )
    except Exception as e:
        LOGGER.error(f"Could not send Dead Mode notification: {e}")


def get_bandwidth_info() -> dict:
    """Returns a snapshot of current bandwidth info. Used by admin panel and /health."""
    kill_threshold = _effective_limit_bytes if _effective_limit_enabled else 0
    percent = (
        round((_bandwidth_in_memory / kill_threshold) * 100, 2)
        if (kill_threshold > 0 and _effective_limit_enabled) else 0.0
    )
    return {
        "used":                 _bandwidth_in_memory,
        "used_human":           humanbytes(_bandwidth_in_memory),
        "is_dead":              IS_DEAD,
        "warning_sent":         _warning_85gb_sent,
        "limit_enabled":        _effective_limit_enabled,
        "kill_threshold":       kill_threshold,
        "kill_threshold_human": humanbytes(kill_threshold) if kill_threshold > 0 else "Unlimited",
        "warning_threshold":    max(0, kill_threshold - 5 * 1024 * 1024 * 1024) if _effective_limit_enabled else 0,
        "percent":              percent
    }


# ── SECTION 10: DYNAMIC BANDWIDTH LIMIT SYSTEM ───────────────────────────────
#
#  DB Schema (bw_limits_collection):
#    Global:   { "_id": "global",               "enabled": bool, "limit_bytes": int }
#    Specific: { "_id": "specific:<stream_url>", "enabled": bool, "limit_bytes": int }
#
#  Override Priority: Specific > Global > Config.DEFAULT_LIMIT_MODE fallback
#
#  In-memory cache (refreshed every 60 s by _bw_limit_checker_task):
#    _effective_limit_enabled  — whether the limit is active for this bot
#    _effective_limit_bytes    — the active kill threshold in bytes
#
#  Auto-Revive: if IS_DEAD and admin increases/disables the limit via the panel,
#  _bw_limit_checker_task detects the change within 60 s and wakes the bot
#  up instantly — no restart required.

_effective_limit_enabled: bool = True
_effective_limit_bytes:   int  = Config.BANDWIDTH_KILL_BYTES


async def get_global_bw_config() -> dict | None:
    """Returns the global BW limit document from DB, or None if never set."""
    return await bw_limits_collection.find_one({"_id": "global"})


async def get_specific_bw_config(stream_url: str) -> dict | None:
    """Returns the per-bot BW limit document from DB, or None if never set."""
    return await bw_limits_collection.find_one({"_id": f"specific:{stream_url}"})


async def set_global_bw_config(enabled: bool, limit_bytes: int):
    """Saves / updates the global BW limit config in MongoDB."""
    await bw_limits_collection.update_one(
        {"_id": "global"},
        {"$set": {"enabled": enabled, "limit_bytes": limit_bytes, "updated_at": datetime.utcnow()}},
        upsert=True
    )
    LOGGER.info(
        f"[BW LIMITS] Global config saved: enabled={enabled}, "
        f"limit={humanbytes(limit_bytes) if enabled else 'Unlimited'}"
    )


async def set_specific_bw_config(stream_url: str, enabled: bool, limit_bytes: int):
    """Saves / updates a per-bot BW limit config in MongoDB."""
    await bw_limits_collection.update_one(
        {"_id": f"specific:{stream_url}"},
        {"$set": {
            "enabled":    enabled,
            "limit_bytes": limit_bytes,
            "stream_url": stream_url,
            "updated_at": datetime.utcnow()
        }},
        upsert=True
    )
    LOGGER.info(
        f"[BW LIMITS] Specific config saved for {stream_url}: enabled={enabled}, "
        f"limit={humanbytes(limit_bytes) if enabled else 'Unlimited'}"
    )


async def delete_specific_bw_config(stream_url: str):
    """Removes a per-bot specific BW config (bot reverts to global rule)."""
    await bw_limits_collection.delete_one({"_id": f"specific:{stream_url}"})
    LOGGER.info(f"[BW LIMITS] Specific config removed for {stream_url}. Reverts to global.")


async def get_effective_bw_config(stream_url: str) -> tuple:
    """
    Returns (limit_enabled: bool, limit_bytes: int) for the given stream_url.
    Specific rule overrides Global. Falls back to Config.DEFAULT_LIMIT_MODE if no DB doc.
    """
    # 1. Check specific rule first
    specific = await get_specific_bw_config(stream_url)
    if specific is not None:
        return specific.get("enabled", True), specific.get("limit_bytes", Config.BANDWIDTH_KILL_BYTES)

    # 2. Check global rule
    global_cfg = await get_global_bw_config()
    if global_cfg is not None:
        return global_cfg.get("enabled", True), global_cfg.get("limit_bytes", Config.BANDWIDTH_KILL_BYTES)

    # 3. No DB config — fall back to DEFAULT_LIMIT_MODE
    if Config.DEFAULT_LIMIT_MODE:
        return True, Config.BANDWIDTH_KILL_BYTES
    else:
        return False, 0  # Unlimited (VPS mode)


async def refresh_effective_limit():
    """
    Fetches current effective BW limit from MongoDB and updates the in-memory cache.
    Also performs Auto-Revive: if IS_DEAD and the new effective limit allows streaming,
    sets IS_DEAD = False and notifies the admin — NO restart required.
    """
    global _effective_limit_enabled, _effective_limit_bytes, IS_DEAD, _warning_85gb_sent

    try:
        enabled, limit_bytes = await get_effective_bw_config(Config.STREAM_URL)
        # Safety: if enabled but limit_bytes is 0 or negative, use the Config default
        if enabled and limit_bytes <= 0:
            limit_bytes = Config.BANDWIDTH_KILL_BYTES

        prev_enabled = _effective_limit_enabled
        prev_bytes   = _effective_limit_bytes

        _effective_limit_enabled = enabled
        _effective_limit_bytes   = limit_bytes if limit_bytes > 0 else Config.BANDWIDTH_KILL_BYTES

        if (prev_enabled != enabled) or (prev_bytes != _effective_limit_bytes):
            limit_str = "Unlimited" if not enabled else humanbytes(_effective_limit_bytes)
            LOGGER.info(f"[BW LIMITS] Effective limit updated: {limit_str}")

        # ── Auto-Revive Logic ─────────────────────────────────────────────────
        # Revive if: limit is turned OFF, or limit is ON but higher than current usage.
        if IS_DEAD:
            should_revive = (not enabled) or (enabled and limit_bytes > _bandwidth_in_memory)
            if should_revive:
                IS_DEAD            = False
                _warning_85gb_sent = False  # Reset warning so it fires again near new threshold
                await bandwidth_collection.update_one(
                    {"_id": Config.STREAM_URL},
                    {"$set": {
                        "is_dead":      False,
                        "warning_sent": False,
                        "last_updated": datetime.utcnow()
                    }},
                    upsert=True
                )
                limit_str = "Unlimited" if not enabled else humanbytes(limit_bytes)
                LOGGER.info(
                    f"[AUTO-REVIVE] Bot revived! New effective limit: {limit_str}. "
                    f"Current usage: {humanbytes(_bandwidth_in_memory)}"
                )
                try:
                    for admin_id in Config.ADMIN_IDS:
                        await main_bot.send_message(
                            admin_id,
                            f"🟢 **BOT AUTO-REVIVED!**\n\n"
                            f"**Bot:** @{BOT_USERNAME}\n"
                            f"**New Effective Limit:** `{limit_str}`\n"
                            f"**Current Usage:** `{humanbytes(_bandwidth_in_memory)}`\n\n"
                            f"The bot is **active and serving streams** again. ✅\n"
                            f"No restart was required!"
                        )
                except Exception as e:
                    LOGGER.error(f"[AUTO-REVIVE] Could not send notification: {e}")

    except Exception as e:
        LOGGER.error(f"[BW LIMITS] Error in refresh_effective_limit: {e}")


async def _bw_limit_checker_task():
    """
    Background task: polls MongoDB every 60 s for updated BW limit settings.
    This is what makes Admin Panel changes take effect on all running bots
    without a restart, and what triggers instant Auto-Revive.
    """
    while True:
        await asyncio.sleep(60)
        try:
            await refresh_effective_limit()
        except Exception as e:
            LOGGER.error(f"[BW LIMIT CHECKER] Unexpected error: {e}")


# ── SECTION 11: LIFETIME GLOBAL STATISTICS ───────────────────────────────────

async def _increment_lifetime_bandwidth_db(bytes_sent: int):
    """Increments the permanent shared lifetime bandwidth counter. Always called."""
    try:
        await lifetime_stats_collection.update_one(
            {"_id": "global_stats"},
            {"$inc": {"total_bandwidth_bytes": bytes_sent}},
            upsert=True
        )
    except Exception as e:
        LOGGER.error(f"[LIFETIME STATS] Failed to increment bandwidth: {e}")


async def increment_lifetime_streams():
    """Increments the permanent shared lifetime stream counter."""
    try:
        await lifetime_stats_collection.update_one(
            {"_id": "global_stats"},
            {"$inc": {"total_streams": 1}},
            upsert=True
        )
    except Exception as e:
        LOGGER.error(f"[LIFETIME STATS] Failed to increment streams: {e}")


async def get_lifetime_stats() -> dict:
    """Fetches and returns the global lifetime stats document from MongoDB."""
    doc = await lifetime_stats_collection.find_one({"_id": "global_stats"})
    if doc:
        return {
            "total_bandwidth_bytes": doc.get("total_bandwidth_bytes", 0),
            "total_bandwidth_human": humanbytes(doc.get("total_bandwidth_bytes", 0)),
            "total_streams":         doc.get("total_streams", 0)
        }
    return {"total_bandwidth_bytes": 0, "total_bandwidth_human": "0 B", "total_streams": 0}


# ── SECTION 12: STREAMING ENGINE — ByteStreamer CLASS ─────────────────────────
# COMPLETELY UNCHANGED FROM V4.1. DO NOT MODIFY ANYTHING IN THIS SECTION.

multi_clients          = {}
work_loads             = {}
class_cache            = {}
processed_media_groups = {}
next_client_idx        = 0
stream_errors          = 0
last_error_reset       = time.time()


class ByteStreamer:
    """
    Original ByteStreamer from V4.1. Completely unchanged.
    Do NOT modify chunk_size or any logic inside yield_file().
    """

    def __init__(self, client: Client):
        self.client: Client  = client
        self.cached_file_ids = {}
        self.session_cache   = {}
        asyncio.create_task(self.clean_cache_regularly())

    async def clean_cache_regularly(self):
        """Clears in-memory file property and session caches every 20 minutes."""
        while True:
            await asyncio.sleep(1200)
            self.cached_file_ids.clear()
            self.session_cache.clear()
            LOGGER.info("Cleared ByteStreamer's cached file properties and sessions.")

    async def get_file_properties(self, message_id: int) -> FileId:
        """Gets file metadata from Telegram (or in-memory cache)."""
        if message_id in self.cached_file_ids:
            return self.cached_file_ids[message_id]

        message = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
        if not message or message.empty or not (message.document or message.video):
            raise FileNotFoundError(f"No media found for message_id={message_id}")

        media   = message.document or message.video
        file_id = FileId.decode(media.file_id)
        setattr(file_id, "file_size", media.file_size or 0)
        setattr(file_id, "mime_type", media.mime_type or "video/mp4")
        setattr(file_id, "file_name", media.file_name or "Unknown.mp4")

        self.cached_file_ids[message_id] = file_id
        return file_id

    async def generate_media_session(self, file_id: FileId) -> Session:
        """Creates or reuses a Pyrogram media session for the file's DC."""
        media_session = self.client.media_sessions.get(file_id.dc_id)
        dc_id         = file_id.dc_id

        if dc_id in self.session_cache:
            session, ts = self.session_cache[dc_id]
            if time.time() - ts < 300:
                LOGGER.debug(f"Reusing TTL-cached media session for DC {dc_id}")
                return session

        if media_session:
            try:
                await media_session.send(raw.functions.help.GetConfig(), timeout=10)
                self.session_cache[dc_id] = (media_session, time.time())
                LOGGER.debug(f"Reusing pinged media session for DC {dc_id}")
                return media_session
            except Exception as e:
                LOGGER.warning(f"Existing session for DC {dc_id} is stale: {e}. Recreating.")
                try:
                    await media_session.stop()
                except Exception:
                    pass
                if dc_id in self.client.media_sessions:
                    del self.client.media_sessions[dc_id]
                media_session = None

        LOGGER.info(f"Creating new media session for DC {dc_id}")
        if dc_id != await self.client.storage.dc_id():
            media_session = Session(
                self.client, dc_id,
                await Auth(self.client, dc_id, await self.client.storage.test_mode()).create(),
                await self.client.storage.test_mode(),
                is_media=True
            )
            await media_session.start()
            for i in range(3):
                try:
                    exported_auth = await self.client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                    )
                    await media_session.send(
                        raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id, bytes=exported_auth.bytes
                        )
                    )
                    break
                except AuthBytesInvalid as e:
                    LOGGER.warning(f"AuthBytesInvalid attempt {i+1}: {e}")
                    if i == 2:
                        raise
                    await asyncio.sleep(1)
        else:
            media_session = Session(
                self.client, dc_id,
                await self.client.storage.auth_key(),
                await self.client.storage.test_mode(),
                is_media=True
            )
            await media_session.start()

        self.client.media_sessions[dc_id] = media_session
        self.session_cache[dc_id]          = (media_session, time.time())
        return media_session

    @staticmethod
    def get_location(file_id: FileId):
        """Builds the Pyrogram raw file location for GetFile calls."""
        if file_id.file_type == FileType.PHOTO:
            return InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )
        else:
            return InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )

    async def yield_file(self, file_id: FileId, offset: int, chunk_size: int, message_id: int):
        """
        COMPLETELY UNCHANGED from V4.1.
        Yields 1 MB chunks from Telegram. chunk_size is always 1024 * 1024.
        FileReferenceExpired refresh logic is untouched.
        """
        media_session  = await self.generate_media_session(file_id)
        location       = self.get_location(file_id)
        current_offset = offset
        retry_count    = 0
        max_retries    = 3

        while True:
            try:
                chunk = await media_session.send(
                    raw.functions.upload.GetFile(
                        location=location, offset=current_offset, limit=chunk_size
                    ),
                    timeout=30
                )
                if isinstance(chunk, raw.types.upload.File) and chunk.bytes:
                    yield chunk.bytes
                    if len(chunk.bytes) < chunk_size:
                        break
                    current_offset += len(chunk.bytes)
                else:
                    break

            except FileReferenceExpired:
                retry_count += 1
                if retry_count > max_retries:
                    raise
                LOGGER.warning(
                    f"FileReferenceExpired for msg {message_id}, "
                    f"retry {retry_count}/{max_retries}. Refreshing..."
                )
                original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                if original_msg:
                    refreshed_msg = await forward_file_safely(original_msg)
                    if refreshed_msg:
                        new_file_id = await self.get_file_properties(refreshed_msg.id)
                        self.cached_file_ids[message_id] = new_file_id

                        post_id = await get_post_id_from_msg_id(message_id)
                        if post_id:
                            media_doc = await get_media_by_post_id(post_id)
                            if media_doc:
                                old_qualities = media_doc['message_ids']
                                quality_key   = next(
                                    (k for k, v in old_qualities.items() if v == message_id), None
                                )
                                new_qualities = old_qualities
                                if quality_key:
                                    new_qualities[quality_key] = refreshed_msg.id
                                else:
                                    new_qualities = {
                                        k: refreshed_msg.id if v == message_id else v
                                        for k, v in old_qualities.items()
                                    }
                                await update_media_links_in_db(
                                    post_id, new_qualities, media_doc['stream_link']
                                )

                        location = self.get_location(new_file_id)
                        await asyncio.sleep(2)
                        continue
                raise

            except FloodWait as e:
                LOGGER.warning(f"FloodWait of {e.value}s on GetFile. Waiting...")
                await asyncio.sleep(e.value)
                continue


# ── SECTION 13: WEB ROUTES ────────────────────────────────────────────────────

routes = web.RouteTableDef()


@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(
        text="Welcome to KeralaCaptain's Streaming Service!",
        content_type='text/html'
    )


@routes.get("/health")
async def health_handler(request):
    """Health check endpoint with bandwidth and client info."""
    global stream_errors, last_error_reset
    if time.time() - last_error_reset > 60:
        stream_errors    = 0
        last_error_reset = time.time()

    bw_info    = get_bandwidth_info()
    cache_size = 0
    if multi_clients:
        sample_client = list(multi_clients.values())[0]
        if sample_client in class_cache:
            cache_size = len(class_cache[sample_client].cached_file_ids)

    active_ip_slots = sum(_ip_connection_counts.values())

    return web.json_response({
        "status":                    "dead" if IS_DEAD else "ok",
        "active_clients":            len(multi_clients),
        "property_cache_size":       cache_size,
        "stream_errors_last_min":    stream_errors,
        "workloads":                 work_loads,
        "bandwidth_used":            bw_info["used_human"],
        "bandwidth_limit":           bw_info["kill_threshold_human"],
        "bandwidth_percent":         f"{bw_info['percent']}%",
        "limit_enabled":             bw_info["limit_enabled"],
        "active_ip_stream_slots":    active_ip_slots,
        "max_conn_per_ip":           Config.MAX_CONNECTIONS_PER_IP,
        "max_connection_lifespan_s": Config.MAX_CONNECTION_LIFESPAN_SECS,
    })


@routes.get("/favicon.ico")
async def favicon_handler(request):
    return web.Response(status=204)


@routes.get("/sw.js")
async def sw_js_handler(request: web.Request):
    """
    Serves the Service Worker JavaScript file (sw.js) from the templates dir.
    Required headers: correct MIME type, Service-Worker-Allowed scope, no-cache.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sw_path  = os.path.join(base_dir, 'templates', 'sw.js')
    try:
        return web.FileResponse(
            sw_path,
            headers={
                'Content-Type':           'application/javascript; charset=utf-8',
                'Service-Worker-Allowed': '/',
                'Cache-Control':          'no-cache, no-store, must-revalidate',
                'Pragma':                 'no-cache',
            }
        )
    except FileNotFoundError:
        LOGGER.error(f"[SW] sw.js not found at: {sw_path}")
        return web.Response(status=404, text='sw.js not found. Place it in the templates/ directory.')


@routes.get("/watch")
async def watch_handler(request: web.Request):
    """Renders and serves player.html. Passes all query params to the template."""
    query_params = dict(request.rel_url.query)
    if 'id' not in query_params and 'view_id' not in query_params:
        return web.Response(status=400, text="400 Bad Request: Missing video ID parameter.")
    return aiohttp_jinja2.render_template('player.html', request, context={'query': query_params})


@routes.get("/api/get_token")
async def get_token_handler(request: web.Request):
    """
    GET /api/get_token?video_id=<id>
    Returns a fresh HMAC token valid for 60 seconds.
    """
    video_id = request.rel_url.query.get('video_id', 'unknown')
    if IS_DEAD:
        return web.json_response(
            {"error": "Service unavailable: bandwidth limit reached."},
            status=503
        )
    token = generate_stream_token(video_id)
    LOGGER.debug(f"[TOKEN] Issued token for video_id={video_id}")
    return web.json_response(
        {"token": token, "expires_in": TOKEN_MAX_AGE_SECS},
        headers={
            "Access-Control-Allow-Origin": Config.STREAM_URL.rstrip('/'),
            "Cache-Control": "no-store, no-cache, must-revalidate"
        }
    )


@routes.get("/api/ping")
async def ping_handler(request: web.Request):
    """Receives the 30-second heartbeat from player.html for live viewer tracking."""
    viewer_id = request.rel_url.query.get('vid')
    if viewer_id:
        await heartbeat_collection.update_one(
            {"_id": viewer_id},
            {"$set": {
                "bot_url":   Config.STREAM_URL,
                "last_seen": datetime.utcnow()
            }},
            upsert=True
        )
    return web.Response(status=204)


async def _cleanup_heartbeats_task():
    """Background task: removes stale viewers (inactive > 90 s) from DB."""
    while True:
        await asyncio.sleep(60)
        stale_time = datetime.utcnow() - timedelta(seconds=90)
        try:
            await heartbeat_collection.delete_many({"last_seen": {"$lt": stale_time}})
        except Exception as e:
            LOGGER.error(f"[HEARTBEAT] Cleanup failed: {e}")


# ── MAIN STREAM HANDLER (V4.5 — Active Loop Enforcement + Advanced Security) ──

@routes.get(r"/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    """
    Main streaming route — V4.5: Telegram → Server → User pipe with 6-gate security.

    GATE ORDER:
      1. Dead Mode check             → 503                    (unchanged)
      2a. User-Agent fingerprinting  → 403  [V4.5]
      2b. Header integrity check     → 403  [V4.5]
      3. Referer protection          → 403                    (unchanged)
      4. Token verification          → 403                    (unchanged)
      5. Per-IP concurrent limit     → 429  [V4.5]
      6. Connection Lifespan (loop)  → force break            [V4.5 THE MAIN FIX]
    """
    global stream_errors

    client_index     = None
    total_bytes_sent = 0
    ip_slot_acquired = False

    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(',')[0].strip()
    else:
        client_ip = request.remote or "0.0.0.0"

    try:
        # ── GATE 1: Dead Mode ─────────────────────────────────────────────────
        if IS_DEAD:
            return web.Response(
                status=503,
                text=(
                    "⚠️ Server bandwidth limit reached.\n"
                    "This bot is no longer serving video files.\n"
                    "Please contact the admin for the updated streaming link."
                ),
                content_type='text/plain'
            )

        # ── GATE 2a: User-Agent Fingerprinting ───────────────────────────────
        user_agent = request.headers.get('User-Agent', '')
        if _is_download_manager_ua(user_agent):
            LOGGER.warning(
                f"[STREAM V4.5] Blocked UA fingerprint. "
                f"UA='{user_agent[:100]}' IP={client_ip}"
            )
            return web.Response(status=403, text="403 Forbidden: Access denied.")

        # ── GATE 2b: HTTP Header Integrity Check ──────────────────────────────
        if not _passes_header_integrity_check(request):
            LOGGER.warning(
                f"[STREAM V4.5] Blocked: missing browser-specific headers. "
                f"IP={client_ip} UA='{user_agent[:80]}'"
            )
            return web.Response(status=403, text="403 Forbidden: Access denied.")

        # ── GATE 3: Referer Protection ────────────────────────────────────────
        referer         = request.headers.get('Referer', '')
        allowed_referer = Config.STREAM_URL.rstrip('/')
        if not referer or not referer.startswith(allowed_referer):
            LOGGER.warning(
                f"[STREAM] Blocked hotlink. Referer='{referer}' "
                f"Expected='{allowed_referer}' IP={client_ip}"
            )
            return web.Response(
                status=403,
                text="403 Forbidden: Direct access is not allowed. Please use the official player."
            )

        # ── GATE 4: Token Verification ────────────────────────────────────────
        token = request.rel_url.query.get('token', '')
        if not token:
            LOGGER.warning(
                f"[STREAM] Missing token for msg_id="
                f"{request.match_info.get('message_id', '?')} IP={client_ip}"
            )
            return web.Response(status=403, text="403 Forbidden: Missing stream token.")
        if not verify_stream_token(token):
            LOGGER.warning(
                f"[STREAM] Invalid/expired token for msg_id="
                f"{request.match_info.get('message_id', '?')} IP={client_ip}"
            )
            return web.Response(status=403, text="403 Forbidden: Invalid or expired stream token.")

        # ── GATE 5: Per-IP Concurrent Connection Limit ────────────────────────
        _ip_connection_counts[client_ip] = _ip_connection_counts.get(client_ip, 0) + 1
        if _ip_connection_counts[client_ip] > Config.MAX_CONNECTIONS_PER_IP:
            _ip_connection_counts[client_ip] -= 1
            LOGGER.warning(
                f"[STREAM V4.5] IP {client_ip} exceeded concurrent connection limit "
                f"({_ip_connection_counts.get(client_ip, 0)}/{Config.MAX_CONNECTIONS_PER_IP})."
            )
            return web.Response(
                status=429,
                text=(
                    "429 Too Many Requests: You have too many concurrent streams active. "
                    "Please close other streams and try again."
                )
            )
        ip_slot_acquired = True

        # ── Parse Request ─────────────────────────────────────────────────────
        message_id   = int(request.match_info['message_id'])
        range_header = request.headers.get("Range", 0)

        # ── Client Load Balancing (round-robin on least-loaded) ───────────────
        min_load   = min(work_loads.values())
        candidates = [cid for cid, load in work_loads.items() if load == min_load]
        global next_client_idx
        if len(candidates) > 1:
            client_index    = candidates[next_client_idx % len(candidates)]
            next_client_idx += 1
        else:
            client_index = candidates[0]

        faster_client = multi_clients[client_index]
        work_loads[client_index] += 1

        if faster_client not in class_cache:
            class_cache[faster_client] = ByteStreamer(faster_client)
        tg_connect = class_cache[faster_client]

        # ── Get File Properties ───────────────────────────────────────────────
        file_id   = await tg_connect.get_file_properties(message_id)
        file_size = file_id.file_size

        # ── Parse Range Header ────────────────────────────────────────────────
        from_bytes = 0
        if range_header:
            from_bytes_str, _ = range_header.replace("bytes=", "").split("-")
            from_bytes = int(from_bytes_str)

        if from_bytes >= file_size:
            return web.Response(status=416, reason="Range Not Satisfiable")

        # chunk_size = 1 MB — DO NOT CHANGE (Pyrogram offset math depends on this)
        chunk_size     = 1024 * 1024
        offset         = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset

        # ── Build Response Headers ────────────────────────────────────────────
        cors_headers = {'Access-Control-Allow-Origin': allowed_referer}

        resp = web.StreamResponse(
            status=206 if range_header else 200,
            headers={
                "Content-Type":   file_id.mime_type,
                "Content-Range":  f"bytes {from_bytes}-{file_size - 1}/{file_size}",
                "Content-Length": str(file_size - from_bytes),
                "Accept-Ranges":  "bytes",
                **cors_headers
            }
        )
        await resp.prepare(request)

        # ── Lifetime Stream Counter ───────────────────────────────────────────
        if from_bytes == 0:
            asyncio.create_task(increment_lifetime_streams())

        # ── Connection Lifespan & Burst Timer ────────────────────────────────
        connection_start_time = time.time()
        is_first_chunk        = True

        # ── Direct Pipe: Telegram → User ─────────────────────────────────────
        async for chunk in tg_connect.yield_file(file_id, offset, chunk_size, message_id):
            try:
                # ── GATE 6: Connection Lifespan Enforcement ───────────────────
                elapsed = time.time() - connection_start_time
                if elapsed >= Config.MAX_CONNECTION_LIFESPAN_SECS:
                    LOGGER.info(
                        f"[STREAM V4.5] Force-drop: lifespan exceeded "
                        f"({elapsed:.1f}s >= {Config.MAX_CONNECTION_LIFESPAN_SECS}s) "
                        f"for msg_id={message_id}, sent={humanbytes(total_bytes_sent)}, "
                        f"IP={client_ip}"
                    )
                    break

                # ── Write chunk ───────────────────────────────────────────────
                if is_first_chunk and first_part_cut > 0:
                    data             = chunk[first_part_cut:]
                    await resp.write(data)
                    total_bytes_sent += len(data)
                    is_first_chunk    = False
                else:
                    await resp.write(chunk)
                    total_bytes_sent += len(chunk)

                # ── Bandwidth tracking ────────────────────────────────────────
                await add_bandwidth(
                    len(chunk) if not (is_first_chunk and first_part_cut > 0)
                    else len(chunk) - first_part_cut
                )

                # ── Advanced Dynamic Throttle (Phase 1 Burst / 2 Throttle / 3 Escalation) ──
                if elapsed > Config.BURST_DURATION_SECS:
                    escalation_threshold = Config.DATA_ESCALATION_MB * 1024 * 1024
                    if total_bytes_sent >= escalation_threshold:
                        base_sleep = Config.DATA_ESCALATION_SLEEP
                    else:
                        base_sleep = Config.THROTTLE_SLEEP_SECS

                    jitter     = random.uniform(
                        -Config.THROTTLE_JITTER_SECS,
                         Config.THROTTLE_JITTER_SECS
                    )
                    sleep_time = max(0.05, base_sleep + jitter)
                    await asyncio.sleep(sleep_time)

            except (ConnectionError, asyncio.CancelledError):
                LOGGER.debug(
                    f"[STREAM] Client disconnected for msg_id={message_id} "
                    f"after {humanbytes(total_bytes_sent)} sent. IP={client_ip}"
                )
                return resp

        return resp

    except (FileReferenceExpired, AuthBytesInvalid) as e:
        LOGGER.error(f"[STREAM] FATAL error for msg_id={message_id}: {type(e).__name__}")
        stream_errors += 1
        return web.Response(status=410, text="Stream link expired. Please refresh the page.")

    except Exception as e:
        LOGGER.critical(
            f"[STREAM] Unhandled error for msg_id={message_id}: {e}", exc_info=True
        )
        stream_errors += 1
        return web.Response(status=500)

    finally:
        if ip_slot_acquired:
            _ip_connection_counts[client_ip] = max(
                0, _ip_connection_counts.get(client_ip, 0) - 1
            )
            if _ip_connection_counts.get(client_ip, 0) == 0:
                _ip_connection_counts.pop(client_ip, None)

        if client_index is not None:
            work_loads[client_index] -= 1
            LOGGER.debug(f"[STREAM] Workload decremented for client {client_index}.")


async def web_server():
    """
    Creates and configures the aiohttp web application.
    V4.5: starts IP-counter cleanup, Heartbeat, and BW-limit-checker background tasks.
    """
    web_app = web.Application(client_max_size=30_000_000)

    base_dir      = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.environ.get("TEMPLATES_DIR", os.path.join(base_dir, "templates"))

    aiohttp_jinja2.setup(web_app, loader=jinja2.FileSystemLoader(templates_dir))
    LOGGER.info(f"[WEB] Jinja2 templates directory: {templates_dir}")

    web_app.add_routes(routes)

    async def on_startup(app):
        asyncio.create_task(_cleanup_ip_counters_task())
        asyncio.create_task(_cleanup_heartbeats_task())
        asyncio.create_task(_bw_limit_checker_task())   # [NEW] Dynamic BW limit checker
        LOGGER.info("[STARTUP] Background tasks started: IP cleanup, Heartbeats, BW Limit Checker.")

    web_app.on_startup.append(on_startup)
    return web_app


# ── SECTION 14: BOT & CLIENT INITIALIZATION ───────────────────────────────────

main_bot = Client(
    "KeralaCaptainBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN
)


class TokenParser:
    def parse_from_env(self):
        return {
            c + 2: t
            for c, (_, t) in enumerate(
                filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items()))
            )
        }


async def initialize_clients():
    """Initialises the main bot + any MULTI_TOKEN_ auxiliary clients."""
    multi_clients[0] = main_bot
    work_loads[0]    = 0

    all_tokens = TokenParser().parse_from_env()
    if not all_tokens:
        LOGGER.info("No MULTI_TOKEN clients found. Running in single-client mode.")
        return

    async def start_client(client_id, token):
        try:
            client = await Client(
                name=str(client_id),
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                bot_token=token,
                no_updates=True,
                in_memory=True
            ).start()
            work_loads[client_id] = 0
            return client_id, client
        except Exception as e:
            LOGGER.error(f"Failed to start Client {client_id}: {e}")
            return None

    results = await asyncio.gather(
        *[start_client(i, token) for i, token in all_tokens.items()]
    )
    multi_clients.update({cid: client for cid, client in results if client is not None})

    if len(multi_clients) > 1:
        LOGGER.info(f"Multi-Client mode ON: {len(multi_clients)} clients initialised.")


async def forward_file_safely(message_to_forward: Message):
    """Resends a Telegram file via the main bot to refresh its file_reference."""
    try:
        media = message_to_forward.document or message_to_forward.video
        if not media:
            LOGGER.error("forward_file_safely: message has no media.")
            return None
        LOGGER.info(f"Sending cached media for msg {message_to_forward.id} via main bot...")
        return await main_bot.send_cached_media(
            chat_id=Config.LOG_CHANNEL_ID,
            file_id=media.file_id,
            caption=getattr(message_to_forward, 'caption', '')
        )
    except Exception as e:
        LOGGER.error(f"forward_file_safely failed: {e}")
        return None


# ── SECTION 15: ADMIN BOT HANDLERS ───────────────────────────────────────────

admin_only = filters.user(Config.ADMIN_IDS)


def _get_main_menu_markup() -> InlineKeyboardMarkup:
    """Returns the main admin panel inline keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics",        callback_data="admin_stats")],
        [InlineKeyboardButton("📈 Lifetime Stats",    callback_data="admin_lifetime_stats")],
        [InlineKeyboardButton("🚦 Bandwidth Limits",  callback_data="admin_bw_limits")],
        [InlineKeyboardButton("⚙️ Settings",          callback_data="admin_settings")],
        [InlineKeyboardButton("🔄 Restart Bot",       callback_data="admin_restart")],
        [InlineKeyboardButton("🛑 Kill Bot (Sleep)",  callback_data="admin_kill_bot")],
    ])


@main_bot.on_message(filters.command("start") & filters.private & admin_only)
async def start_command(client, message: Message):
    await message.reply_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel.",
        reply_markup=_get_main_menu_markup()
    )
    await update_user_conversation(message.chat.id, None)


# ── Statistics ────────────────────────────────────────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_stats$") & admin_only)
async def stats_callback(client, cb: CallbackQuery):
    """Shows live bot statistics: bandwidth, system info, streaming workloads."""
    await cb.answer("Fetching stats...")

    uptime = get_readable_time(int(time.time() - start_time))

    try:
        cpu_usage  = psutil.cpu_percent()
        ram_usage  = psutil.virtual_memory().percent
        ram_total  = humanbytes(psutil.virtual_memory().total)
        disk_usage = psutil.disk_usage('/').percent
    except Exception:
        cpu_usage = ram_usage = disk_usage = "N/A"
        ram_total = "N/A"

    bw_info      = get_bandwidth_info()
    dead_status  = "🔴 DEAD (Sleep Mode)" if bw_info["is_dead"] else "🟢 Active"
    workload_str = "\n".join(
        [f"  - Client {cid}: {load} active streams" for cid, load in work_loads.items()]
    )

    active_ip_slots = sum(_ip_connection_counts.values())
    unique_ips      = len(_ip_connection_counts)

    stale_threshold  = datetime.utcnow() - timedelta(seconds=90)
    total_live_all   = await heartbeat_collection.count_documents({"last_seen": {"$gte": stale_threshold}})
    total_live_this  = await heartbeat_collection.count_documents({
        "bot_url": Config.STREAM_URL, "last_seen": {"$gte": stale_threshold}
    })

    limit_line = (
        f"  - Limit: `Unlimited` ♾️\n"
        if not bw_info["limit_enabled"]
        else f"  - Limit: `{bw_info['used_human']}` / `{bw_info['kill_threshold_human']}` ({bw_info['percent']}%)\n"
    )

    text = (
        f"**📊 Bot Statistics (V4.5)**\n\n"
        f"**Bot:** @{BOT_USERNAME or 'Unknown'}  |  **Status:** {dead_status}\n"
        f"**Uptime:** `{uptime}`\n\n"
        f"**🔥 LIVE VIEWERS (Real-time):**\n"
        f"  🌍 `All Bots Live Count :` **{total_live_all}** watching\n"
        f"  📍 `This Bot Live Count :` **{total_live_this}** watching\n\n"
        f"**🌐 Bandwidth (This Bot):**\n"
        f"{limit_line}"
        f"  - Warning Sent: `{bw_info['warning_sent']}`\n\n"
        f"**🖥️ System:**\n"
        f"  - CPU: `{cpu_usage}%`\n"
        f"  - RAM: `{ram_usage}%` (Total: `{ram_total}`)\n"
        f"  - Disk: `{disk_usage}%`\n\n"
        f"**📡 Streaming (Backend Data):**\n"
        f"  - Active Clients: `{len(multi_clients)}`\n"
        f"  - Active IP Slots: `{active_ip_slots}` across `{unique_ips}` IPs\n"
        f"  - Workloads:\n{workload_str}"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]
        )
    )


@main_bot.on_callback_query(filters.regex("^admin_lifetime_stats$") & admin_only)
async def lifetime_stats_callback(client, cb: CallbackQuery):
    """Shows the PERMANENT global lifetime stats shared across ALL bots."""
    await cb.answer("Fetching lifetime stats...")
    stats = await get_lifetime_stats()
    text = (
        f"**📈 Lifetime Global Statistics**\n\n"
        f"Combined total across **ALL bots** ever deployed on this system.\n"
        f"Stored in MongoDB — **permanent, never deleted.**\n\n"
        f"**💾 Total Bandwidth Served:**\n"
        f"  `{stats['total_bandwidth_human']}`\n\n"
        f"**▶️ Total Video Streams Started:**\n"
        f"  `{stats['total_streams']:,}` streams"
    )
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]
        )
    )


# ── Bandwidth Limits (NEW) ────────────────────────────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_bw_limits$") & admin_only)
async def bw_limits_menu_callback(client, cb: CallbackQuery):
    """Main Bandwidth Limits menu — choose Global or Specific bot settings."""
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)

    global_cfg   = await get_global_bw_config()
    specific_cfg = await get_specific_bw_config(Config.STREAM_URL)

    if specific_cfg is not None:
        active_rule = (
            f"📍 **This Bot:** {'🟢 ON' if specific_cfg['enabled'] else '🔴 OFF (Unlimited)'} "
            f"— `{humanbytes(specific_cfg['limit_bytes']) if specific_cfg['enabled'] else 'Unlimited'}`"
        )
    elif global_cfg is not None:
        active_rule = (
            f"🌍 **Global (fallback):** {'🟢 ON' if global_cfg['enabled'] else '🔴 OFF (Unlimited)'} "
            f"— `{humanbytes(global_cfg['limit_bytes']) if global_cfg['enabled'] else 'Unlimited'}`"
        )
    else:
        active_rule = (
            f"⚙️ **Config Default:** "
            f"{'🟢 ON — ' + humanbytes(Config.BANDWIDTH_KILL_BYTES) if Config.DEFAULT_LIMIT_MODE else '🔴 Unlimited'}"
        )

    current_effective = (
        f"♾️ Unlimited" if not _effective_limit_enabled
        else humanbytes(_effective_limit_bytes)
    )

    text = (
        f"**🚦 Bandwidth Limit Controls**\n\n"
        f"**Current Effective Limit:** `{current_effective}`\n"
        f"**Active Rule:** {active_rule}\n\n"
        f"**Override Priority:**\n"
        f"  Specific Bot > Global > Config Default\n\n"
        f"Choose what to configure:"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🌍 Global Settings (All Bots)", callback_data="admin_bw_global")],
            [InlineKeyboardButton("📍 Specific Bot Settings",       callback_data="admin_bw_specific")],
            [InlineKeyboardButton("⬅️ Back to Menu",               callback_data="admin_main_menu")],
        ])
    )


# ── Global BW Settings ────────────────────────────────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_bw_global$") & admin_only)
async def bw_global_callback(client, cb: CallbackQuery):
    """Shows the current global BW limit and toggle/set options."""
    await cb.answer()

    cfg = await get_global_bw_config()
    if cfg is None:
        status_str = (
            f"Not set — using Config default: "
            f"{'🟢 ON (' + humanbytes(Config.BANDWIDTH_KILL_BYTES) + ')' if Config.DEFAULT_LIMIT_MODE else '🔴 Unlimited'}"
        )
        enabled    = Config.DEFAULT_LIMIT_MODE
        limit_bytes = Config.BANDWIDTH_KILL_BYTES
    else:
        enabled     = cfg["enabled"]
        limit_bytes = cfg.get("limit_bytes", Config.BANDWIDTH_KILL_BYTES)
        status_str  = (
            f"{'🟢 ON' if enabled else '🔴 OFF (Unlimited)'} — "
            f"`{humanbytes(limit_bytes) if enabled else 'Unlimited'}`"
        )

    toggle_label = "🔴 Turn OFF (Unlimited)" if enabled else "🟢 Turn ON"

    text = (
        f"**🌍 Global Bandwidth Settings**\n\n"
        f"This rule applies to **all bots** that share this MongoDB database, "
        f"unless overridden by a Specific Bot rule.\n\n"
        f"**Current Status:** {status_str}\n\n"
        f"What would you like to do?"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(toggle_label,             callback_data="admin_bw_global_tog")],
            [InlineKeyboardButton("✏️ Set Custom Limit (GB)", callback_data="admin_bw_global_setlimit")],
            [InlineKeyboardButton("⬅️ Back",                 callback_data="admin_bw_limits")],
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_global_tog$") & admin_only)
async def bw_global_toggle_confirm_callback(client, cb: CallbackQuery):
    """Shows confirmation before toggling the global BW limit."""
    await cb.answer()

    cfg     = await get_global_bw_config()
    enabled = cfg["enabled"] if cfg else Config.DEFAULT_LIMIT_MODE

    new_state_str = "🔴 OFF (Unlimited — all bots run forever)" if enabled else "🟢 ON"
    warning_str   = (
        "\n\n⚠️ **Warning:** Turning the limit OFF means ALL bots with no specific rule "
        "will run with **no bandwidth cap at all.** Make sure this is intended."
        if enabled else ""
    )

    await cb.message.edit_text(
        f"**⚠️ Confirm Global Limit Change**\n\n"
        f"You are about to toggle the Global Limit to: **{new_state_str}**\n"
        f"This will affect **all bots** sharing this database.{warning_str}\n\n"
        f"Are you sure?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Apply",   callback_data="admin_bw_global_tog_yes"),
                InlineKeyboardButton("❌ No, Cancel",   callback_data="admin_bw_global"),
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_global_tog_yes$") & admin_only)
async def bw_global_toggle_apply_callback(client, cb: CallbackQuery):
    """Applies the global BW limit toggle."""
    await cb.answer("Applying...")

    cfg         = await get_global_bw_config()
    enabled     = cfg["enabled"] if cfg else Config.DEFAULT_LIMIT_MODE
    limit_bytes = cfg.get("limit_bytes", Config.BANDWIDTH_KILL_BYTES) if cfg else Config.BANDWIDTH_KILL_BYTES

    new_enabled = not enabled
    await set_global_bw_config(new_enabled, limit_bytes)
    await refresh_effective_limit()

    new_str = "🔴 OFF (Unlimited)" if not new_enabled else f"🟢 ON ({humanbytes(limit_bytes)})"
    await cb.message.edit_text(
        f"✅ **Global Bandwidth Limit updated!**\n\n"
        f"**New Status:** {new_str}\n\n"
        f"All bots without a specific rule will pick this up within **60 seconds** automatically.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Global Settings", callback_data="admin_bw_global")],
            [InlineKeyboardButton("🏠 Main Menu",               callback_data="admin_main_menu")],
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_global_setlimit$") & admin_only)
async def bw_global_setlimit_callback(client, cb: CallbackQuery):
    """Prompts admin to type a new global limit in GB."""
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "bw_awaiting_global_limit"})
    await cb.message.edit_text(
        "**✏️ Set Global Bandwidth Limit**\n\n"
        "Send the new limit as a **number in GB**.\n\n"
        "Examples: `90`, `150`, `200`\n\n"
        "This limit will apply to **all bots** that have no specific rule set.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_bw_global")]]
        )
    )


# ── Specific Bot BW Settings ──────────────────────────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_bw_specific$") & admin_only)
async def bw_specific_list_callback(client, cb: CallbackQuery):
    """Lists all bots from bandwidth_collection for specific rule management."""
    await cb.answer("Loading bot list...")

    cursor   = bandwidth_collection.find({}, {"_id": 1, "bot_username": 1, "bandwidth_used": 1, "is_dead": 1})
    bot_docs = await cursor.to_list(length=20)

    if not bot_docs:
        await cb.message.edit_text(
            "**📍 Specific Bot Settings**\n\nNo bots found in the database yet.\n"
            "Bots appear here once they have streamed at least once.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_bw_limits")]]
            )
        )
        return

    # Save ordered list in conv state for index-based selection
    bot_urls = [doc["_id"] for doc in bot_docs]
    await update_user_conversation(
        cb.message.chat.id,
        {"stage": "bw_selecting_bot", "bw_bot_list": bot_urls}
    )

    buttons = []
    for idx, doc in enumerate(bot_docs):
        url       = doc["_id"]
        username  = doc.get("bot_username") or "unknown"
        used      = humanbytes(doc.get("bandwidth_used", 0))
        dead_icon = "🔴" if doc.get("is_dead") else "🟢"
        is_this   = " ← This Bot" if url == Config.STREAM_URL else ""
        label     = f"{dead_icon} @{username} | {used}{is_this}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"admin_bw_sel_{idx}")])

    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_bw_limits")])

    await cb.message.edit_text(
        f"**📍 Specific Bot Settings**\n\n"
        f"Select a bot to configure its individual bandwidth rule.\n"
        f"Specific rules **always override** the global rule.\n\n"
        f"({len(bot_docs)} bots found)",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@main_bot.on_callback_query(filters.regex(r"^admin_bw_sel_(\d+)$") & admin_only)
async def bw_specific_bot_panel_callback(client, cb: CallbackQuery):
    """Shows the specific BW rule panel for the selected bot."""
    await cb.answer()

    idx  = int(cb.data.split("_")[-1])
    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_bot_list" not in conv:
        await cb.message.edit_text(
            "⚠️ Session expired. Please go back and select a bot again.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_bw_specific")]]
            )
        )
        return

    bot_list = conv["bw_bot_list"]
    if idx >= len(bot_list):
        await cb.answer("Invalid selection. Please go back.", show_alert=True)
        return

    target_url  = bot_list[idx]
    specific_cfg = await get_specific_bw_config(target_url)
    bw_doc       = await bandwidth_collection.find_one({"_id": target_url})

    username  = bw_doc.get("bot_username", "unknown") if bw_doc else "unknown"
    used      = humanbytes(bw_doc.get("bandwidth_used", 0)) if bw_doc else "N/A"
    is_dead   = bw_doc.get("is_dead", False) if bw_doc else False

    if specific_cfg is not None:
        enabled    = specific_cfg["enabled"]
        limit_bytes = specific_cfg.get("limit_bytes", Config.BANDWIDTH_KILL_BYTES)
        rule_str   = (
            f"{'🟢 ON' if enabled else '🔴 OFF (Unlimited)'} — "
            f"`{humanbytes(limit_bytes) if enabled else 'Unlimited'}`"
        )
        rule_source = "📍 **Specific Rule Set**"
    else:
        global_cfg = await get_global_bw_config()
        rule_source = "🌍 **No specific rule — using Global/Default**"
        if global_cfg:
            enabled     = global_cfg["enabled"]
            limit_bytes = global_cfg.get("limit_bytes", Config.BANDWIDTH_KILL_BYTES)
        else:
            enabled     = Config.DEFAULT_LIMIT_MODE
            limit_bytes = Config.BANDWIDTH_KILL_BYTES
        rule_str = (
            f"{'🟢 ON' if enabled else '🔴 OFF'} — "
            f"`{humanbytes(limit_bytes) if enabled else 'Unlimited'}` (inherited)"
        )

    # Store selected URL in conv state for subsequent actions
    await update_user_conversation(
        cb.message.chat.id,
        {"stage": "bw_bot_panel", "bw_bot_list": bot_list, "bw_selected_url": target_url}
    )

    toggle_label = "🔴 Turn OFF for This Bot" if enabled else "🟢 Turn ON for This Bot"

    text = (
        f"**📍 Specific Rule: @{username}**\n\n"
        f"**URL:** `{target_url}`\n"
        f"**Status:** {'🔴 DEAD' if is_dead else '🟢 Active'}\n"
        f"**BW Used:** `{used}`\n\n"
        f"**Current Rule:** {rule_str}\n"
        f"**Source:** {rule_source}\n\n"
        f"What would you like to do for **this bot specifically**?"
    )

    buttons = [
        [InlineKeyboardButton(toggle_label,                 callback_data="admin_bw_seltog")],
        [InlineKeyboardButton("✏️ Set Custom Limit (GB)",   callback_data="admin_bw_selset")],
    ]
    if specific_cfg is not None:
        buttons.append([InlineKeyboardButton("🗑️ Remove Specific Rule (Use Global)", callback_data="admin_bw_selrem")])
    buttons.append([InlineKeyboardButton("⬅️ Back to Bot List", callback_data="admin_bw_specific")])

    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))


@main_bot.on_callback_query(filters.regex("^admin_bw_seltog$") & admin_only)
async def bw_specific_toggle_confirm_callback(client, cb: CallbackQuery):
    """Confirms toggling the specific bot BW limit."""
    await cb.answer()

    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_selected_url" not in conv:
        await cb.answer("Session expired. Go back and reselect.", show_alert=True)
        return

    target_url   = conv["bw_selected_url"]
    specific_cfg = await get_specific_bw_config(target_url)
    enabled      = specific_cfg["enabled"] if specific_cfg else Config.DEFAULT_LIMIT_MODE

    new_state_str = "🔴 OFF (Unlimited)" if enabled else "🟢 ON"
    await cb.message.edit_text(
        f"**⚠️ Confirm Specific Rule Toggle**\n\n"
        f"Bot URL: `{target_url}`\n\n"
        f"Toggle limit to: **{new_state_str}**\n\n"
        f"Are you sure? This only affects **this specific bot**.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Apply",  callback_data="admin_bw_seltog_yes"),
                InlineKeyboardButton("❌ Cancel",       callback_data="admin_bw_specific"),
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_seltog_yes$") & admin_only)
async def bw_specific_toggle_apply_callback(client, cb: CallbackQuery):
    """Applies the specific bot BW toggle."""
    await cb.answer("Applying...")

    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_selected_url" not in conv:
        await cb.answer("Session expired.", show_alert=True)
        return

    target_url   = conv["bw_selected_url"]
    specific_cfg = await get_specific_bw_config(target_url)
    enabled      = specific_cfg["enabled"] if specific_cfg else Config.DEFAULT_LIMIT_MODE
    limit_bytes  = specific_cfg.get("limit_bytes", Config.BANDWIDTH_KILL_BYTES) if specific_cfg else Config.BANDWIDTH_KILL_BYTES

    new_enabled = not enabled
    await set_specific_bw_config(target_url, new_enabled, limit_bytes)

    # If this bot is the running one, refresh immediately
    if target_url == Config.STREAM_URL:
        await refresh_effective_limit()

    new_str = "🔴 OFF (Unlimited)" if not new_enabled else f"🟢 ON ({humanbytes(limit_bytes)})"
    await cb.message.edit_text(
        f"✅ **Specific Rule Updated!**\n\n"
        f"**Bot:** `{target_url}`\n"
        f"**New Status:** {new_str}\n\n"
        f"The bot will pick up the change within **60 seconds** (or instantly if it's this bot).",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Bot List", callback_data="admin_bw_specific")],
            [InlineKeyboardButton("🏠 Main Menu",        callback_data="admin_main_menu")],
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_selset$") & admin_only)
async def bw_specific_setlimit_callback(client, cb: CallbackQuery):
    """Prompts admin to type a custom limit in GB for the selected bot."""
    await cb.answer()

    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_selected_url" not in conv:
        await cb.answer("Session expired. Go back.", show_alert=True)
        return

    target_url = conv["bw_selected_url"]
    # Update stage but preserve selected URL and bot list
    await update_user_conversation(cb.message.chat.id, {
        "stage":           "bw_awaiting_specific_limit",
        "bw_selected_url": target_url,
        "bw_bot_list":     conv.get("bw_bot_list", [])
    })

    await cb.message.edit_text(
        f"**✏️ Set Specific Limit for This Bot**\n\n"
        f"Bot: `{target_url}`\n\n"
        f"Send the new limit as a **number in GB**.\n\n"
        f"Examples: `90`, `150`, `180`, `200`",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_bw_specific")]]
        )
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_selrem$") & admin_only)
async def bw_specific_remove_confirm_callback(client, cb: CallbackQuery):
    """Confirms removing the specific rule for the selected bot."""
    await cb.answer()

    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_selected_url" not in conv:
        await cb.answer("Session expired. Go back.", show_alert=True)
        return

    target_url = conv["bw_selected_url"]
    await cb.message.edit_text(
        f"**⚠️ Confirm Remove Specific Rule**\n\n"
        f"Bot: `{target_url}`\n\n"
        f"Removing the specific rule will make this bot fall back to the **Global rule** "
        f"(or Config default if no global is set).\n\n"
        f"Are you sure?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Remove",  callback_data="admin_bw_selrem_yes"),
                InlineKeyboardButton("❌ Cancel",        callback_data="admin_bw_specific"),
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_selrem_yes$") & admin_only)
async def bw_specific_remove_apply_callback(client, cb: CallbackQuery):
    """Removes the specific BW rule, reverting the bot to global settings."""
    await cb.answer("Removing...")

    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_selected_url" not in conv:
        await cb.answer("Session expired.", show_alert=True)
        return

    target_url = conv["bw_selected_url"]
    await delete_specific_bw_config(target_url)

    if target_url == Config.STREAM_URL:
        await refresh_effective_limit()

    await cb.message.edit_text(
        f"✅ **Specific Rule Removed!**\n\n"
        f"Bot `{target_url}` now follows the **Global rule** (or Config default).\n\n"
        f"Change takes effect within 60 seconds.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Bot List", callback_data="admin_bw_specific")],
            [InlineKeyboardButton("🏠 Main Menu",        callback_data="admin_main_menu")],
        ])
    )


# ── Settings ──────────────────────────────────────────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_settings$") & admin_only)
async def settings_callback(client, cb: CallbackQuery):
    """Shows current settings and allows changing the protected domain."""
    await cb.answer()
    current_domain = await get_protected_domain()
    bw_info        = get_bandwidth_info()

    text = (
        f"**⚙️ Settings**\n\n"
        f"**Protected Domain:**\n"
        f"Streams are only allowed when the Referer header matches this domain.\n\n"
        f"Current: `{current_domain}`\n\n"
        f"**V4.5 Security Parameters:**\n"
        f"  - Max Connection Lifespan: `{Config.MAX_CONNECTION_LIFESPAN_SECS}s`\n"
        f"  - Max Connections/IP: `{Config.MAX_CONNECTIONS_PER_IP}`\n"
        f"  - Burst Duration: `{Config.BURST_DURATION_SECS}s`\n"
        f"  - Throttle Sleep: `{Config.THROTTLE_SLEEP_SECS}s ± {Config.THROTTLE_JITTER_SECS}s`\n"
        f"  - Escalation at: `{Config.DATA_ESCALATION_MB} MB` → `{Config.DATA_ESCALATION_SLEEP}s`\n\n"
        f"**Bot:** @{BOT_USERNAME or 'Unknown'}\n"
        f"**Bandwidth Used:** `{bw_info['used_human']}`"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Set New Domain", callback_data="admin_set_domain")],
            [InlineKeyboardButton("⬅️ Back",           callback_data="admin_main_menu")]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_set_domain$") & admin_only)
async def set_domain_callback(client, cb: CallbackQuery):
    """Prompts the admin to type the new protected domain."""
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_domain"})
    await cb.message.edit_text(
        "**✏️ Set New Domain**\n\n"
        "Send the new protected domain as a plain text message.\n\n"
        "Example: `https://keralacaptain.in` or just `keralacaptain.in`",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_conv")]]
        )
    )


# ── Kill Bot ──────────────────────────────────────────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_kill_bot$") & admin_only)
async def kill_bot_callback(client, cb: CallbackQuery):
    """Shows the Kill Bot confirmation dialog."""
    await cb.answer()

    if IS_DEAD:
        await cb.message.edit_text(
            "🔴 **This bot is already in Sleep (Dead) Mode.**\n\n"
            "It is not serving any video streams.\n"
            "Use **🚦 Bandwidth Limits** in the admin panel to revive it instantly.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]
            )
        )
        return

    bw_info = get_bandwidth_info()
    await cb.message.edit_text(
        f"**⚠️ Are you sure you want to kill this bot?**\n\n"
        f"**Bot:** @{BOT_USERNAME or 'Unknown'}\n"
        f"**Bandwidth Used:** `{bw_info['used_human']}`\n\n"
        f"The bot will **permanently stop serving all video files.**\n"
        f"This state is saved to MongoDB and **survives restarts.**\n\n"
        f"You can revive it via **Admin Panel → 🚦 Bandwidth Limits** without restarting.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Kill It",  callback_data="admin_kill_bot_confirm"),
                InlineKeyboardButton("❌ No, Cancel",    callback_data="admin_main_menu")
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_kill_bot_confirm$") & admin_only)
async def kill_bot_confirm_callback(client, cb: CallbackQuery):
    """Admin confirmed kill — triggers Dead Mode manually."""
    await cb.answer("Killing bot...")
    await cb.message.edit_text(
        "🔴 **Bot is now in Sleep (Dead) Mode.**\n\n"
        "All video streams have been blocked immediately.\n"
        "This state is saved to the database and will persist on restart.\n\n"
        "**To revive:** Admin Panel → 🚦 Bandwidth Limits → increase limit or turn OFF."
    )
    await trigger_dead_mode(reason="manual")


# ── Restart ───────────────────────────────────────────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_restart$") & admin_only)
async def restart_callback(client, cb: CallbackQuery):
    """Shows restart confirmation."""
    await cb.answer()
    await cb.message.edit_text(
        "**⚠️ Are you sure you want to restart the bot?**\n\n"
        "The bandwidth counter will be safely flushed to MongoDB before restart.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Restart", callback_data="admin_restart_confirm"),
                InlineKeyboardButton("❌ No, Go Back",  callback_data="admin_main_menu")
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_restart_confirm$") & admin_only)
async def restart_confirm_callback(client, cb: CallbackQuery):
    """Admin confirmed restart — flushes bandwidth to DB then restarts process."""
    await cb.answer("Restarting...")
    await cb.message.edit_text("✅ **Restarting...**\n\nBot will be back online shortly.")
    try:
        LOGGER.info("RESTART triggered by admin.")
        await flush_bandwidth_to_db()
        LOGGER.info("Bandwidth flushed to DB before restart.")
        if main_bot and main_bot.is_connected:
            await main_bot.stop()
    except Exception as e:
        LOGGER.error(f"Error during pre-restart cleanup: {e}")
    os.execl(sys.executable, sys.executable, *sys.argv)


# ── Navigation / Cancel ───────────────────────────────────────────────────────

@main_bot.on_callback_query(
    filters.regex("^(admin_main_menu|admin_cancel_conv)$") & admin_only
)
async def main_menu_callback(client, cb: CallbackQuery):
    """Returns to the main admin menu and clears any active conversation state."""
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)
    await cb.message.edit_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel.",
        reply_markup=_get_main_menu_markup()
    )


# ── Text Message Handler (all free-text admin input flows) ────────────────────

@main_bot.on_message(filters.private & filters.text & admin_only)
async def text_message_handler(client, message: Message):
    """Handles free-text input from admin during active conversation flows."""
    chat_id = message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return

    stage = conv.get("stage")

    # ── Existing: Protected Domain ────────────────────────────────────────────
    if stage == "awaiting_domain":
        new_domain = message.text.strip()
        if "." not in new_domain or " " in new_domain:
            return await message.reply_text(
                "❌ Invalid format. Please send a valid domain like `keralacaptain.in`."
            )
        try:
            status_msg   = await message.reply_text("⏳ Saving...")
            saved_domain = await set_protected_domain(new_domain)
            await status_msg.edit_text(
                f"✅ **Success!**\n\nProtected domain updated to:\n`{saved_domain}`",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_settings")]]
                )
            )
            await update_user_conversation(chat_id, None)
        except Exception as e:
            await message.reply_text(f"❌ **Error!**\nCould not save domain: `{e}`")

    # ── New: Global BW Limit Value ────────────────────────────────────────────
    elif stage == "bw_awaiting_global_limit":
        raw_val = message.text.strip().replace("gb", "").replace("GB", "").strip()
        try:
            gb_val = float(raw_val)
            if gb_val <= 0:
                raise ValueError("Must be positive")
        except ValueError:
            return await message.reply_text(
                "❌ Invalid value. Please send a positive number in GB, e.g. `150`."
            )

        limit_bytes = int(gb_val * 1024 * 1024 * 1024)
        status_msg  = await message.reply_text("⏳ Saving...")

        # Ask for confirmation
        await update_user_conversation(chat_id, {
            "stage":                 "bw_confirm_global_limit",
            "bw_pending_limit_bytes": limit_bytes
        })
        await status_msg.edit_text(
            f"**⚠️ Confirm Global Limit Change**\n\n"
            f"New Global Limit: **{humanbytes(limit_bytes)}** (`{gb_val:.1f} GB`)\n\n"
            f"This will apply to **all bots** sharing this database.\n"
            f"Are you sure?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, Save",  callback_data="admin_bw_global_limit_confirm"),
                    InlineKeyboardButton("❌ Cancel",      callback_data="admin_bw_global"),
                ]
            ])
        )

    # ── New: Specific Bot BW Limit Value ──────────────────────────────────────
    elif stage == "bw_awaiting_specific_limit":
        raw_val = message.text.strip().replace("gb", "").replace("GB", "").strip()
        try:
            gb_val = float(raw_val)
            if gb_val <= 0:
                raise ValueError("Must be positive")
        except ValueError:
            return await message.reply_text(
                "❌ Invalid value. Please send a positive number in GB, e.g. `150`."
            )

        target_url  = conv.get("bw_selected_url", "")
        limit_bytes = int(gb_val * 1024 * 1024 * 1024)
        status_msg  = await message.reply_text("⏳ Saving...")

        await update_user_conversation(chat_id, {
            "stage":                  "bw_confirm_specific_limit",
            "bw_selected_url":        target_url,
            "bw_bot_list":            conv.get("bw_bot_list", []),
            "bw_pending_limit_bytes": limit_bytes
        })
        await status_msg.edit_text(
            f"**⚠️ Confirm Specific Limit Change**\n\n"
            f"Bot: `{target_url}`\n"
            f"New Limit: **{humanbytes(limit_bytes)}** (`{gb_val:.1f} GB`)\n\n"
            f"Are you sure?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, Save",  callback_data="admin_bw_specific_limit_confirm"),
                    InlineKeyboardButton("❌ Cancel",      callback_data="admin_bw_specific"),
                ]
            ])
        )


# ── Confirmation callbacks for custom limit values ────────────────────────────

@main_bot.on_callback_query(filters.regex("^admin_bw_global_limit_confirm$") & admin_only)
async def bw_global_limit_confirm_callback(client, cb: CallbackQuery):
    """Saves the confirmed global bandwidth limit."""
    await cb.answer("Saving...")

    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_pending_limit_bytes" not in conv:
        await cb.answer("Session expired.", show_alert=True)
        return

    limit_bytes = conv["bw_pending_limit_bytes"]
    # Keep existing enabled state, just update limit
    existing = await get_global_bw_config()
    enabled  = existing["enabled"] if existing else Config.DEFAULT_LIMIT_MODE

    await set_global_bw_config(enabled, limit_bytes)
    await refresh_effective_limit()
    await update_user_conversation(cb.message.chat.id, None)

    await cb.message.edit_text(
        f"✅ **Global Limit Saved!**\n\n"
        f"**New Limit:** `{humanbytes(limit_bytes)}`\n"
        f"**Status:** {'🟢 ON' if enabled else '🔴 OFF (Unlimited)'}\n\n"
        f"All bots without a specific rule will update within **60 seconds** automatically.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Global Settings", callback_data="admin_bw_global")],
            [InlineKeyboardButton("🏠 Main Menu",       callback_data="admin_main_menu")],
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_bw_specific_limit_confirm$") & admin_only)
async def bw_specific_limit_confirm_callback(client, cb: CallbackQuery):
    """Saves the confirmed specific bot bandwidth limit."""
    await cb.answer("Saving...")

    conv = await get_user_conversation(cb.message.chat.id)
    if not conv or "bw_pending_limit_bytes" not in conv or "bw_selected_url" not in conv:
        await cb.answer("Session expired.", show_alert=True)
        return

    target_url  = conv["bw_selected_url"]
    limit_bytes = conv["bw_pending_limit_bytes"]

    # Keep existing enabled state, just update limit
    existing = await get_specific_bw_config(target_url)
    enabled  = existing["enabled"] if existing else Config.DEFAULT_LIMIT_MODE

    await set_specific_bw_config(target_url, enabled, limit_bytes)

    if target_url == Config.STREAM_URL:
        await refresh_effective_limit()

    await update_user_conversation(cb.message.chat.id, None)

    await cb.message.edit_text(
        f"✅ **Specific Limit Saved!**\n\n"
        f"**Bot:** `{target_url}`\n"
        f"**New Limit:** `{humanbytes(limit_bytes)}`\n"
        f"**Status:** {'🟢 ON' if enabled else '🔴 OFF (Unlimited)'}\n\n"
        f"The bot will update within **60 seconds** (or instantly if it's this bot).",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back to Bot List", callback_data="admin_bw_specific")],
            [InlineKeyboardButton("🏠 Main Menu",        callback_data="admin_main_menu")],
        ])
    )


# ── SECTION 16: APPLICATION LIFECYCLE ────────────────────────────────────────

async def ping_server():
    """Keeps the Render/Heroku dyno alive by self-pinging at a regular interval."""
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(
                timeout=ClientTimeout(total=10)
            ) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"[PING] Self-ping status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"[PING] Self-ping failed: {e}")


if __name__ == "__main__":

    async def main_startup_shutdown_logic():
        """Full startup sequence. Runs until SIGINT/SIGTERM is received."""
        global CURRENT_PROTECTED_DOMAIN, BOT_USERNAME

        LOGGER.info("========== KeralaCaptain Bot V4.5 Starting ==========")

        # ── Step 1: Load protected domain from DB ─────────────────────────────
        CURRENT_PROTECTED_DOMAIN = await get_protected_domain()
        LOGGER.info(f"Protected domain: {CURRENT_PROTECTED_DOMAIN}")

        # ── Step 2: Ensure MongoDB indexes ────────────────────────────────────
        await media_collection.create_index("tmdb_id", unique=True)
        await media_collection.create_index("wp_post_id", unique=True)
        LOGGER.info("MongoDB indexes ensured.")

        # ── Step 3: Start main bot and get its username ───────────────────────
        try:
            await main_bot.start()
            bot_info     = await main_bot.get_me()
            BOT_USERNAME = bot_info.username
            LOGGER.info(f"Main Bot @{BOT_USERNAME} started.")
        except FloodWait as e:
            LOGGER.warning(f"FloodWait on startup: {e.value}s. Waiting...")
            await asyncio.sleep(e.value + 5)
            await main_bot.start()
            bot_info     = await main_bot.get_me()
            BOT_USERNAME = bot_info.username
            LOGGER.info(f"Main Bot @{BOT_USERNAME} started after wait.")
        except Exception as e:
            LOGGER.critical(f"Failed to start main bot: {e}", exc_info=True)
            raise

        # ── Step 4: Load bandwidth state (also calls refresh_effective_limit) ─
        await load_bandwidth_state()
        eff_limit_str = "Unlimited" if not _effective_limit_enabled else humanbytes(_effective_limit_bytes)
        LOGGER.info(
            f"Bandwidth state: Used={humanbytes(_bandwidth_in_memory)}, "
            f"Dead={IS_DEAD}, WarningSent={_warning_85gb_sent}, "
            f"EffectiveLimit={eff_limit_str}"
        )

        # ── Step 5: Initialize multi-client streaming ─────────────────────────
        await initialize_clients()

        # ── Step 6: Start keep-alive ping (Render/Heroku) ─────────────────────
        if Config.ON_HEROKU:
            asyncio.create_task(ping_server())

        # ── Step 7: Start the aiohttp web server ──────────────────────────────
        web_app = await web_server()
        runner  = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"Web server started on port {Config.PORT}.")

        # ── Step 8: Send startup notification to admin ────────────────────────
        try:
            bw_info     = get_bandwidth_info()
            dead_notice = (
                "\n\n🔴 **WARNING: This bot is STILL in Dead Mode from the previous run!**\n"
                "Use **Admin Panel → 🚦 Bandwidth Limits** to revive without restarting."
                if IS_DEAD else ""
            )
            eff_str = "♾️ Unlimited" if not _effective_limit_enabled else humanbytes(_effective_limit_bytes)
            await main_bot.send_message(
                Config.ADMIN_IDS[0],
                f"**✅ Bot @{BOT_USERNAME} is online! (V4.5)**\n\n"
                f"**Bandwidth Used:** `{bw_info['used_human']}`\n"
                f"**Effective Limit:** `{eff_str}`\n"
                f"**Status:** {'🔴 DEAD' if IS_DEAD else '🟢 Active'}\n\n"
                f"**🔒 V4.5 Security Active:**\n"
                f"  - Connection Lifespan: `{Config.MAX_CONNECTION_LIFESPAN_SECS}s`\n"
                f"  - Max Connections/IP: `{Config.MAX_CONNECTIONS_PER_IP}`\n"
                f"  - UA Fingerprinting: `ON ({len(_BLOCKED_UA_FRAGMENTS)} patterns)`\n"
                f"  - Header Integrity Check: `ON`\n"
                f"  - Throttle Jitter: `±{Config.THROTTLE_JITTER_SECS}s`\n"
                f"  - Escalation: `>{Config.DATA_ESCALATION_MB}MB → {Config.DATA_ESCALATION_SLEEP}s`\n\n"
                f"**🚦 Dynamic BW Limit System: ACTIVE**\n"
                f"  - Default Mode: `{'Limit ON' if Config.DEFAULT_LIMIT_MODE else 'Unlimited (VPS)'}`\n"
                f"  - Auto-Revive: `ON` (checks DB every 60s)"
                f"{dead_notice}"
            )
        except Exception as e:
            LOGGER.warning(f"Could not send startup notification: {e}")

        LOGGER.info("========== Bot V4.5 is fully operational. ==========")

        await asyncio.Event().wait()

    # ── Event loop and signal handling ────────────────────────────────────────
    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"Received signal {sig.name}. Shutting down gracefully...")

        await flush_bandwidth_to_db()
        LOGGER.info("Bandwidth flushed to DB on shutdown.")

        if main_bot and main_bot.is_connected:
            LOGGER.info("Stopping main bot...")
            await main_bot.stop()

        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} outstanding tasks...")
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)

        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown_handler(s))
        )

    try:
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except Exception as e:
        LOGGER.critical(f"Critical error forced shutdown: {e}", exc_info=True)
    finally:
        LOGGER.info("Event loop stopped.")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("Shutdown complete. Goodbye!")
