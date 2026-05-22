# ================================================================================
# KeralaCaptain Bot - Pure Streaming Engine V4.2
# ================================================================================
# WHAT'S NEW IN V4.2 (added on top of V4.1 base):
#
# FEATURE 1 - SMART DISK CACHE:
#   - Streams from Telegram to user AND saves to ./cache/ folder simultaneously.
#   - Future users get the same file served directly from disk (no Telegram fetch).
#   - Background download continues even if the first user disconnects.
#   - Background cleanup task deletes old files every 25 minutes.
#
# FEATURE 2 - BANDWIDTH AUTO-KILL:
#   - Tracks every byte sent to users.
#   - Saved in MongoDB using the bot's @username as the unique key.
#   - At 85GB: sends a Telegram warning to admin.
#   - At 90GB: bot automatically enters "Dead Mode" and stops serving files.
#   - New bot = new @username = bandwidth counter starts at 0 automatically.
#
# FEATURE 3 - MANUAL ADMIN KILL SWITCH:
#   - "Kill Bot (Sleep)" button in the admin panel.
#   - Asks for confirmation before killing.
#   - Saves Dead Mode to MongoDB so it survives restarts.
#
# FEATURE 4 - LIFETIME GLOBAL STATS:
#   - Permanent MongoDB document tracking total bandwidth & total streams
#     across ALL bots ever deployed. Never deleted.
#   - Viewable from the admin panel.
#
# STRICT RULES FOLLOWED:
#   - chunk_size = 1024 * 1024 is UNCHANGED.
#   - ByteStreamer.yield_file() and FileReferenceExpired logic are UNCHANGED.
#   - NO anti-leech, NO connection limiters, NO IP blockers.
#   - NO RAM caching of file chunks. Only disk (./cache/).
# ================================================================================

import os
import re
import math
import time
import json
import base64
import signal
import asyncio
import logging
import aiohttp
import urllib.parse
import sys
import psutil
from datetime import datetime, timedelta
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web, ClientConnectionError, ClientTimeout
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait, UserNotParticipant, AuthBytesInvalid,
    PeerIdInvalid, LimitInvalid, Timeout, FileReferenceExpired
)
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId, FileType
from pyrogram import raw
from pyrogram.raw.types import InputPhotoFileLocation, InputDocumentFileLocation

# Load .env file
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

# Bot start time (for uptime display)
start_time = time.time()


# ================================================================================
# CONFIGURATION
# ================================================================================

class Config:
    API_ID              = int(os.environ.get("API_ID", 0))
    API_HASH            = os.environ.get("API_HASH", "")
    BOT_TOKEN           = os.environ.get("BOT_TOKEN", "")
    ADMIN_IDS           = list(int(x) for x in os.environ.get("ADMIN_IDS", "6644681404").split())
    PROTECTED_DOMAIN    = os.environ.get("PROTECTED_DOMAIN", "https://www.keralacaptain.shop/").rstrip('/') + '/'
    MONGO_URI           = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID      = int(os.environ.get("LOG_CHANNEL_ID", 0))
    STREAM_URL          = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT                = int(os.environ.get("PORT", 8080))
    PING_INTERVAL       = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU           = 'DYNO' in os.environ

    # ---- FEATURE 1: Disk Cache Settings ----
    CACHE_DIR               = Path("./cache")
    # Run cleanup every 25 minutes
    CACHE_CLEANUP_INTERVAL  = 1500
    # Delete files not accessed for 2 hours
    CACHE_MAX_AGE_SECONDS   = 7200
    # Max total cache size on disk: 25 GB (leaving 5 GB buffer from your 30 GB)
    CACHE_MAX_DISK_BYTES    = 25 * 1024 * 1024 * 1024

    # ---- FEATURE 2: Bandwidth Thresholds ----
    # Send admin warning at 85 GB
    BANDWIDTH_WARNING_BYTES = 85 * 1024 * 1024 * 1024
    # Trigger Dead Mode at 90 GB
    BANDWIDTH_KILL_BYTES    = 90 * 1024 * 1024 * 1024
    # Flush in-memory counter to MongoDB every 500 MB to reduce DB writes
    BANDWIDTH_FLUSH_EVERY   = 500 * 1024 * 1024


# Validate required environment variables
required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
    Config.ADMIN_IDS
]
if not all(required_vars) or Config.ADMIN_IDS == [0]:
    LOGGER.critical(
        "FATAL: One or more required variables "
        "(API_ID, API_HASH, BOT_TOKEN, MONGO_URI, LOG_CHANNEL_ID, STREAM_URL, ADMIN_IDS) "
        "are missing. Cannot start."
    )
    exit(1)

# Create the cache directory on startup if it doesn't exist
Config.CACHE_DIR.mkdir(exist_ok=True)

# Global dynamic protected domain (loaded from DB, can be changed by admin)
CURRENT_PROTECTED_DOMAIN = Config.PROTECTED_DOMAIN


# ================================================================================
# HELPER FUNCTIONS
# ================================================================================

async def encode(string: str) -> str:
    """Base64-encodes a string for use in stream URLs."""
    string_bytes = string.encode("ascii")
    base64_bytes = base64.urlsafe_b64encode(string_bytes)
    return (base64_bytes.decode("ascii")).strip("=")

async def decode(base64_string: str) -> str:
    """Decodes a base64-encoded stream URL string."""
    base64_string = base64_string.strip("=")
    base64_bytes = (base64_string + "=" * (-len(base64_string) % 4)).encode("ascii")
    string_bytes = base64.urlsafe_b64decode(base64_bytes)
    return string_bytes.decode("ascii")

def humanbytes(size):
    """Converts a byte count into a human-readable string (KB, MB, GB, TB)."""
    if not size:
        return "0 B"
    power = 1024
    n = 0
    power_labels = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{round(size, 2)} {power_labels[n]}B"

def get_readable_time(seconds: int) -> str:
    """Converts seconds into a human-readable duration (e.g. 1d 2h 3m 4s)."""
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days != 0:
        result += f"{days}d "
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours != 0:
        result += f"{hours}h "
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes != 0:
        result += f"{minutes}m "
    seconds = int(seconds)
    result += f"{seconds}s"
    return result


# ================================================================================
# DATABASE SETUP
# ================================================================================

db_client = AsyncIOMotorClient(Config.MONGO_URI)
db = db_client['KeralaCaptainBotDB']

# Original collections (unchanged)
media_collection          = db['media']
media_backup_collection   = db['media_backup']
user_conversations_col    = db['conversations']
settings_collection       = db['settings']

# NEW: Collection that tracks per-bot bandwidth usage
# Documents look like: { "_id": "BotUsername", "bandwidth_used": 12345, "is_dead": False, ... }
bandwidth_collection      = db['bandwidth']

# NEW: Collection for permanent global lifetime statistics across ALL bots
# Only one document ever exists: { "_id": "global_stats", "total_bandwidth_bytes": ..., "total_streams": ... }
lifetime_stats_collection = db['lifetime_stats']


# ================================================================================
# ORIGINAL DATABASE FUNCTIONS (unchanged from V4.1)
# ================================================================================

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
    """Gets user conversation state."""
    return await user_conversations_col.find_one({"_id": chat_id})

async def update_user_conversation(chat_id, data):
    """Sets or clears user conversation state."""
    if data:
        await user_conversations_col.update_one({"_id": chat_id}, {"$set": data}, upsert=True)
    else:
        await user_conversations_col.delete_one({"_id": chat_id})

async def get_post_id_from_msg_id(msg_id: int):
    """Helper for stream refreshing - finds which post_id owns a given message_id."""
    doc = await media_collection.find_one({"message_ids": {"$in": [msg_id]}})
    return doc['wp_post_id'] if doc else None

async def get_protected_domain() -> str:
    """Fetches the protected domain from DB settings, falls back to Config default."""
    try:
        doc = await settings_collection.find_one({"_id": "bot_settings"})
        if doc and "protected_domain" in doc:
            return doc["protected_domain"]
    except Exception as e:
        LOGGER.error(f"Could not fetch domain from DB: {e}. Using default.")
    return Config.PROTECTED_DOMAIN

async def set_protected_domain(new_domain: str):
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


# ================================================================================
# FEATURE 2 & 3: BANDWIDTH TRACKING & AUTO-KILL (Dead Mode)
# ================================================================================

# The bot's Telegram username, set during startup (e.g., "MyStreamBot")
# This is the unique key used in MongoDB to track THIS bot's bandwidth.
# When you deploy a new bot with a new token/username, it automatically starts at 0.
BOT_USERNAME = ""

# In-memory bandwidth counter.
# We don't write to DB on every single byte - we accumulate here and flush periodically.
_bandwidth_in_memory = 0

# Tracks how many bytes have been added since the last DB flush.
_bandwidth_since_flush = 0

# Is this bot in Dead Mode? If True, all /stream/ requests are rejected.
IS_DEAD = False

# Flag to avoid sending the 85GB warning more than once per bot lifetime.
_warning_85gb_sent = False


async def load_bandwidth_state():
    """
    Called on bot startup. Loads this bot's bandwidth state from MongoDB.

    HOW THE USERNAME KEY WORKS:
    - This bot saves data under its own @username (e.g., "MyBot1").
    - When you deploy a completely new bot (new token = new username), MongoDB
      will find no record for that new username, so bandwidth starts at 0.
    - The old bot's 90GB record remains in DB but is irrelevant to the new bot.
    """
    global _bandwidth_in_memory, IS_DEAD, _warning_85gb_sent

    if not BOT_USERNAME:
        LOGGER.error("BOT_USERNAME not set yet. Cannot load bandwidth state.")
        return

    doc = await bandwidth_collection.find_one({"_id": BOT_USERNAME})
    if doc:
        # Existing bot - load its state
        _bandwidth_in_memory = doc.get("bandwidth_used", 0)
        IS_DEAD             = doc.get("is_dead", False)
        _warning_85gb_sent  = doc.get("warning_sent", False)
        LOGGER.info(
            f"[BANDWIDTH] Loaded state for @{BOT_USERNAME}: "
            f"Used={humanbytes(_bandwidth_in_memory)}, Dead={IS_DEAD}"
        )
    else:
        # New bot - create a fresh record
        _bandwidth_in_memory = 0
        IS_DEAD              = False
        _warning_85gb_sent   = False
        await bandwidth_collection.insert_one({
            "_id":           BOT_USERNAME,
            "bandwidth_used": 0,
            "is_dead":        False,
            "warning_sent":   False,
            "created_at":     datetime.utcnow()
        })
        LOGGER.info(f"[BANDWIDTH] New bot @{BOT_USERNAME} - bandwidth counter starts at 0.")


async def flush_bandwidth_to_db():
    """
    Writes the current in-memory bandwidth counter to MongoDB.
    Called every 500 MB and on graceful shutdown/restart.
    """
    if not BOT_USERNAME:
        return
    await bandwidth_collection.update_one(
        {"_id": BOT_USERNAME},
        {"$set": {
            "bandwidth_used": _bandwidth_in_memory,
            "is_dead":        IS_DEAD,
            "warning_sent":   _warning_85gb_sent,
            "last_updated":   datetime.utcnow()
        }},
        upsert=True
    )


async def add_bandwidth(bytes_sent: int):
    """
    Called once after each stream response completes.
    Adds the bytes to the in-memory counter, checks thresholds,
    and triggers warnings or Dead Mode as needed.

    Also increments the permanent lifetime stats counter.
    """
    global _bandwidth_in_memory, _bandwidth_since_flush, IS_DEAD, _warning_85gb_sent

    if IS_DEAD:
        return  # Bot is already dead, no need to track

    _bandwidth_in_memory  += bytes_sent
    _bandwidth_since_flush += bytes_sent

    # Always add to lifetime global stats (permanent, never deleted)
    asyncio.create_task(_increment_lifetime_bandwidth_db(bytes_sent))

    # Flush in-memory counter to MongoDB every BANDWIDTH_FLUSH_EVERY bytes (e.g., 500 MB)
    if _bandwidth_since_flush >= Config.BANDWIDTH_FLUSH_EVERY:
        _bandwidth_since_flush = 0
        await flush_bandwidth_to_db()
        LOGGER.info(f"[BANDWIDTH] Flushed to DB. Total used: {humanbytes(_bandwidth_in_memory)}")

    # --- Check 85 GB Warning Threshold ---
    if not _warning_85gb_sent and _bandwidth_in_memory >= Config.BANDWIDTH_WARNING_BYTES:
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
                    f"You are approaching the **90 GB auto-kill limit.**\n"
                    f"Please **prepare to deploy a new bot** on a new Render account soon!"
                )
        except Exception as e:
            LOGGER.error(f"Could not send bandwidth warning to admin: {e}")

    # --- Check 90 GB Auto-Kill Threshold ---
    if _bandwidth_in_memory >= Config.BANDWIDTH_KILL_BYTES:
        await trigger_dead_mode(reason="auto")


async def trigger_dead_mode(reason: str = "auto"):
    """
    Puts the bot into Dead Mode permanently.

    - Sets IS_DEAD = True in memory (stream handler immediately stops serving).
    - Saves the state to MongoDB so Dead Mode survives restarts.
    - Sends a notification to the admin.

    reason: "auto" = hit 90GB automatically | "manual" = admin pressed Kill button
    """
    global IS_DEAD

    if IS_DEAD:
        return  # Already dead, don't run again

    IS_DEAD = True
    LOGGER.critical(
        f"[DEAD MODE] Bot @{BOT_USERNAME} is now DEAD. "
        f"Reason: {reason}. Bandwidth used: {humanbytes(_bandwidth_in_memory)}"
    )

    # Save to DB immediately so it persists on restart
    await bandwidth_collection.update_one(
        {"_id": BOT_USERNAME},
        {"$set": {
            "bandwidth_used": _bandwidth_in_memory,
            "is_dead":        True,
            "warning_sent":   _warning_85gb_sent,
            "dead_reason":    reason,
            "dead_at":        datetime.utcnow()
        }},
        upsert=True
    )

    # Notify admin
    try:
        reason_text = (
            "automatically (**90 GB** bandwidth limit reached)"
            if reason == "auto"
            else "**manually** by admin"
        )
        for admin_id in Config.ADMIN_IDS:
            await main_bot.send_message(
                admin_id,
                f"🔴 **BOT IS NOW IN SLEEP (DEAD) MODE**\n\n"
                f"**Bot:** @{BOT_USERNAME}\n"
                f"**Killed:** {reason_text}\n"
                f"**Total bandwidth used:** `{humanbytes(_bandwidth_in_memory)}`\n\n"
                f"The bot will **no longer serve any video streams.**\n"
                f"Deploy a new bot on a new Render account to continue service."
            )
    except Exception as e:
        LOGGER.error(f"Could not send dead mode notification: {e}")


def get_bandwidth_info() -> dict:
    """Returns current bandwidth info as a dict (used by admin panel)."""
    return {
        "used":              _bandwidth_in_memory,
        "used_human":        humanbytes(_bandwidth_in_memory),
        "is_dead":           IS_DEAD,
        "warning_sent":      _warning_85gb_sent,
        "kill_threshold":    Config.BANDWIDTH_KILL_BYTES,
        "warning_threshold": Config.BANDWIDTH_WARNING_BYTES,
        "percent":           round((_bandwidth_in_memory / Config.BANDWIDTH_KILL_BYTES) * 100, 2)
    }


# ================================================================================
# FEATURE 4: LIFETIME GLOBAL STATISTICS
# ================================================================================

async def _increment_lifetime_bandwidth_db(bytes_sent: int):
    """
    Increments the permanent lifetime bandwidth counter in MongoDB.
    Uses $inc so it never overwrites - just adds.
    This data belongs to the global document and is NEVER deleted.
    """
    try:
        await lifetime_stats_collection.update_one(
            {"_id": "global_stats"},
            {"$inc": {"total_bandwidth_bytes": bytes_sent}},
            upsert=True
        )
    except Exception as e:
        LOGGER.error(f"[LIFETIME STATS] Failed to increment bandwidth: {e}")


async def increment_lifetime_streams():
    """
    Increments the permanent lifetime stream counter in MongoDB.
    Called once per new stream (from_bytes == 0).
    """
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
    return {
        "total_bandwidth_bytes": 0,
        "total_bandwidth_human": "0 B",
        "total_streams":         0
    }


# ================================================================================
# FEATURE 1: SMART DISK CACHE ENGINE
# ================================================================================

# In-memory registry that tracks the status of each cached file.
# Structure: { message_id (int): { "status": str, "written": int, "size": int, "last_access": float } }
#
# "status" values:
#   "downloading" - file is currently being written to disk from Telegram
#   "complete"    - file is fully on disk and ready to serve
#   "error"       - download failed; partial file was deleted
cache_registry: dict = {}

# Lock to prevent race conditions when multiple requests try to start
# a download for the same message_id at the same time.
cache_registry_lock = asyncio.Lock()

# Tracks currently running background download tasks { message_id: asyncio.Task }
active_cache_tasks: dict = {}


def get_cache_path(message_id: int) -> Path:
    """Returns the disk path where a message's file is cached: ./cache/{message_id}.bin"""
    return Config.CACHE_DIR / f"{message_id}.bin"


async def is_fully_cached(message_id: int) -> bool:
    """
    Returns True only if the file is 100% downloaded and exists on disk.
    Also verifies the file physically exists (protects against manual deletions).
    """
    entry = cache_registry.get(message_id)
    if entry and entry["status"] == "complete":
        if get_cache_path(message_id).exists():
            return True
        else:
            # File was deleted externally; remove stale registry entry
            cache_registry.pop(message_id, None)
    return False


async def is_being_cached(message_id: int) -> bool:
    """Returns True if a background download is currently in progress for this file."""
    entry = cache_registry.get(message_id)
    return entry is not None and entry["status"] == "downloading"


async def start_cache_download(message_id: int, file_id: FileId, tg_connect):
    """
    Starts a background asyncio task that downloads the FULL file from Telegram
    and saves it to ./cache/{message_id}.bin.

    IMPORTANT: This task is INDEPENDENT of the user's stream connection.
    Even if the first user disconnects mid-video, this task continues until
    the full file is saved on disk. This protects User B's experience.

    Only one download task is ever started per message_id (checked via lock).
    """
    async with cache_registry_lock:
        # If already downloading or complete, don't start again
        if message_id in cache_registry:
            return

        # Register the file as "downloading"
        cache_registry[message_id] = {
            "status":      "downloading",
            "written":     0,
            "size":        file_id.file_size,
            "last_access": time.time()
        }

    # Create the background task
    task = asyncio.create_task(
        _cache_download_worker(message_id, file_id, tg_connect)
    )
    active_cache_tasks[message_id] = task
    LOGGER.info(
        f"[CACHE] Background download started for msg_id={message_id}, "
        f"size={humanbytes(file_id.file_size)}"
    )


async def _cache_download_worker(message_id: int, file_id: FileId, tg_connect):
    """
    The actual background worker.
    Downloads from Telegram byte-by-byte using the ORIGINAL yield_file logic
    and writes each chunk to disk immediately (f.flush() after every chunk).

    After each chunk is written:
    - cache_registry[message_id]["written"] is updated.
    - Readers (serve_from_disk) poll this value to know how much is available.

    If download fails: partial file is deleted, status set to "error".
    If download succeeds: status set to "complete".
    """
    cache_path = get_cache_path(message_id)

    try:
        with open(cache_path, 'wb') as f:
            # yield_file is the ORIGINAL UNCHANGED Pyrogram streaming generator
            async for chunk in tg_connect.yield_file(file_id, 0, 1024 * 1024, message_id):
                f.write(chunk)
                f.flush()  # Flush each chunk so disk readers can access it immediately

                # Update the written byte counter so other users can start reading
                if message_id in cache_registry:
                    cache_registry[message_id]["written"] += len(chunk)

        # Mark as fully complete
        if message_id in cache_registry:
            cache_registry[message_id]["status"]      = "complete"
            cache_registry[message_id]["written"]     = file_id.file_size
            cache_registry[message_id]["last_access"] = time.time()

        LOGGER.info(f"[CACHE] Download complete: msg_id={message_id}, path={cache_path}")

    except Exception as e:
        LOGGER.error(f"[CACHE] Download FAILED for msg_id={message_id}: {e}")

        # Mark as error
        if message_id in cache_registry:
            cache_registry[message_id]["status"] = "error"

        # Delete the partial/corrupted file
        if cache_path.exists():
            try:
                cache_path.unlink()
                LOGGER.info(f"[CACHE] Deleted partial file for msg_id={message_id}")
            except Exception as del_err:
                LOGGER.warning(f"[CACHE] Could not delete partial file: {del_err}")

    finally:
        active_cache_tasks.pop(message_id, None)


async def serve_from_disk(message_id: int, from_bytes: int, file_size: int, resp: web.StreamResponse) -> int:
    """
    Streams a file from disk to the user.

    Handles TWO cases:
    1. COMPLETE file: reads and sends straight through.
    2. PARTIAL file (still downloading): reads available bytes, then waits
       (polls every 0.5s) for more bytes to be written by the background task.
       This is how User B can watch while User A's download is still going.

    Returns the total number of bytes sent to the user.
    """
    cache_path   = get_cache_path(message_id)
    bytes_sent   = 0
    chunk_size   = 1024 * 1024  # 1 MB read chunks - same as original
    current_pos  = from_bytes

    # Update last_access time (used by cleanup to decide what to delete)
    if message_id in cache_registry:
        cache_registry[message_id]["last_access"] = time.time()

    try:
        with open(cache_path, 'rb') as f:
            f.seek(from_bytes)

            while current_pos < file_size:
                entry            = cache_registry.get(message_id)
                written_so_far   = entry["written"] if entry else file_size
                available        = written_so_far - current_pos

                if available <= 0:
                    # No new data available yet
                    if entry and entry["status"] == "downloading":
                        # Background task is still running - wait briefly then retry
                        await asyncio.sleep(0.5)
                        continue
                    else:
                        # Download finished or errored - stop serving
                        break

                # Read up to 1 MB of available data
                to_read = min(chunk_size, available)
                chunk   = f.read(to_read)

                if not chunk:
                    break

                await resp.write(chunk)
                bytes_sent  += len(chunk)
                current_pos += len(chunk)

    except (ConnectionError, asyncio.CancelledError):
        LOGGER.debug(f"[CACHE] Client disconnected during disk serve for msg_id={message_id}")

    except Exception as e:
        LOGGER.error(f"[CACHE] Error serving from disk for msg_id={message_id}: {e}")

    return bytes_sent


async def load_existing_cache_on_startup():
    """
    On startup, scans ./cache/ and loads any existing .bin files into the registry.
    This means files cached in a PREVIOUS run are immediately available.
    No need to re-download them from Telegram.
    """
    count = 0
    for f in Config.CACHE_DIR.glob("*.bin"):
        try:
            msg_id    = int(f.stem)
            file_size = f.stat().st_size
            if file_size > 0:
                cache_registry[msg_id] = {
                    "status":      "complete",
                    "written":     file_size,
                    "size":        file_size,
                    "last_access": f.stat().st_atime  # Use filesystem access time
                }
                count += 1
        except Exception:
            pass

    if count > 0:
        LOGGER.info(f"[CACHE] Loaded {count} existing cached files from previous run.")


async def cache_cleanup_task():
    """
    Background task. Runs every 25 minutes.
    Keeps the ./cache/ folder from filling up your 30 GB disk.

    STRATEGY:
    1. Scan all .bin files and record their size + last access time.
    2. Delete files not accessed for > 2 hours (CACHE_MAX_AGE_SECONDS).
    3. If total cache size still exceeds CACHE_MAX_DISK_BYTES (25 GB),
       delete oldest-accessed files first until under the limit.
    4. Never delete files that are currently being downloaded.
    """
    while True:
        await asyncio.sleep(Config.CACHE_CLEANUP_INTERVAL)  # Wait 25 minutes

        try:
            LOGGER.info("[CACHE] Running cleanup task...")
            now              = time.time()
            total_cache_size = 0
            cache_files      = []  # (Path, last_access_time, size_bytes)

            for f in Config.CACHE_DIR.glob("*.bin"):
                try:
                    stat      = f.stat()
                    f_size    = stat.st_size
                    try:
                        msg_id     = int(f.stem)
                        entry      = cache_registry.get(msg_id)
                        last_acc   = entry["last_access"] if entry else stat.st_atime
                    except Exception:
                        last_acc   = stat.st_atime

                    total_cache_size += f_size
                    cache_files.append((f, last_acc, f_size))
                except Exception as e:
                    LOGGER.warning(f"[CACHE] Could not stat {f}: {e}")

            LOGGER.info(
                f"[CACHE] Cache status: {humanbytes(total_cache_size)} across {len(cache_files)} files."
            )

            # Sort oldest-accessed first so we delete those first
            cache_files.sort(key=lambda x: x[1])

            deleted_count = 0
            freed_bytes   = 0

            for f_path, last_access, f_size in cache_files:
                # Never delete a file that is currently being downloaded
                try:
                    msg_id = int(f_path.stem)
                except Exception:
                    msg_id = None

                if msg_id and msg_id in active_cache_tasks:
                    continue  # Skip - download in progress

                should_delete = False

                # Rule 1: Delete if file hasn't been accessed in 2 hours
                if now - last_access > Config.CACHE_MAX_AGE_SECONDS:
                    should_delete = True

                # Rule 2: Delete (oldest first) if total cache exceeds 25 GB limit
                if total_cache_size > Config.CACHE_MAX_DISK_BYTES:
                    should_delete = True

                if should_delete:
                    try:
                        f_path.unlink()
                        total_cache_size -= f_size
                        freed_bytes      += f_size
                        deleted_count    += 1

                        if msg_id and msg_id in cache_registry:
                            del cache_registry[msg_id]

                    except Exception as e:
                        LOGGER.warning(f"[CACHE] Could not delete {f_path}: {e}")

            LOGGER.info(
                f"[CACHE] Cleanup done. Deleted {deleted_count} files, "
                f"freed {humanbytes(freed_bytes)}. "
                f"Remaining: {humanbytes(total_cache_size)}"
            )

        except Exception as e:
            LOGGER.error(f"[CACHE] Cleanup task encountered an error: {e}")


# ================================================================================
# STREAMING ENGINE - ByteStreamer CLASS (ORIGINAL - COMPLETELY UNCHANGED)
# ================================================================================

multi_clients        = {}
work_loads           = {}
class_cache          = {}
processed_media_groups = {}
next_client_idx      = 0
stream_errors        = 0
last_error_reset     = time.time()


class ByteStreamer:
    """
    ORIGINAL ByteStreamer from V4.1.
    This class and its yield_file() method are COMPLETELY UNCHANGED.
    Do not modify chunk sizes or Pyrogram session logic.
    """

    def __init__(self, client: Client):
        self.client: Client  = client
        self.cached_file_ids = {}
        self.session_cache   = {}
        asyncio.create_task(self.clean_cache_regularly())

    async def clean_cache_regularly(self):
        while True:
            await asyncio.sleep(1200)  # Every 20 minutes
            self.cached_file_ids.clear()
            self.session_cache.clear()
            LOGGER.info("Cleared ByteStreamer's cached file properties and sessions.")

    async def get_file_properties(self, message_id: int):
        if message_id in self.cached_file_ids:
            return self.cached_file_ids[message_id]

        message = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
        if not message or message.empty or not (message.document or message.video):
            raise FileNotFoundError

        media   = message.document or message.video
        file_id = FileId.decode(media.file_id)
        setattr(file_id, "file_size", media.file_size or 0)
        setattr(file_id, "mime_type", media.mime_type or "video/mp4")
        setattr(file_id, "file_name", media.file_name or "Unknown.mp4")

        self.cached_file_ids[message_id] = file_id
        return file_id

    async def generate_media_session(self, file_id: FileId) -> Session:
        media_session = self.client.media_sessions.get(file_id.dc_id)
        dc_id         = file_id.dc_id

        if dc_id in self.session_cache:
            session, ts = self.session_cache[dc_id]
            if time.time() - ts < 300:  # 5-minute TTL
                LOGGER.debug(f"Reusing TTL-cached media session for DC {dc_id}")
                return session

        if media_session:
            try:
                await media_session.send(raw.functions.help.GetConfig(), timeout=10)
                self.session_cache[dc_id] = (media_session, time.time())
                LOGGER.debug(f"Reusing pinged media session for DC {dc_id}")
                return media_session
            except Exception as e:
                LOGGER.warning(f"Existing media session for DC {dc_id} is stale: {e}. Recreating.")
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
                    LOGGER.warning(f"AuthBytesInvalid on attempt {i+1}: {e}")
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
        ORIGINAL yield_file - COMPLETELY UNCHANGED.
        chunk_size is always 1024 * 1024 (1 MB).
        FileReferenceExpired handling is UNCHANGED.
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
                # ORIGINAL refresh logic - UNCHANGED
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
                raise  # Refresh failed - give up

            except FloodWait as e:
                LOGGER.warning(f"FloodWait of {e.value} seconds on GetFile. Waiting...")
                await asyncio.sleep(e.value)
                continue


# ================================================================================
# WEB ROUTES
# ================================================================================

routes = web.RouteTableDef()


@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(
        text="Welcome to KeralaCaptain's Streaming Service!",
        content_type='text/html'
    )


@routes.get("/health")
async def health_handler(request):
    """Health check endpoint. Also shows bandwidth and cache status."""
    global stream_errors, last_error_reset
    if time.time() - last_error_reset > 60:
        stream_errors    = 0
        last_error_reset = time.time()

    bw_info        = get_bandwidth_info()
    active_sessions = len(multi_clients)
    cache_size     = 0
    if multi_clients:
        sample_client = list(multi_clients.values())[0]
        if sample_client in class_cache:
            cache_size = len(class_cache[sample_client].cached_file_ids)

    return web.json_response({
        "status":                 "dead" if IS_DEAD else "ok",
        "active_clients":         active_sessions,
        "property_cache_size":    cache_size,
        "stream_errors_last_min": stream_errors,
        "workloads":              work_loads,
        "bandwidth_used":         bw_info["used_human"],
        "bandwidth_percent":      f"{bw_info['percent']}%",
        "disk_cached_files":      len([e for e in cache_registry.values() if e["status"] == "complete"]),
        "active_downloads":       len(active_cache_tasks),
    })


@routes.get("/favicon.ico")
async def favicon_handler(request):
    return web.Response(status=204)


@routes.get(r"/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    """
    Main streaming route. Enhanced with:
    1. Dead Mode gate (Feature 2 & 3): if IS_DEAD, return 503 immediately.
    2. Disk cache logic (Feature 1): serve from disk if available.
    3. Bandwidth tracking (Feature 2): count bytes sent.

    Original logic (referer check, load balancing, ByteStreamer) is UNCHANGED.
    """
    global stream_errors
    client_index = None

    try:
        # ----------------------------------------------------------------
        # GATE 1: DEAD MODE CHECK (Feature 2 & 3)
        # If the bot has been killed (manually or by hitting 90 GB),
        # ALL stream requests are immediately rejected.
        # ----------------------------------------------------------------
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

        # ----------------------------------------------------------------
        # GATE 2: REFERER CHECK (Original security - UNCHANGED)
        # ----------------------------------------------------------------
        referer         = request.headers.get('Referer')
        allowed_referer = CURRENT_PROTECTED_DOMAIN

        if not referer or not referer.startswith(allowed_referer):
            LOGGER.warning(
                f"Blocked hotlink. Referer: '{referer}'. Allowed: '{allowed_referer}'"
            )
            return web.Response(status=403, text="403 Forbidden: Direct access is not allowed.")

        # ----------------------------------------------------------------
        # ORIGINAL: Parse message ID and Range header
        # ----------------------------------------------------------------
        message_id   = int(request.match_info['message_id'])
        range_header = request.headers.get("Range", 0)

        # ----------------------------------------------------------------
        # ORIGINAL: Client load balancing (round-robin on least-loaded)
        # ----------------------------------------------------------------
        min_load    = min(work_loads.values())
        candidates  = [cid for cid, load in work_loads.items() if load == min_load]
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

        # ----------------------------------------------------------------
        # ORIGINAL: Get file properties from Telegram or in-memory cache
        # ----------------------------------------------------------------
        file_id   = await tg_connect.get_file_properties(message_id)
        file_size = file_id.file_size

        # ----------------------------------------------------------------
        # ORIGINAL: Parse range header for seek support
        # ----------------------------------------------------------------
        from_bytes = 0
        if range_header:
            from_bytes_str, _ = range_header.replace("bytes=", "").split("-")
            from_bytes = int(from_bytes_str)

        if from_bytes >= file_size:
            return web.Response(status=416, reason="Range Not Satisfiable")

        # ORIGINAL: These values are UNCHANGED
        chunk_size     = 1024 * 1024  # 1 MB - DO NOT CHANGE
        offset         = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset

        # ----------------------------------------------------------------
        # ORIGINAL: Build response headers with CORS
        # ----------------------------------------------------------------
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

        # ----------------------------------------------------------------
        # NEW: Count this as a new stream in lifetime stats
        # Only count when from_bytes == 0 (fresh start, not a seek/resume)
        # ----------------------------------------------------------------
        if from_bytes == 0:
            asyncio.create_task(increment_lifetime_streams())

        # ----------------------------------------------------------------
        # FEATURE 1: DISK CACHE DECISION TREE
        #
        # Case A: File is FULLY on disk → serve from disk (no Telegram needed)
        # Case B: File is PARTIALLY on disk and requested position is available
        #         → serve from disk, poll for new bytes as background task writes
        # Case C: Not cached OR seek past written bytes → serve from Telegram
        #         AND trigger a background download of the full file for future users
        # ----------------------------------------------------------------
        total_bytes_sent = 0
        served_from_disk = False

        # ---- Case A: Fully cached ----
        if await is_fully_cached(message_id):
            LOGGER.info(
                f"[CACHE HIT - FULL] msg_id={message_id}, from_bytes={from_bytes}"
            )
            total_bytes_sent = await serve_from_disk(message_id, from_bytes, file_size, resp)
            served_from_disk = True

        # ---- Case B: Partially cached and requested byte is already written ----
        elif await is_being_cached(message_id):
            entry = cache_registry.get(message_id)
            if entry and from_bytes < entry["written"]:
                LOGGER.info(
                    f"[CACHE HIT - PARTIAL] msg_id={message_id}, "
                    f"from_bytes={from_bytes}, written={entry['written']}"
                )
                total_bytes_sent = await serve_from_disk(message_id, from_bytes, file_size, resp)
                served_from_disk = True
            else:
                LOGGER.info(
                    f"[CACHE MISS - SEEK AHEAD] msg_id={message_id}, "
                    f"from_bytes={from_bytes} is ahead of written={entry['written'] if entry else 0}"
                )

        # ---- Case C: Serve from Telegram (original logic) ----
        if not served_from_disk:

            # Start a background download if not already running.
            # This saves the file to disk for future users.
            if not await is_being_cached(message_id) and not await is_fully_cached(message_id):
                await start_cache_download(message_id, file_id, tg_connect)

            # ---- ORIGINAL Telegram streaming loop - COMPLETELY UNCHANGED ----
            body_generator = tg_connect.yield_file(file_id, offset, chunk_size, message_id)
            is_first_chunk  = True

            async for chunk in body_generator:
                try:
                    if is_first_chunk and first_part_cut > 0:
                        await resp.write(chunk[first_part_cut:])
                        total_bytes_sent += len(chunk) - first_part_cut
                        is_first_chunk    = False
                    else:
                        await resp.write(chunk)
                        total_bytes_sent += len(chunk)
                except (ConnectionError, asyncio.CancelledError):
                    LOGGER.warning(
                        f"Client disconnected while writing chunk for message {message_id}."
                    )
                    return resp

        # ----------------------------------------------------------------
        # FEATURE 2: Track bandwidth after stream completes
        # Called once per completed response, NOT per chunk.
        # ----------------------------------------------------------------
        if total_bytes_sent > 0:
            await add_bandwidth(total_bytes_sent)

        return resp

    except (FileReferenceExpired, AuthBytesInvalid) as e:
        LOGGER.error(f"FATAL STREAM ERROR for {message_id}: {type(e).__name__}.")
        stream_errors += 1
        return web.Response(status=410, text="Stream link expired, please refresh the page.")

    except Exception as e:
        LOGGER.critical(f"Unhandled stream error for {message_id}: {e}", exc_info=True)
        stream_errors += 1
        return web.Response(status=500)

    finally:
        if client_index is not None:
            work_loads[client_index] -= 1
            LOGGER.debug(f"Decremented workload for client {client_index}.")


async def web_server():
    web_app = web.Application(client_max_size=30_000_000)
    web_app.add_routes(routes)
    return web_app


# ================================================================================
# BOT & CLIENT INITIALIZATION (ORIGINAL - UNCHANGED)
# ================================================================================

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
    multi_clients[0] = main_bot
    work_loads[0]    = 0

    all_tokens = TokenParser().parse_from_env()
    if not all_tokens:
        LOGGER.info("No additional MULTI_TOKEN clients found.")
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

    clients = await asyncio.gather(
        *[start_client(i, token) for i, token in all_tokens.items()]
    )
    multi_clients.update({cid: client for cid, client in clients if client is not None})

    if len(multi_clients) > 1:
        LOGGER.info(
            f"Successfully initialized {len(multi_clients)} clients. Multi-Client mode is ON."
        )


async def forward_file_safely(message_to_forward: Message):
    """
    Original forward_file_safely - UNCHANGED.
    Used by FileReferenceExpired refresh logic in ByteStreamer.
    """
    try:
        media = message_to_forward.document or message_to_forward.video
        if not media:
            LOGGER.error("Message has no media to send.")
            return None

        file_id = media.file_id
        LOGGER.info(
            f"Sending cached media for message {message_to_forward.id} using main bot..."
        )
        return await main_bot.send_cached_media(
            chat_id=Config.LOG_CHANNEL_ID,
            file_id=file_id,
            caption=getattr(message_to_forward, 'caption', '')
        )
    except Exception as e:
        LOGGER.error(f"Main bot failed to send cached media: {e}")
        return None


# ================================================================================
# ADMIN BOT HANDLERS
# ================================================================================

admin_only = filters.user(Config.ADMIN_IDS)


def _get_main_menu_markup() -> InlineKeyboardMarkup:
    """Returns the main admin panel keyboard. Includes all new buttons."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics",          callback_data="admin_stats")],
        [InlineKeyboardButton("📈 Lifetime Stats",      callback_data="admin_lifetime_stats")],
        [InlineKeyboardButton("⚙️ Settings",            callback_data="admin_settings")],
        [InlineKeyboardButton("🔄 Restart Bot",         callback_data="admin_restart")],
        [InlineKeyboardButton("🛑 Kill Bot (Sleep)",    callback_data="admin_kill_bot")],
    ])


@main_bot.on_message(filters.command("start") & filters.private & admin_only)
async def start_command(client, message: Message):
    await message.reply_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel.",
        reply_markup=_get_main_menu_markup()
    )
    await update_user_conversation(message.chat.id, None)


@main_bot.on_callback_query(filters.regex("^admin_stats$") & admin_only)
async def stats_callback(client, cb: CallbackQuery):
    """Shows bot statistics including bandwidth, disk cache, and system info."""
    await cb.answer("Fetching stats...")

    uptime = get_readable_time(time.time() - start_time)

    try:
        cpu_usage  = psutil.cpu_percent()
        ram_usage  = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').percent
        ram_total  = humanbytes(psutil.virtual_memory().total)
    except Exception:
        cpu_usage = ram_usage = disk_usage = "N/A"
        ram_total = "N/A"

    bw_info     = get_bandwidth_info()
    dead_status = "🔴 DEAD (Sleep Mode)" if bw_info["is_dead"] else "🟢 Active"

    complete_files  = len([e for e in cache_registry.values() if e["status"] == "complete"])
    workload_str    = "\n".join(
        [f"  - Client {cid}: {load} streams" for cid, load in work_loads.items()]
    )

    text = (
        f"**📊 Bot Statistics**\n\n"
        f"**Bot:** @{BOT_USERNAME or 'Unknown'}  |  **Status:** {dead_status}\n"
        f"**Uptime:** `{uptime}`\n\n"
        f"**🌐 Bandwidth (This Bot):**\n"
        f"  - Used: `{bw_info['used_human']}` / 90 GB\n"
        f"  - Progress: `{bw_info['percent']}%`\n"
        f"  - Warning Sent: `{bw_info['warning_sent']}`\n\n"
        f"**💾 Disk Cache:**\n"
        f"  - Fully Cached Files: `{complete_files}`\n"
        f"  - Active Downloads: `{len(active_cache_tasks)}`\n\n"
        f"**🖥️ System:**\n"
        f"  - CPU: `{cpu_usage}%`\n"
        f"  - RAM: `{ram_usage}%` (Total: `{ram_total}`)\n"
        f"  - Disk: `{disk_usage}%`\n\n"
        f"**📡 Streaming:**\n"
        f"  - Active Clients: `{len(multi_clients)}`\n"
        f"  - Stream Errors (last min): `{stream_errors}`\n"
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
    """
    FEATURE 4: Shows permanent lifetime stats across ALL bots ever deployed.
    This data lives in MongoDB and is never deleted.
    """
    await cb.answer("Fetching lifetime stats...")

    stats = await get_lifetime_stats()

    text = (
        f"**📈 Lifetime Global Statistics**\n\n"
        f"These figures track ALL bots you have ever deployed.\n"
        f"This data is **permanent** and will never be deleted.\n\n"
        f"**Total Bandwidth Served to Users:**\n"
        f"  `{stats['total_bandwidth_human']}`\n\n"
        f"**Total Video Streams Served:**\n"
        f"  `{stats['total_streams']:,}` streams"
    )

    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]
        )
    )


@main_bot.on_callback_query(filters.regex("^admin_settings$") & admin_only)
async def settings_callback(client, cb: CallbackQuery):
    await cb.answer()
    current_domain = await get_protected_domain()
    bw_info        = get_bandwidth_info()

    text = (
        f"**⚙️ Settings**\n\n"
        f"**Protected Domain:**\n"
        f"The bot only allows streaming from this Referer URL.\n\n"
        f"Current: `{current_domain}`\n\n"
        f"**Current Bot:** @{BOT_USERNAME or 'Unknown'}\n"
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
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_domain"})
    await cb.message.edit_text(
        "**✏️ Set New Domain**\n\n"
        "Send the new protected domain.\n\n"
        "Example: `https://keralacaptain.in` or `keralacaptain.in`",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_conv")]]
        )
    )


# ---- FEATURE 3: Manual Kill Switch ----

@main_bot.on_callback_query(filters.regex("^admin_kill_bot$") & admin_only)
async def kill_bot_callback(client, cb: CallbackQuery):
    """Shows the Kill Bot confirmation dialog."""
    await cb.answer()

    if IS_DEAD:
        await cb.message.edit_text(
            "🔴 **This bot is already in Sleep (Dead) Mode.**\n\n"
            "It is not serving any video streams.",
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
        f"The bot will **permanently stop serving video files.**\n"
        f"This is saved to the database and survives restarts.\n\n"
        f"You will need to deploy a new bot to continue.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Kill It",  callback_data="admin_kill_bot_confirm"),
                InlineKeyboardButton("❌ No, Cancel",    callback_data="admin_main_menu")
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_kill_bot_confirm$") & admin_only)
async def kill_bot_confirm_callback(client, cb: CallbackQuery):
    """Confirmed kill - triggers Dead Mode manually."""
    await cb.answer("Killing bot...")

    await cb.message.edit_text(
        "🔴 **Bot is now in Sleep (Dead) Mode.**\n\n"
        "All video streams have been stopped immediately.\n"
        "This state is saved to the database.\n\n"
        "Deploy a new bot on a new Render account to continue service."
    )

    # Trigger Dead Mode with reason="manual"
    await trigger_dead_mode(reason="manual")


# ---- Restart Handler (original + flush before restart) ----

@main_bot.on_callback_query(filters.regex("^admin_restart$") & admin_only)
async def restart_callback(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**⚠️ Are you sure?**\n\nThis will perform a full restart of the bot.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Restart", callback_data="admin_restart_confirm"),
                InlineKeyboardButton("❌ No, Go Back",  callback_data="admin_main_menu")
            ]
        ])
    )


@main_bot.on_callback_query(filters.regex("^admin_restart_confirm$") & admin_only)
async def restart_confirm_callback(client, cb: CallbackQuery):
    await cb.answer("Restarting...")
    await cb.message.edit_text("✅ **Restarting...**\n\nBot will be back online shortly.")

    try:
        LOGGER.info("RESTART triggered by admin.")
        # NEW: Flush bandwidth to DB before restarting so no data is lost
        await flush_bandwidth_to_db()
        LOGGER.info("Bandwidth flushed before restart.")
        if main_bot and main_bot.is_connected:
            await main_bot.stop()
    except Exception as e:
        LOGGER.error(f"Error during pre-restart cleanup: {e}")

    # Replace current process with a new instance
    os.execl(sys.executable, sys.executable, *sys.argv)


@main_bot.on_callback_query(filters.regex("^(admin_main_menu|admin_cancel_conv)$") & admin_only)
async def main_menu_callback(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)
    await cb.message.edit_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel.",
        reply_markup=_get_main_menu_markup()
    )


@main_bot.on_message(filters.private & filters.text & admin_only)
async def text_message_handler(client, message: Message):
    """Handles text input from admin during conversation flows (e.g., setting domain)."""
    chat_id = message.chat.id
    conv    = await get_user_conversation(chat_id)
    if not conv:
        return

    stage = conv.get("stage")

    if stage == "awaiting_domain":
        new_domain = message.text.strip()
        if "." not in new_domain or " " in new_domain:
            return await message.reply_text(
                "Invalid format. Please send a valid domain like `keralacaptain.in`."
            )
        try:
            status_msg   = await message.reply_text("Saving...")
            saved_domain = await set_protected_domain(new_domain)
            await status_msg.edit_text(
                f"✅ **Success!**\n\nProtected domain updated to:\n`{saved_domain}`",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_settings")]]
                )
            )
            await update_user_conversation(chat_id, None)
        except Exception as e:
            await status_msg.edit_text(f"❌ **Error!**\nCould not save domain: `{e}`")


# ================================================================================
# APPLICATION LIFECYCLE (ORIGINAL + NEW STARTUP TASKS)
# ================================================================================

async def ping_server():
    """Keeps the Render/Heroku server alive by pinging it periodically."""
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"Pinged server with status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Failed to ping server: {e}")


if __name__ == "__main__":

    async def main_startup_shutdown_logic():
        """Handles startup and runs the bot until interrupted."""
        global CURRENT_PROTECTED_DOMAIN, BOT_USERNAME

        LOGGER.info("Application starting up...")

        # Load protected domain from DB
        CURRENT_PROTECTED_DOMAIN = await get_protected_domain()
        LOGGER.info(f"Domain loaded: {CURRENT_PROTECTED_DOMAIN}")

        # Ensure DB indexes
        await media_collection.create_index("tmdb_id", unique=True)
        await media_collection.create_index("wp_post_id", unique=True)
        LOGGER.info("DB indexes ensured.")

        # Start main bot and get its username
        try:
            await main_bot.start()
            bot_info     = await main_bot.get_me()
            BOT_USERNAME = bot_info.username
            LOGGER.info(f"Main Bot @{BOT_USERNAME} started.")
        except FloodWait as e:
            LOGGER.error(f"FloodWait on main bot startup. Waiting {e.value}s.")
            await asyncio.sleep(e.value + 5)
            await main_bot.start()
            bot_info     = await main_bot.get_me()
            BOT_USERNAME = bot_info.username
            LOGGER.info(f"Main Bot @{BOT_USERNAME} started after wait.")
        except Exception as e:
            LOGGER.critical(f"Failed to start main bot: {e}", exc_info=True)
            raise

        # FEATURE 2: Load this bot's bandwidth state from MongoDB
        # (BOT_USERNAME must be set before calling this)
        await load_bandwidth_state()
        LOGGER.info(
            f"Bandwidth state: Used={humanbytes(_bandwidth_in_memory)}, "
            f"Dead={IS_DEAD}, WarningAlreadySent={_warning_85gb_sent}"
        )

        # FEATURE 1: Load any files cached in a previous run
        await load_existing_cache_on_startup()

        # Initialize multi-client streaming
        await initialize_clients()

        # FEATURE 1: Start the background cache cleanup task (runs every 25 min)
        asyncio.create_task(cache_cleanup_task())
        LOGGER.info("Cache cleanup background task started.")

        # Keep-alive ping for Heroku/Render
        if Config.ON_HEROKU:
            asyncio.create_task(ping_server())

        # Start web server
        web_app = await web_server()
        runner  = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"Web server started on port {Config.PORT}.")

        # Send startup notification to admin
        try:
            bw_info     = get_bandwidth_info()
            dead_notice = (
                "\n\n🔴 **WARNING: This bot is still in DEAD MODE from before the restart!**"
                if IS_DEAD else ""
            )
            await main_bot.send_message(
                Config.ADMIN_IDS[0],
                f"**✅ Bot @{BOT_USERNAME} is online!**\n\n"
                f"**Bandwidth Used:** `{bw_info['used_human']}`\n"
                f"**Disk Cached Files:** `{len(cache_registry)}`\n"
                f"**Status:** {'🔴 DEAD' if IS_DEAD else '🟢 Active'}"
                f"{dead_notice}"
            )
        except Exception as e:
            LOGGER.warning(f"Could not send startup message: {e}")

        # Wait forever (until SIGINT/SIGTERM)
        await asyncio.Event().wait()

    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"Received exit signal {sig.name}... shutting down gracefully.")

        # NEW: Flush bandwidth counter to DB before shutdown
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
        LOGGER.info("Application starting up...")
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except Exception as e:
        LOGGER.critical(f"A critical error forced the application to stop: {e}", exc_info=True)
    finally:
        LOGGER.info("Event loop stopped. Final cleanup.")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("Shutdown complete. Goodbye!")
