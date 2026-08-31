import asyncio
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, time as dtime, timezone

import httpx
from fastmcp import FastMCP

try:
    from zoneinfo import ZoneInfo
    _PT_TZ = ZoneInfo("America/Los_Angeles")
except Exception:
    # Fallback if the zoneinfo/tzdata database isn't available on this system.
    # This approximates Pacific Daylight Time (UTC-7) and won't auto-adjust
    # for standard time (UTC-8), but keeps the daily sync roughly on schedule.
    _PT_TZ = timezone(timedelta(hours=-7))

_DAILY_SYNC_HOUR_PT = 14  # 2pm PT
_MICRO_SYNC_INTERVAL_SECONDS = 30 * 60  # every 30 minutes
_THREAD_RESTART_DELAY_SECONDS = 30


def _log(msg: str) -> None:
    print(f"[poll] {msg}", file=sys.stderr, flush=True)

# ── Config ─────────────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID   = os.environ.get("CHAT_ID", "")
DB_PATH   = os.environ.get("DB_PATH", "/data/messages.db")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")
if not CHAT_ID:
    raise RuntimeError("CHAT_ID environment variable is required")

try:
    _CHAT_ID_INT = int(CHAT_ID)
except ValueError as exc:
    raise RuntimeError(f"CHAT_ID must be a valid integer (got {CHAT_ID!r})") from exc

_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
mcp   = FastMCP("telegram")


def _mask_token(token: str) -> str:
    return f"...{token[-8:]}" if len(token) >= 8 else "***"


def _log_startup_debug_info() -> None:
    db_exists = os.path.exists(DB_PATH)
    _log(
        f"startup config: BOT_TOKEN={_mask_token(BOT_TOKEN)} "
        f"CHAT_ID(parsed)={_CHAT_ID_INT} DB_PATH={DB_PATH} (exists={db_exists})"
    )

# ── Database ───────────────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _connect()
    conn.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 5000;

        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY,
            sender     TEXT    NOT NULL,
            text       TEXT    NOT NULL,
            date       TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS state (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages (date);
    """)
    conn.commit()
    conn.close()


def _get_state(key: str, default: str = "0") -> str:
    conn = _connect()
    row = conn.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def _set_state(key: str, value: str) -> None:
    conn = _connect()
    conn.execute(
        "INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()
    conn.close()


def _store_messages(rows: list[dict]) -> None:
    if not rows:
        return
    conn = _connect()
    conn.executemany(
        "INSERT OR IGNORE INTO messages (message_id, sender, text, date) VALUES (?, ?, ?, ?)",
        [(r["message_id"], r["sender"], r["text"], r["date"]) for r in rows],
    )
    conn.commit()
    conn.close()

# ── Telegram helpers ───────────────────────────────────────────────────────────

_CONFLICT_MAX_RETRIES = 3
_CONFLICT_BACKOFF_START_SECONDS = 1
_CONFLICT_BACKOFF_MAX_SECONDS = 10


async def _call(method: str, payload: dict | None = None) -> object:
    """
    Call a Telegram Bot API method.

    getUpdates in particular can return HTTP 409 Conflict if another process
    (or a leftover overlapping request) is polling the same bot token
    concurrently. Rather than propagating that error and aborting the whole
    sync, retry with exponential backoff. If retries are exhausted, raise so
    the caller can decide how to proceed (fetch_and_store treats this as
    non-fatal and keeps whatever it already fetched).
    """
    backoff = _CONFLICT_BACKOFF_START_SECONDS
    last_exc: Exception | None = None

    for attempt in range(1, _CONFLICT_MAX_RETRIES + 2):  # initial try + retries
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.post(f"{_BASE}/{method}", json=payload or {})
                r.raise_for_status()
                body = r.json()
                if not body.get("ok"):
                    raise RuntimeError(body.get("description", "Telegram API error"))
                return body["result"]
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 409:
                last_exc = exc
                if attempt > _CONFLICT_MAX_RETRIES:
                    _log(
                        f"{method}: giving up after {_CONFLICT_MAX_RETRIES} retries "
                        f"on 409 Conflict: {exc!r}"
                    )
                    raise
                _log(
                    f"{method}: got 409 Conflict (attempt {attempt}/{_CONFLICT_MAX_RETRIES}), "
                    f"retrying in {backoff}s"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _CONFLICT_BACKOFF_MAX_SECONDS)
                continue
            raise

    # Should be unreachable, but keep mypy/linters happy and avoid silent None return.
    if last_exc:
        raise last_exc
    raise RuntimeError(f"{method}: exhausted retries with no response")


def _fmt_msg(msg: dict) -> dict:
    sender = msg.get("from") or {}
    name = " ".join(filter(None, [sender.get("first_name"), sender.get("last_name")]))
    return {
        "message_id": msg["message_id"],
        "sender": name or sender.get("username") or "Unknown",
        "text": msg.get("text") or msg.get("caption") or "",
        "date": datetime.fromtimestamp(msg["date"], tz=timezone.utc).isoformat(),
    }

# ── Fetch & persist ────────────────────────────────────────────────────────────

async def fetch_and_store() -> int:
    """
    Drain the Telegram update queue into the database.
    Passes the last seen update_id as offset so Telegram advances the queue pointer.
    Loops until fewer than 100 updates are returned (queue exhausted).
    Returns the number of messages stored this run.

    getUpdates 409 Conflicts are retried internally by `_call()`. If retries
    are exhausted on a given iteration, we stop draining further but keep
    (and persist) whatever was already fetched in this run instead of
    propagating the exception and losing progress.
    """
    last_update_id = int(_get_state("last_update_id", "0"))
    total = 0

    while True:
        params: dict = {
            "limit": 100,
            "allowed_updates": ["message", "channel_post"],
        }
        if last_update_id > 0:
            params["offset"] = last_update_id + 1

        try:
            updates = await _call("getUpdates", params)
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 409:
                _log(
                    "getUpdates: exhausted 409 Conflict retries for this iteration; "
                    f"stopping this sync run with {total} message(s) stored so far "
                    "(will retry on the next scheduled sync)"
                )
                break
            raise

        _log(f"getUpdates returned {len(updates)} update(s)")

        if not updates:
            break

        rows: list[dict] = []
        for u in updates:
            msg = u.get("message") or u.get("channel_post")
            update_id = u.get("update_id")

            if not msg:
                _log(f"update_id={update_id}: no message/channel_post payload, skipping")
                last_update_id = max(last_update_id, update_id)
                continue

            actual_chat_id = msg.get("chat", {}).get("id")
            sender = msg.get("from") or {}
            sender_name = " ".join(
                filter(None, [sender.get("first_name"), sender.get("last_name")])
            ) or sender.get("username") or "Unknown"
            passed_filter = str(actual_chat_id) == str(CHAT_ID)

            _log(
                f"update_id={update_id} chat_id={actual_chat_id} sender={sender_name!r} "
                f"passed_chat_id_filter={passed_filter}"
            )

            if passed_filter:
                rows.append(_fmt_msg(msg))
            last_update_id = max(last_update_id, update_id)

        _store_messages(rows)
        total += len(rows)
        _set_state("last_update_id", str(last_update_id))

        if len(updates) < 100:
            break

    return total


def _next_daily_sync_at(now_utc: datetime) -> datetime:
    """
    Return the next 2pm Pacific Time occurrence (converted to UTC) after `now_utc`.
    Handles PDT/PST transitions automatically when zoneinfo/tzdata is available.
    """
    now_pt = now_utc.astimezone(_PT_TZ)
    candidate_pt = now_pt.replace(
        hour=_DAILY_SYNC_HOUR_PT, minute=0, second=0, microsecond=0
    )
    if candidate_pt <= now_pt:
        candidate_pt += timedelta(days=1)
    return candidate_pt.astimezone(timezone.utc)


async def _micro_sync_loop() -> None:
    """
    Every _MICRO_SYNC_INTERVAL_SECONDS, attempt a quick incremental fetch so
    messages sent between the daily syncs still show up promptly. Failures
    here are logged but never propagate — this loop must not die.
    """
    while True:
        await asyncio.sleep(_MICRO_SYNC_INTERVAL_SECONDS)
        try:
            n = await fetch_and_store()
            if n:
                _log(f"micro-sync stored {n} new message(s)")
        except Exception as exc:
            _log(f"micro-sync failed (will retry in {_MICRO_SYNC_INTERVAL_SECONDS}s): {exc!r}")


async def _daily_sync_loop() -> None:
    """
    Sleep until the next 2pm PT (9pm UTC during PDT) and run a full sync,
    then repeat forever. Errors are logged and never stop the loop.
    """
    while True:
        now = datetime.now(timezone.utc)
        next_sync = _next_daily_sync_at(now)
        sleep_seconds = max((next_sync - now).total_seconds(), 0)
        _log(f"next daily sync scheduled for {next_sync.isoformat()} ({sleep_seconds:.0f}s from now)")
        await asyncio.sleep(sleep_seconds)

        try:
            n = await fetch_and_store()
            _log(f"daily sync stored {n} new message(s)")
        except Exception as exc:
            _log(f"daily sync failed: {exc!r}")


async def _poll_forever() -> None:
    """
    Fetch on startup (covers any gap since the last run), then run the daily
    2pm PT sync and the 30-minute micro-sync concurrently, forever.
    """
    try:
        n = await fetch_and_store()
        _log(f"startup sync stored {n} new message(s)")
    except Exception as exc:
        _log(f"startup sync failed: {exc!r}")  # don't abort the loop on a transient startup error

    await asyncio.gather(_daily_sync_loop(), _micro_sync_loop())


def _start_poll_thread() -> None:
    """
    Run the async poll loop in a dedicated daemon thread with its own event loop.

    The loop is wrapped in a restart supervisor: if `_poll_forever()` ever raises
    (which it shouldn't, given the internal try/except blocks) or the event loop
    itself dies, the thread logs the failure, waits briefly, and spins up a fresh
    event loop rather than exiting silently. This guarantees syncing keeps
    happening even after unexpected errors.
    """
    _log(f"CHAT_ID parsed successfully as int: {_CHAT_ID_INT}")
    _log_startup_debug_info()

    def runner() -> None:
        while True:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(_poll_forever())
                finally:
                    loop.close()
            except Exception as exc:
                _log(f"poll thread crashed, restarting in {_THREAD_RESTART_DELAY_SECONDS}s: {exc!r}")
                time.sleep(_THREAD_RESTART_DELAY_SECONDS)
            else:
                # _poll_forever() should never return normally (it's an infinite
                # loop), but if it somehow does, restart rather than exit.
                _log(f"poll loop exited unexpectedly, restarting in {_THREAD_RESTART_DELAY_SECONDS}s")
                time.sleep(_THREAD_RESTART_DELAY_SECONDS)

    threading.Thread(target=runner, daemon=True, name="poll-thread").start()

# ── MCP tools ──────────────────────────────────────────────────────────────────

@mcp.tool()
def get_recent_messages(limit: int = 50) -> list[dict]:
    """
    Return up to `limit` of the most recent messages from the local database,
    ordered oldest → newest.  Pull from the last 24 h by default; increase
    `limit` to get more history (the database holds up to 7 days).
    """
    conn = _connect()
    rows = conn.execute(
        "SELECT message_id, sender, text, date FROM messages ORDER BY date DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


@mcp.tool()
def search_messages(keyword: str, days: int = 7, limit: int = 200) -> list[dict]:
    """
    Search messages from the past `days` days (default 7) for `keyword`
    (case-insensitive).  Returns up to `limit` results ordered oldest → newest.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn = _connect()
    rows = conn.execute(
        """
        SELECT message_id, sender, text, date
          FROM messages
         WHERE date >= ? AND lower(text) LIKE lower(?)
         ORDER BY date ASC
         LIMIT ?
        """,
        (since, f"%{keyword}%", limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@mcp.tool()
async def sync_now() -> dict:
    """
    Trigger an immediate sync with Telegram instead of waiting for the next
    scheduled poll. Returns the number of new messages stored.
    """
    try:
        n = await fetch_and_store()
        _log(f"manual sync_now stored {n} new message(s)")
        return {"status": "ok", "new_messages": n}
    except Exception as exc:
        _log(f"manual sync_now failed: {exc!r}")
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def send_message(text: str) -> dict:
    """Send a plain-text message to the configured Telegram chat."""
    result = await _call("sendMessage", {
        "chat_id": int(CHAT_ID),
        "text": text,
    })
    sent: dict = {
        "message_id": result["message_id"],
        "sender": "bot",
        "text": text,
        "date": datetime.fromtimestamp(result["date"], tz=timezone.utc).isoformat(),
    }
    _store_messages([sent])
    return {"message_id": sent["message_id"], "date": sent["date"], "status": "sent"}


@mcp.tool()
async def get_chat_info() -> dict:
    """
    Return metadata about the configured Telegram chat plus local database stats
    (total messages stored, date range).
    """
    chat = await _call("getChat", {"chat_id": int(CHAT_ID)})
    info: dict = {
        "id":          chat["id"],
        "type":        chat["type"],
        "title":       chat.get("title"),
        "username":    chat.get("username"),
        "description": chat.get("description"),
    }
    try:
        info["member_count"] = await _call(
            "getChatMemberCount", {"chat_id": int(CHAT_ID)}
        )
    except Exception:
        pass

    conn = _connect()
    row = conn.execute(
        "SELECT COUNT(*) AS n, MIN(date) AS oldest, MAX(date) AS newest FROM messages"
    ).fetchone()
    conn.close()
    info["db_message_count"] = row["n"]
    if row["oldest"]:
        info["db_oldest_message"] = row["oldest"]
        info["db_newest_message"] = row["newest"]

    return {k: v for k, v in info.items() if v is not None}


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    _start_poll_thread()
    port = int(os.environ.get("PORT", 8080))
    mcp.run(transport="sse", host="0.0.0.0", port=port)
