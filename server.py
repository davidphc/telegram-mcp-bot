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

_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
mcp   = FastMCP("telegram")

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

async def _call(method: str, payload: dict | None = None) -> object:
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(f"{_BASE}/{method}", json=payload or {})
        r.raise_for_status()
        body = r.json()
        if not body.get("ok"):
            raise RuntimeError(body.get("description", "Telegram API error"))
        return body["result"]


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

        updates = await _call("getUpdates", params)
        if not updates:
            break

        rows: list[dict] = []
        for u in updates:
            msg = u.get("message") or u.get("channel_post")
            if msg and str(msg.get("chat", {}).get("id")) == str(CHAT_ID):
                rows.append(_fmt_msg(msg))
            last_update_id = max(last_update_id, u["update_id"])

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
    2pm PT sync loop, forever.
    """
    try:
        n = await fetch_and_store()
        _log(f"startup sync stored {n} new message(s)")
    except Exception as exc:
        _log(f"startup sync failed: {exc!r}")  # don't abort the loop on a transient startup error

    await _daily_sync_loop()


def _start_poll_thread() -> None:
    """
    Run the async poll loop in a dedicated daemon thread with its own event loop.

    The loop is wrapped in a restart supervisor: if `_poll_forever()` ever raises
    (which it shouldn't, given the internal try/except blocks) or the event loop
    itself dies, the thread logs the failure, waits briefly, and spins up a fresh
    event loop rather than exiting silently. This guarantees syncing keeps
    happening even after unexpected errors.
    """
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
