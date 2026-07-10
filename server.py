import asyncio
import os
import sqlite3
from datetime import datetime, timedelta, time as dtime, timezone

import httpx
from fastmcp import FastMCP

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


DAILY_RUN_TIME_UTC = dtime(21, 0)  # 2pm PT == 9pm UTC


def _next_run_at(now: datetime) -> datetime:
    """Return the next occurrence of DAILY_RUN_TIME_UTC (today if still ahead, else tomorrow)."""
    candidate = datetime.combine(now.date(), DAILY_RUN_TIME_UTC, tzinfo=timezone.utc)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


async def _poll_forever() -> None:
    """
    Fetch on startup (covers any gap since the last run), then sleep until the
    next 2pm PT (9pm UTC) and repeat — one fetch per day.

    This coroutine is scheduled as an asyncio task owned by the server's event
    loop (see `main()`), so it runs for the lifetime of the process rather than
    a daemon thread that can silently die independently of the server.
    """
    try:
        await fetch_and_store()
    except Exception:
        pass  # don't abort the loop on a transient startup error

    while True:
        now = datetime.now(timezone.utc)
        next_run = _next_run_at(now)
        await asyncio.sleep((next_run - now).total_seconds())

        try:
            await fetch_and_store()
        except Exception:
            pass  # keep running even if Telegram is briefly unavailable

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

async def main() -> None:
    init_db()
    port = int(os.environ.get("PORT", 8080))

    # Schedule the daily poll loop as an asyncio task owned by this event loop,
    # rather than a daemon thread. Daemon threads are killed the instant the
    # process is torn down and are not tied into the server's own lifecycle,
    # which meant a restart (or a subtly-crashed thread) could silently stop
    # polling forever. A task scheduled on the same loop that drives the MCP
    # server runs for as long as the server itself runs, and is restarted
    # along with it on every deploy — so there is no separate lifecycle to
    # fall out of sync.
    asyncio.create_task(_poll_forever(), name="telegram-poll-task")

    await mcp.run_async(transport="sse", host="0.0.0.0", port=port)


if __name__ == "__main__":
    asyncio.run(main())
