# telegram-mcp

An MCP server that connects to a Telegram group chat via the Telegram Bot API, persists messages to a local SQLite database, and exposes them over Server-Sent Events (SSE). Designed to be deployed on Railway with a persistent volume so you can query the full 7-day history.

---

## How it works

```
Telegram Bot API
      │
      │  getUpdates (daily at midnight UTC + on startup)
      ▼
 SQLite DB  (/data/messages.db)
      │
      │  SQL queries
      ▼
 MCP tools  ──SSE──►  Claude / MCP client
```

- A **background thread** runs a daily fetch at midnight UTC. On startup it also fetches immediately to cover any gap since the last run.
- Each fetch drains the Telegram update queue and writes new messages to SQLite, deduplicating by `message_id`.
- The **MCP tools** (`get_recent_messages`, `search_messages`) query the local database — not Telegram — so they can return any time range within the stored history.

---

## Tools

| Tool | Parameters | Description |
|---|---|---|
| `get_recent_messages` | `limit=50` | Return the N most recent messages, oldest → newest |
| `search_messages` | `keyword`, `days=7`, `limit=200` | Full-text keyword search over the past N days |
| `send_message` | `text` | Post a message to the chat (also stored in DB) |
| `get_chat_info` | — | Chat metadata + DB stats (count, date range) |

> **History depth:** The database accumulates messages indefinitely. The Telegram Bot API's update queue only holds messages from the last 24 hours (max 100 per poll), so a daily fetch captures everything as long as the chat doesn't exceed ~100 messages per day. A startup fetch ensures no messages are missed across restarts.

---

## Step 1 — Create a Telegram bot and get your credentials

### Bot token

1. Open Telegram and message **@BotFather**.
2. Send `/newbot`, follow the prompts, and copy the token (e.g. `123456:ABCdef…`).
3. Add the bot to your group chat with at least **Read messages** and **Send messages** permissions (admin is easiest).

### Chat ID

1. Add the bot to the group and send any message in the group.
2. Open this URL in a browser (replace `<TOKEN>`):
   ```
   https://api.telegram.org/bot<TOKEN>/getUpdates
   ```
3. Find `"chat": {"id": ...}` in the response — that is your `CHAT_ID`.
   - Regular groups: negative integer like `-987654321`
   - Supergroups / channels: large negative integer like `-1001234567890`

> **Webhook conflict:** If you previously set a webhook on this bot, clear it or `getUpdates` will return a 409:
> ```
> https://api.telegram.org/bot<TOKEN>/deleteWebhook
> ```

---

## Step 2 — Deploy on Railway

### Option A — Deploy from GitHub (recommended)

1. Push this folder to a GitHub repository.
2. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**.
3. Select the repository. Railway detects the `Dockerfile` automatically.
4. Open the service's **Variables** tab and add:

   | Variable | Value |
   |---|---|
   | `BOT_TOKEN` | your bot token |
   | `CHAT_ID` | your chat ID (include the leading `-`) |

5. Click **Deploy**. Railway builds the image and starts the service.

6. **Create the persistent volume:**
   - In the Railway dashboard, open the service → **Volumes** tab → **Add Volume**.
   - Set the mount path to `/data`.
   - Railway will restart the service with the volume attached.
   - The `railway.toml` already includes `[[mounts]] mountPath = "/data"` which Railway respects after the volume is created.

7. Go to **Settings → Networking → Generate Domain** to get a public URL.

Your SSE endpoint:
```
https://<your-service>.railway.app/sse
```

### Option B — Railway CLI

```bash
npm install -g @railway/cli
railway login
railway init
railway up
railway variables set BOT_TOKEN=<token> CHAT_ID=<id>
railway volume create --mount /data
railway domain
```

---

## Step 3 — Connect your MCP client

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "telegram": {
      "url": "https://<your-service>.railway.app/sse"
    }
  }
}
```

Restart Claude Desktop. The four Telegram tools appear in the tool list.

### Claude Code (CLI / VS Code extension)

```bash
claude mcp add telegram --transport sse https://<your-service>.railway.app/sse
```

---

## Local development

```bash
pip install -r requirements.txt

export BOT_TOKEN=your_token
export CHAT_ID=your_chat_id
export DB_PATH=./messages.db   # override the /data default for local dev

python server.py
# Listens on http://localhost:8080/sse
```

Test with the MCP inspector:

```bash
npx @modelcontextprotocol/inspector http://localhost:8080/sse
```

---

## Security

- **No message content is logged to stdout** — the server emits no application-level logs containing message text.
- **No analytics or tracking** — the only outbound connection is to `api.telegram.org`.
- **Secrets are environment variables** — `BOT_TOKEN` and `CHAT_ID` are never written to disk or echoed.
- **Data at rest** — messages are stored in SQLite on the Railway volume, which is private to your project.
