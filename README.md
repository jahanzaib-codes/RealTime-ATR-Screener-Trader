## NASDAQ Screener + ATR Trading Bot (Polygon.io + IBKR + Telegram)

Real-time NASDAQ screener with automatic ATR-based entries and trailing stops. Streams live ticks from Polygon.io, screens by price/change/volume/market cap, auto-buys via IB Gateway (paper/live), manages stops with chandelier logic, and provides a Telegram bot UI with operations logs and summaries. Stores daily JSON for trades/summary/ops.

### Features
- **Polygon.io WebSocket** for real-time ticks and **REST** for snapshots/aggregates
- **Screener filters**:
  - Price: 0 → 5 USD
  - % Change Today: 0.00% → 218.92%
  - Volume: 2,000,000 → 513,960,000
  - Market Cap: 0 → 3,207.06B USD
- **Auto-entry** when a symbol first enters the filter (idempotent per day)
- **ATR-based** initial stop + **chandelier trailing stop** (stop never decreases)
- Flatten all positions N minutes before market close (default 10)
- Telegram bot with reply keyboard:
  - 🧾 Portfolio Status, 📈 Open Positions, 📤 Closed Trades, 🌀 Stocks List (Filter)
  - 🔍 Stop Status, 📄 Ops Log, 🔄 Force Scan, 📊 Summary, 🟢 Live/🟡 Dry-Run, ❌ Close All
- **Alerts**: Buy, Stop Update, Sell (reason, PnL), Errors
- Daily JSON in `data/` and rotating logs in `logs/`
- DRY-RUN and LIVE mode toggle via Telegram

### File Structure
```
project_root/
  app/
    main.py
    config.py
    logger_setup.py
    storage.py
    bus.py
    risk.py
    screener.py
    trader.py
    telegram_bot.py
  data/
  logs/
  .env
  requirements.txt
  README.md
```

### Requirements
- Python 3.11+
- IB Gateway/TWS running with API enabled

Install deps:
```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Configuration (.env)
Create `.env` in project root:
```
POLYGON_API_KEY=your_polygon_key
POLYGON_WS_URL=wss://socket.polygon.io/stocks
POLYGON_REST_URL=https://api.polygon.io
POLYGON_SUBSCRIBE_CHANNEL=A.*

IB_HOST=127.0.0.1
IB_PORT=7497
IB_CLIENT_ID=7

TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=123456789

ATR_TIMEFRAME=5m
ATR_PERIOD=14
ATR_MULTIPLIER=2.0

FILTER_PRICE_MIN=0
FILTER_PRICE_MAX=5
FILTER_PCT_MIN=0
FILTER_PCT_MAX=218.92
FILTER_VOL_MIN=2000000
FILTER_VOL_MAX=513960000
FILTER_MKT_CAP_MIN=0
FILTER_MKT_CAP_MAX=3207060000000

TIMEZONE=US/Eastern
FLATTEN_MIN_BEFORE_CLOSE=10
DAILY_EQUITY_RISK_PCT=0.01
DRY_RUN=true
```

#### Polygon subscription modes

- `POLYGON_SUBSCRIBE_CHANNEL=A.*` (default): Subscribe to aggregate-minute events. Light-weight and typically allowed on more plans. Symbols are discovered from aggregates; prices for trading decisions are pulled from REST/yfinance.
- `POLYGON_SUBSCRIBE_CHANNEL=T.*`: Subscribe to all trade ticks. Heavier and may require a paid Polygon plan. Enables real-time `PRICE_UPDATE` events from trades.
- `POLYGON_SUBSCRIBE_CHANNEL=auto`: The app will authenticate and briefly probe `T.AAPL`. If trade ticks are received, it selects `T.*`; otherwise it falls back to `A.*`. The choice and status are logged to `logs/app.log` and `data/opslog_YYYY-MM-DD.json`.
- `POLYGON_SUBSCRIBE_CHANNEL=auto_ibkr`: The app will connect to IBKR and briefly request market data for AAPL to infer IBKR's marketDataType. If IBKR returns real-time/frozen real-time (1/2), it selects `T.*`; if delayed/delayed-frozen (3/4) or unknown, it selects `A.*`.

Notes:

- If your IBKR market data is limited (e.g., delayed), using `A.*`, `auto`, or `auto_ibkr` is recommended to reduce WS load.
- You can also test with a single symbol: `T.AAPL` or `A.AAPL` to verify access before widening.

### Usage

Start the app:

```bash
python -m app.main
```
Once running, the bot sends a start-up message to your Telegram chat. Use the reply keyboard buttons to query status, force scans, toggle modes, and close positions.

### Telegram Bot Setup
1. Use @BotFather to create a bot and get the token.
2. DM your bot, then retrieve your chat ID (e.g., using a simple echo bot or web utils) and put it in `.env`.
3. Ensure the bot can message your chat (start a conversation first).

### DRY-RUN vs LIVE
- Start in DRY-RUN (`DRY_RUN=true`). Toggle to LIVE from Telegram (🟢 Live / 🟡 Dry-Run).
- In DRY-RUN, orders are simulated and recorded in JSON/logs without sending to IBKR.

### Data & Logs
- Daily JSON in `data/`:
  - `trades_YYYY-MM-DD.json`
  - `summary_YYYY-MM-DD.json`
  - `opslog_YYYY-MM-DD.json`
- Rotating logs in `logs/` for app and errors

### Troubleshootings
- **Polygon WebSocket disconnects**:
  - The screener auto-reconnects with backoff. Ensure `POLYGON_API_KEY` is valid and network is stable.
  - Check auth/status lines in `logs/app.log`. On failures you’ll see entries like `[WS STATUS] {"status":"error", ...}` and `WS closed ... code=...`.
  - Try `POLYGON_SUBSCRIBE_CHANNEL=A.*` or `A.AAPL` if `T.*` is not permitted on your plan.
- **IBKR API connection refused**:
  - Verify IB Gateway/TWS is running, API enabled, and `IB_HOST/PORT/CLIENT_ID` match. Check API permissions.
- **Telegram bot not responding**:
  - Confirm `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Ensure the bot can message you (start chat). Network/firewall must allow Telegram.
- **JSON file permission errors**:
  - Ensure the process can write to `data/` and `logs/`. On Windows, run terminal with sufficient permissions. Avoid locking files by opening them elsewhere.
- **Timezone mismatch for market close**:
  - Set `TIMEZONE` to your desired IANA tz (e.g., `US/Eastern`). The guard triggers N minutes before 16:00 local market close.

### Notes
- Assumes IB Gateway/TWS is already running and API enabled.
- For production, run under a process supervisor (e.g., systemd, PM2, NSSM on Windows) and set robust environment secrets.


