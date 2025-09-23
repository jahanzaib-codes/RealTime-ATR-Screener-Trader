import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

import pytz

try:
    # Allow running as script from inside the app folder: `python main.py`
    if __package__ is None:
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
except Exception:
    pass

from app.bus import EventBus
from app.config import load_config
from app.logger_setup import setup_logging
from app.screener import PolygonScreener
from app.storage import append_opslog, append_summary
from app.telegram_bot import TelegramService
from app.trader import Trader


logger = logging.getLogger(__name__)


async def market_hours_guard(config, bus: EventBus):
    tz = pytz.timezone(config.timezone)
    while True:
        now = datetime.now(tz)
        # Market close 16:00 Eastern for NYSE/NASDAQ
        close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
        trigger_time = close_time - timedelta(minutes=config.flatten_minutes_before_close)
        if now >= trigger_time and now.weekday() < 5:
            await bus.publish("FORCE_FLATTEN_ALL", {"ts": now.isoformat()})
            await append_opslog({"level": "info", "component": "guard", "msg": "Flatten-before-close trigger"})
            await asyncio.sleep(60 * 10)
        await asyncio.sleep(30)


async def summary_loop(trader: Trader):
    while True:
        try:
            # refresh equity frequently to include accurate balance
            await trader._refresh_equity()
            open_positions = {
                sym: {"qty": d["qty"], "entry": d["entry"], "stop": d["stop"]}
                for sym, d in trader.open_positions.items()
            }
            await append_summary({
                "open_positions": open_positions,
                "equity": trader.account_equity_cache,
            })
        except Exception:
            pass
        await asyncio.sleep(60)


async def main():
    setup_logging()
    # Clear console for a clean real-time view (Windows/Unix)
    try:
        if os.name == "nt":  # Windows
            os.system("cls")
        else:  # Unix/Linux/Mac
            os.system("clear")
        print("\n" * 50)  # Extra clear space
    except Exception:
        pass
    config = load_config()
    logger.info("Starting app")
    bus = EventBus()

    screener = PolygonScreener(config, bus)
    trader = Trader(config, bus)
    bot = TelegramService(config, bus)

    # Add force scan event handler
    async def handle_force_scan():
        async for event in bus.subscribe():
            if event.type == "FORCE_SCAN":
                logger.info("🔄 Force scan requested via Telegram")
                await screener.force_scan()

    # 1) Ensure IBKR connectivity
    try:
        await trader._connect_ib()
        await trader._refresh_equity()
        logger.info("IBKR connection OK")
    except Exception:
        logger.error("IBKR not connected. Exiting.")
        print("[ERROR] IBKR not connected. Check IB Gateway/TWS and config.")
        return

    # 2) Start Telegram bot
    bot_task = asyncio.create_task(bot.run(), name="bot")
    logger.info("Telegram bot started ✅")
    print("[OK] ✅ Telegram bot started")
    
    # Show current filter settings
    f = config.screener
    print(f"\n🔍 Screener Filters:")
    print(f"💰 Price: ${f.price_min:.2f} - ${f.price_max:.2f}")
    print(f"📈 Change: {f.pct_change_min:.2f}% - {f.pct_change_max:.2f}%")
    print(f"📊 Volume: {f.volume_min:,} - {f.volume_max:,}")
    print(f"🏢 Market Cap: ${f.market_cap_min:,.0f} - ${f.market_cap_max:,.0f}")
    print("⚠️  These filters are very restrictive! Most stocks won't pass.")
    print("💡 Use Telegram '⚙️ Filter Settings' to see details.\n")

    # 3) Check Polygon REST availability, then start screener/trader
    rest_ok = await screener.check_rest_available()
    if not rest_ok:
        logger.error("Polygon REST not available ❌")
        print("[ERROR] ❌ Polygon API not available. Screener will not start.")
        tasks = [
            bot_task,
            asyncio.create_task(market_hours_guard(config, bus), name="guard"),
            asyncio.create_task(summary_loop(trader), name="summary"),
        ]
    else:
        print("[OK] 📡 Polygon API available")
        tasks = [
            bot_task,
            asyncio.create_task(screener.run(), name="screener"),
            asyncio.create_task(trader.run(), name="trader"),
            asyncio.create_task(market_hours_guard(config, bus), name="guard"),
            asyncio.create_task(summary_loop(trader), name="summary"),
            asyncio.create_task(handle_force_scan(), name="force_scan"),
        ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        await append_opslog({"level": "fatal", "component": "main", "msg": str(e)})
    finally:
        await screener.stop()
        await trader.stop()
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())


