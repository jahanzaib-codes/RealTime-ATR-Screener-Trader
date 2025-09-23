from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Deque, Dict, Optional, List
from collections import deque

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup, FSInputFile
from pathlib import Path

from .bus import EventBus
from .config import AppConfig
from .storage import append_opslog, read_summary, read_trades, DATA_DIR


logger = logging.getLogger(__name__)


class TelegramService:
    def __init__(self, config: AppConfig, bus: EventBus) -> None:
        self.config = config
        self.bus = bus
        self.bot = Bot(token=self.config.telegram.token)
        self.dp = Dispatcher()
        self.router = Router()
        self.live_mode = not self.config.dry_run
        self._stop = asyncio.Event()
        self.recent_screener: Deque[Dict] = deque(maxlen=50)

        self.router.message.register(self.cmd_start, Command(commands=["start"]))
        self.router.message.register(self.on_portfolio, F.text == "🧾 Portfolio Status")
        self.router.message.register(self.on_open_positions, F.text == "📈 Open Positions")
        self.router.message.register(self.on_closed_trades, F.text == "📤 Closed Trades")
        self.router.message.register(self.on_stocks_list, F.text == "🌀 Stocks List (Filter)")
        self.router.message.register(self.on_stop_status, F.text == "🔍 Stop Status")
        self.router.message.register(self.on_ops_log, F.text == "📄 Ops Log")
        self.router.message.register(self.on_force_scan, F.text == "🔄 Force Scan")
        self.router.message.register(self.on_summary, F.text == "📊 Summary")
        self.router.message.register(self.on_mode_toggle, F.text.in_({"🟢 Live", "🟡 Dry-Run"}))
        self.router.message.register(self.on_close_all, F.text == "❌ Close All")
        self.router.message.register(self.on_filter_settings, F.text == "⚙️ Filter Settings")

        self.dp.include_router(self.router)

    @property
    def keyboard(self) -> ReplyKeyboardMarkup:
        rows = [
            [KeyboardButton(text="🧾 Portfolio Status"), KeyboardButton(text="📈 Open Positions")],
            [KeyboardButton(text="📤 Closed Trades"), KeyboardButton(text="🌀 Stocks List (Filter)")],
            [KeyboardButton(text="🔍 Stop Status"), KeyboardButton(text="📄 Ops Log")],
            [KeyboardButton(text="🔄 Force Scan"), KeyboardButton(text="📊 Summary")],
            [KeyboardButton(text="⚙️ Filter Settings"), KeyboardButton(text="🟢 Live" if self.live_mode else "🟡 Dry-Run")],
            [KeyboardButton(text="❌ Close All")],
        ]
        return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

    async def stop(self) -> None:
        self._stop.set()
        await self.bot.session.close()

    async def send_alert(self, msg: str) -> None:
        try:
            # Add clear formatting and timestamp
            formatted_msg = f"🔔 {datetime.now().strftime('%H:%M:%S')}\n\n{msg}"
            await self.bot.send_message(chat_id=self.config.telegram.chat_id, text=formatted_msg)
        except Exception as e:
            logger.warning("telegram send failed: %s", e)

    async def cmd_start(self, message: Message) -> None:
        mode_text = "🟢 Live" if self.live_mode else "🟡 Dry-Run"
        f = self.config.screener
        intro = "\n".join([
            "🤖 Bot ready",
            "",
            "NASDAQ 0–5 USD Screener + ATR Trading (IBKR Execution)",
            f"Mode: {mode_text}",
            "",
            "Filters:",
            f"• Price: ${f.price_min:.2f} → ${f.price_max:.2f}",
            f"• Change: {f.pct_change_min:.2f}% → {f.pct_change_max:.2f}%",
            f"• Volume: {f.volume_min:,} → {f.volume_max:,}",
            f"• MktCap: ${f.market_cap_min:,.0f} → ${f.market_cap_max:,.0f}",
            "",
            "Controls: use the keyboard below to query status, force scans, toggle mode, and close all.",
        ])
        await message.answer(intro, reply_markup=self.keyboard)

    async def on_portfolio(self, message: Message) -> None:
        summary = await read_summary()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not summary:
            await message.answer(
                "\n".join([
                    "💼 Account Status",
                    "📊 Net Assets: N/A",
                    "💵 Available Cash: N/A",
                    "📈 Unrealized P&L: $0.00",
                    "💰 Realized P&L: $0.00",
                    f"⏱️ Current Time: {now}",
                ]),
                reply_markup=self.keyboard,
            )
            return
        last = summary[-1]
        equity = last.get("equity")
        open_positions = last.get("open_positions", {})
        # Unknown fields default to 0.00 until IBKR enrichment is added
        net_assets = f"${equity:,.2f}" if equity is not None else "N/A"
        available_cash = "N/A"
        unreal = 0.0
        realized = 0.0
        lines: List[str] = [
            "💼 Account Status",
            f"📊 Net Assets: {net_assets}",
            f"💵 Available Cash: {available_cash}",
            f"📈 Unrealized P&L: ${unreal:.2f}",
            f"💰 Realized P&L: ${realized:.2f}",
            f"⏱️ Current Time: {now}",
        ]
        if open_positions:
            lines.append("")
            lines.append("📈 Open Positions")
            for sym, d in open_positions.items():
                lines.append(f"• {sym}: qty={d['qty']} @ {d['entry']} | Stop {d['stop']}")
        await message.answer("\n".join(lines), reply_markup=self.keyboard)

    async def on_open_positions(self, message: Message) -> None:
        summary = await read_summary()
        if not summary or not summary[-1].get("open_positions"):
            await message.answer("📭 No open positions.", reply_markup=self.keyboard)
            return
        open_positions = summary[-1]["open_positions"]
        symbols = list(open_positions.keys())
        quotes: Dict[str, Dict] = {}
        # Fetch live quotes from Yahoo Finance free endpoint
        try:
            import aiohttp  # defer import
            url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ",".join(symbols)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    js = await resp.json()
                    for q in js.get("quoteResponse", {}).get("result", []):
                        sym = q.get("symbol")
                        if not sym:
                            continue
                        quotes[sym] = {
                            "last": float(q.get("regularMarketPrice") or 0.0),
                            "chg_pct": float(q.get("regularMarketChangePercent") or 0.0),
                        }
        except Exception:
            pass

        lines: List[str] = ["📈 Open Positions"]
        total_unreal_usd = 0.0
        for sym, d in open_positions.items():
            qty = float(d.get("qty", 0))
            entry = float(d.get("entry", 0))
            stop = float(d.get("stop", 0))
            last = quotes.get(sym, {}).get("last", entry)
            unreal_pct = ((last - entry) / entry * 100.0) if entry else 0.0
            unreal_usd = (last - entry) * qty
            total_unreal_usd += unreal_usd
            lines.append(
                f"• {sym}: {int(qty)} @ {entry:.4f} | Px {last:.4f} | Stop {stop:.4f} | PnL {unreal_pct:+.2f}% (${unreal_usd:+.2f})"
            )
        lines.append("")
        lines.append(f"Σ Unrealized PnL: ${total_unreal_usd:+.2f}")
        await message.answer("\n".join(lines), reply_markup=self.keyboard)

    async def on_closed_trades(self, message: Message) -> None:
        trades = await read_trades()
        closed = [t for t in trades if t.get("exit_price")]
        if not closed:
            await message.answer("No closed trades today.")
            return
        lines = [
            f"{t['symbol']} qty={t['quantity']} entry={t['entry_price']} exit={t['exit_price']} pnl={t.get('pnl', 0)}"
            for t in closed[-20:]
        ]
        await message.answer("Closed Trades (last 20):\n" + "\n".join(lines))

    async def on_stocks_list(self, message: Message) -> None:
        # Render recent screener matches captured from alerts
        if not self.recent_screener:
            await message.answer("🌀 No recent screener matches.", reply_markup=self.keyboard)
            return
        # Sort by % change descending
        items = sorted(list(self.recent_screener), key=lambda x: float(x.get("todays_change_perc", 0.0)), reverse=True)[:10]
        blocks: List[str] = []
        for p in items:
            price = float(p.get("price", 0.0))
            t1 = price * 1.10
            t2 = price * 1.25
            stop = max(0.0, price * 0.85)
            blocks.append("\n".join([
                f"⚡ {p['symbol']}",
                f"💰 Price: ${price:.2f}",
                f"🎯 Target 1: ${t1:.2f}",
                f"🌟 Target 2: ${t2:.2f}",
                f"⚠ Stop: ${stop:.2f}",
                f"📈 Change: {float(p.get('todays_change_perc', 0.0)):.2f}%",
            ]))
        await message.answer("\n\n".join(blocks), reply_markup=self.keyboard)

    async def on_stop_status(self, message: Message) -> None:
        await self.bus.publish("BOT_QUERY_STOPS", {"reply_to": message.chat.id})
        await message.answer("🔍 Requesting stop status...", reply_markup=self.keyboard)

    async def on_ops_log(self, message: Message) -> None:
        # Send today's JSON files if present
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            base = Path(DATA_DIR)
            candidates = [
                base / f"opslog_{today}.json",
                base / f"trades_{today}.json",
                base / f"summary_{today}.json",
                base / f"screener_{today}.json",
            ]
            sent_any = False
            for path in candidates:
                if path.exists() and path.is_file():
                    try:
                        await self.bot.send_document(
                            chat_id=self.config.telegram.chat_id,
                            document=FSInputFile(str(path)),
                            caption=path.name,
                        )
                        sent_any = True
                    except Exception:
                        pass
            if not sent_any:
                await message.answer("📭 No logs yet for today.", reply_markup=self.keyboard)
        except Exception:
            await message.answer("⚠️ Failed to fetch logs.", reply_markup=self.keyboard)

    async def on_force_scan(self, message: Message) -> None:
        await self.bus.publish("FORCE_SCAN", {"by": message.chat.id})
        await message.answer("🔄 Triggered scan.")

    async def on_summary(self, message: Message) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        trades = await read_trades()
        successful = len([t for t in trades if t.get("exit_price") and float(t.get("pnl", 0.0)) > 0])
        failed = len([t for t in trades if t.get("exit_price") and float(t.get("pnl", 0.0)) <= 0])
        lines = [
            "Summery",
            "",
            f"📊 Performance report for {today}",
            "",
            "📈 Strongest Stocks",
            f"✅ Successful: {successful if successful else 0}",
            f"❌ Failed: {failed if failed else 0}",
            "💣 Explosive Stocks",
            f"✅ Successful: {successful if successful else 0}",
            f"❌ Failed: {failed if failed else 0}",
            "🚀 High Movement Stocks",
            f"✅ Successful: {successful if successful else 0}",
            f"❌ Failed: {failed if failed else 0}",
            "",
            f"📈 Total successful trades: {successful}",
            f"📉 Total failed trades: {failed}",
        ]
        await message.answer("\n".join(lines), reply_markup=self.keyboard)

    async def on_mode_toggle(self, message: Message) -> None:
        self.live_mode = not self.live_mode
        await append_opslog({"level": "info", "component": "bot", "msg": f"MODE {'LIVE' if self.live_mode else 'DRY'}"})
        await self.bus.publish("MODE_TOGGLE", {"live": self.live_mode})
        await message.answer(f"{'🟢 LIVE' if self.live_mode else '🟡 DRY-RUN'}", reply_markup=self.keyboard)

    async def on_close_all(self, message: Message) -> None:
        await self.bus.publish("FORCE_FLATTEN_ALL", {"by": message.chat.id})
        await message.answer("❌ Closing all positions.")

    async def on_filter_settings(self, message: Message) -> None:
        f = self.config.screener
        settings = f"""⚙️ Current Filter Settings:

💰 Price: ${f.price_min:.2f} - ${f.price_max:.2f}
📈 Change: {f.pct_change_min:.2f}% - {f.pct_change_max:.2f}%
📊 Volume: {f.volume_min:,} - {f.volume_max:,}
🏢 Market Cap: ${f.market_cap_min:,.0f} - ${f.market_cap_max:,.0f}

These filters are very restrictive! Most stocks won't pass.
Try lowering volume requirement or increasing price range."""
        await message.answer(settings, reply_markup=self.keyboard)

    async def _alerts_listener(self) -> None:
        async for event in self.bus.subscribe():
            if self._stop.is_set():
                break
            if event.type == "ALERT_BUY":
                p = event.payload
                await self.send_alert(
                    f"🟢 **BUY SIGNAL**\n"
                    f"Symbol: {p['symbol']}\n"
                    f"Quantity: {p['qty']}\n"
                    f"Entry Price: ${p['entry']:.4f}\n"
                    f"ATR: {p['atr']:.3f}\n"
                    f"Stop Loss: ${p['stop']:.4f}"
                )
            elif event.type == "ALERT_SCREENER":
                p = event.payload
                # Keep recent screener items for rendering in Stocks List
                try:
                    self.recent_screener.appendleft({
                        "symbol": p.get("symbol"),
                        "price": float(p.get("price", 0.0)),
                        "todays_change_perc": float(p.get("todays_change_perc", 0.0)),
                        "day_volume": float(p.get("day_volume", 0.0)),
                        "market_cap": float(p.get("market_cap", 0.0)),
                    })
                except Exception:
                    pass
                await self.send_alert(
                    f"🌀 **SCREENER MATCH**\n"
                    f"Symbol: {p['symbol']}\n"
                    f"Price: ${p['price']:.4f}\n"
                    f"Change: {p['todays_change_perc']:.2f}%\n"
                    f"Volume: {int(p['day_volume']):,}\n"
                    f"Market Cap: ${int(p['market_cap']):,}"
                )
            elif event.type == "ALERT_STOP_UPDATE":
                p = event.payload
                await self.send_alert(f"🛑 **STOP UPDATE**\n{p['symbol']} → ${p['stop']:.4f}")
            elif event.type == "ALERT_SELL":
                p = event.payload
                await self.send_alert(
                    f"🔴 **SELL SIGNAL**\n"
                    f"Symbol: {p['symbol']}\n"
                    f"Quantity: {p['qty']}\n"
                    f"Exit Price: ${p['exit']:.4f}\n"
                    f"PnL: ${p['pnl']:.2f}\n"
                    f"Reason: {p['reason']}"
                )
            elif event.type == "ALERT_STOP_STATUS":
                p = event.payload or {}
                stops = p.get("stops", {}) or {}
                if not stops:
                    await self.bot.send_message(self.config.telegram.chat_id, "🔍 No open positions / stops.")
                    continue
                lines: List[str] = ["🔍 Current Stop Status", ""]
                for sym, d in stops.items():
                    try:
                        qty = int(float(d.get("qty", 0)))
                    except Exception:
                        qty = int(d.get("qty", 0) or 0)
                    entry = float(d.get("entry", 0) or 0)
                    stop = float(d.get("stop", 0) or 0)
                    lines.append(f"• {sym}: qty={qty} @ {entry:.4f} | Stop {stop:.4f}")
                await self.bot.send_message(self.config.telegram.chat_id, "\n".join(lines), reply_markup=self.keyboard)

    async def run(self) -> None:
        mode_text = "🟢 Live" if self.live_mode else "🟡 Dry-Run"
        f = self.config.screener
        details = "\n".join([
            "🚀 Bot starting…",
            "",
            "NASDAQ 0–5 USD Screener + ATR Trading (IBKR Execution)",
            f"Mode: {mode_text}",
            "",
            "Filters:",
            f"• Price: ${f.price_min:.2f} → ${f.price_max:.2f}",
            f"• Change: {f.pct_change_min:.2f}% → {f.pct_change_max:.2f}%",
            f"• Volume: {f.volume_min:,} → {f.volume_max:,}",
            f"• MktCap: ${f.market_cap_min:,.0f} → ${f.market_cap_max:,.0f}",
        ])
        await self.bot.send_message(self.config.telegram.chat_id, details, reply_markup=self.keyboard)
        # Start alert listener in background
        asyncio.create_task(self._alerts_listener())
        # Run polling until cancelled by main
        await self.dp.start_polling(self.bot, allowed_updates=self.dp.resolve_used_update_types())


