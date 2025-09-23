from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict, Optional

import aiohttp
from ib_insync import IB, Stock, MarketOrder

from .bus import EventBus
from .config import AppConfig
from .risk import OHLCV, chandelier_stop_long, compute_atr, pnl_for_close, position_size_by_risk, update_trailing_stop
from .storage import Trade, append_opslog, record_trade_close, record_trade_open


logger = logging.getLogger(__name__)


class Trader:
    def __init__(self, config: AppConfig, bus: EventBus) -> None:
        self.config = config
        self.bus = bus
        self.ib = IB()
        self.account_equity_cache: float = 25_000.0  # fallback
        self.price_cache: Dict[str, float] = {}
        self.ohlcv_cache: Dict[str, Deque[OHLCV]] = defaultdict(lambda: deque(maxlen=max(100, self.config.atr.period + 5)))
        self.open_positions: Dict[str, Dict] = {}
        self._stop = asyncio.Event()

    async def stop(self) -> None:
        self._stop.set()
        if self.ib.isConnected():
            await asyncio.to_thread(self.ib.disconnect)

    async def _connect_ib(self) -> None:
        try:
            # If already connected, do nothing (prevents duplicate connects from main + run)
            if self.ib.isConnected():
                logger.info("IBKR already connected; skipping reconnect")
                return
            # Prefer native async connection to avoid thread loop issues
            if hasattr(self.ib, "connectAsync"):
                await self.ib.connectAsync(
                    self.config.ib.host,
                    self.config.ib.port,
                    clientId=self.config.ib.client_id,
                    timeout=10,
                )
            else:
                await asyncio.to_thread(
                    self.ib.connect,
                    self.config.ib.host,
                    self.config.ib.port,
                    None,
                    self.config.ib.client_id,
                    10,
                )
            logger.info("Connected IBKR")

            # Decide Polygon subscription channel based on IBKR market data type if requested
            try:
                ch = (self.config.polygon.subscribe_channel or "").lower()
                if ch in {"auto", "auto_ibkr"}:
                    contract = Stock('AAPL', 'SMART', 'USD')
                    # Request market data briefly to infer data type
                    if hasattr(self.ib, "reqMktDataAsync"):
                        ticker = await self.ib.reqMktDataAsync(contract, '', False, False)
                    else:
                        ticker = await asyncio.to_thread(self.ib.reqMktData, contract, '', False, False)
                    # Allow a short window for first ticks / dataType to come in
                    await asyncio.sleep(1.5)
                    mdt = getattr(ticker, 'marketDataType', None)
                    # Cancel the subscription
                    try:
                        await asyncio.to_thread(self.ib.cancelMktData, contract)
                    except Exception:
                        pass

                    # Map IBKR market data type to Polygon channel
                    # 1=RealTime, 2=Frozen, 3=Delayed, 4=DelayedFrozen
                    if mdt in (1, 2):
                        chosen = 'T.*'
                    else:
                        chosen = 'A.*'
                    prev = self.config.polygon.subscribe_channel
                    self.config.polygon.subscribe_channel = chosen
                    logger.info("[AUTO] IBKR marketDataType=%s → Polygon channel '%s' (was '%s')", mdt, chosen, prev)
                    await append_opslog({
                        "level": "info",
                        "component": "trader",
                        "msg": f"auto polygon channel via ibkr mdt={mdt} -> {chosen}",
                    })
            except Exception as e:
                logger.warning("auto channel selection via IBKR failed: %s", e)
        except Exception as e:
            logger.error("IBKR connect failed: %s", e)
            await append_opslog({"level": "error", "component": "trader", "msg": f"ib connect {e}"})
            raise

    async def _refresh_equity(self) -> None:
        try:
            if not self.ib.isConnected():
                return
            # accountSummary requires a tagged request; use built-in async req
            try:
                if hasattr(self.ib, "accountSummaryAsync"):
                    vals = await self.ib.accountSummaryAsync()
                else:
                    vals = await asyncio.to_thread(self.ib.accountSummary)
            except Exception:
                # Fallback to accountValues
                vals = await asyncio.to_thread(self.ib.accountValues)

            for v in vals:
                tag = getattr(v, "tag", getattr(v, "tag_", ""))
                if tag == "NetLiquidation":
                    self.account_equity_cache = float(v.value)
                    break
        except Exception as e:
            logger.debug("Equity refresh error: %s", e)

    async def _fetch_ohlcv(self, symbol: str) -> None:
        # Use Polygon aggregates for ATR timeframe
        tf = self.config.atr.timeframe
        mult = 5 if tf.endswith("m") else 1
        timespan = "minute" if tf.endswith("m") else "day"
        limit = max(50, self.config.atr.period + 20)
        now = datetime.utcnow().strftime("%Y-%m-%d")
        url = (
            f"{self.config.polygon.rest_url}/v2/aggs/ticker/{symbol}/range/{mult}/{timespan}/"
            f"{now}/{now}?adjusted=true&sort=asc&limit={limit}&apiKey={self.config.polygon.api_key}"
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        js = await resp.json()
                        results = js.get("results", [])
                        q = self.ohlcv_cache[symbol]
                        q.clear()
                        for r in results:
                            q.append(
                                OHLCV(
                                    ts=int(r["t"]),
                                    open=float(r["o"]),
                                    high=float(r["h"]),
                                    low=float(r["l"]),
                                    close=float(r["c"]),
                                    volume=float(r["v"]),
                                )
                            )
        except Exception as e:
            logger.warning("ohlcv fetch error %s: %s", symbol, e)

    async def _ensure_atr(self, symbol: str) -> Optional[float]:
        q = self.ohlcv_cache[symbol]
        if len(q) < self.config.atr.period + 1:
            await self._fetch_ohlcv(symbol)
        try:
            atr = compute_atr(list(q), self.config.atr.period)
            return atr
        except Exception:
            return None

    async def _enter_position(self, symbol: str, entry_price: float) -> None:
        if symbol in self.open_positions:
            return  # idempotent: already open

        atr = await self._ensure_atr(symbol)
        if atr is None or atr <= 0:
            return

        # deque doesn't support slicing; convert to list before slicing
        recent_high = max(c.high for c in list(self.ohlcv_cache[symbol])[-self.config.atr.period :])
        stop_price = chandelier_stop_long(recent_high, atr, self.config.atr.multiplier)

        await self._refresh_equity()
        qty = position_size_by_risk(
            self.account_equity_cache,
            self.config.daily_equity_risk_pct,
            entry_price,
            stop_price,
        )
        if qty <= 0:
            return

        if self.config.dry_run:
            order_id = f"DRY-{symbol}-{int(datetime.now(timezone.utc).timestamp())}"
        else:
            try:
                contract = Stock(symbol, 'SMART', 'USD')
                if hasattr(self.ib, "qualifyContractsAsync"):
                    await self.ib.qualifyContractsAsync(contract)
                else:
                    await asyncio.to_thread(self.ib.qualifyContracts, contract)
                order = MarketOrder('BUY', qty)
                if hasattr(self.ib, "placeOrderAsync"):
                    trade = await self.ib.placeOrderAsync(contract, order)
                else:
                    trade = await asyncio.to_thread(self.ib.placeOrder, contract, order)
                order_id = str(trade.order.orderId)
            except Exception as e:
                logger.error("IB place order failed: %s", e)
                await append_opslog({"level": "error", "component": "trader", "msg": f"order error {e}"})
                return

        self.open_positions[symbol] = {
            "qty": qty,
            "entry": entry_price,
            "atr": atr,
            "stop": stop_price,
            "order_id": order_id,
        }

        await record_trade_open(
            Trade(
                symbol=symbol,
                side="BUY",
                quantity=qty,
                entry_price=entry_price,
                atr=atr,
                stop_price=stop_price,
                ts=datetime.utcnow().isoformat() + "Z",
            )
        )
        await append_opslog({"level": "info", "component": "trader", "msg": f"BUY {symbol} qty={qty} entry={entry_price} atr={atr} stop={stop_price}"})
        await self.bus.publish("ALERT_BUY", {"symbol": symbol, "qty": qty, "entry": entry_price, "atr": atr, "stop": stop_price})

    async def _maybe_update_stop(self, symbol: str, last_price: float) -> None:
        pos = self.open_positions.get(symbol)
        if not pos:
            return
        atr = pos["atr"]
        # deque doesn't support slicing; convert to list before slicing
        recent_high = max(c.high for c in list(self.ohlcv_cache[symbol])[-self.config.atr.period :])
        new_stop = chandelier_stop_long(recent_high, atr, self.config.atr.multiplier)
        updated = update_trailing_stop(pos["stop"], new_stop)
        if updated > pos["stop"]:
            pos["stop"] = updated
            await append_opslog({"level": "info", "component": "trader", "msg": f"STOP UPDATE {symbol} -> {updated}"})
            await self.bus.publish("ALERT_STOP_UPDATE", {"symbol": symbol, "stop": updated})

        # stop-out check
        if last_price <= pos["stop"]:
            await self._exit_position(symbol, last_price, reason="Stop hit")

    async def _exit_position(self, symbol: str, exit_price: float, reason: str) -> None:
        pos = self.open_positions.get(symbol)
        if not pos:
            return

        qty = pos["qty"]
        entry = pos["entry"]

        if not self.config.dry_run and self.ib.isConnected():
            try:
                contract = Stock(symbol, 'SMART', 'USD')
                if hasattr(self.ib, "qualifyContractsAsync"):
                    await self.ib.qualifyContractsAsync(contract)
                else:
                    await asyncio.to_thread(self.ib.qualifyContracts, contract)
                order = MarketOrder('SELL', qty)
                if hasattr(self.ib, "placeOrderAsync"):
                    await self.ib.placeOrderAsync(contract, order)
                else:
                    await asyncio.to_thread(self.ib.placeOrder, contract, order)
            except Exception as e:
                logger.error("IB exit order failed: %s", e)
                await append_opslog({"level": "error", "component": "trader", "msg": f"exit error {e}"})

        pnl = pnl_for_close(entry, exit_price, qty)
        del self.open_positions[symbol]
        await record_trade_close(symbol, exit_price, pnl, reason)
        await append_opslog({"level": "info", "component": "trader", "msg": f"SELL {symbol} qty={qty} exit={exit_price} pnl={pnl} reason={reason}"})
        await self.bus.publish("ALERT_SELL", {"symbol": symbol, "qty": qty, "exit": exit_price, "pnl": pnl, "reason": reason})

    async def _poll_prices_fallback(self) -> None:
        """Fallback price poller to drive trailing stops when Polygon trade ticks are unavailable.
        Uses Yahoo quote API once per minute for all open positions.
        """
        while not self._stop.is_set():
            try:
                symbols = list(self.open_positions.keys())
                if symbols:
                    url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ",".join(symbols)
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, timeout=10) as resp:
                            js = await resp.json()
                            for q in js.get("quoteResponse", {}).get("result", []):
                                sym = q.get("symbol")
                                last = q.get("regularMarketPrice")
                                if not sym or last is None:
                                    continue
                                price = float(last)
                                self.price_cache[sym] = price
                                # Update trailing stops based on latest price
                                await self._maybe_update_stop(sym, price)
            except Exception as e:
                logger.debug("fallback price poller error: %s", e)
            # Run every 60 seconds
            await asyncio.sleep(60)

    async def _handle_events(self) -> None:
        async for event in self.bus.subscribe():
            if self._stop.is_set():
                break
            et = event.type
            p = event.payload
            try:
                if et == "BUY_SIGNAL":
                    await self._enter_position(p["symbol"], float(p["price"]))
                elif et == "PRICE_UPDATE":
                    sym = p["symbol"]
                    price = float(p["price"]) if p.get("price") is not None else None
                    if price is not None:
                        self.price_cache[sym] = price
                        await self._maybe_update_stop(sym, price)
                elif et == "MODE_TOGGLE":
                    # Toggle live/dry-run at runtime from Telegram
                    live = bool(p.get("live", False))
                    prev = self.config.dry_run
                    self.config.dry_run = not live
                    mode_txt = "LIVE" if live else "DRY-RUN"
                    logger.info("Mode toggled via bot -> %s (dry_run=%s -> %s)", mode_txt, prev, self.config.dry_run)
                    await append_opslog({
                        "level": "info",
                        "component": "trader",
                        "msg": f"mode -> {mode_txt}"
                    })
                    # Ensure IB connection if switching to LIVE
                    if live and not self.ib.isConnected():
                        try:
                            await self._connect_ib()
                            await self._refresh_equity()
                        except Exception as e:
                            logger.error("IBKR reconnect after mode toggle failed: %s", e)
                            await append_opslog({"level": "error", "component": "trader", "msg": f"ib reconnect {e}"})
                elif et == "FORCE_FLATTEN_ALL":
                    for sym in list(self.open_positions.keys()):
                        last = self.price_cache.get(sym, self.open_positions[sym]["entry"])  # fallback
                        await self._exit_position(sym, last, reason="Flatten before close")
                elif et == "BOT_QUERY_STOPS":
                    # Compile current stops and publish for Telegram to render
                    stops = {
                        sym: {
                            "qty": d.get("qty"),
                            "entry": d.get("entry"),
                            "stop": d.get("stop"),
                        }
                        for sym, d in self.open_positions.items()
                    }
                    await self.bus.publish("ALERT_STOP_STATUS", {"stops": stops})
            except Exception as e:
                logger.error("event handler error: %s", e)

    async def run(self) -> None:
        await self._connect_ib()
        await self._refresh_equity()
        # Run event handler and fallback price poller concurrently
        await asyncio.gather(
            self._handle_events(),
            self._poll_prices_fallback(),
        )


