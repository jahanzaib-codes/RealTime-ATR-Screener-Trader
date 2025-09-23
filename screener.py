from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict, List, Set, Tuple

import aiohttp
import asyncio
import yfinance as yf

from .bus import EventBus
from .config import AppConfig
from .storage import append_opslog, append_screener


logger = logging.getLogger(__name__)


class PolygonScreener:
    def __init__(self, config: AppConfig, bus: EventBus) -> None:
        self.config = config
        self.bus = bus
        self.seen_symbols: Set[str] = set()
        self.market_cap_cache: Dict[str, float] = {}
        self.exchange_cache: Dict[str, str] = {}
        self.entered_today: Set[str] = set()
        self._stop = asyncio.Event()
        self.available = False

    async def stop(self) -> None:
        self._stop.set()

    async def _auth_ws(self, ws) -> None:
        # Send only auth here; subscription happens after auth validation
        await ws.send_json({"action": "auth", "params": self.config.polygon.api_key})

    async def _ws_loop(self) -> None:
        url = self.config.polygon.ws_url
        backoff = 1
        connection_count = 0
        while not self._stop.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        await self._auth_ws(ws)
                        connection_count += 1
                        logger.info(f"📡 Polygon WS connected (attempt #{connection_count})")
                        backoff = 1

                        # Validate auth handshake/status messages before entering main loop
                        auth_ok = False
                        try:
                            for _ in range(3):
                                msg = await asyncio.wait_for(ws.receive(), timeout=5)
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    try:
                                        data = json.loads(msg.data)
                                        # Polygon may send a list of status events or a single dict
                                        events = data if isinstance(data, list) else [data]
                                        for ev in events:
                                            if ev.get("ev") == "status":
                                                logger.info("[WS STATUS] %s", ev)
                                                if ev.get("status") == "authenticated":
                                                    auth_ok = True
                                                if ev.get("status") == "error":
                                                    # Log and record error details, then break to reconnect with backoff
                                                    logger.error("Polygon auth/status error: %s", ev)
                                                    await append_opslog({
                                                        "level": "error",
                                                        "component": "screener",
                                                        "msg": f"WS status error {ev}",
                                                    })
                                                    # Increase backoff aggressively on auth/permission errors
                                                    backoff = max(backoff, 15)
                                                    raise RuntimeError(f"Polygon WS status error: {ev}")
                                    except Exception as e:
                                        logger.warning("WS status parse error: %s", e)
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                        except asyncio.TimeoutError:
                            logger.warning("No WS status/auth message received within timeout; proceeding")

                        if not auth_ok:
                            logger.warning("Polygon WS auth not confirmed; continuing but stream may close.")

                        # Decide and send subscription
                        chosen_channel = self.config.polygon.subscribe_channel
                        if chosen_channel.lower() == "auto":
                            # Probe trades for a well-known symbol
                            logger.info("[WS] Auto mode: probing 'T.AAPL' to decide channel")
                            await ws.send_json({"action": "subscribe", "params": "T.AAPL"})
                            got_trade = False
                            try:
                                probe_msg = await asyncio.wait_for(ws.receive(), timeout=3)
                                if probe_msg.type == aiohttp.WSMsgType.TEXT:
                                    try:
                                        pdata = json.loads(probe_msg.data)
                                        plist = pdata if isinstance(pdata, list) else [pdata]
                                        for ev in plist:
                                            if ev.get("ev") == "T":
                                                got_trade = True
                                                break
                                            if ev.get("ev") == "status" and ev.get("status") == "error":
                                                logger.warning("[WS] Probe status error: %s", ev)
                                    except Exception:
                                        pass
                            except asyncio.TimeoutError:
                                pass

                            # Unsubscribe the probe symbol
                            try:
                                await ws.send_json({"action": "unsubscribe", "params": "T.AAPL"})
                            except Exception:
                                pass

                            chosen_channel = "T.*" if got_trade else "A.*"
                            logger.info("[WS] Auto mode selected channel: %s", chosen_channel)
                            await append_opslog({
                                "level": "info",
                                "component": "screener",
                                "msg": f"auto channel -> {chosen_channel}",
                            })

                        # Send final subscription
                        await ws.send_json({"action": "subscribe", "params": chosen_channel})
                        logger.info("[WS] Subscribed '%s'", chosen_channel)

                        tick_count = 0
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    if isinstance(data, list):
                                        for ev in data:
                                            ev_type = ev.get("ev")
                                            if ev_type in ("T", "A"):
                                                sym = ev.get("sym")
                                                if sym:
                                                    self.seen_symbols.add(sym)
                                                    tick_count += 1
                                                    if ev_type == "T":
                                                        # Relay price updates for trader stop adjustments (trades only)
                                                        await self.bus.publish(
                                                            "PRICE_UPDATE",
                                                            {"symbol": sym, "price": float(ev.get("p", 0.0))},
                                                        )

                                                    # Log tick activity every 50 ticks with last seen symbol
                                                    if tick_count % 50 == 0:
                                                        logger.info(
                                                            f"[DATA] Ticks={tick_count} last={sym} tracking={len(self.seen_symbols)}"
                                                        )
                                except Exception as e:
                                    logger.warning("WS parse error: %s", e)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                # Log close details for visibility
                                try:
                                    close_code = ws.close_code
                                except Exception:
                                    close_code = None
                                logger.warning("Polygon WS closed: type=%s code=%s exc=%s", msg.type, close_code, ws.exception())
                                await append_opslog({
                                    "level": "error",
                                    "component": "screener",
                                    "msg": f"WS closed type={msg.type} code={close_code} exc={ws.exception()}",
                                })
                                break
            except Exception as e:
                logger.error("Polygon WS error: %s", e)
                await append_opslog({"level": "error", "component": "screener", "msg": f"WS error {e}"})
            if not self._stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _seed_symbols_rest(self) -> None:
        """Seed `seen_symbols` using Polygon REST (gainers/losers/most active).
        This keeps the screener active even if WS is unavailable or auth fails.
        """
        urls = [
            f"{self.config.polygon.rest_url}/v2/snapshot/locale/us/markets/stocks/gainers?apiKey={self.config.polygon.api_key}",
            f"{self.config.polygon.rest_url}/v2/snapshot/locale/us/markets/stocks/losers?apiKey={self.config.polygon.api_key}",
            f"{self.config.polygon.rest_url}/v2/snapshot/locale/us/markets/stocks/mostactive?apiKey={self.config.polygon.api_key}",
        ]
        added = 0
        try:
            async with aiohttp.ClientSession() as session:
                for url in urls:
                    try:
                        async with session.get(url, timeout=10) as resp:
                            if resp.status != 200:
                                continue
                            js = await resp.json()
                            for t in js.get("tickers", []) or []:
                                sym = t.get("ticker") or t.get("T")
                                if sym and sym not in self.seen_symbols:
                                    self.seen_symbols.add(sym)
                                    added += 1
                    except Exception:
                        continue
        except Exception as e:
            logger.debug("seed symbols (REST) failed: %s", e)
        if added:
            logger.info("[REST SEED] Added %d symbols (seen=%d)", added, len(self.seen_symbols))

    async def _fetch_refdata(self, session: aiohttp.ClientSession, symbol: str) -> float:
        """Fetch and cache reference data (market cap and exchange). Returns market cap.
        Exchange MIC for NASDAQ is typically 'XNAS'. We cache whatever is returned to filter later.
        """
        if symbol in self.market_cap_cache and symbol in self.exchange_cache:
            return self.market_cap_cache[symbol]
        try:
            url = f"{self.config.polygon.rest_url}/v3/reference/tickers/{symbol}?apiKey={self.config.polygon.api_key}"
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    js = await resp.json()
                    res = js.get("results", {}) or {}
                    mc = float(res.get("market_cap", 0.0))
                    # primary_exchange may be MIC like 'XNAS'; fallback to listing_exchange if present
                    exch = res.get("primary_exchange") or res.get("listing_exchange") or ""
                    self.market_cap_cache[symbol] = mc
                    if exch:
                        self.exchange_cache[symbol] = str(exch)
                    return mc
        except Exception as e:
            logger.debug("refdata fetch failed for %s: %s", symbol, e)
        return 0.0

    def _is_nasdaq(self, symbol: str) -> bool:
        """Accept only NASDAQ (NMS). Polygon returns MIC 'XNAS' for NASDAQ.
        If unknown, default to False to be strict.
        """
        exch = self.exchange_cache.get(symbol, "").upper()
        return exch in {"XNAS", "NASDAQ", "NASDAQ NMS"}

    async def _batch_snapshots(self, session: aiohttp.ClientSession, symbols: List[str]) -> List[Dict]:
        if not symbols:
            return []
        # Polygon v2 bulk snapshots supports up to ~50-100 tickers per call; batch accordingly
        out: List[Dict] = []
        batch_size = 50
        for i in range(0, len(symbols), batch_size):
            batch = ",".join(symbols[i : i + batch_size])
            url = (
                f"{self.config.polygon.rest_url}/v2/snapshot/locale/us/markets/stocks/tickers"
                f"?tickers={batch}&apiKey={self.config.polygon.api_key}"
            )
            try:
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        js = await resp.json()
                        out.extend(js.get("tickers", []))
            except Exception as e:
                logger.warning("snapshot batch failed: %s", e)
        return out

    async def check_rest_available(self) -> bool:
        try:
            url = f"{self.config.polygon.rest_url}/v2/reference/markets?apiKey={self.config.polygon.api_key}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    self.available = resp.status == 200
                    return self.available
        except Exception:
            self.available = False
            return False

    def _passes_filters(self, snap: Dict, market_cap: float) -> Tuple[bool, Dict]:
        # Robust price: prefer lastTrade.p, then lastQuote.p, then day.c (close)
        lt = (snap.get("lastTrade", {}) or {})
        lq = (snap.get("lastQuote", {}) or {})
        day = (snap.get("day", {}) or {})
        price_val = (
            lt.get("p")
            if lt.get("p") is not None
            else (lq.get("p") if lq.get("p") is not None else day.get("c"))
        )
        try:
            price = float(price_val or 0.0)
        except Exception:
            price = 0.0
        todays_change_perc = float(snap.get("todaysChangePerc") or 0.0)
        day_volume = float(day.get("v") or 0.0)

        f = self.config.screener
        passes = (
            f.price_min <= price <= f.price_max
            and f.pct_change_min <= todays_change_perc <= f.pct_change_max
            and f.volume_min <= day_volume <= f.volume_max
            and f.market_cap_min <= market_cap <= f.market_cap_max
        )
        details = {
            "price": price,
            "todays_change_perc": todays_change_perc,
            "day_volume": day_volume,
            "market_cap": market_cap,
        }
        return passes, details

    async def _screen_loop(self) -> None:
        # Periodically evaluate seen symbols using REST snapshots
        while not self._stop.is_set():
            # Ensure we have a working symbol universe; fall back to REST seeding when empty/small
            if len(self.seen_symbols) < 50:
                await self._seed_symbols_rest()
            symbols = list(self.seen_symbols)
            if not symbols:
                await asyncio.sleep(5)
                continue
            
            # Log screener activity every 20 seconds
            logger.info(f"🔍 Screener: Processing {len(symbols)} symbols from WebSocket")
            
            try:
                await self._process_symbols(symbols)
            except Exception as e:
                logger.error("screen loop error: %s", e)
                await append_opslog({"level": "error", "component": "screener", "msg": f"screen error {e}"})
            await asyncio.sleep(20)

    async def _process_symbols(self, symbols: list) -> None:
        """Process symbols through filters and publish alerts"""
        async with aiohttp.ClientSession() as session:
            snaps = await self._batch_snapshots(session, symbols)
            logger.info(f"📊 Screener: Got {len(snaps)} snapshots from Polygon")
            
            filtered_count = 0
            for snap in snaps:
                symbol = snap.get("ticker")
                if not symbol:
                    continue
                market_cap = await self._fetch_refdata(session, symbol)
                # Enforce NASDAQ-only (NMS)
                if not self._is_nasdaq(symbol):
                    continue
                ok, details = self._passes_filters(snap, market_cap)
                
                # Log every symbol being checked (first 10 for debugging)
                if filtered_count < 10:
                    logger.info(f"🔍 Checking {symbol}: price={details['price']:.4f} Δ={details['todays_change_perc']:.2f}% vol={details['day_volume']:.0f} mcap={details['market_cap']:.0f} -> {'✅' if ok else '❌'}")
                
                if ok and symbol not in self.entered_today:
                    filtered_count += 1
                    self.entered_today.add(symbol)
                    # Pull historical data via Yahoo for ATR context (non-blocking thread)
                    tf = self.config.atr.timeframe
                    interval = "5m" if tf.lower().startswith("5") else "1m"
                    try:
                        hist = await asyncio.to_thread(
                            lambda: yf.Ticker(symbol).history(period="2d", interval=interval)
                        )
                        if not hist.empty:
                            closes = hist["Close"].tolist()
                            highs = hist["High"].tolist()
                            lows = hist["Low"].tolist()
                            # Log last bar details for transparency
                            logger.info(
                                "[HIST] %s %s bars=%d last_close=%.4f",
                                symbol,
                                interval,
                                len(closes),
                                closes[-1] if closes else 0.0,
                            )
                    except Exception as e:
                        logger.debug("yfinance history failed for %s: %s", symbol, e)
                    await append_opslog(
                        {
                            "level": "info",
                            "component": "screener",
                            "msg": f"Filter enter {symbol}",
                            "details": details,
                        }
                    )
                    # Log to console with emojis for real-time visibility
                    logger.info(
                        "🌀 Screener: %s matches filters • price=%.4f • Δ=%.2f%% • vol=%.0f • mcap=%.0f",
                        symbol,
                        details["price"],
                        details["todays_change_perc"],
                        details["day_volume"],
                        details["market_cap"],
                    )
                    # Publish an alert for Telegram/UI consumers
                    await self.bus.publish(
                        "ALERT_SCREENER",
                        {
                            "symbol": symbol,
                            "price": details["price"],
                            "todays_change_perc": details["todays_change_perc"],
                            "day_volume": details["day_volume"],
                            "market_cap": details["market_cap"],
                        },
                    )
                    # Persist to daily screener file
                    try:
                        await append_screener({
                            "symbol": symbol,
                            "price": details["price"],
                            "todays_change_perc": details["todays_change_perc"],
                            "day_volume": details["day_volume"],
                            "market_cap": details["market_cap"],
                        })
                    except Exception:
                        pass
                    await self.bus.publish(
                        "BUY_SIGNAL",
                        {
                            "symbol": symbol,
                            "price": details["price"],
                            "todays_change_perc": details["todays_change_perc"],
                            "day_volume": details["day_volume"],
                            "market_cap": details["market_cap"],
                        },
                    )
            
            logger.info(f"📈 Screener: {filtered_count} symbols passed filters this cycle")

    async def force_scan(self) -> None:
        """Force an immediate scan of all seen symbols"""
        symbols = list(self.seen_symbols)
        if not symbols:
            logger.info("🔄 Force scan: No symbols to scan")
            return
        
        logger.info(f"🔄 Force scan: Scanning {len(symbols)} symbols immediately")
        await self._process_symbols(symbols)

    async def run(self) -> None:
        await asyncio.gather(self._ws_loop(), self._screen_loop())


