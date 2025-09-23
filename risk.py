from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class OHLCV:
    ts: int  # epoch ms
    open: float
    high: float
    low: float
    close: float
    volume: float


def compute_true_range(prev_close: float, high: float, low: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def compute_atr(ohlcv: List[OHLCV], period: int) -> float:
    if len(ohlcv) < period + 1:
        raise ValueError("Not enough data for ATR computation")
    trs: List[float] = []
    for i in range(1, len(ohlcv)):
        prev = ohlcv[i - 1]
        cur = ohlcv[i]
        trs.append(compute_true_range(prev.close, cur.high, cur.low))
    # Wilder's smoothing approximation: simple moving average of TR for initial ATR
    last_trs = trs[-period:]
    return sum(last_trs) / float(period)


def chandelier_stop_long(recent_high: float, atr: float, multiplier: float) -> float:
    return recent_high - multiplier * atr


def update_trailing_stop(prev_stop: float, new_stop: float) -> float:
    # Stop never decreases
    return max(prev_stop, new_stop)


def position_size_by_risk(account_equity: float, risk_pct: float, entry_price: float, stop_price: float) -> int:
    risk_per_share = max(entry_price - stop_price, 0.01)
    capital_at_risk = account_equity * risk_pct
    shares = math.floor(capital_at_risk / risk_per_share)
    return max(shares, 0)


def pnl_for_close(entry_price: float, exit_price: float, quantity: int) -> float:
    return (exit_price - entry_price) * quantity


