import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import os


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _today_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")


def _file_path(prefix: str) -> Path:
    return DATA_DIR / f"{prefix}_{_today_str()}.json"


async def _atomic_write_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(data, indent=2, sort_keys=False, default=str)
    await asyncio.to_thread(tmp.write_text, text, encoding="utf-8")
    await asyncio.to_thread(tmp.replace, path)


async def _read_json_list(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        content = await asyncio.to_thread(path.read_text, encoding="utf-8")
        arr = json.loads(content)
        if isinstance(arr, list):
            return arr
        return []
    except Exception:
        return []


async def append_opslog(entry: Dict[str, Any]) -> None:
    path = _file_path("opslog")
    entries = await _read_json_list(path)
    entry_with_ts = {"ts": datetime.utcnow().isoformat() + "Z", **entry}
    entries.append(entry_with_ts)
    await _atomic_write_json(path, entries)


@dataclass
class Trade:
    symbol: str
    side: str
    quantity: int
    entry_price: float
    atr: float
    stop_price: float
    ts: str
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    reason: Optional[str] = None


async def record_trade_open(trade: Trade) -> None:
    path = _file_path("trades")
    entries = await _read_json_list(path)
    entries.append(asdict(trade))
    await _atomic_write_json(path, entries)


async def record_trade_close(symbol: str, exit_price: float, pnl: float, reason: str) -> None:
    path = _file_path("trades")
    entries = await _read_json_list(path)
    for item in reversed(entries):
        if item.get("symbol") == symbol and item.get("exit_price") in (None, 0):
            item["exit_price"] = exit_price
            item["pnl"] = pnl
            item["reason"] = reason
            break
    await _atomic_write_json(path, entries)


async def read_trades() -> List[Dict[str, Any]]:
    return await _read_json_list(_file_path("trades"))


async def append_summary(summary: Dict[str, Any]) -> None:
    path = _file_path("summary")
    entries = await _read_json_list(path)
    entries.append({"ts": datetime.utcnow().isoformat() + "Z", **summary})
    await _atomic_write_json(path, entries)


async def read_summary() -> List[Dict[str, Any]]:
    return await _read_json_list(_file_path("summary"))


async def append_screener(event: Dict[str, Any]) -> None:
    """Append a screener match event into a daily JSON file.
    The event should minimally include: symbol, price, todays_change_perc, day_volume, market_cap.
    """
    path = _file_path("screener")
    entries = await _read_json_list(path)
    entries.append({"ts": datetime.utcnow().isoformat() + "Z", **event})
    await _atomic_write_json(path, entries)


async def read_screener() -> List[Dict[str, Any]]:
    return await _read_json_list(_file_path("screener"))


