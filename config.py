import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class PolygonConfig:
    api_key: str
    ws_url: str = "wss://socket.polygon.io/stocks"
    rest_url: str = "https://api.polygon.io"
    subscribe_channel: str = "A.*"


@dataclass
class IBConfig:
    host: str
    port: int
    client_id: int


@dataclass
class ATRConfig:
    timeframe: str = "5m"
    period: int = 14
    multiplier: float = 2.0


@dataclass
class ScreenerFilters:
    price_min: float = 0.0
    price_max: float = 5.0
    pct_change_min: float = 0.0
    pct_change_max: float = 218.92
    volume_min: int = 2_000_000
    volume_max: int = 513_960_000
    market_cap_min: float = 0.0
    market_cap_max: float = 3_207_060_000_000.0


@dataclass
class TelegramConfig:
    token: str
    chat_id: str


@dataclass
class AppConfig:
    polygon: PolygonConfig
    ib: IBConfig
    atr: ATRConfig
    screener: ScreenerFilters
    telegram: TelegramConfig
    timezone: str = "US/Eastern"
    flatten_minutes_before_close: int = 10
    daily_equity_risk_pct: float = 0.01
    dry_run: bool = True


def load_config() -> AppConfig:
    load_dotenv()

    polygon = PolygonConfig(
        api_key=os.getenv("POLYGON_API_KEY", ""),
        ws_url=os.getenv("POLYGON_WS_URL", "wss://socket.polygon.io/stocks"),
        rest_url=os.getenv("POLYGON_REST_URL", "https://api.polygon.io"),
        subscribe_channel=os.getenv("POLYGON_SUBSCRIBE_CHANNEL", "A.*"),
    )

    ib = IBConfig(
        host=os.getenv("IB_HOST", "127.0.0.1"),
        port=int(os.getenv("IB_PORT", "7497")),
        client_id=int(os.getenv("IB_CLIENT_ID", "7")),
    )

    atr = ATRConfig(
        timeframe=os.getenv("ATR_TIMEFRAME", "5m"),
        period=int(os.getenv("ATR_PERIOD", "14")),
        multiplier=float(os.getenv("ATR_MULTIPLIER", "2.0")),
    )

    screener = ScreenerFilters(
        price_min=float(os.getenv("FILTER_PRICE_MIN", "0")),
        price_max=float(os.getenv("FILTER_PRICE_MAX", "5")),
        pct_change_min=float(os.getenv("FILTER_PCT_MIN", "0")),
        pct_change_max=float(os.getenv("FILTER_PCT_MAX", "218.92")),
        volume_min=int(os.getenv("FILTER_VOL_MIN", "2000000")),
        volume_max=int(os.getenv("FILTER_VOL_MAX", "513960000")),
        market_cap_min=float(os.getenv("FILTER_MKT_CAP_MIN", "0")),
        market_cap_max=float(os.getenv("FILTER_MKT_CAP_MAX", "3207060000000")),
    )

    telegram = TelegramConfig(
        token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
    )

    timezone = os.getenv("TIMEZONE", "US/Eastern")
    flatten_minutes_before_close = int(os.getenv("FLATTEN_MIN_BEFORE_CLOSE", "10"))
    daily_equity_risk_pct = float(os.getenv("DAILY_EQUITY_RISK_PCT", "0.01"))
    dry_run_env = os.getenv("DRY_RUN", "true").lower() in {"1", "true", "yes"}

    return AppConfig(
        polygon=polygon,
        ib=ib,
        atr=atr,
        screener=screener,
        telegram=telegram,
        timezone=timezone,
        flatten_minutes_before_close=flatten_minutes_before_close,
        daily_equity_risk_pct=daily_equity_risk_pct,
        dry_run=dry_run_env,
    )


