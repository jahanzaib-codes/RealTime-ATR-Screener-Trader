import logging
import logging.handlers
from pathlib import Path
import os
import sys


def setup_logging() -> None:
    base_dir = Path(__file__).resolve().parent
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    app_handler = logging.handlers.RotatingFileHandler(
        logs_dir / "app.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    app_handler.setFormatter(formatter)
    app_handler.setLevel(logging.INFO)

    err_handler = logging.handlers.RotatingFileHandler(
        logs_dir / "error.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    err_handler.setFormatter(formatter)
    err_handler.setLevel(logging.WARNING)

    # Bot-specific log file (telegram/aiogram)
    bot_handler = logging.handlers.RotatingFileHandler(
        logs_dir / "bot.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    bot_handler.setFormatter(formatter)
    bot_handler.setLevel(logging.INFO)

    class BotLogFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
            name = record.name or ""
            return (
                "app.telegram_bot" in name
                or name.startswith("aiogram")
            )

    bot_handler.addFilter(BotLogFilter())

    # Ensure stdout uses UTF-8 to support emojis on Windows consoles
    stream = getattr(sys, "stdout", None)
    if stream is not None:
        try:
            # Reconfigure to UTF-8 when supported (Py3.7+)
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    console = logging.StreamHandler(stream)
    console.setFormatter(formatter)
    console.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid adding multiple handlers if called twice
    if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
        root.addHandler(app_handler)
        root.addHandler(err_handler)
        root.addHandler(bot_handler)
        root.addHandler(console)


