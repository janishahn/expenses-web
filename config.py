from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


class Settings:
    def __init__(self, database_url: str, timezone: str, csrf_secret: str) -> None:
        self.database_url = database_url
        self.timezone = timezone
        self.csrf_secret = csrf_secret


def _ensure_data_dir() -> Path:
    root = Path(os.getenv("EXPENSES_DATA_DIR", "./data")).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    data_dir = _ensure_data_dir()
    default_db = data_dir / "expenses.db"
    database_url = os.getenv("EXPENSES_DATABASE_URL", f"sqlite:///{default_db}")
    timezone = os.getenv("EXPENSES_TIMEZONE", "Europe/Berlin")
    csrf_secret = os.getenv("EXPENSES_CSRF_SECRET", "change-me")
    return Settings(database_url=database_url, timezone=timezone, csrf_secret=csrf_secret)
