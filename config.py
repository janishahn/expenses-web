import os
from functools import lru_cache
from pathlib import Path


class Settings:
    def __init__(
        self,
        database_url: str,
        timezone: str,
        csrf_secret: str,
        fx_provider: str,
        fx_markup_bps: int,
        fx_timeout_secs: float,
    ) -> None:
        self.database_url = database_url
        self.timezone = timezone
        self.csrf_secret = csrf_secret
        self.fx_provider = fx_provider
        self.fx_markup_bps = fx_markup_bps
        self.fx_timeout_secs = fx_timeout_secs


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
    csrf_secret = os.getenv(
        "EXPENSES_CSRF_SECRET",
        "ebf511a733bdc213d6ccc715d338ad1c05bef4ad0ab32bb7eb60bb90f382380a",
    )
    fx_provider = os.getenv("EXPENSES_FX_PROVIDER", "frankfurter")
    fx_markup_bps = int(os.getenv("EXPENSES_FX_MARKUP_BPS", "0"))
    fx_timeout_secs = float(os.getenv("EXPENSES_FX_TIMEOUT_SECS", "5"))
    return Settings(
        database_url=database_url,
        timezone=timezone,
        csrf_secret=csrf_secret,
        fx_provider=fx_provider,
        fx_markup_bps=fx_markup_bps,
        fx_timeout_secs=fx_timeout_secs,
    )
