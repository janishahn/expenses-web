from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from urllib.error import URLError
from urllib.request import Request, urlopen

from config import get_settings


@dataclass(frozen=True)
class FxQuote:
    provider: str
    base: str
    quote: str
    rate: Decimal  # quote per 1 base
    rate_date: date
    fetched_at: datetime


class FxRateService:
    def __init__(self) -> None:
        self.settings = get_settings()

    def usd_to_eur_quote_for_date(self, on_date: date) -> FxQuote:
        provider = (self.settings.fx_provider or "frankfurter").lower()
        if provider != "frankfurter":
            raise ValueError(f"Unsupported FX provider: {provider}")

        quote = _fetch_frankfurter_usd_eur_quote(
            on_date, timeout=self.settings.fx_timeout_secs
        )
        markup_bps = self.settings.fx_markup_bps
        if markup_bps:
            factor = Decimal("1") - (Decimal(markup_bps) / Decimal("10000"))
            quote = FxQuote(
                provider=quote.provider,
                base=quote.base,
                quote=quote.quote,
                rate=(quote.rate * factor),
                rate_date=quote.rate_date,
                fetched_at=quote.fetched_at,
            )
        return quote

    def convert_usd_cents_to_eur_cents(
        self, usd_cents: int, on_date: date
    ) -> tuple[int, FxQuote]:
        quote = self.usd_to_eur_quote_for_date(on_date)
        eur_cents_decimal = (Decimal(usd_cents) * quote.rate).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
        return int(eur_cents_decimal), quote

    @staticmethod
    def rate_to_micros(rate: Decimal) -> int:
        return int(
            (rate * Decimal("1000000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )


@lru_cache(maxsize=2048)
def _fetch_frankfurter_usd_eur_quote(on_date: date, *, timeout: float) -> FxQuote:
    url = f"https://api.frankfurter.app/{on_date.isoformat()}?from=USD&to=EUR"
    req = Request(url, headers={"Accept": "application/json"})
    fetched_at = datetime.now(timezone.utc)
    try:
        with urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Failed to fetch FX rate from Frankfurter for {on_date}"
        ) from exc

    try:
        rate_value = payload["rates"]["EUR"]
        effective_date = date.fromisoformat(payload["date"])
    except Exception as exc:
        raise RuntimeError("Unexpected FX provider response") from exc

    rate = Decimal(str(rate_value))
    return FxQuote(
        provider="frankfurter",
        base="USD",
        quote="EUR",
        rate=rate,
        rate_date=effective_date,
        fetched_at=fetched_at,
    )
