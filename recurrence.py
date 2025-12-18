from datetime import date, datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from config import get_settings
from models import (
    CurrencyCode,
    IntervalUnit,
    MonthDayPolicy,
    RecurringRule,
    Transaction,
)


def local_today() -> date:
    settings = get_settings()
    tz = ZoneInfo(settings.timezone)
    return datetime.now(tz).date()


def days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - date(year, month, 1)).days


def _add_months(
    base: date,
    months: int,
    *,
    desired_day: int,
    policy: MonthDayPolicy,
) -> date:
    total_months = base.month - 1 + months
    year = base.year + total_months // 12
    month = total_months % 12 + 1

    dim = days_in_month(year, month)
    if policy == MonthDayPolicy.skip and desired_day > dim:
        max_skips = 24  # Prevent infinite loops - max 2 years of skipping
        skips = 0
        while desired_day > dim and skips < max_skips:
            total_months += 1
            year = base.year + total_months // 12
            month = total_months % 12 + 1
            dim = days_in_month(year, month)
            skips += 1

        if skips >= max_skips:
            raise ValueError(
                f"Cannot find suitable month for day {desired_day} after {max_skips} attempts"
            )

    if desired_day > dim:
        day = dim
    else:
        day = desired_day
    return date(year, month, day)


def calculate_next_date(rule: RecurringRule, from_date: date) -> date:
    if rule.interval_unit == IntervalUnit.day:
        next_date = from_date + timedelta(days=rule.interval_count)
    elif rule.interval_unit == IntervalUnit.week:
        next_date = from_date + timedelta(weeks=rule.interval_count)
    elif rule.interval_unit == IntervalUnit.month:
        anchor_day = (
            from_date.day
            if rule.month_day_policy == MonthDayPolicy.carry_forward
            else rule.anchor_date.day
        )
        next_date = _add_months(
            from_date,
            rule.interval_count,
            desired_day=anchor_day,
            policy=rule.month_day_policy,
        )
    else:
        next_date = _add_months(
            from_date,
            12 * rule.interval_count,
            desired_day=rule.anchor_date.day,
            policy=rule.month_day_policy,
        )

    if rule.skip_weekends:
        max_skip_days = 14  # Prevent infinite loops - max 2 weeks of skipping
        skip_count = 0
        while next_date.weekday() >= 5 and skip_count < max_skip_days:
            next_date += timedelta(days=1)
            skip_count += 1

        # If we hit the limit, log a warning and use the original date
        if skip_count >= max_skip_days:
            print(
                f"Warning: Weekend skip limit reached for rule {rule.name}, using weekday date"
            )
    return next_date


class RecurringEngine:
    def __init__(self, session: Session) -> None:
        self.session = session

    def catch_up_rule(self, rule: RecurringRule, today: Optional[date] = None) -> None:
        today = today or local_today()
        iterations = 0
        max_iterations = 365
        while rule.next_occurrence <= today and iterations < max_iterations:
            if rule.end_date and rule.next_occurrence > rule.end_date:
                break
            occurrence_date = rule.next_occurrence
            try:
                posted = self._post_occurrence(rule, occurrence_date)
            except Exception:
                break
            next_date = calculate_next_date(rule, occurrence_date)
            rule.next_occurrence = next_date
            if not posted and occurrence_date == next_date:
                break
            iterations += 1

    def post_due_rules(self, today: Optional[date] = None) -> int:
        today = today or local_today()
        stmt = (
            select(RecurringRule)
            .where(
                RecurringRule.auto_post.is_(True),
                RecurringRule.next_occurrence <= today,
            )
            .order_by(RecurringRule.next_occurrence)
        )
        rules = self.session.scalars(stmt).all()
        count = 0
        for rule in rules:
            prev = rule.next_occurrence
            self.catch_up_rule(rule, today)
            if rule.next_occurrence != prev:
                count += 1
        return count

    def _post_occurrence(self, rule: RecurringRule, occurrence_date: date) -> bool:
        from services import recompute_monthly_rollup_for_date
        from fx_rates import FxRateService

        exists_stmt = (
            select(Transaction.id)
            .where(
                Transaction.user_id == rule.user_id,
                Transaction.origin_rule_id == rule.id,
                Transaction.occurrence_date == occurrence_date,
            )
            .limit(1)
        )
        existing = self.session.execute(exists_stmt).scalar_one_or_none()
        if existing:
            return False

        amount_eur_cents = rule.amount_cents
        source_currency_code = None
        source_amount_cents = None
        fx_rate_micros = None
        fx_rate_date = None
        fx_provider = None
        fx_fetched_at = None

        if rule.currency_code == CurrencyCode.usd:
            fx = FxRateService()
            amount_eur_cents, quote = fx.convert_usd_cents_to_eur_cents(
                rule.amount_cents, occurrence_date
            )
            source_currency_code = CurrencyCode.usd
            source_amount_cents = rule.amount_cents
            fx_rate_micros = FxRateService.rate_to_micros(quote.rate)
            fx_rate_date = quote.rate_date
            fx_provider = quote.provider
            fx_fetched_at = quote.fetched_at

        txn = Transaction(
            user_id=rule.user_id,
            date=occurrence_date,
            occurred_at=datetime.combine(occurrence_date, time(12, 0)),
            type=rule.type,
            amount_cents=amount_eur_cents,
            source_currency_code=source_currency_code,
            source_amount_cents=source_amount_cents,
            fx_rate_micros=fx_rate_micros,
            fx_rate_date=fx_rate_date,
            fx_provider=fx_provider,
            fx_fetched_at=fx_fetched_at,
            category_id=rule.category_id,
            origin_rule_id=rule.id,
            occurrence_date=occurrence_date,
            note=rule.name,
        )
        self.session.add(txn)
        self.session.flush()
        recompute_monthly_rollup_for_date(self.session, rule.user_id, occurrence_date)
        return True
