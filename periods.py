from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class Period:
    slug: str
    start: date
    end: date


def resolve_period(
    period: Optional[str],
    start: Optional[str],
    end: Optional[str],
    *,
    today: Optional[date] = None,
) -> Period:
    today = today or date.today()
    if not period or period == "all":
        return Period("all", date(1970, 1, 1), today)
    if period == "last_month":
        first_this = today.replace(day=1)
        last_month_end = first_this - date.resolution
        last_month_start = last_month_end.replace(day=1)
        return Period("last_month", last_month_start, last_month_end)
    if period == "custom":
        if not start or not end:
            raise ValueError("Custom period requires start and end dates")
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
        if start_date > end_date:
            raise ValueError("Start date must be before end date")
        return Period("custom", start_date, end_date)

    # this month
    first = today.replace(day=1)
    if first.month == 12:
        next_month = first.replace(year=first.year + 1, month=1)
    else:
        next_month = first.replace(month=first.month + 1)
    end_this = next_month - date.resolution
    return Period("this_month", first, end_this)
