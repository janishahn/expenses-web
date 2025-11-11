from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field

from models import IntervalUnit, MonthDayPolicy, TransactionKind, TransactionType


class ReportOptions(BaseModel):
    start: date
    end: date
    sections: list[str] = Field(default_factory=lambda: ["summary", "kpis", "category_breakdown", "recent_transactions"])
    currency_symbol: str = "€"
    page_size: str = "A4"
    include_cents: bool = True
    recent_transactions_count: int = 50
    notes: Optional[str] = None


class CategoryIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    type: TransactionType
    color: Optional[str] = Field(None, pattern=r"^#[0-9a-fA-F]{6}$")
    order: int = 0


class TransactionIn(BaseModel):
    date: date
    type: TransactionType
    kind: TransactionKind = TransactionKind.normal
    amount_cents: int = Field(..., ge=0)
    category_id: int
    note: Optional[str]


class RecurringRuleIn(BaseModel):
    name: Optional[str]
    type: TransactionType
    amount_cents: int = Field(..., ge=0)
    category_id: int
    anchor_date: date
    interval_unit: IntervalUnit
    interval_count: int = Field(..., gt=0)
    next_occurrence: date
    end_date: Optional[date]
    auto_post: bool = True
    skip_weekends: bool = False
    month_day_policy: MonthDayPolicy = MonthDayPolicy.snap_to_end


class CSVRow(BaseModel):
    date: date
    type: TransactionType
    kind: TransactionKind = TransactionKind.normal
    amount_cents: int
    category: str
    note: Optional[str]
