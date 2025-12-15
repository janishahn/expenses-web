from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

from models import IntervalUnit, MonthDayPolicy, TransactionType


class ReportOptions(BaseModel):
    start: date
    end: date
    sections: list[str] = Field(
        default_factory=lambda: [
            "summary",
            "category_breakdown",
            "recent_transactions",
        ]
    )
    include_cents: bool = True
    notes: Optional[str] = None
    transaction_type: Optional[TransactionType] = None
    category_ids: Optional[list[int]] = None
    transactions_sort: Literal["newest", "oldest"] = "newest"
    show_running_balance: bool = False
    include_category_subtotals: bool = False


class BudgetIn(BaseModel):
    year: int = Field(..., ge=1970, le=3000)
    month: int = Field(..., ge=1, le=12)
    category_id: Optional[int] = None
    amount_cents: int = Field(..., ge=0)


class CategoryIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    type: TransactionType
    order: int = 0


class TransactionIn(BaseModel):
    date: date
    occurred_at: datetime
    type: TransactionType
    amount_cents: int = Field(..., ge=0)
    category_id: int
    note: str = Field(..., min_length=1, max_length=200)


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
    amount_cents: int
    category: str
    note: Optional[str]


class BalanceAnchorIn(BaseModel):
    as_of_at: datetime
    balance_cents: int
    note: Optional[str] = Field(default=None, max_length=200)
