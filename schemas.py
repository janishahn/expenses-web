import datetime as dt
from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from models import (
    BudgetFrequency,
    CurrencyCode,
    IntervalUnit,
    MonthDayPolicy,
    RuleMatchType,
    TransactionType,
)


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


class BudgetOverrideIn(BaseModel):
    year: int = Field(..., ge=1970, le=3000)
    month: int = Field(..., ge=1, le=12)
    category_id: Optional[int] = None
    amount_cents: int = Field(..., ge=0)


class BudgetTemplateIn(BaseModel):
    frequency: BudgetFrequency
    category_id: Optional[int] = None
    amount_cents: int = Field(..., ge=0)
    starts_on: date
    ends_on: Optional[date] = None


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
    tags: list[str] = Field(default_factory=list)


class IngestTransactionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount_cents: int = Field(..., ge=0)
    note: str = Field(..., min_length=1, max_length=200)
    date: Optional[dt.date] = None
    category: Optional[str] = Field(default=None, max_length=100)


class IngestTransactionOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    date: date
    occurred_at: datetime
    type: Literal["expense"]
    amount_cents: int
    category: str
    note: str


class TagIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=50)
    color: Optional[str] = Field(None, max_length=9)
    is_hidden_from_budget: bool = False


class RecurringRuleIn(BaseModel):
    name: Optional[str]
    type: TransactionType
    currency_code: CurrencyCode = CurrencyCode.eur
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


class RuleIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    enabled: bool = True
    priority: int = Field(default=100, ge=0, le=10_000)
    match_type: RuleMatchType
    match_value: str = Field(..., min_length=1, max_length=200)
    transaction_type: Optional[TransactionType] = None
    min_amount_cents: Optional[int] = Field(default=None, ge=0)
    max_amount_cents: Optional[int] = Field(default=None, ge=0)
    set_category_id: Optional[int] = None
    add_tags: list[str] = Field(default_factory=list)
    budget_exclude_tag_id: Optional[int] = None
