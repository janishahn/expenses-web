from datetime import date, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


class TransactionType(str, Enum):
    income = "income"
    expense = "expense"


class RuleMatchType(str, Enum):
    contains = "contains"
    equals = "equals"
    starts_with = "starts_with"
    regex = "regex"


class CurrencyCode(str, Enum):
    eur = "EUR"
    usd = "USD"


CURRENCY_CODE_ENUM = SAEnum(
    CurrencyCode,
    name="currencycode",
    values_callable=lambda enum_cls: [member.value for member in enum_cls],
)


class IntervalUnit(str, Enum):
    day = "day"
    week = "week"
    month = "month"
    year = "year"


class MonthDayPolicy(str, Enum):
    snap_to_end = "snap_to_end"
    skip = "skip"
    carry_forward = "carry_forward"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )


class Category(Base, TimestampMixin):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    type: Mapped[TransactionType] = mapped_column(
        SAEnum(TransactionType), nullable=False
    )
    color: Mapped[Optional[str]] = mapped_column(String(7))
    order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    archived_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    transactions: Mapped[list["Transaction"]] = relationship(
        "Transaction", back_populates="category"
    )
    recurring_rules: Mapped[list["RecurringRule"]] = relationship(
        "RecurringRule", back_populates="category"
    )

    __table_args__ = (
        UniqueConstraint("user_id", "type", "name", name="uq_category_user_type_name"),
    )


class Tag(Base, TimestampMixin):
    __tablename__ = "tags"
    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_tag_user_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    color: Mapped[Optional[str]] = mapped_column(String(9))
    is_hidden_from_budget: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    transactions: Mapped[list["Transaction"]] = relationship(
        "Transaction", secondary="transaction_tags", back_populates="tags"
    )


transaction_tags = Table(
    "transaction_tags",
    Base.metadata,
    Column("transaction_id", Integer, ForeignKey("transactions.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id"), primary_key=True),
)


class Transaction(Base, TimestampMixin):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    type: Mapped[TransactionType] = mapped_column(
        SAEnum(TransactionType), nullable=False
    )
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    source_currency_code: Mapped[Optional[CurrencyCode]] = mapped_column(
        CURRENCY_CODE_ENUM
    )
    source_amount_cents: Mapped[Optional[int]] = mapped_column(Integer)
    fx_rate_micros: Mapped[Optional[int]] = mapped_column(Integer)
    fx_rate_date: Mapped[Optional[date]] = mapped_column(Date)
    fx_provider: Mapped[Optional[str]] = mapped_column(String(40))
    fx_fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    category_id: Mapped[int] = mapped_column(
        ForeignKey("categories.id"), nullable=False
    )
    note: Mapped[Optional[str]] = mapped_column(Text)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    origin_rule_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("recurring_rules.id")
    )
    occurrence_date: Mapped[Optional[date]] = mapped_column(Date)

    category: Mapped["Category"] = relationship(
        "Category", back_populates="transactions"
    )
    origin_rule: Mapped[Optional["RecurringRule"]] = relationship(
        "RecurringRule", back_populates="transactions"
    )
    tags: Mapped[list["Tag"]] = relationship(
        "Tag", secondary="transaction_tags", back_populates="transactions"
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "origin_rule_id",
            "occurrence_date",
            name="uq_txn_origin_occurrence",
        ),
        Index("ix_transactions_user_date", "user_id", "date"),
        Index("ix_transactions_user_category_date", "user_id", "category_id", "date"),
        Index("ix_transactions_user_type_date", "user_id", "type", "date"),
        CheckConstraint("amount_cents >= 0", name="ck_transactions_amount_positive"),
    )


class RecurringRule(Base, TimestampMixin):
    __tablename__ = "recurring_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[Optional[str]] = mapped_column(String(120))
    type: Mapped[TransactionType] = mapped_column(
        SAEnum(TransactionType), nullable=False
    )
    currency_code: Mapped[CurrencyCode] = mapped_column(
        CURRENCY_CODE_ENUM, nullable=False, default=CurrencyCode.eur
    )
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    category_id: Mapped[int] = mapped_column(
        ForeignKey("categories.id"), nullable=False
    )
    anchor_date: Mapped[date] = mapped_column(Date, nullable=False)
    interval_unit: Mapped[IntervalUnit] = mapped_column(
        SAEnum(IntervalUnit), nullable=False
    )
    interval_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    next_occurrence: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[Optional[date]] = mapped_column(Date)
    auto_post: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    skip_weekends: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    month_day_policy: Mapped[MonthDayPolicy] = mapped_column(
        SAEnum(MonthDayPolicy),
        default=MonthDayPolicy.snap_to_end,
        nullable=False,
    )

    category: Mapped["Category"] = relationship(
        "Category", back_populates="recurring_rules"
    )
    transactions: Mapped[list["Transaction"]] = relationship(
        "Transaction", back_populates="origin_rule"
    )

    __table_args__ = (
        CheckConstraint("interval_count > 0", name="ck_rule_interval_positive"),
        CheckConstraint("amount_cents >= 0", name="ck_rule_amount_positive"),
    )


class MonthlyRollup(Base, TimestampMixin):
    __tablename__ = "monthly_rollups"
    __table_args__ = (
        UniqueConstraint("user_id", "year", "month", name="uq_rollup_user_month"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    income_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    expense_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class BudgetFrequency(str, Enum):
    monthly = "monthly"
    yearly = "yearly"


class BudgetTemplate(Base, TimestampMixin):
    __tablename__ = "budget_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    frequency: Mapped[BudgetFrequency] = mapped_column(
        SAEnum(BudgetFrequency), nullable=False
    )
    category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    starts_on: Mapped[date] = mapped_column(Date, nullable=False)
    ends_on: Mapped[Optional[date]] = mapped_column(Date)

    category: Mapped[Optional["Category"]] = relationship("Category")

    __table_args__ = (
        CheckConstraint("amount_cents >= 0", name="ck_budget_template_amount_positive"),
        UniqueConstraint(
            "user_id",
            "frequency",
            "category_id",
            "starts_on",
            name="uq_budget_template_scope_start",
        ),
        Index("ix_budget_template_user_freq", "user_id", "frequency"),
    )


class BudgetOverride(Base, TimestampMixin):
    __tablename__ = "budget_overrides"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    category: Mapped[Optional["Category"]] = relationship("Category")

    __table_args__ = (
        CheckConstraint("amount_cents >= 0", name="ck_budget_override_amount_positive"),
        UniqueConstraint(
            "user_id",
            "year",
            "month",
            "category_id",
            name="uq_budget_override_user_month_category",
        ),
        Index("ix_budget_override_user_month", "user_id", "year", "month"),
    )


class Rule(Base, TimestampMixin):
    __tablename__ = "rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=100, nullable=False)

    match_type: Mapped[RuleMatchType] = mapped_column(
        SAEnum(RuleMatchType), nullable=False
    )
    match_value: Mapped[str] = mapped_column(String(200), nullable=False)
    transaction_type: Mapped[Optional[TransactionType]] = mapped_column(
        SAEnum(TransactionType)
    )
    min_amount_cents: Mapped[Optional[int]] = mapped_column(Integer)
    max_amount_cents: Mapped[Optional[int]] = mapped_column(Integer)

    set_category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    add_tags_json: Mapped[Optional[str]] = mapped_column(Text)
    budget_exclude_tag_id: Mapped[Optional[int]] = mapped_column(ForeignKey("tags.id"))

    set_category: Mapped[Optional["Category"]] = relationship(
        "Category", foreign_keys=[set_category_id]
    )
    budget_exclude_tag: Mapped[Optional["Tag"]] = relationship(
        "Tag", foreign_keys=[budget_exclude_tag_id]
    )

    __table_args__ = (
        Index("ix_rules_user_enabled_priority", "user_id", "enabled", "priority", "id"),
    )


class BalanceAnchor(Base, TimestampMixin):
    __tablename__ = "balance_anchors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    as_of_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    balance_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    note: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (Index("ix_balance_anchor_user_at", "user_id", "as_of_at"),)
