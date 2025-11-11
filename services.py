from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from models import (
    Category,
    IntervalUnit,
    MonthDayPolicy,
    RecurringRule,
    Transaction,
    TransactionType,
)
from periods import Period
from recurrence import RecurringEngine
from csv_utils import export_transactions, parse_csv
from schemas import CategoryIn, RecurringRuleIn, TransactionIn


def get_current_user_id() -> int:
    return 1


def cents_to_euros(cents: int) -> float:
    return cents / 100


@dataclass
class TransactionFilters:
    type: Optional[TransactionType] = None
    category_id: Optional[int] = None
    query: Optional[str] = None


class CategoryService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def list_all(self, include_archived: bool = False) -> list[Category]:
        stmt = (
            select(Category)
            .where(Category.user_id == self.user_id)
            .order_by(Category.type, Category.order, Category.name)
        )
        if not include_archived:
            stmt = stmt.where(Category.archived_at.is_(None))
        return self.session.scalars(stmt).all()

    def create(self, data: CategoryIn) -> Category:
        existing = self.session.scalar(
            select(Category).where(
                Category.user_id == self.user_id,
                Category.type == data.type,
                func.lower(Category.name) == data.name.lower(),
            )
        )
        if existing:
            raise ValueError("Category with this name already exists")
        category = Category(
            user_id=self.user_id,
            name=data.name.strip(),
            type=data.type,
            color=data.color,
            order=data.order,
        )
        self.session.add(category)
        self.session.commit()
        self.session.refresh(category)
        return category

    def rename(self, category_id: int, name: str) -> Category:
        category = self.session.get(Category, category_id)
        if not category or category.user_id != self.user_id:
            raise ValueError("Category not found")
        category.name = name.strip()
        self.session.commit()
        return category

    def archive(self, category_id: int) -> None:
        category = self.session.get(Category, category_id)
        if not category or category.user_id != self.user_id:
            raise ValueError("Category not found")
        category.archived_at = datetime.utcnow()
        self.session.commit()


class TransactionService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def create(self, data: TransactionIn) -> Transaction:
        category = self.session.get(Category, data.category_id)
        if not category or category.user_id != self.user_id:
            raise ValueError("Category not found")
        if category.type != data.type:
            raise ValueError("Category type mismatch")
        txn = Transaction(
            user_id=self.user_id,
            date=data.date,
            type=data.type,
            amount_cents=data.amount_cents,
            category_id=data.category_id,
            note=data.note,
        )
        self.session.add(txn)
        self.session.commit()
        self.session.refresh(txn)
        return txn

    def list(self, period: Period, filters: TransactionFilters, limit: int = 50, offset: int = 0) -> list[Transaction]:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(period.start, period.end),
            )
            .order_by(Transaction.date.desc(), Transaction.id.desc())
            .offset(offset)
            .limit(limit)
        )
        if filters.type:
            stmt = stmt.where(Transaction.type == filters.type)
        if filters.category_id:
            stmt = stmt.where(Transaction.category_id == filters.category_id)
        if filters.query:
            like = f"%{filters.query.lower()}%"
            stmt = stmt.where(
                func.lower(func.coalesce(Transaction.note, "")).like(like)
            )
        return self.session.scalars(stmt).all()

    def all_for_period(self, period: Period, filters: Optional[TransactionFilters] = None) -> list[Transaction]:
        filters = filters or TransactionFilters()
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(period.start, period.end),
            )
            .order_by(Transaction.date.asc(), Transaction.id.asc())
        )
        if filters.type:
            stmt = stmt.where(Transaction.type == filters.type)
        if filters.category_id:
            stmt = stmt.where(Transaction.category_id == filters.category_id)
        if filters.query:
            like = f"%{filters.query.lower()}%"
            stmt = stmt.where(
                func.lower(func.coalesce(Transaction.note, "")).like(like)
            )
        return self.session.scalars(stmt).all()

    def recent(self, limit: int = 10) -> list[Transaction]:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
            .where(Transaction.user_id == self.user_id, Transaction.deleted_at.is_(None))
            .order_by(Transaction.date.desc(), Transaction.id.desc())
            .limit(limit)
        )
        return self.session.scalars(stmt).all()

    def soft_delete(self, transaction_id: int) -> None:
        txn = self.session.get(Transaction, transaction_id)
        if not txn or txn.user_id != self.user_id:
            raise ValueError("Transaction not found")
        txn.deleted_at = datetime.utcnow()
        self.session.commit()


class MetricsService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def kpis(self, period: Period) -> dict[str, int]:
        stmt = (
            select(Transaction.type, func.sum(Transaction.amount_cents))
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(period.start, period.end),
            )
            .group_by(Transaction.type)
        )
        totals = {TransactionType.income: 0, TransactionType.expense: 0}
        for row in self.session.execute(stmt):
            totals[row[0]] = row[1] or 0
        balance = totals[TransactionType.income] - totals[TransactionType.expense]
        return {
            "income": totals[TransactionType.income],
            "expenses": totals[TransactionType.expense],
            "balance": balance,
        }

    def category_breakdown(self, period: Period) -> list[dict[str, object]]:
        stmt = (
            select(Category.name, func.sum(Transaction.amount_cents).label("total"))
            .join(Category, Category.id == Transaction.category_id)
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
                Transaction.date.between(period.start, period.end),
            )
            .group_by(Category.name)
            .order_by(func.sum(Transaction.amount_cents).desc())
        )
        rows = self.session.execute(stmt).all()
        total = sum(row.total or 0 for row in rows)
        breakdown = []
        for row in rows:
            amount = row.total or 0
            percent = (amount / total * 100) if total else 0
            breakdown.append({"name": row.name, "amount_cents": amount, "percent": percent})
        return breakdown


class RecurringRuleService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def list(self) -> list[RecurringRule]:
        stmt = (
            select(RecurringRule)
            .options(joinedload(RecurringRule.category))
            .where(RecurringRule.user_id == self.user_id)
            .order_by(RecurringRule.next_occurrence)
        )
        return self.session.scalars(stmt).all()

    def create(self, data: RecurringRuleIn) -> RecurringRule:
        category = self.session.get(Category, data.category_id)
        if not category or category.user_id != self.user_id:
            raise ValueError("Category not found")
        if category.type != data.type:
            raise ValueError("Category type mismatch")
        rule = RecurringRule(
            user_id=self.user_id,
            name=data.name,
            type=data.type,
            amount_cents=data.amount_cents,
            category_id=data.category_id,
            anchor_date=data.anchor_date,
            interval_unit=data.interval_unit,
            interval_count=data.interval_count,
            next_occurrence=data.next_occurrence,
            end_date=data.end_date,
            auto_post=data.auto_post,
            skip_weekends=data.skip_weekends,
            month_day_policy=data.month_day_policy,
        )
        self.session.add(rule)
        self.session.commit()
        self.session.refresh(rule)
        return rule

    def update(self, rule_id: int, data: RecurringRuleIn) -> RecurringRule:
        rule = self.session.get(RecurringRule, rule_id)
        if not rule or rule.user_id != self.user_id:
            raise ValueError("Rule not found")
        if data.category_id != rule.category_id:
            category = self.session.get(Category, data.category_id)
            if not category or category.user_id != self.user_id:
                raise ValueError("Category not found")
            if category.type != data.type:
                raise ValueError("Category type mismatch")
        for field, value in data.model_dump().items():
            setattr(rule, field, value)
        self.session.commit()
        self.session.refresh(rule)
        return rule

    def toggle_auto_post(self, rule_id: int, auto_post: bool) -> None:
        rule = self.session.get(RecurringRule, rule_id)
        if not rule or rule.user_id != self.user_id:
            raise ValueError("Rule not found")
        rule.auto_post = auto_post
        self.session.commit()

    def catch_up_all(self) -> int:
        engine = RecurringEngine(self.session)
        return engine.post_due_rules()


class CSVService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def _category_lookup(self) -> dict[tuple[TransactionType, str], int]:
        stmt = select(Category.id, Category.type, Category.name).where(
            Category.user_id == self.user_id, Category.archived_at.is_(None)
        )
        lookup: dict[tuple[TransactionType, str], int] = {}
        for row in self.session.execute(stmt):
            lookup[(row.type, row.name.lower())] = row.id
        return lookup

    def preview(self, content: str) -> tuple[list[dict[str, object]], list[str]]:
        rows, errors = parse_csv(content)
        lookup = self._category_lookup()
        preview_rows: list[dict[str, object]] = []
        for row in rows:
            category_id = lookup.get((row.type, row.category.lower()))
            if not category_id:
                errors.append(f"Missing category '{row.category}' for {row.type.value}")
            preview_rows.append(
                {
                    "date": row.date,
                    "type": row.type.value,
                    "amount_cents": row.amount_cents,
                    "category": row.category,
                    "note": row.note,
                    "category_id": category_id,
                }
            )
        return preview_rows, errors

    def commit(self, content: str) -> int:
        preview_rows, errors = self.preview(content)
        if errors:
            raise ValueError("; ".join(errors))
        for row in preview_rows:
            txn = Transaction(
                user_id=self.user_id,
                date=row["date"],
                type=TransactionType(row["type"]),
                amount_cents=row["amount_cents"],
                category_id=row["category_id"],
                note=row["note"],
            )
            self.session.add(txn)
        self.session.commit()
        return len(preview_rows)

    def export(self, transactions: list[Transaction]) -> str:
        return export_transactions(transactions)
