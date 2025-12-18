from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import case, delete, func, or_, select, tuple_, update
from sqlalchemy.orm import Session, aliased, joinedload

from rapidfuzz.distance import Levenshtein

from config import get_settings
from models import (
    BalanceAnchor,
    BudgetFrequency,
    BudgetOverride,
    BudgetTemplate,
    Category,
    CurrencyCode,
    MonthlyRollup,
    ReimbursementAllocation,
    RecurringRule,
    Rule,
    RuleMatchType,
    Tag,
    transaction_tags,
    Transaction,
    TransactionType,
)
from periods import Period
from recurrence import RecurringEngine
from csv_utils import export_transactions, parse_csv
from schemas import (
    BalanceAnchorIn,
    BudgetOverrideIn,
    BudgetTemplateIn,
    CategoryIn,
    IngestTransactionIn,
    RecurringRuleIn,
    ReportOptions,
    RuleIn,
    TransactionIn,
)


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - date.resolution
    return date(year, month + 1, 1) - date.resolution


def recompute_monthly_rollup(
    session: Session, user_id: int, year: int, month: int
) -> None:
    start = _month_start(year, month)
    end = _month_end(year, month)

    income = int(
        session.execute(
            select(func.coalesce(func.sum(Transaction.amount_cents), 0)).where(
                Transaction.user_id == user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.income,
                Transaction.is_reimbursement.is_(False),
                Transaction.date.between(start, end),
            )
        ).scalar_one()
        or 0
    )

    expense_gross = int(
        session.execute(
            select(func.coalesce(func.sum(Transaction.amount_cents), 0)).where(
                Transaction.user_id == user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
                Transaction.date.between(start, end),
            )
        ).scalar_one()
        or 0
    )

    ExpenseTxn = aliased(Transaction)
    ReimbursementTxn = aliased(Transaction)
    reimbursed = int(
        session.execute(
            select(func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0))
            .join(
                ExpenseTxn,
                ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
            )
            .join(
                ReimbursementTxn,
                ReimbursementAllocation.reimbursement_transaction_id
                == ReimbursementTxn.id,
            )
            .where(
                ReimbursementAllocation.user_id == user_id,
                ExpenseTxn.user_id == user_id,
                ReimbursementTxn.user_id == user_id,
                ExpenseTxn.deleted_at.is_(None),
                ExpenseTxn.type == TransactionType.expense,
                ExpenseTxn.date.between(start, end),
                ReimbursementTxn.deleted_at.is_(None),
                ReimbursementTxn.type == TransactionType.income,
                ReimbursementTxn.is_reimbursement.is_(True),
            )
        ).scalar_one()
        or 0
    )

    expenses = max(0, expense_gross - reimbursed)

    rollup = session.scalar(
        select(MonthlyRollup).where(
            MonthlyRollup.user_id == user_id,
            MonthlyRollup.year == year,
            MonthlyRollup.month == month,
        )
    )
    if income == 0 and expenses == 0:
        if rollup:
            session.delete(rollup)
        return

    if not rollup:
        rollup = MonthlyRollup(
            user_id=user_id,
            year=year,
            month=month,
            income_cents=0,
            expense_cents=0,
        )
        session.add(rollup)
        session.flush()

    rollup.income_cents = income
    rollup.expense_cents = expenses


def recompute_monthly_rollup_for_date(
    session: Session, user_id: int, txn_date: date
) -> None:
    recompute_monthly_rollup(session, user_id, txn_date.year, txn_date.month)


def get_current_user_id() -> int:
    return 1


def cents_to_euros(cents: int) -> float:
    return cents / 100


def rebuild_monthly_rollups(session: Session, user_id: int) -> None:
    session.execute(delete(MonthlyRollup).where(MonthlyRollup.user_id == user_id))
    session.flush()

    year = func.strftime("%Y", Transaction.date).label("year")
    month = func.strftime("%m", Transaction.date).label("month")
    keys = session.execute(
        select(year, month)
        .where(Transaction.user_id == user_id, Transaction.deleted_at.is_(None))
        .group_by(year, month)
    ).all()

    for row in keys:
        y = int(row.year)
        m = int(row.month)
        recompute_monthly_rollup(session, user_id, y, m)

    session.commit()


@dataclass
class TransactionFilters:
    type: Optional[TransactionType] = None
    category_id: Optional[int] = None
    query: Optional[str] = None
    tag_id: Optional[int] = None


class TagService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def list_all(self) -> list[Tag]:
        stmt = select(Tag).where(Tag.user_id == self.user_id).order_by(Tag.name)
        return self.session.scalars(stmt).all()

    def get_or_create(self, name: str) -> Tag:
        clean_name = name.strip()
        if not clean_name:
            raise ValueError("Tag name cannot be empty")

        stmt = select(Tag).where(
            Tag.user_id == self.user_id, func.lower(Tag.name) == clean_name.lower()
        )
        existing = self.session.scalar(stmt)
        if existing:
            return existing

        tag = Tag(user_id=self.user_id, name=clean_name)
        self.session.add(tag)
        self.session.flush()
        return tag

    def create(self, name: str, is_hidden_from_budget: bool = False) -> Tag:
        clean_name = name.strip()
        if not clean_name:
            raise ValueError("Tag name cannot be empty")

        stmt = select(Tag).where(
            Tag.user_id == self.user_id, func.lower(Tag.name) == clean_name.lower()
        )
        existing = self.session.scalar(stmt)
        if existing:
            raise ValueError("Tag already exists")

        tag = Tag(
            user_id=self.user_id,
            name=clean_name,
            is_hidden_from_budget=is_hidden_from_budget,
        )
        self.session.add(tag)
        self.session.commit()
        self.session.refresh(tag)
        return tag

    def update(self, tag_id: int, name: str, is_hidden_from_budget: bool) -> Tag:
        tag = self.session.get(Tag, tag_id)
        if not tag or tag.user_id != self.user_id:
            raise ValueError("Tag not found")

        clean_name = name.strip()
        if not clean_name:
            raise ValueError("Tag name cannot be empty")

        stmt = select(Tag).where(
            Tag.user_id == self.user_id,
            func.lower(Tag.name) == clean_name.lower(),
            Tag.id != tag_id,
        )
        if self.session.scalar(stmt):
            raise ValueError("Tag with this name already exists")

        tag.name = clean_name
        tag.is_hidden_from_budget = is_hidden_from_budget
        self.session.commit()
        self.session.refresh(tag)
        return tag

    def delete(self, tag_id: int) -> None:
        tag = self.session.get(Tag, tag_id)
        if not tag or tag.user_id != self.user_id:
            raise ValueError("Tag not found")

        self.session.execute(
            delete(transaction_tags).where(transaction_tags.c.tag_id == tag.id)
        )
        self.session.execute(
            update(Rule)
            .where(Rule.user_id == self.user_id, Rule.budget_exclude_tag_id == tag.id)
            .values(budget_exclude_tag_id=None)
        )
        self.session.delete(tag)
        self.session.commit()


class RuleService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def list_all(self) -> list[Rule]:
        stmt = (
            select(Rule)
            .options(
                joinedload(Rule.set_category),
                joinedload(Rule.budget_exclude_tag),
            )
            .where(Rule.user_id == self.user_id)
            .order_by(Rule.priority.asc(), Rule.id.asc())
        )
        return self.session.scalars(stmt).all()

    def get(self, rule_id: int) -> Rule:
        rule = self.session.get(Rule, rule_id)
        if not rule or rule.user_id != self.user_id:
            raise ValueError("Rule not found")
        return rule

    def create(self, data: RuleIn) -> Rule:
        category_id = data.set_category_id
        if category_id is not None:
            category = self.session.get(Category, category_id)
            if not category or category.user_id != self.user_id:
                raise ValueError("Category not found")
            if data.transaction_type and category.type != data.transaction_type:
                raise ValueError("Category type mismatch")

        budget_exclude_tag_id = data.budget_exclude_tag_id
        if budget_exclude_tag_id is not None:
            tag = self.session.get(Tag, budget_exclude_tag_id)
            if not tag or tag.user_id != self.user_id:
                raise ValueError("Tag not found")

        rule = Rule(
            user_id=self.user_id,
            name=data.name.strip(),
            enabled=data.enabled,
            priority=data.priority,
            match_type=data.match_type,
            match_value=data.match_value.strip(),
            transaction_type=data.transaction_type,
            min_amount_cents=data.min_amount_cents,
            max_amount_cents=data.max_amount_cents,
            set_category_id=category_id,
            add_tags_json=json.dumps([t.strip() for t in data.add_tags if t.strip()]),
            budget_exclude_tag_id=budget_exclude_tag_id,
        )
        self.session.add(rule)
        self.session.commit()
        self.session.refresh(rule)
        return rule

    def update(self, rule_id: int, data: RuleIn) -> Rule:
        rule = self.get(rule_id)

        category_id = data.set_category_id
        if category_id is not None:
            category = self.session.get(Category, category_id)
            if not category or category.user_id != self.user_id:
                raise ValueError("Category not found")
            if data.transaction_type and category.type != data.transaction_type:
                raise ValueError("Category type mismatch")

        budget_exclude_tag_id = data.budget_exclude_tag_id
        if budget_exclude_tag_id is not None:
            tag = self.session.get(Tag, budget_exclude_tag_id)
            if not tag or tag.user_id != self.user_id:
                raise ValueError("Tag not found")

        rule.name = data.name.strip()
        rule.enabled = data.enabled
        rule.priority = data.priority
        rule.match_type = data.match_type
        rule.match_value = data.match_value.strip()
        rule.transaction_type = data.transaction_type
        rule.min_amount_cents = data.min_amount_cents
        rule.max_amount_cents = data.max_amount_cents
        rule.set_category_id = category_id
        rule.add_tags_json = json.dumps([t.strip() for t in data.add_tags if t.strip()])
        rule.budget_exclude_tag_id = budget_exclude_tag_id

        self.session.commit()
        self.session.refresh(rule)
        return rule

    def toggle(self, rule_id: int, enabled: bool) -> None:
        rule = self.get(rule_id)
        rule.enabled = enabled
        self.session.commit()

    def delete(self, rule_id: int) -> None:
        rule = self.get(rule_id)
        self.session.delete(rule)
        self.session.commit()

    def apply_rules(self, txn: Transaction) -> dict[str, object]:
        """
        Apply enabled rules to a transaction (category + tags only).
        Returns a lightweight summary for UI/debugging.
        """
        stmt = (
            select(Rule)
            .options(joinedload(Rule.set_category), joinedload(Rule.budget_exclude_tag))
            .where(Rule.user_id == self.user_id, Rule.enabled.is_(True))
            .order_by(Rule.priority.asc(), Rule.id.asc())
        )
        rules = self.session.scalars(stmt).all()
        if not rules:
            return {"matched": 0, "applied": 0}

        note = (txn.note or "").strip()
        note_lower = note.lower()

        applied = 0
        matched = 0
        category_set = False

        existing_tag_names = {t.name.lower() for t in (txn.tags or [])}

        def matches(rule: Rule) -> bool:
            if rule.transaction_type and rule.transaction_type != txn.type:
                return False
            if (
                rule.min_amount_cents is not None
                and txn.amount_cents < rule.min_amount_cents
            ):
                return False
            if (
                rule.max_amount_cents is not None
                and txn.amount_cents > rule.max_amount_cents
            ):
                return False

            needle = (rule.match_value or "").strip()
            if not needle:
                return False
            if rule.match_type == RuleMatchType.contains:
                return needle.lower() in note_lower
            if rule.match_type == RuleMatchType.equals:
                return note_lower == needle.lower()
            if rule.match_type == RuleMatchType.starts_with:
                return note_lower.startswith(needle.lower())
            if rule.match_type == RuleMatchType.regex:
                try:
                    return re.search(needle, note, flags=re.IGNORECASE) is not None
                except re.error:
                    return False
            return False

        tag_service = TagService(self.session, self.user_id)

        for rule in rules:
            if not matches(rule):
                continue
            matched += 1

            if rule.set_category_id and not category_set:
                cat = rule.set_category
                if cat and cat.user_id == self.user_id and cat.type == txn.type:
                    if txn.category_id != cat.id:
                        txn.category_id = cat.id
                        applied += 1
                    category_set = True

            add_names: list[str] = []
            if rule.add_tags_json:
                try:
                    add_names.extend(json.loads(rule.add_tags_json) or [])
                except Exception:
                    add_names = []
            if rule.budget_exclude_tag:
                add_names.append(rule.budget_exclude_tag.name)

            for name in add_names:
                clean = str(name).strip()
                if not clean:
                    continue
                if clean.lower() in existing_tag_names:
                    continue
                tag = tag_service.get_or_create(clean)
                txn.tags.append(tag)
                existing_tag_names.add(clean.lower())
                applied += 1

        return {"matched": matched, "applied": applied}


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

    def restore(self, category_id: int) -> None:
        category = self.session.get(Category, category_id)
        if not category or category.user_id != self.user_id:
            raise ValueError("Category not found")
        category.archived_at = None
        self.session.commit()


class TransactionService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def has_any(self) -> bool:
        stmt = select(func.count(Transaction.id)).where(
            Transaction.user_id == self.user_id,
            Transaction.deleted_at.is_(None),
        )
        return (self.session.execute(stmt).scalar_one() or 0) > 0

    def create(self, data: TransactionIn) -> Transaction:
        category = self.session.get(Category, data.category_id)
        if not category or category.user_id != self.user_id:
            raise ValueError("Category not found")
        if category.type != data.type:
            raise ValueError("Category type mismatch")
        if data.is_reimbursement and data.type != TransactionType.income:
            raise ValueError("Reimbursements must be income transactions")
        is_reimbursement = (
            bool(data.is_reimbursement)
            if data.type == TransactionType.income
            else False
        )
        txn = Transaction(
            user_id=self.user_id,
            date=data.date,
            occurred_at=data.occurred_at,
            type=data.type,
            is_reimbursement=is_reimbursement,
            amount_cents=data.amount_cents,
            category_id=data.category_id,
            note=data.note,
        )
        if data.tags:
            tag_service = TagService(self.session, self.user_id)
            tags: list[Tag] = []
            tag_ids: set[int] = set()
            for name in data.tags:
                tag = tag_service.get_or_create(name)
                if tag.id not in tag_ids:
                    tags.append(tag)
                    tag_ids.add(tag.id)
            txn.tags = tags

        self.session.add(txn)
        RuleService(self.session, self.user_id).apply_rules(txn)
        self.session.flush()
        recompute_monthly_rollup_for_date(self.session, self.user_id, data.date)
        period = Period("transaction", data.date, data.date)
        metrics = MetricsService(self.session, self.user_id)
        metrics._invalidate_period_cache(period)
        self.session.commit()
        self.session.refresh(txn)
        return txn

    def get(self, transaction_id: int, *, include_deleted: bool = False) -> Transaction:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category), joinedload(Transaction.tags))
            .where(
                Transaction.user_id == self.user_id, Transaction.id == transaction_id
            )
        )
        if not include_deleted:
            stmt = stmt.where(Transaction.deleted_at.is_(None))
        txn = self.session.scalar(stmt)
        if not txn:
            raise ValueError("Transaction not found")
        return txn

    def update(self, transaction_id: int, data: TransactionIn) -> Transaction:
        txn = self.get(transaction_id, include_deleted=False)
        category = self.session.get(Category, data.category_id)
        if not category or category.user_id != self.user_id:
            raise ValueError("Category not found")
        if category.type != data.type:
            raise ValueError("Category type mismatch")

        old_date = txn.date
        old_type = txn.type
        old_is_reimbursement = txn.is_reimbursement

        if old_type == TransactionType.expense and data.type == TransactionType.income:
            has_allocations_in = int(
                self.session.execute(
                    select(func.count(ReimbursementAllocation.id)).where(
                        ReimbursementAllocation.user_id == self.user_id,
                        ReimbursementAllocation.expense_transaction_id == txn.id,
                    )
                ).scalar_one()
                or 0
            )
            if has_allocations_in:
                raise ValueError(
                    "Cannot convert reimbursed expense to income; remove allocations first"
                )

        if old_type == TransactionType.income and data.type == TransactionType.expense:
            has_allocations_out = int(
                self.session.execute(
                    select(func.count(ReimbursementAllocation.id)).where(
                        ReimbursementAllocation.user_id == self.user_id,
                        ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                    )
                ).scalar_one()
                or 0
            )
            if has_allocations_out:
                raise ValueError(
                    "Cannot convert reimbursement income to expense; remove allocations first"
                )

        txn.date = data.date
        txn.occurred_at = data.occurred_at
        txn.type = data.type
        txn.amount_cents = data.amount_cents
        txn.category_id = data.category_id
        txn.note = data.note
        if txn.type == TransactionType.income and data.is_reimbursement is not None:
            txn.is_reimbursement = bool(data.is_reimbursement)
        if txn.type == TransactionType.expense:
            txn.is_reimbursement = False

        if data.tags is not None:
            tag_service = TagService(self.session, self.user_id)
            tags: list[Tag] = []
            tag_ids: set[int] = set()
            for name in data.tags:
                tag = tag_service.get_or_create(name)
                if tag.id not in tag_ids:
                    tags.append(tag)
                    tag_ids.add(tag.id)
            txn.tags = tags

        RuleService(self.session, self.user_id).apply_rules(txn)

        allocations_deleted_expense_dates: list[date] = []
        if (
            old_type == TransactionType.income
            and old_is_reimbursement
            and txn.type == TransactionType.income
            and not txn.is_reimbursement
        ):
            allocations_deleted_expense_dates = [
                row[0]
                for row in self.session.execute(
                    select(Transaction.date)
                    .join(
                        ReimbursementAllocation,
                        ReimbursementAllocation.expense_transaction_id
                        == Transaction.id,
                    )
                    .where(
                        ReimbursementAllocation.user_id == self.user_id,
                        ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                    )
                ).all()
            ]
            self.session.execute(
                delete(ReimbursementAllocation).where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                )
            )

        if txn.type == TransactionType.expense:
            reimbursed_total = int(
                self.session.execute(
                    select(
                        func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0)
                    ).where(
                        ReimbursementAllocation.user_id == self.user_id,
                        ReimbursementAllocation.expense_transaction_id == txn.id,
                    )
                ).scalar_one()
                or 0
            )
            if txn.amount_cents < reimbursed_total:
                raise ValueError("Expense amount cannot be less than reimbursed total")

        if txn.type == TransactionType.income and txn.is_reimbursement:
            allocated_total = int(
                self.session.execute(
                    select(
                        func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0)
                    ).where(
                        ReimbursementAllocation.user_id == self.user_id,
                        ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                    )
                ).scalar_one()
                or 0
            )
            if txn.amount_cents < allocated_total:
                raise ValueError(
                    "Reimbursement amount cannot be less than allocated total"
                )

        self.session.flush()

        months_to_recompute: set[tuple[int, int]] = {
            (old_date.year, old_date.month),
            (txn.date.year, txn.date.month),
        }
        for d in allocations_deleted_expense_dates:
            months_to_recompute.add((d.year, d.month))
        for y, m in months_to_recompute:
            recompute_monthly_rollup(self.session, self.user_id, y, m)

        metrics = MetricsService(self.session, self.user_id)
        metrics._invalidate_period_cache(Period("transaction", old_date, old_date))
        metrics._invalidate_period_cache(Period("transaction", data.date, data.date))

        self.session.commit()
        self.session.refresh(txn)
        return txn

    def list(
        self,
        period: Period,
        filters: TransactionFilters,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Transaction]:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category), joinedload(Transaction.tags))
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(period.start, period.end),
            )
            .order_by(Transaction.occurred_at.desc(), Transaction.id.desc())
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
        if filters.tag_id:
            stmt = stmt.join(Transaction.tags).where(Tag.id == filters.tag_id)
        return self.session.scalars(stmt).unique().all()

    def all_for_period(
        self, period: Period, filters: Optional[TransactionFilters] = None
    ) -> list[Transaction]:
        filters = filters or TransactionFilters()
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category), joinedload(Transaction.tags))
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(period.start, period.end),
            )
            .order_by(Transaction.occurred_at.asc(), Transaction.id.asc())
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
        if filters.tag_id:
            stmt = stmt.join(Transaction.tags).where(Tag.id == filters.tag_id)
        return self.session.scalars(stmt).unique().all()

    def recent(self, limit: int = 10) -> list[Transaction]:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category), joinedload(Transaction.tags))
            .where(
                Transaction.user_id == self.user_id, Transaction.deleted_at.is_(None)
            )
            .order_by(Transaction.occurred_at.desc(), Transaction.id.desc())
            .limit(limit)
        )
        return self.session.scalars(stmt).all()

    def soft_delete(self, transaction_id: int) -> None:
        txn = self.session.get(Transaction, transaction_id)
        if not txn or txn.user_id != self.user_id:
            raise ValueError("Transaction not found")
        if txn.deleted_at is not None:
            return
        txn.deleted_at = datetime.utcnow()
        self.session.flush()

        months_to_recompute: set[tuple[int, int]] = {(txn.date.year, txn.date.month)}
        if txn.type == TransactionType.income and txn.is_reimbursement:
            expense_dates = self.session.execute(
                select(Transaction.date)
                .join(
                    ReimbursementAllocation,
                    ReimbursementAllocation.expense_transaction_id == Transaction.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                )
            ).all()
            for row in expense_dates:
                d = row[0]
                months_to_recompute.add((d.year, d.month))
        for y, m in months_to_recompute:
            recompute_monthly_rollup(self.session, self.user_id, y, m)

        period = Period("transaction", txn.date, txn.date)
        metrics = MetricsService(self.session, self.user_id)
        metrics._invalidate_period_cache(period)
        self.session.commit()

    def restore(self, transaction_id: int) -> None:
        txn = self.session.get(Transaction, transaction_id)
        if not txn or txn.user_id != self.user_id:
            raise ValueError("Transaction not found")
        if txn.deleted_at is None:
            return
        txn.deleted_at = None
        self.session.flush()
        months_to_recompute: set[tuple[int, int]] = {(txn.date.year, txn.date.month)}
        if txn.type == TransactionType.income and txn.is_reimbursement:
            expense_dates = self.session.execute(
                select(Transaction.date)
                .join(
                    ReimbursementAllocation,
                    ReimbursementAllocation.expense_transaction_id == Transaction.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                )
            ).all()
            for row in expense_dates:
                d = row[0]
                months_to_recompute.add((d.year, d.month))
        for y, m in months_to_recompute:
            recompute_monthly_rollup(self.session, self.user_id, y, m)
        metrics = MetricsService(self.session, self.user_id)
        metrics._invalidate_period_cache(Period("transaction", txn.date, txn.date))
        self.session.commit()

    def deleted(self, limit: int = 200) -> list[Transaction]:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category), joinedload(Transaction.tags))
            .where(
                Transaction.user_id == self.user_id, Transaction.deleted_at.isnot(None)
            )
            .order_by(Transaction.deleted_at.desc(), Transaction.id.desc())
            .limit(limit)
        )
        return self.session.scalars(stmt).all()


class ReimbursementService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def set_reimbursement(self, transaction_id: int, is_reimbursement: bool) -> None:
        txn = self.session.get(Transaction, transaction_id)
        if not txn or txn.user_id != self.user_id:
            raise ValueError("Transaction not found")
        if txn.type != TransactionType.income:
            raise ValueError("Only income transactions can be reimbursements")
        if txn.is_reimbursement == is_reimbursement:
            return

        affected_expense_months: set[tuple[int, int]] = set()
        if not is_reimbursement:
            expense_dates = self.session.execute(
                select(Transaction.date)
                .join(
                    ReimbursementAllocation,
                    ReimbursementAllocation.expense_transaction_id == Transaction.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                )
            ).all()
            for row in expense_dates:
                d = row[0]
                affected_expense_months.add((d.year, d.month))
            self.session.execute(
                delete(ReimbursementAllocation).where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id == txn.id,
                )
            )

        txn.is_reimbursement = is_reimbursement
        self.session.flush()

        recompute_monthly_rollup_for_date(self.session, self.user_id, txn.date)
        for y, m in affected_expense_months:
            recompute_monthly_rollup(self.session, self.user_id, y, m)
        self.session.commit()

    def allocated_total_for_reimbursement(
        self, reimbursement_transaction_id: int
    ) -> int:
        expense = aliased(Transaction)
        return int(
            self.session.execute(
                select(func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0))
                .join(
                    expense,
                    ReimbursementAllocation.expense_transaction_id == expense.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == reimbursement_transaction_id,
                    expense.user_id == self.user_id,
                    expense.deleted_at.is_(None),
                    expense.type == TransactionType.expense,
                )
            ).scalar_one()
            or 0
        )

    def reimbursed_total_for_expense(self, expense_transaction_id: int) -> int:
        active_reimb = aliased(Transaction)
        return int(
            self.session.execute(
                select(func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0))
                .join(
                    active_reimb,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == active_reimb.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.expense_transaction_id
                    == expense_transaction_id,
                    active_reimb.deleted_at.is_(None),
                    active_reimb.type == TransactionType.income,
                    active_reimb.is_reimbursement.is_(True),
                )
            ).scalar_one()
            or 0
        )

    def allocations_for_reimbursement(
        self, reimbursement_transaction_id: int
    ) -> list[ReimbursementAllocation]:
        expense = aliased(Transaction)
        stmt = (
            select(ReimbursementAllocation)
            .join(expense, ReimbursementAllocation.expense_transaction_id == expense.id)
            .options(
                joinedload(ReimbursementAllocation.expense_transaction).joinedload(
                    Transaction.category
                )
            )
            .where(
                ReimbursementAllocation.user_id == self.user_id,
                ReimbursementAllocation.reimbursement_transaction_id
                == reimbursement_transaction_id,
            )
            .order_by(expense.date.desc(), expense.id.desc())
        )
        return self.session.scalars(stmt).all()

    def allocations_for_expense(
        self, expense_transaction_id: int
    ) -> list[ReimbursementAllocation]:
        reimb = aliased(Transaction)
        stmt = (
            select(ReimbursementAllocation)
            .join(
                reimb, ReimbursementAllocation.reimbursement_transaction_id == reimb.id
            )
            .options(
                joinedload(
                    ReimbursementAllocation.reimbursement_transaction
                ).joinedload(Transaction.category)
            )
            .where(
                ReimbursementAllocation.user_id == self.user_id,
                ReimbursementAllocation.expense_transaction_id
                == expense_transaction_id,
            )
            .order_by(reimb.date.desc(), reimb.id.desc())
        )
        return self.session.scalars(stmt).all()

    def upsert_allocation(
        self,
        reimbursement_transaction_id: int,
        expense_transaction_id: int,
        amount_cents: int,
    ) -> ReimbursementAllocation:
        if amount_cents <= 0:
            raise ValueError("Allocation amount must be positive")
        reimbursement = self.session.get(Transaction, reimbursement_transaction_id)
        if not reimbursement or reimbursement.user_id != self.user_id:
            raise ValueError("Reimbursement transaction not found")
        if reimbursement.deleted_at is not None:
            raise ValueError("Reimbursement transaction is deleted")
        if (
            reimbursement.type != TransactionType.income
            or not reimbursement.is_reimbursement
        ):
            raise ValueError("Transaction is not marked as a reimbursement")

        expense = self.session.get(Transaction, expense_transaction_id)
        if not expense or expense.user_id != self.user_id:
            raise ValueError("Expense transaction not found")
        if expense.deleted_at is not None:
            raise ValueError("Expense transaction is deleted")
        if expense.type != TransactionType.expense:
            raise ValueError("Allocations can only target expense transactions")

        existing = self.session.scalar(
            select(ReimbursementAllocation).where(
                ReimbursementAllocation.user_id == self.user_id,
                ReimbursementAllocation.reimbursement_transaction_id
                == reimbursement_transaction_id,
                ReimbursementAllocation.expense_transaction_id
                == expense_transaction_id,
            )
        )

        allocated_other = int(
            self.session.execute(
                select(func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0))
                .join(
                    Transaction,
                    ReimbursementAllocation.expense_transaction_id == Transaction.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == reimbursement_transaction_id,
                    ReimbursementAllocation.expense_transaction_id
                    != expense_transaction_id,
                    Transaction.user_id == self.user_id,
                    Transaction.deleted_at.is_(None),
                    Transaction.type == TransactionType.expense,
                )
            ).scalar_one()
            or 0
        )
        if allocated_other + amount_cents > reimbursement.amount_cents:
            raise ValueError("Allocation exceeds reimbursement amount")

        reimbursed_other = int(
            self.session.execute(
                select(
                    func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0)
                ).where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.expense_transaction_id
                    == expense_transaction_id,
                    ReimbursementAllocation.reimbursement_transaction_id
                    != reimbursement_transaction_id,
                )
            ).scalar_one()
            or 0
        )
        if reimbursed_other + amount_cents > expense.amount_cents:
            raise ValueError("Allocation exceeds expense amount")

        if existing:
            existing.amount_cents = amount_cents
            allocation = existing
        else:
            allocation = ReimbursementAllocation(
                user_id=self.user_id,
                reimbursement_transaction_id=reimbursement_transaction_id,
                expense_transaction_id=expense_transaction_id,
                amount_cents=amount_cents,
            )
            self.session.add(allocation)

        self.session.flush()
        recompute_monthly_rollup_for_date(self.session, self.user_id, expense.date)
        self.session.commit()
        self.session.refresh(allocation)
        return allocation

    def delete_allocation(self, allocation_id: int) -> None:
        allocation = self.session.get(ReimbursementAllocation, allocation_id)
        if not allocation or allocation.user_id != self.user_id:
            raise ValueError("Allocation not found")
        expense = self.session.get(Transaction, allocation.expense_transaction_id)
        expense_date = expense.date if expense else None
        self.session.delete(allocation)
        self.session.flush()
        if expense_date:
            recompute_monthly_rollup_for_date(self.session, self.user_id, expense_date)
        self.session.commit()

    def search_expenses_for_reimbursement(
        self, reimbursement_transaction_id: int, *, query: str, limit: int = 25
    ) -> list[dict[str, object]]:
        reimbursement = self.session.get(Transaction, reimbursement_transaction_id)
        if not reimbursement or reimbursement.user_id != self.user_id:
            raise ValueError("Reimbursement transaction not found")
        if reimbursement.deleted_at is not None:
            raise ValueError("Reimbursement transaction is deleted")
        if (
            reimbursement.type != TransactionType.income
            or not reimbursement.is_reimbursement
        ):
            raise ValueError("Transaction is not marked as a reimbursement")

        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
            )
        )
        query_clean = query.strip()
        if query_clean:
            like = f"%{query_clean.lower()}%"
            stmt = stmt.join(Category, Category.id == Transaction.category_id).where(
                or_(
                    func.lower(func.coalesce(Transaction.note, "")).like(like),
                    func.lower(Category.name).like(like),
                )
            )
        stmt = stmt.order_by(Transaction.date.desc(), Transaction.id.desc()).limit(
            limit
        )
        expenses = self.session.scalars(stmt).all()
        if not expenses:
            return []

        expense_ids = [e.id for e in expenses]

        active_reimb = aliased(Transaction)
        reimbursed_totals = {
            int(r.expense_transaction_id): int(r.total or 0)
            for r in self.session.execute(
                select(
                    ReimbursementAllocation.expense_transaction_id,
                    func.coalesce(
                        func.sum(ReimbursementAllocation.amount_cents), 0
                    ).label("total"),
                )
                .join(
                    active_reimb,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == active_reimb.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.expense_transaction_id.in_(expense_ids),
                    active_reimb.deleted_at.is_(None),
                    active_reimb.type == TransactionType.income,
                    active_reimb.is_reimbursement.is_(True),
                )
                .group_by(ReimbursementAllocation.expense_transaction_id)
            )
        }

        allocated_to_this = {
            int(r.expense_transaction_id): int(r.total or 0)
            for r in self.session.execute(
                select(
                    ReimbursementAllocation.expense_transaction_id,
                    func.coalesce(
                        func.sum(ReimbursementAllocation.amount_cents), 0
                    ).label("total"),
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == reimbursement_transaction_id,
                    ReimbursementAllocation.expense_transaction_id.in_(expense_ids),
                )
                .group_by(ReimbursementAllocation.expense_transaction_id)
            )
        }

        remaining_reimbursement = max(
            0,
            reimbursement.amount_cents
            - self.allocated_total_for_reimbursement(reimbursement_transaction_id),
        )

        results: list[dict[str, object]] = []
        for expense in expenses:
            reimbursed_total = int(reimbursed_totals.get(expense.id, 0))
            remaining_unreimbursed = max(0, expense.amount_cents - reimbursed_total)
            suggested = min(remaining_reimbursement, remaining_unreimbursed)
            results.append(
                {
                    "expense": expense,
                    "reimbursed_total_cents": reimbursed_total,
                    "remaining_unreimbursed_cents": remaining_unreimbursed,
                    "allocated_to_this_cents": int(
                        allocated_to_this.get(expense.id, 0)
                    ),
                    "suggested_amount_cents": suggested,
                }
            )
        return results


class IngestCategoryNotFound(ValueError):
    pass


class IngestCategoryAmbiguous(ValueError):
    pass


class IngestService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def ingest_expense(self, data: IngestTransactionIn) -> Transaction:
        now_local = (
            datetime.now(ZoneInfo(get_settings().timezone))
            .replace(tzinfo=None)
            .replace(second=0, microsecond=0)
        )
        txn_date = data.date or now_local.date()

        category_name_raw = (data.category or "").strip()
        if category_name_raw:
            input_lower = category_name_raw.lower()
            exact = self.session.scalar(
                select(Category).where(
                    Category.user_id == self.user_id,
                    Category.type == TransactionType.expense,
                    Category.archived_at.is_(None),
                    func.lower(Category.name) == input_lower,
                )
            )
            if exact:
                category_id = exact.id
            else:
                categories = self.session.scalars(
                    select(Category).where(
                        Category.user_id == self.user_id,
                        Category.type == TransactionType.expense,
                        Category.archived_at.is_(None),
                    )
                ).all()
                best_distance: Optional[int] = None
                best: list[Category] = []
                for category in categories:
                    name_lower = (category.name or "").strip().lower()
                    dist = int(Levenshtein.distance(input_lower, name_lower))
                    if best_distance is None or dist < best_distance:
                        best_distance = dist
                        best = [category]
                    elif dist == best_distance:
                        best.append(category)

                if best_distance is not None and best_distance <= 1:
                    if len(best) > 1:
                        options = ", ".join(sorted({c.name for c in best}))
                        raise IngestCategoryAmbiguous(
                            f"Category '{category_name_raw}' is ambiguous; matches: {options}"
                        )
                    category_id = best[0].id
                else:
                    try:
                        created = CategoryService(self.session, self.user_id).create(
                            CategoryIn(
                                name=category_name_raw,
                                type=TransactionType.expense,
                                order=0,
                            )
                        )
                        category_id = created.id
                    except ValueError as exc:
                        existing = self.session.scalar(
                            select(Category).where(
                                Category.user_id == self.user_id,
                                Category.type == TransactionType.expense,
                                func.lower(Category.name) == input_lower,
                            )
                        )
                        if existing:
                            if existing.archived_at is not None:
                                CategoryService(self.session, self.user_id).restore(
                                    existing.id
                                )
                            category_id = existing.id
                        else:
                            raise IngestCategoryNotFound(str(exc)) from exc
        else:
            default_name = "Uncategorized"
            default = self.session.scalar(
                select(Category).where(
                    Category.user_id == self.user_id,
                    Category.type == TransactionType.expense,
                    Category.archived_at.is_(None),
                    func.lower(Category.name) == default_name.lower(),
                )
            )
            if default:
                category_id = default.id
            else:
                archived_default = self.session.scalar(
                    select(Category).where(
                        Category.user_id == self.user_id,
                        Category.type == TransactionType.expense,
                        Category.archived_at.is_not(None),
                        func.lower(Category.name) == default_name.lower(),
                    )
                )
                if archived_default:
                    CategoryService(self.session, self.user_id).restore(
                        archived_default.id
                    )
                    category_id = archived_default.id
                else:
                    created_default = CategoryService(
                        self.session, self.user_id
                    ).create(
                        CategoryIn(
                            name=default_name, type=TransactionType.expense, order=0
                        )
                    )
                    category_id = created_default.id

        txn_in = TransactionIn(
            date=txn_date,
            occurred_at=now_local,
            type=TransactionType.expense,
            amount_cents=data.amount_cents,
            category_id=category_id,
            note=data.note,
            tags=[],
        )
        return TransactionService(self.session, self.user_id).create(txn_in)


class BalanceAnchorService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def list_all(self) -> list[BalanceAnchor]:
        stmt = (
            select(BalanceAnchor)
            .where(BalanceAnchor.user_id == self.user_id)
            .order_by(BalanceAnchor.as_of_at.desc(), BalanceAnchor.id.desc())
        )
        return self.session.scalars(stmt).all()

    def create(self, data: BalanceAnchorIn) -> BalanceAnchor:
        anchor = BalanceAnchor(
            user_id=self.user_id,
            as_of_at=data.as_of_at,
            balance_cents=data.balance_cents,
            note=data.note,
        )
        self.session.add(anchor)
        self.session.commit()
        self.session.refresh(anchor)
        return anchor

    def update(self, anchor_id: int, data: BalanceAnchorIn) -> BalanceAnchor:
        anchor = self.session.get(BalanceAnchor, anchor_id)
        if not anchor or anchor.user_id != self.user_id:
            raise ValueError("Balance snapshot not found")
        anchor.as_of_at = data.as_of_at
        anchor.balance_cents = data.balance_cents
        anchor.note = data.note
        self.session.commit()
        self.session.refresh(anchor)
        return anchor

    def delete(self, anchor_id: int) -> None:
        anchor = self.session.get(BalanceAnchor, anchor_id)
        if not anchor or anchor.user_id != self.user_id:
            raise ValueError("Balance anchor not found")
        self.session.delete(anchor)
        self.session.commit()

    def balance_as_of(self, target: datetime) -> int:
        earliest = datetime(1970, 1, 1, 0, 0, 0)
        if target < earliest:
            return 0

        anchor = self.session.scalar(
            select(BalanceAnchor)
            .where(
                BalanceAnchor.user_id == self.user_id,
                BalanceAnchor.as_of_at <= target,
            )
            .order_by(BalanceAnchor.as_of_at.desc(), BalanceAnchor.id.desc())
            .limit(1)
        )
        if anchor:
            baseline = int(anchor.balance_cents)
            start = anchor.as_of_at
            if start >= target:
                return baseline
        else:
            baseline = 0
            start = earliest

        stmt = select(
            func.coalesce(
                func.sum(
                    case(
                        (
                            Transaction.type == TransactionType.income,
                            Transaction.amount_cents,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("income"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            Transaction.type == TransactionType.expense,
                            Transaction.amount_cents,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("expenses"),
        ).where(
            Transaction.user_id == self.user_id,
            Transaction.deleted_at.is_(None),
            Transaction.occurred_at > start,
            Transaction.occurred_at <= target,
        )
        row = self.session.execute(stmt).one()
        income = int(row.income)
        expenses = int(row.expenses)
        return baseline + income - expenses


class MetricsService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()
        self._category_breakdown_cache: dict[str, list[dict[str, object]]] = {}

    def _invalidate_period_cache(self, period: Period) -> None:
        period_base = f"{period.start.isoformat()}_{period.end.isoformat()}"

        for type_suffix in ["expense", "income"]:
            period_key = f"{period_base}_{type_suffix}"
            if period_key in self._category_breakdown_cache:
                del self._category_breakdown_cache[period_key]

        old_key = period_base
        if old_key in self._category_breakdown_cache:
            del self._category_breakdown_cache[old_key]

    def kpis(
        self, period: Period, *, tag_ids: Optional[list[int]] = None
    ) -> dict[str, int]:
        def month_start(d: date) -> date:
            return d.replace(day=1)

        def month_end(d: date) -> date:
            first = month_start(d)
            if first.month == 12:
                next_month = first.replace(year=first.year + 1, month=1)
            else:
                next_month = first.replace(month=first.month + 1)
            return next_month - date.resolution

        def add_months(d: date, count: int) -> date:
            month_index = (d.year * 12) + (d.month - 1) + count
            year = month_index // 12
            month = (month_index % 12) + 1
            return date(year, month, 1)

        def kpis_from_transactions(start: date, end: date) -> tuple[int, int]:
            income_stmt = select(
                func.coalesce(
                    func.sum(
                        case(
                            (
                                (Transaction.type == TransactionType.income)
                                & (Transaction.is_reimbursement.is_(False)),
                                Transaction.amount_cents,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                ).label("income")
            ).where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(start, end),
            )
            expense_stmt = select(
                func.coalesce(
                    func.sum(
                        case(
                            (
                                Transaction.type == TransactionType.expense,
                                Transaction.amount_cents,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                ).label("expenses")
            ).where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(start, end),
            )
            if tag_ids:
                income_stmt = income_stmt.where(
                    Transaction.tags.any(Tag.id.in_(tag_ids))
                )
                expense_stmt = expense_stmt.where(
                    Transaction.tags.any(Tag.id.in_(tag_ids))
                )

            income = int(self.session.execute(income_stmt).scalar_one() or 0)
            expense_gross = int(self.session.execute(expense_stmt).scalar_one() or 0)

            ExpenseTxn = aliased(Transaction)
            ReimbursementTxn = aliased(Transaction)
            reimbursed_stmt = (
                select(func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0))
                .join(
                    ExpenseTxn,
                    ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
                )
                .join(
                    ReimbursementTxn,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == ReimbursementTxn.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ExpenseTxn.user_id == self.user_id,
                    ReimbursementTxn.user_id == self.user_id,
                    ExpenseTxn.deleted_at.is_(None),
                    ExpenseTxn.type == TransactionType.expense,
                    ExpenseTxn.date.between(start, end),
                    ReimbursementTxn.deleted_at.is_(None),
                    ReimbursementTxn.type == TransactionType.income,
                    ReimbursementTxn.is_reimbursement.is_(True),
                )
            )
            if tag_ids:
                reimbursed_stmt = reimbursed_stmt.where(
                    ExpenseTxn.tags.any(Tag.id.in_(tag_ids))
                )
            reimbursed = int(self.session.execute(reimbursed_stmt).scalar_one() or 0)

            expenses = max(0, expense_gross - reimbursed)
            return income, expenses

        # Balance calculation currently ignores tags because it's account-level.
        # Ideally, we should support calculating balance for a tag (income - expense),
        # but BalanceAnchor is global.
        # For now, if tag_ids are present, "balance" in KPI means "net flow for this tag".

        balance_at_end = 0
        if not tag_ids:
            balance_at_end = BalanceAnchorService(
                self.session, self.user_id
            ).balance_as_of(datetime.combine(period.end, time.max))

        # If filtering by tags, we cannot use MonthlyRollup as it doesn't have tag info.
        if tag_ids:
            income, expenses = kpis_from_transactions(period.start, period.end)
            return {
                "income": income,
                "expenses": expenses,
                "balance": balance_at_end if not tag_ids else (income - expenses),
            }

        is_single_full_month = (
            period.start == month_start(period.start)
            and period.end == month_end(period.start)
            and period.start.year == period.end.year
            and period.start.month == period.end.month
        )
        if is_single_full_month:
            rollup = self.session.scalar(
                select(MonthlyRollup).where(
                    MonthlyRollup.user_id == self.user_id,
                    MonthlyRollup.year == period.start.year,
                    MonthlyRollup.month == period.start.month,
                )
            )
            income = rollup.income_cents if rollup else 0
            expenses = rollup.expense_cents if rollup else 0
            return {
                "income": income,
                "expenses": expenses,
                "balance": balance_at_end,
            }

        if (
            period.start.year == period.end.year
            and period.start.month == period.end.month
        ):
            income, expenses = kpis_from_transactions(period.start, period.end)
            return {
                "income": income,
                "expenses": expenses,
                "balance": balance_at_end,
            }

        start_month_end = month_end(period.start)
        end_month_start = month_start(period.end)

        start_income, start_expenses = kpis_from_transactions(
            period.start, start_month_end
        )
        end_income, end_expenses = kpis_from_transactions(end_month_start, period.end)

        full_months_start = add_months(month_start(period.start), 1)
        full_months_end = add_months(month_start(period.end), -1)
        full_income = 0
        full_expenses = 0
        if full_months_start <= full_months_end:
            start_key = full_months_start.year * 12 + (full_months_start.month - 1)
            end_key = full_months_end.year * 12 + (full_months_end.month - 1)
            stmt = select(
                func.coalesce(func.sum(MonthlyRollup.income_cents), 0).label("income"),
                func.coalesce(func.sum(MonthlyRollup.expense_cents), 0).label(
                    "expenses"
                ),
            ).where(
                MonthlyRollup.user_id == self.user_id,
                (MonthlyRollup.year * 12 + (MonthlyRollup.month - 1)).between(
                    start_key, end_key
                ),
            )
            row = self.session.execute(stmt).one()
            full_income = int(row.income)
            full_expenses = int(row.expenses)

        income = start_income + full_income + end_income
        expenses = start_expenses + full_expenses + end_expenses
        return {
            "income": income,
            "expenses": expenses,
            "balance": balance_at_end,
        }

    def kpi_sparklines(
        self,
        period: Period,
        *,
        max_points: int = 12,
        tag_ids: Optional[list[int]] = None,
    ) -> dict[str, str]:
        def month_start(d: date) -> date:
            return d.replace(day=1)

        def month_end(d: date) -> date:
            first = month_start(d)
            if first.month == 12:
                next_month = first.replace(year=first.year + 1, month=1)
            else:
                next_month = first.replace(month=first.month + 1)
            return next_month - date.resolution

        def add_months(d: date, count: int) -> date:
            month_index = (d.year * 12) + (d.month - 1) + count
            year = month_index // 12
            month = (month_index % 12) + 1
            return date(year, month, 1)

        def income_expense_between(start: date, end: date) -> tuple[int, int]:
            income_stmt = select(
                func.coalesce(
                    func.sum(
                        case(
                            (
                                (Transaction.type == TransactionType.income)
                                & (Transaction.is_reimbursement.is_(False)),
                                Transaction.amount_cents,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                ).label("income")
            ).where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(start, end),
            )
            expense_stmt = select(
                func.coalesce(
                    func.sum(
                        case(
                            (
                                Transaction.type == TransactionType.expense,
                                Transaction.amount_cents,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                ).label("expenses")
            ).where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.date.between(start, end),
            )
            if tag_ids:
                income_stmt = income_stmt.where(
                    Transaction.tags.any(Tag.id.in_(tag_ids))
                )
                expense_stmt = expense_stmt.where(
                    Transaction.tags.any(Tag.id.in_(tag_ids))
                )

            income = int(self.session.execute(income_stmt).scalar_one() or 0)
            expense_gross = int(self.session.execute(expense_stmt).scalar_one() or 0)

            ExpenseTxn = aliased(Transaction)
            ReimbursementTxn = aliased(Transaction)
            reimbursed_stmt = (
                select(func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0))
                .join(
                    ExpenseTxn,
                    ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
                )
                .join(
                    ReimbursementTxn,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == ReimbursementTxn.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ExpenseTxn.user_id == self.user_id,
                    ReimbursementTxn.user_id == self.user_id,
                    ExpenseTxn.deleted_at.is_(None),
                    ExpenseTxn.type == TransactionType.expense,
                    ExpenseTxn.date.between(start, end),
                    ReimbursementTxn.deleted_at.is_(None),
                    ReimbursementTxn.type == TransactionType.income,
                    ReimbursementTxn.is_reimbursement.is_(True),
                )
            )
            if tag_ids:
                reimbursed_stmt = reimbursed_stmt.where(
                    ExpenseTxn.tags.any(Tag.id.in_(tag_ids))
                )
            reimbursed = int(self.session.execute(reimbursed_stmt).scalar_one() or 0)

            expenses = max(0, expense_gross - reimbursed)
            return income, expenses

        def build_points(values: list[int]) -> str:
            if not values:
                return ""
            if len(values) == 1:
                values = [values[0], values[0]]
            min_v = min(values)
            max_v = max(values)
            pad_top = 2.0
            pad_bottom = 2.0
            height = 30.0
            width = 100.0
            usable_h = height - pad_top - pad_bottom
            step = width / (len(values) - 1)
            points: list[str] = []
            for idx, v in enumerate(values):
                x = idx * step
                if max_v == min_v:
                    y = height / 2
                else:
                    t = (v - min_v) / (max_v - min_v)
                    y = pad_top + (1 - t) * usable_h
                points.append(f"{x:.2f},{y:.2f}")
            return " ".join(points)

        start_month = month_start(period.start)
        end_month = month_start(period.end)
        months: list[date] = []
        current = start_month
        while current <= end_month:
            months.append(current)
            current = add_months(current, 1)

        if len(months) > max_points:
            months = months[-max_points:]

        rollup_map = {}
        if not tag_ids:
            keys = [(m.year, m.month) for m in months]
            rollups = self.session.scalars(
                select(MonthlyRollup).where(
                    MonthlyRollup.user_id == self.user_id,
                    tuple_(MonthlyRollup.year, MonthlyRollup.month).in_(keys),
                )
            ).all()
            rollup_map = {(r.year, r.month): r for r in rollups}

        income_series: list[int] = []
        expense_series: list[int] = []
        balance_series: list[int] = []
        balance_service = BalanceAnchorService(self.session, self.user_id)

        current_balance_offset = 0
        if tag_ids:
            # For tags, balance is cumulative net flow
            current_balance_offset = 0

        for month in months:
            bucket_start = month
            bucket_end = month_end(month)
            if bucket_start < period.start:
                bucket_start = period.start
            if bucket_end > period.end:
                bucket_end = period.end

            full_month = bucket_start == month and bucket_end == month_end(month)

            income = 0
            expenses = 0

            if full_month and not tag_ids:
                rollup = rollup_map.get((month.year, month.month))
                income = rollup.income_cents if rollup else 0
                expenses = rollup.expense_cents if rollup else 0
            else:
                income, expenses = income_expense_between(bucket_start, bucket_end)

            income_series.append(income)
            expense_series.append(expenses)

            if tag_ids:
                current_balance_offset += income - expenses
                balance_series.append(current_balance_offset)
            else:
                balance_series.append(
                    balance_service.balance_as_of(
                        datetime.combine(bucket_end, time.max)
                    )
                )

        return {
            "income": build_points(income_series),
            "expenses": build_points(expense_series),
            "balance": build_points(balance_series),
        }

    def category_breakdown(
        self,
        period: Period,
        transaction_type: Optional[TransactionType] = None,
        *,
        category_ids: Optional[list[int]] = None,
        tag_ids: Optional[list[int]] = None,
    ) -> list[dict[str, object]]:
        if transaction_type is None:
            transaction_type = TransactionType.expense

        type_suffix = transaction_type.value if transaction_type else "expense"
        category_suffix = (
            "all"
            if not category_ids
            else "cats_" + "_".join(str(i) for i in sorted(set(category_ids)))
        )
        tag_suffix = (
            "all"
            if not tag_ids
            else "tags_" + "_".join(str(i) for i in sorted(set(tag_ids)))
        )
        period_key = f"{period.start.isoformat()}_{period.end.isoformat()}_{type_suffix}_{category_suffix}_{tag_suffix}"
        if period_key in self._category_breakdown_cache:
            return self._category_breakdown_cache[period_key]

        if transaction_type == TransactionType.income:
            stmt = (
                select(Category.name, func.sum(Transaction.amount_cents).label("total"))
                .join(Category, Category.id == Transaction.category_id)
                .where(
                    Transaction.user_id == self.user_id,
                    Transaction.deleted_at.is_(None),
                    Transaction.type == TransactionType.income,
                    Transaction.is_reimbursement.is_(False),
                    Transaction.date.between(period.start, period.end),
                )
                .group_by(Category.name)
                .order_by(func.sum(Transaction.amount_cents).desc())
            )
            if category_ids:
                stmt = stmt.where(Transaction.category_id.in_(category_ids))
            if tag_ids:
                stmt = stmt.where(Transaction.tags.any(Tag.id.in_(tag_ids)))

            rows = self.session.execute(stmt).all()
            total = sum(row.total or 0 for row in rows)
            breakdown = []
            for row in rows:
                amount = int(row.total or 0)
                percent = (amount / total * 100) if total else 0
                breakdown.append(
                    {"name": row.name, "amount_cents": amount, "percent": percent}
                )
            self._category_breakdown_cache[period_key] = breakdown
            return breakdown

        gross_stmt = (
            select(
                Category.id.label("category_id"),
                Category.name.label("name"),
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("gross"),
            )
            .join(Category, Category.id == Transaction.category_id)
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
                Transaction.date.between(period.start, period.end),
            )
            .group_by(Category.id, Category.name)
        )
        if category_ids:
            gross_stmt = gross_stmt.where(Transaction.category_id.in_(category_ids))
        if tag_ids:
            gross_stmt = gross_stmt.where(Transaction.tags.any(Tag.id.in_(tag_ids)))
        gross_rows = self.session.execute(gross_stmt).all()

        ExpenseTxn = aliased(Transaction)
        ReimbursementTxn = aliased(Transaction)
        reimb_stmt = (
            select(
                ExpenseTxn.category_id.label("category_id"),
                func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0).label(
                    "reimbursed"
                ),
            )
            .join(
                ExpenseTxn,
                ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
            )
            .join(
                ReimbursementTxn,
                ReimbursementAllocation.reimbursement_transaction_id
                == ReimbursementTxn.id,
            )
            .where(
                ReimbursementAllocation.user_id == self.user_id,
                ExpenseTxn.user_id == self.user_id,
                ReimbursementTxn.user_id == self.user_id,
                ExpenseTxn.deleted_at.is_(None),
                ExpenseTxn.type == TransactionType.expense,
                ExpenseTxn.date.between(period.start, period.end),
                ReimbursementTxn.deleted_at.is_(None),
                ReimbursementTxn.type == TransactionType.income,
                ReimbursementTxn.is_reimbursement.is_(True),
            )
            .group_by(ExpenseTxn.category_id)
        )
        if category_ids:
            reimb_stmt = reimb_stmt.where(ExpenseTxn.category_id.in_(category_ids))
        if tag_ids:
            reimb_stmt = reimb_stmt.where(ExpenseTxn.tags.any(Tag.id.in_(tag_ids)))

        reimb_rows = self.session.execute(reimb_stmt).all()
        reimb_map = {row.category_id: int(row.reimbursed or 0) for row in reimb_rows}

        breakdown = []
        total = 0
        for row in gross_rows:
            gross = int(row.gross or 0)
            reimbursed = int(reimb_map.get(row.category_id, 0))
            net = max(0, gross - reimbursed)
            if net <= 0:
                continue
            total += net
            breakdown.append({"name": row.name, "amount_cents": net, "percent": 0})
        breakdown.sort(key=lambda r: int(r["amount_cents"]), reverse=True)
        for item in breakdown:
            amount = int(item["amount_cents"])
            item["percent"] = (amount / total * 100) if total else 0
        self._category_breakdown_cache[period_key] = breakdown
        return breakdown


class InsightsService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()
        self.metrics = MetricsService(session, self.user_id)

    @staticmethod
    def _month_start(d: date) -> date:
        return d.replace(day=1)

    @staticmethod
    def _add_months(d: date, count: int) -> date:
        month_index = (d.year * 12) + (d.month - 1) + count
        year = month_index // 12
        month = (month_index % 12) + 1
        return date(year, month, 1)

    def monthly_series(
        self,
        period: Period,
        *,
        months_back: int = 12,
        tag_ids: Optional[list[int]] = None,
    ) -> list[dict[str, object]]:
        start_month = self._month_start(period.start)
        end_month = self._month_start(period.end)
        months: list[date] = []
        current = start_month
        while current <= end_month:
            months.append(current)
            current = self._add_months(current, 1)
        if len(months) > months_back:
            months = months[-months_back:]

        base_start = months[0]
        base_end = period.end

        income_stmt = (
            select(
                func.strftime("%Y", Transaction.date).label("year"),
                func.strftime("%m", Transaction.date).label("month"),
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("total"),
            )
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.income,
                Transaction.is_reimbursement.is_(False),
                Transaction.date.between(base_start, base_end),
            )
            .group_by("year", "month")
        )
        expense_gross_stmt = (
            select(
                func.strftime("%Y", Transaction.date).label("year"),
                func.strftime("%m", Transaction.date).label("month"),
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("total"),
            )
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
                Transaction.date.between(base_start, base_end),
            )
            .group_by("year", "month")
        )
        if tag_ids:
            income_stmt = income_stmt.where(Transaction.tags.any(Tag.id.in_(tag_ids)))
            expense_gross_stmt = expense_gross_stmt.where(
                Transaction.tags.any(Tag.id.in_(tag_ids))
            )

        income_totals: dict[tuple[int, int], int] = {}
        for row in self.session.execute(income_stmt):
            income_totals[(int(row.year), int(row.month))] = int(row.total or 0)

        expense_gross_totals: dict[tuple[int, int], int] = {}
        for row in self.session.execute(expense_gross_stmt):
            expense_gross_totals[(int(row.year), int(row.month))] = int(row.total or 0)

        ExpenseTxn = aliased(Transaction)
        ReimbursementTxn = aliased(Transaction)
        reimb_stmt = (
            select(
                func.strftime("%Y", ExpenseTxn.date).label("year"),
                func.strftime("%m", ExpenseTxn.date).label("month"),
                func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0).label(
                    "total"
                ),
            )
            .join(
                ExpenseTxn,
                ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
            )
            .join(
                ReimbursementTxn,
                ReimbursementAllocation.reimbursement_transaction_id
                == ReimbursementTxn.id,
            )
            .where(
                ReimbursementAllocation.user_id == self.user_id,
                ExpenseTxn.user_id == self.user_id,
                ReimbursementTxn.user_id == self.user_id,
                ExpenseTxn.deleted_at.is_(None),
                ExpenseTxn.type == TransactionType.expense,
                ExpenseTxn.date.between(base_start, base_end),
                ReimbursementTxn.deleted_at.is_(None),
                ReimbursementTxn.type == TransactionType.income,
                ReimbursementTxn.is_reimbursement.is_(True),
            )
            .group_by("year", "month")
        )
        if tag_ids:
            reimb_stmt = reimb_stmt.where(ExpenseTxn.tags.any(Tag.id.in_(tag_ids)))

        reimb_totals: dict[tuple[int, int], int] = {}
        for row in self.session.execute(reimb_stmt):
            reimb_totals[(int(row.year), int(row.month))] = int(row.total or 0)

        out: list[dict[str, object]] = []
        for month in months:
            key = (month.year, month.month)
            income = income_totals.get(key, 0)
            expense_gross = expense_gross_totals.get(key, 0)
            reimbursed = reimb_totals.get(key, 0)
            expense = max(0, expense_gross - reimbursed)
            out.append(
                {
                    "year": month.year,
                    "month": month.month,
                    "label": f"{month.year:04d}-{month.month:02d}",
                    "income_cents": income,
                    "expense_cents": expense,
                    "net_cents": income - expense,
                }
            )
        return out

    def top_tags(
        self,
        period: Period,
        *,
        transaction_type: TransactionType = TransactionType.expense,
        limit: int = 12,
    ) -> list[dict[str, object]]:
        stmt = (
            select(
                Tag.id.label("tag_id"),
                Tag.name.label("tag_name"),
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("total"),
            )
            .select_from(Transaction)
            .join(Transaction.tags)
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == transaction_type,
                Transaction.date.between(period.start, period.end),
            )
            .group_by(Tag.id, Tag.name)
            .order_by(func.sum(Transaction.amount_cents).desc())
            .limit(limit)
        )
        if transaction_type == TransactionType.income:
            stmt = stmt.where(Transaction.is_reimbursement.is_(False))
        return [
            {"id": int(r.tag_id), "name": str(r.tag_name), "amount_cents": int(r.total)}
            for r in self.session.execute(stmt)
        ]

    def category_trend(
        self,
        category_id: int,
        *,
        end: date,
        months_back: int = 12,
        tag_ids: Optional[list[int]] = None,
    ) -> list[dict[str, object]]:
        end_month = self._month_start(end)
        start_month = self._add_months(end_month, -(months_back - 1))
        months: list[date] = []
        current = start_month
        while current <= end_month:
            months.append(current)
            current = self._add_months(current, 1)

        stmt = (
            select(
                func.strftime("%Y", Transaction.date).label("year"),
                func.strftime("%m", Transaction.date).label("month"),
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("total"),
            )
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.category_id == category_id,
                Transaction.date.between(start_month, end),
            )
            .group_by("year", "month")
        )
        if tag_ids:
            stmt = stmt.where(Transaction.tags.any(Tag.id.in_(tag_ids)))

        gross_totals = {
            (int(r.year), int(r.month)): int(r.total or 0)
            for r in self.session.execute(stmt)
        }

        ExpenseTxn = aliased(Transaction)
        ReimbursementTxn = aliased(Transaction)
        reimb_stmt = (
            select(
                func.strftime("%Y", ExpenseTxn.date).label("year"),
                func.strftime("%m", ExpenseTxn.date).label("month"),
                func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0).label(
                    "total"
                ),
            )
            .join(
                ExpenseTxn,
                ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
            )
            .join(
                ReimbursementTxn,
                ReimbursementAllocation.reimbursement_transaction_id
                == ReimbursementTxn.id,
            )
            .where(
                ReimbursementAllocation.user_id == self.user_id,
                ExpenseTxn.user_id == self.user_id,
                ReimbursementTxn.user_id == self.user_id,
                ExpenseTxn.deleted_at.is_(None),
                ExpenseTxn.type == TransactionType.expense,
                ExpenseTxn.category_id == category_id,
                ExpenseTxn.date.between(start_month, end),
                ReimbursementTxn.deleted_at.is_(None),
                ReimbursementTxn.type == TransactionType.income,
                ReimbursementTxn.is_reimbursement.is_(True),
            )
            .group_by("year", "month")
        )
        if tag_ids:
            reimb_stmt = reimb_stmt.where(ExpenseTxn.tags.any(Tag.id.in_(tag_ids)))
        reimb_totals = {
            (int(r.year), int(r.month)): int(r.total or 0)
            for r in self.session.execute(reimb_stmt)
        }

        out: list[dict[str, object]] = []
        for month in months:
            key = (month.year, month.month)
            gross = gross_totals.get(key, 0)
            reimbursed = reimb_totals.get(key, 0)
            net = max(0, gross - reimbursed)
            out.append(
                {
                    "year": month.year,
                    "month": month.month,
                    "label": f"{month.year:04d}-{month.month:02d}",
                    "amount_cents": net,
                }
            )
        return out

    def expense_category_deltas(
        self, period: Period, *, tag_ids: Optional[list[int]] = None, limit: int = 8
    ) -> dict[str, list[dict[str, object]]]:
        duration_days = (period.end - period.start).days + 1
        prev_end = period.start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=duration_days - 1)
        prev = Period("prev", prev_start, prev_end)

        def totals_for(p: Period) -> dict[int, int]:
            gross_stmt = (
                select(
                    Transaction.category_id,
                    func.coalesce(func.sum(Transaction.amount_cents), 0).label("total"),
                )
                .where(
                    Transaction.user_id == self.user_id,
                    Transaction.deleted_at.is_(None),
                    Transaction.type == TransactionType.expense,
                    Transaction.date.between(p.start, p.end),
                )
                .group_by(Transaction.category_id)
            )
            if tag_ids:
                gross_stmt = gross_stmt.where(Transaction.tags.any(Tag.id.in_(tag_ids)))
            gross = {
                int(r.category_id): int(r.total or 0)
                for r in self.session.execute(gross_stmt)
            }

            ExpenseTxn = aliased(Transaction)
            ReimbursementTxn = aliased(Transaction)
            reimb_stmt = (
                select(
                    ExpenseTxn.category_id.label("category_id"),
                    func.coalesce(
                        func.sum(ReimbursementAllocation.amount_cents), 0
                    ).label("total"),
                )
                .join(
                    ExpenseTxn,
                    ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
                )
                .join(
                    ReimbursementTxn,
                    ReimbursementAllocation.reimbursement_transaction_id
                    == ReimbursementTxn.id,
                )
                .where(
                    ReimbursementAllocation.user_id == self.user_id,
                    ExpenseTxn.user_id == self.user_id,
                    ReimbursementTxn.user_id == self.user_id,
                    ExpenseTxn.deleted_at.is_(None),
                    ExpenseTxn.type == TransactionType.expense,
                    ExpenseTxn.date.between(p.start, p.end),
                    ReimbursementTxn.deleted_at.is_(None),
                    ReimbursementTxn.type == TransactionType.income,
                    ReimbursementTxn.is_reimbursement.is_(True),
                )
                .group_by(ExpenseTxn.category_id)
            )
            if tag_ids:
                reimb_stmt = reimb_stmt.where(ExpenseTxn.tags.any(Tag.id.in_(tag_ids)))
            reimb = {
                int(r.category_id): int(r.total or 0)
                for r in self.session.execute(reimb_stmt)
            }

            net: dict[int, int] = {}
            for cid, gross_amount in gross.items():
                net[cid] = max(0, gross_amount - reimb.get(cid, 0))
            return net

        cur_totals = totals_for(period)
        prev_totals = totals_for(prev)

        all_category_ids = set(cur_totals.keys()) | set(prev_totals.keys())
        if not all_category_ids:
            return {"increases": [], "decreases": []}

        categories = self.session.scalars(
            select(Category).where(
                Category.user_id == self.user_id,
                Category.id.in_(list(all_category_ids)),
            )
        ).all()
        names = {c.id: c.name for c in categories}

        deltas: list[dict[str, object]] = []
        for cid in all_category_ids:
            cur = cur_totals.get(cid, 0)
            prev_amount = prev_totals.get(cid, 0)
            delta = cur - prev_amount
            deltas.append(
                {
                    "category_id": cid,
                    "category_name": names.get(cid, "Unknown"),
                    "current_cents": cur,
                    "previous_cents": prev_amount,
                    "delta_cents": delta,
                }
            )

        increases = sorted(deltas, key=lambda r: r["delta_cents"], reverse=True)[:limit]
        decreases = sorted(deltas, key=lambda r: r["delta_cents"])[:limit]
        return {"increases": increases, "decreases": decreases}


class RecurringRuleService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def get(self, rule_id: int) -> RecurringRule:
        rule = self.session.get(RecurringRule, rule_id)
        if not rule or rule.user_id != self.user_id:
            raise ValueError("Rule not found")
        return rule

    def list(self) -> list[RecurringRule]:
        stmt = (
            select(RecurringRule)
            .options(joinedload(RecurringRule.category))
            .where(RecurringRule.user_id == self.user_id)
            .order_by(RecurringRule.next_occurrence)
        )
        return self.session.scalars(stmt).all()

    def get_statistics(self) -> dict[str, object]:
        from fx_rates import FxRateService
        from models import IntervalUnit
        from recurrence import local_today

        rules = self.list()
        fx = FxRateService()
        today = local_today()

        def monthly_amount(rule: RecurringRule) -> int:
            interval = rule.interval_unit
            count = rule.interval_count
            amount = rule.amount_cents
            if rule.currency_code == CurrencyCode.usd:
                try:
                    amount, _quote = fx.convert_usd_cents_to_eur_cents(amount, today)
                except Exception:
                    amount = 0

            if interval == IntervalUnit.day:
                return int(amount * 30.44 / count)
            elif interval == IntervalUnit.week:
                return int(amount * 4.35 / count)
            elif interval == IntervalUnit.month:
                return int(amount / count)
            elif interval == IntervalUnit.year:
                return int(amount / (12 * count))
            return amount

        total_income = 0
        total_expenses = 0
        income_by_category: dict[str, int] = {}
        expense_by_category: dict[str, int] = {}
        income_count = 0
        expense_count = 0

        for rule in rules:
            monthly = monthly_amount(rule)
            category_name = rule.category.name if rule.category else "Uncategorized"

            if rule.type == TransactionType.income:
                total_income += monthly
                income_count += 1
                income_by_category[category_name] = (
                    income_by_category.get(category_name, 0) + monthly
                )
            else:
                total_expenses += monthly
                expense_count += 1
                expense_by_category[category_name] = (
                    expense_by_category.get(category_name, 0) + monthly
                )

        coverage_ratio = (
            (total_income / total_expenses * 100) if total_expenses > 0 else 100.0
        )

        def build_breakdown(by_category: dict[str, int], total: int) -> list[dict]:
            if total == 0:
                return []
            items = sorted(by_category.items(), key=lambda x: x[1], reverse=True)
            return [
                {
                    "name": name,
                    "amount_cents": amount,
                    "percent": (amount / total * 100) if total > 0 else 0,
                }
                for name, amount in items
            ]

        return {
            "total_monthly_income": total_income,
            "total_monthly_expenses": total_expenses,
            "net_monthly": total_income - total_expenses,
            "coverage_ratio": coverage_ratio,
            "expense_breakdown": build_breakdown(expense_by_category, total_expenses),
            "income_breakdown": build_breakdown(income_by_category, total_income),
            "rule_counts": {
                "income": income_count,
                "expense": expense_count,
                "total": income_count + expense_count,
            },
        }

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
            currency_code=data.currency_code,
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

    def delete(self, rule_id: int) -> None:
        rule = self.session.get(RecurringRule, rule_id)
        if not rule or rule.user_id != self.user_id:
            raise ValueError("Rule not found")
        self.session.delete(rule)
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
            if row.is_reimbursement and row.type != TransactionType.income:
                errors.append("IsReimbursement can only be set for income transactions")
            preview_rows.append(
                {
                    "date": row.date,
                    "type": row.type.value,
                    "is_reimbursement": row.is_reimbursement,
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
        dates = set()
        rule_service = RuleService(self.session, self.user_id)
        months: set[tuple[int, int]] = set()
        for row in preview_rows:
            txn_type = TransactionType(row["type"])
            txn = Transaction(
                user_id=self.user_id,
                date=row["date"],
                occurred_at=datetime.combine(row["date"], time(12, 0)),
                type=txn_type,
                is_reimbursement=bool(row["is_reimbursement"])
                if txn_type == TransactionType.income
                else False,
                amount_cents=row["amount_cents"],
                category_id=row["category_id"],
                note=row["note"],
            )
            self.session.add(txn)
            rule_service.apply_rules(txn)
            dates.add(row["date"])
            months.add((row["date"].year, row["date"].month))
        self.session.flush()
        for y, m in months:
            recompute_monthly_rollup(self.session, self.user_id, y, m)
        metrics = MetricsService(self.session, self.user_id)
        for txn_date in dates:
            period = Period("transaction", txn_date, txn_date)
            metrics._invalidate_period_cache(period)
        self.session.commit()
        return len(preview_rows)

    def export(self, transactions: list[Transaction]) -> str:
        return export_transactions(transactions)


class ReportService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()
        self.metrics_service = MetricsService(session, self.user_id)
        self.txn_service = TransactionService(session, self.user_id)
        self.rule_service = RecurringRuleService(session, self.user_id)

    def gather_data(self, options: ReportOptions) -> dict[str, object]:
        period = Period("report", options.start, options.end)
        data: dict[str, object] = {
            "period": period,
            "options": options,
        }

        kpis = None
        wants_overview = "summary" in options.sections or "kpis" in options.sections
        if wants_overview:
            if options.transaction_type is None and options.category_ids is None:
                kpis = self.metrics_service.kpis(period)
            else:
                income = 0
                expenses = 0

                if options.transaction_type in (None, TransactionType.income):
                    income_stmt = select(
                        func.coalesce(
                            func.sum(Transaction.amount_cents),
                            0,
                        ).label("income")
                    ).where(
                        Transaction.user_id == self.user_id,
                        Transaction.deleted_at.is_(None),
                        Transaction.type == TransactionType.income,
                        Transaction.is_reimbursement.is_(False),
                        Transaction.date.between(options.start, options.end),
                    )
                    if options.category_ids:
                        income_stmt = income_stmt.where(
                            Transaction.category_id.in_(options.category_ids)
                        )
                    income = int(self.session.execute(income_stmt).scalar_one() or 0)

                if options.transaction_type in (None, TransactionType.expense):
                    expense_stmt = select(
                        func.coalesce(
                            func.sum(Transaction.amount_cents),
                            0,
                        ).label("expenses")
                    ).where(
                        Transaction.user_id == self.user_id,
                        Transaction.deleted_at.is_(None),
                        Transaction.type == TransactionType.expense,
                        Transaction.date.between(options.start, options.end),
                    )
                    if options.category_ids:
                        expense_stmt = expense_stmt.where(
                            Transaction.category_id.in_(options.category_ids)
                        )
                    expense_gross = int(
                        self.session.execute(expense_stmt).scalar_one() or 0
                    )

                    ExpenseTxn = aliased(Transaction)
                    ReimbursementTxn = aliased(Transaction)
                    reimb_stmt = (
                        select(
                            func.coalesce(
                                func.sum(ReimbursementAllocation.amount_cents), 0
                            )
                        )
                        .join(
                            ExpenseTxn,
                            ReimbursementAllocation.expense_transaction_id
                            == ExpenseTxn.id,
                        )
                        .join(
                            ReimbursementTxn,
                            ReimbursementAllocation.reimbursement_transaction_id
                            == ReimbursementTxn.id,
                        )
                        .where(
                            ReimbursementAllocation.user_id == self.user_id,
                            ExpenseTxn.user_id == self.user_id,
                            ReimbursementTxn.user_id == self.user_id,
                            ExpenseTxn.deleted_at.is_(None),
                            ExpenseTxn.type == TransactionType.expense,
                            ExpenseTxn.date.between(options.start, options.end),
                            ReimbursementTxn.deleted_at.is_(None),
                            ReimbursementTxn.type == TransactionType.income,
                            ReimbursementTxn.is_reimbursement.is_(True),
                        )
                    )
                    if options.category_ids:
                        reimb_stmt = reimb_stmt.where(
                            ExpenseTxn.category_id.in_(options.category_ids)
                        )
                    reimbursed = int(self.session.execute(reimb_stmt).scalar_one() or 0)
                    expenses = max(0, expense_gross - reimbursed)

                kpis = {
                    "income": income,
                    "expenses": expenses,
                    "balance": income - expenses,
                }

        if wants_overview:
            assert kpis is not None
            data["summary"] = {
                "period": period,
                "total_income": kpis["income"],
                "total_expenses": kpis["expenses"],
                "balance": kpis["balance"],
            }

        if "category_breakdown" in options.sections:
            breakdown_type = (
                options.transaction_type
                if options.transaction_type is not None
                else TransactionType.expense
            )
            breakdown = self.metrics_service.category_breakdown(
                period, breakdown_type, category_ids=options.category_ids
            )
            data["category_breakdown"] = breakdown

        if "top_categories" in options.sections:
            breakdown_type = (
                options.transaction_type
                if options.transaction_type is not None
                else TransactionType.expense
            )
            breakdown = self.metrics_service.category_breakdown(
                period, breakdown_type, category_ids=options.category_ids
            )
            data["top_categories"] = breakdown[:5]

        if "trend" in options.sections:
            trend_type = (
                options.transaction_type
                if options.transaction_type is not None
                else TransactionType.expense
            )
            if trend_type == TransactionType.income:
                stmt = (
                    select(Transaction.date, func.sum(Transaction.amount_cents))
                    .where(
                        Transaction.user_id == self.user_id,
                        Transaction.deleted_at.is_(None),
                        Transaction.type == TransactionType.income,
                        Transaction.is_reimbursement.is_(False),
                        Transaction.date.between(options.start, options.end),
                    )
                    .group_by(Transaction.date)
                    .order_by(Transaction.date)
                )
                if options.category_ids:
                    stmt = stmt.where(Transaction.category_id.in_(options.category_ids))
                rows = self.session.execute(stmt).all()
                data["trend"] = [
                    {"date": row[0], "amount_cents": int(row[1] or 0)} for row in rows
                ]
            else:
                gross_stmt = (
                    select(
                        Transaction.date,
                        func.sum(Transaction.amount_cents).label("gross"),
                    )
                    .where(
                        Transaction.user_id == self.user_id,
                        Transaction.deleted_at.is_(None),
                        Transaction.type == TransactionType.expense,
                        Transaction.date.between(options.start, options.end),
                    )
                    .group_by(Transaction.date)
                    .order_by(Transaction.date)
                )
                if options.category_ids:
                    gross_stmt = gross_stmt.where(
                        Transaction.category_id.in_(options.category_ids)
                    )
                gross_rows = self.session.execute(gross_stmt).all()
                gross_map = {row[0]: int(row.gross or 0) for row in gross_rows}

                ExpenseTxn = aliased(Transaction)
                ReimbursementTxn = aliased(Transaction)
                reimb_stmt = (
                    select(
                        ExpenseTxn.date,
                        func.coalesce(
                            func.sum(ReimbursementAllocation.amount_cents), 0
                        ).label("reimbursed"),
                    )
                    .join(
                        ExpenseTxn,
                        ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
                    )
                    .join(
                        ReimbursementTxn,
                        ReimbursementAllocation.reimbursement_transaction_id
                        == ReimbursementTxn.id,
                    )
                    .where(
                        ReimbursementAllocation.user_id == self.user_id,
                        ExpenseTxn.user_id == self.user_id,
                        ReimbursementTxn.user_id == self.user_id,
                        ExpenseTxn.deleted_at.is_(None),
                        ExpenseTxn.type == TransactionType.expense,
                        ExpenseTxn.date.between(options.start, options.end),
                        ReimbursementTxn.deleted_at.is_(None),
                        ReimbursementTxn.type == TransactionType.income,
                        ReimbursementTxn.is_reimbursement.is_(True),
                    )
                    .group_by(ExpenseTxn.date)
                )
                if options.category_ids:
                    reimb_stmt = reimb_stmt.where(
                        ExpenseTxn.category_id.in_(options.category_ids)
                    )
                reimb_rows = self.session.execute(reimb_stmt).all()
                reimb_map = {row[0]: int(row.reimbursed or 0) for row in reimb_rows}

                dates = sorted(set(gross_map.keys()) | set(reimb_map.keys()))
                data["trend"] = [
                    {
                        "date": d,
                        "amount_cents": max(
                            0, gross_map.get(d, 0) - reimb_map.get(d, 0)
                        ),
                    }
                    for d in dates
                ]

        if "recent_transactions" in options.sections:
            sort_order = options.transactions_sort
            if options.show_running_balance:
                sort_order = "oldest"

            stmt = (
                select(Transaction)
                .options(joinedload(Transaction.category))
                .where(
                    Transaction.user_id == self.user_id,
                    Transaction.deleted_at.is_(None),
                    Transaction.date.between(options.start, options.end),
                )
            )
            if options.transaction_type is not None:
                stmt = stmt.where(Transaction.type == options.transaction_type)
            if options.category_ids:
                stmt = stmt.where(Transaction.category_id.in_(options.category_ids))
            if sort_order == "newest":
                stmt = stmt.order_by(
                    Transaction.occurred_at.desc(), Transaction.id.desc()
                )
            else:
                stmt = stmt.order_by(
                    Transaction.occurred_at.asc(), Transaction.id.asc()
                )

            transactions = self.session.scalars(stmt).all()
            if options.show_running_balance:
                use_account_balance = (
                    options.transaction_type is None and not options.category_ids
                )
                if use_account_balance:
                    balance_service = BalanceAnchorService(self.session, self.user_id)
                    start_dt = datetime.combine(options.start, time.min)
                    opening_balance = balance_service.balance_as_of(
                        start_dt - timedelta(seconds=1)
                    )
                    anchors = self.session.scalars(
                        select(BalanceAnchor)
                        .where(
                            BalanceAnchor.user_id == self.user_id,
                            BalanceAnchor.as_of_at.between(
                                datetime.combine(options.start, time.min),
                                datetime.combine(options.end, time.max),
                            ),
                        )
                        .order_by(BalanceAnchor.as_of_at.asc(), BalanceAnchor.id.asc())
                    ).all()
                    next_anchor_idx = 0
                else:
                    opening_stmt = select(
                        func.coalesce(
                            func.sum(
                                case(
                                    (
                                        Transaction.type == TransactionType.income,
                                        Transaction.amount_cents,
                                    ),
                                    else_=0,
                                )
                            ),
                            0,
                        ).label("income"),
                        func.coalesce(
                            func.sum(
                                case(
                                    (
                                        Transaction.type == TransactionType.expense,
                                        Transaction.amount_cents,
                                    ),
                                    else_=0,
                                )
                            ),
                            0,
                        ).label("expenses"),
                    ).where(
                        Transaction.user_id == self.user_id,
                        Transaction.deleted_at.is_(None),
                        Transaction.date < options.start,
                    )
                    if options.transaction_type is not None:
                        opening_stmt = opening_stmt.where(
                            Transaction.type == options.transaction_type
                        )
                    if options.category_ids:
                        opening_stmt = opening_stmt.where(
                            Transaction.category_id.in_(options.category_ids)
                        )

                    opening_row = self.session.execute(opening_stmt).one()
                    opening_income = int(opening_row.income)
                    opening_expenses = int(opening_row.expenses)
                    opening_balance = opening_income - opening_expenses
                data["opening_balance_cents"] = opening_balance

                running = opening_balance
                for txn in transactions:
                    if use_account_balance:
                        while (
                            next_anchor_idx < len(anchors)
                            and anchors[next_anchor_idx].as_of_at <= txn.occurred_at
                        ):
                            running = int(anchors[next_anchor_idx].balance_cents)
                            next_anchor_idx += 1
                    if txn.type == TransactionType.income:
                        running += txn.amount_cents
                    else:
                        running -= txn.amount_cents
                    setattr(txn, "running_balance_cents", running)
            data["recent_transactions"] = transactions
            if options.include_category_subtotals and transactions:
                totals: dict[tuple[str, TransactionType], int] = {}
                for txn in transactions:
                    name = txn.category.name if txn.category else "Uncategorized"
                    key = (name, txn.type)
                    totals[key] = totals.get(key, 0) + txn.amount_cents
                subtotals = [
                    {
                        "name": name,
                        "type": txn_type,
                        "amount_cents": amount,
                    }
                    for (name, txn_type), amount in totals.items()
                ]
                subtotals.sort(key=lambda row: row["amount_cents"], reverse=True)
                data["category_subtotals"] = subtotals

        if "recurring_upcoming" in options.sections:
            end_date = options.end + timedelta(days=30)
            upcoming_rules = []
            for rule in self.rule_service.list():
                if rule.auto_post and rule.next_occurrence <= end_date:
                    upcoming_rules.append(rule)
            data["recurring_upcoming"] = upcoming_rules

        return data


class BudgetService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    @staticmethod
    def _month_start(year: int, month: int) -> date:
        return date(year, month, 1)

    @staticmethod
    def _month_end(year: int, month: int) -> date:
        if month == 12:
            return date(year + 1, 1, 1) - date.resolution
        return date(year, month + 1, 1) - date.resolution

    def list_templates(
        self, *, frequency: Optional[BudgetFrequency] = None
    ) -> list[BudgetTemplate]:
        stmt = (
            select(BudgetTemplate)
            .options(joinedload(BudgetTemplate.category))
            .where(BudgetTemplate.user_id == self.user_id)
            .order_by(
                BudgetTemplate.frequency.asc(),
                BudgetTemplate.category_id.is_(None).desc(),
                BudgetTemplate.starts_on.desc(),
                BudgetTemplate.id.desc(),
            )
        )
        if frequency:
            stmt = stmt.where(BudgetTemplate.frequency == frequency)
        return self.session.scalars(stmt).all()

    def upsert_template(self, data: BudgetTemplateIn) -> BudgetTemplate:
        if data.category_id is not None:
            category = self.session.get(Category, data.category_id)
            if not category or category.user_id != self.user_id:
                raise ValueError("Category not found")
            if category.type != TransactionType.expense:
                raise ValueError("Budgets can only be set for expense categories")

        stmt = select(BudgetTemplate).where(
            BudgetTemplate.user_id == self.user_id,
            BudgetTemplate.frequency == data.frequency,
            BudgetTemplate.starts_on == data.starts_on,
            BudgetTemplate.category_id.is_(None)
            if data.category_id is None
            else BudgetTemplate.category_id == data.category_id,
        )
        existing = self.session.scalar(stmt)
        if existing:
            existing.amount_cents = data.amount_cents
            existing.ends_on = data.ends_on
            self.session.commit()
            self.session.refresh(existing)
            return existing

        tmpl = BudgetTemplate(
            user_id=self.user_id,
            frequency=data.frequency,
            category_id=data.category_id,
            amount_cents=data.amount_cents,
            starts_on=data.starts_on,
            ends_on=data.ends_on,
        )
        self.session.add(tmpl)
        self.session.commit()
        self.session.refresh(tmpl)
        return tmpl

    def delete_template(self, template_id: int) -> None:
        tmpl = self.session.get(BudgetTemplate, template_id)
        if not tmpl or tmpl.user_id != self.user_id:
            raise ValueError("Template not found")
        self.session.delete(tmpl)
        self.session.commit()

    def upsert_override(self, data: BudgetOverrideIn) -> BudgetOverride:
        if data.category_id is not None:
            category = self.session.get(Category, data.category_id)
            if not category or category.user_id != self.user_id:
                raise ValueError("Category not found")
            if category.type != TransactionType.expense:
                raise ValueError("Budgets can only be set for expense categories")

        stmt = select(BudgetOverride).where(
            BudgetOverride.user_id == self.user_id,
            BudgetOverride.year == data.year,
            BudgetOverride.month == data.month,
            BudgetOverride.category_id.is_(None)
            if data.category_id is None
            else BudgetOverride.category_id == data.category_id,
        )
        existing = self.session.scalar(stmt)
        if existing:
            existing.amount_cents = data.amount_cents
            self.session.commit()
            self.session.refresh(existing)
            return existing

        override = BudgetOverride(
            user_id=self.user_id,
            year=data.year,
            month=data.month,
            category_id=data.category_id,
            amount_cents=data.amount_cents,
        )
        self.session.add(override)
        self.session.commit()
        self.session.refresh(override)
        return override

    def delete_override(self, override_id: int) -> None:
        override = self.session.get(BudgetOverride, override_id)
        if not override or override.user_id != self.user_id:
            raise ValueError("Override not found")
        self.session.delete(override)
        self.session.commit()

    @dataclass(frozen=True)
    class EffectiveBudget:
        scope_category_id: Optional[int]
        scope_label: str
        amount_cents: int
        source: str  # "override" | "template"
        source_id: int

    def _active_templates_for_date(
        self, target: date, *, frequency: BudgetFrequency
    ) -> list[BudgetTemplate]:
        stmt = (
            select(BudgetTemplate)
            .options(joinedload(BudgetTemplate.category))
            .where(
                BudgetTemplate.user_id == self.user_id,
                BudgetTemplate.frequency == frequency,
                BudgetTemplate.starts_on <= target,
                (BudgetTemplate.ends_on.is_(None) | (BudgetTemplate.ends_on >= target)),
            )
            .order_by(
                BudgetTemplate.category_id.is_(None).desc(),
                BudgetTemplate.starts_on.desc(),
                BudgetTemplate.id.desc(),
            )
        )
        return self.session.scalars(stmt).all()

    def effective_budgets_for_month(
        self, year: int, month: int
    ) -> list[EffectiveBudget]:
        month_start = self._month_start(year, month)
        overrides = self.session.scalars(
            select(BudgetOverride)
            .options(joinedload(BudgetOverride.category))
            .where(
                BudgetOverride.user_id == self.user_id,
                BudgetOverride.year == year,
                BudgetOverride.month == month,
            )
        ).all()
        overrides_by_scope = {o.category_id: o for o in overrides}

        templates = self._active_templates_for_date(
            month_start, frequency=BudgetFrequency.monthly
        )
        templates_latest: dict[Optional[int], BudgetTemplate] = {}
        for tmpl in templates:
            if tmpl.category_id in templates_latest:
                continue
            templates_latest[tmpl.category_id] = tmpl

        effective: list[BudgetService.EffectiveBudget] = []
        scopes = set(overrides_by_scope.keys()) | set(templates_latest.keys())
        for category_id in sorted(scopes, key=lambda v: (-1 if v is None else v)):
            override = overrides_by_scope.get(category_id)
            if override:
                label = override.category.name if override.category else "Overall"
                effective.append(
                    BudgetService.EffectiveBudget(
                        scope_category_id=category_id,
                        scope_label=label,
                        amount_cents=override.amount_cents,
                        source="override",
                        source_id=override.id,
                    )
                )
                continue
            tmpl = templates_latest.get(category_id)
            if tmpl:
                label = tmpl.category.name if tmpl.category else "Overall"
                effective.append(
                    BudgetService.EffectiveBudget(
                        scope_category_id=category_id,
                        scope_label=label,
                        amount_cents=tmpl.amount_cents,
                        source="template",
                        source_id=tmpl.id,
                    )
                )
        return effective

    def spent_by_category_for_month(
        self, year: int, month: int
    ) -> dict[Optional[int], int]:
        start = self._month_start(year, month)
        end = self._month_end(year, month)
        gross_stmt = (
            select(
                Transaction.category_id,
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("spent"),
            )
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
                Transaction.date.between(start, end),
                ~Transaction.tags.any(Tag.is_hidden_from_budget),
            )
            .group_by(Transaction.category_id)
        )
        gross_by_category = {
            row.category_id: int(row.spent or 0)
            for row in self.session.execute(gross_stmt)
        }

        ExpenseTxn = aliased(Transaction)
        ReimbursementTxn = aliased(Transaction)
        reimb_stmt = (
            select(
                ExpenseTxn.category_id,
                func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0).label(
                    "reimbursed"
                ),
            )
            .join(
                ExpenseTxn,
                ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
            )
            .join(
                ReimbursementTxn,
                ReimbursementAllocation.reimbursement_transaction_id
                == ReimbursementTxn.id,
            )
            .where(
                ReimbursementAllocation.user_id == self.user_id,
                ExpenseTxn.user_id == self.user_id,
                ReimbursementTxn.user_id == self.user_id,
                ExpenseTxn.deleted_at.is_(None),
                ExpenseTxn.type == TransactionType.expense,
                ExpenseTxn.date.between(start, end),
                ~ExpenseTxn.tags.any(Tag.is_hidden_from_budget),
                ReimbursementTxn.deleted_at.is_(None),
                ReimbursementTxn.type == TransactionType.income,
                ReimbursementTxn.is_reimbursement.is_(True),
            )
            .group_by(ExpenseTxn.category_id)
        )
        reimb_by_category = {
            row.category_id: int(row.reimbursed or 0)
            for row in self.session.execute(reimb_stmt)
        }

        net_by_category: dict[Optional[int], int] = {}
        for category_id, gross in gross_by_category.items():
            net_by_category[category_id] = max(
                0, gross - reimb_by_category.get(category_id, 0)
            )
        net_by_category[None] = sum(net_by_category.values())
        return net_by_category

    def progress_for_month(
        self, year: int, month: int
    ) -> dict[Optional[int], dict[str, int]]:
        effective = self.effective_budgets_for_month(year, month)
        spent_by_scope = self.spent_by_category_for_month(year, month)
        progress: dict[Optional[int], dict[str, int]] = {}
        for row in effective:
            spent = spent_by_scope.get(row.scope_category_id, 0)
            progress[row.scope_category_id] = {
                "spent_cents": spent,
                "remaining_cents": row.amount_cents - spent,
            }
        return progress

    def yearly_budgets_for_year(self, year: int) -> list[EffectiveBudget]:
        year_start = date(year, 1, 1)
        templates = self._active_templates_for_date(
            year_start, frequency=BudgetFrequency.yearly
        )
        templates_latest: dict[Optional[int], BudgetTemplate] = {}
        for tmpl in templates:
            if tmpl.category_id in templates_latest:
                continue
            templates_latest[tmpl.category_id] = tmpl

        effective: list[BudgetService.EffectiveBudget] = []
        for category_id in sorted(
            templates_latest.keys(), key=lambda v: (-1 if v is None else v)
        ):
            tmpl = templates_latest[category_id]
            label = tmpl.category.name if tmpl.category else "Overall"
            effective.append(
                BudgetService.EffectiveBudget(
                    scope_category_id=category_id,
                    scope_label=label,
                    amount_cents=tmpl.amount_cents,
                    source="template",
                    source_id=tmpl.id,
                )
            )
        return effective

    def spent_by_category_for_year(self, year: int) -> dict[Optional[int], int]:
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1) - date.resolution
        gross_stmt = (
            select(
                Transaction.category_id,
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("spent"),
            )
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
                Transaction.date.between(start, end),
                ~Transaction.tags.any(Tag.is_hidden_from_budget),
            )
            .group_by(Transaction.category_id)
        )
        gross_by_category = {
            row.category_id: int(row.spent or 0)
            for row in self.session.execute(gross_stmt)
        }

        ExpenseTxn = aliased(Transaction)
        ReimbursementTxn = aliased(Transaction)
        reimb_stmt = (
            select(
                ExpenseTxn.category_id,
                func.coalesce(func.sum(ReimbursementAllocation.amount_cents), 0).label(
                    "reimbursed"
                ),
            )
            .join(
                ExpenseTxn,
                ReimbursementAllocation.expense_transaction_id == ExpenseTxn.id,
            )
            .join(
                ReimbursementTxn,
                ReimbursementAllocation.reimbursement_transaction_id
                == ReimbursementTxn.id,
            )
            .where(
                ReimbursementAllocation.user_id == self.user_id,
                ExpenseTxn.user_id == self.user_id,
                ReimbursementTxn.user_id == self.user_id,
                ExpenseTxn.deleted_at.is_(None),
                ExpenseTxn.type == TransactionType.expense,
                ExpenseTxn.date.between(start, end),
                ~ExpenseTxn.tags.any(Tag.is_hidden_from_budget),
                ReimbursementTxn.deleted_at.is_(None),
                ReimbursementTxn.type == TransactionType.income,
                ReimbursementTxn.is_reimbursement.is_(True),
            )
            .group_by(ExpenseTxn.category_id)
        )
        reimb_by_category = {
            row.category_id: int(row.reimbursed or 0)
            for row in self.session.execute(reimb_stmt)
        }

        net_by_category: dict[Optional[int], int] = {}
        for category_id, gross in gross_by_category.items():
            net_by_category[category_id] = max(
                0, gross - reimb_by_category.get(category_id, 0)
            )
        net_by_category[None] = sum(net_by_category.values())
        return net_by_category
