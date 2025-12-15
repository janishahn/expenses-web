from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Optional

from sqlalchemy import case, delete, func, select, tuple_
from sqlalchemy.orm import Session, joinedload

from models import (
    BalanceAnchor,
    Budget,
    Category,
    MonthlyRollup,
    RecurringRule,
    Transaction,
    TransactionType,
)
from periods import Period
from recurrence import RecurringEngine
from csv_utils import export_transactions, parse_csv
from schemas import (
    BalanceAnchorIn,
    BudgetIn,
    CategoryIn,
    RecurringRuleIn,
    ReportOptions,
    TransactionIn,
)


def update_monthly_rollup(
    session: Session,
    user_id: int,
    txn_date: date,
    txn_type: TransactionType,
    amount_cents: int,
    increment: bool = True,
) -> None:
    """Update or create a monthly rollup for the given transaction."""
    year = txn_date.year
    month = txn_date.month
    sign = 1 if increment else -1

    rollup = session.scalar(
        select(MonthlyRollup).where(
            MonthlyRollup.user_id == user_id,
            MonthlyRollup.year == year,
            MonthlyRollup.month == month,
        )
    )

    if not rollup:
        rollup = MonthlyRollup(
            user_id=user_id,
            year=year,
            month=month,
            income_cents=0,
            expense_cents=0,
        )
        session.add(rollup)

    if txn_type == TransactionType.income:
        rollup.income_cents = max(0, rollup.income_cents + sign * amount_cents)
    else:
        rollup.expense_cents = max(0, rollup.expense_cents + sign * amount_cents)


def get_current_user_id() -> int:
    return 1


def cents_to_euros(cents: int) -> float:
    return cents / 100


def rebuild_monthly_rollups(session: Session, user_id: int) -> None:
    session.execute(delete(MonthlyRollup).where(MonthlyRollup.user_id == user_id))
    session.flush()

    year = func.strftime("%Y", Transaction.date).label("year")
    month = func.strftime("%m", Transaction.date).label("month")
    stmt = (
        select(
            year,
            month,
            Transaction.type,
            func.coalesce(func.sum(Transaction.amount_cents), 0).label("total"),
        )
        .where(
            Transaction.user_id == user_id,
            Transaction.deleted_at.is_(None),
        )
        .group_by(year, month, Transaction.type)
    )

    rollups: dict[tuple[int, int], MonthlyRollup] = {}
    for row in session.execute(stmt):
        y = int(row.year)
        m = int(row.month)
        key = (y, m)
        rollup = rollups.get(key)
        if not rollup:
            rollup = MonthlyRollup(
                user_id=user_id, year=y, month=m, income_cents=0, expense_cents=0
            )
            rollups[key] = rollup
        if row.type == TransactionType.income:
            rollup.income_cents = int(row.total or 0)
        else:
            rollup.expense_cents = int(row.total or 0)

    session.add_all(rollups.values())
    session.commit()


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
        txn = Transaction(
            user_id=self.user_id,
            date=data.date,
            occurred_at=data.occurred_at,
            type=data.type,
            amount_cents=data.amount_cents,
            category_id=data.category_id,
            note=data.note,
        )
        self.session.add(txn)
        update_monthly_rollup(
            self.session,
            self.user_id,
            data.date,
            data.type,
            data.amount_cents,
            increment=True,
        )
        # Invalidate donut cache for the transaction period
        period = Period("transaction", data.date, data.date)
        metrics = MetricsService(self.session, self.user_id)
        metrics._invalidate_period_cache(period)
        self.session.commit()
        self.session.refresh(txn)
        return txn

    def get(self, transaction_id: int, *, include_deleted: bool = False) -> Transaction:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
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
        old_amount = txn.amount_cents
        update_monthly_rollup(
            self.session,
            self.user_id,
            old_date,
            old_type,
            old_amount,
            increment=False,
        )
        update_monthly_rollup(
            self.session,
            self.user_id,
            data.date,
            data.type,
            data.amount_cents,
            increment=True,
        )

        txn.date = data.date
        txn.occurred_at = data.occurred_at
        txn.type = data.type
        txn.amount_cents = data.amount_cents
        txn.category_id = data.category_id
        txn.note = data.note

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
            .options(joinedload(Transaction.category))
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
        return self.session.scalars(stmt).all()

    def all_for_period(
        self, period: Period, filters: Optional[TransactionFilters] = None
    ) -> list[Transaction]:
        filters = filters or TransactionFilters()
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
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
        return self.session.scalars(stmt).all()

    def recent(self, limit: int = 10) -> list[Transaction]:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
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
        update_monthly_rollup(
            self.session,
            self.user_id,
            txn.date,
            txn.type,
            txn.amount_cents,
            increment=False,
        )
        # Invalidate donut cache for the transaction period
        period = Period("transaction", txn.date, txn.date)
        metrics = MetricsService(self.session, self.user_id)
        metrics._invalidate_period_cache(period)
        txn.deleted_at = datetime.utcnow()
        self.session.commit()

    def restore(self, transaction_id: int) -> None:
        txn = self.session.get(Transaction, transaction_id)
        if not txn or txn.user_id != self.user_id:
            raise ValueError("Transaction not found")
        if txn.deleted_at is None:
            return
        txn.deleted_at = None
        update_monthly_rollup(
            self.session,
            self.user_id,
            txn.date,
            txn.type,
            txn.amount_cents,
            increment=True,
        )
        metrics = MetricsService(self.session, self.user_id)
        metrics._invalidate_period_cache(Period("transaction", txn.date, txn.date))
        self.session.commit()

    def deleted(self, limit: int = 200) -> list[Transaction]:
        stmt = (
            select(Transaction)
            .options(joinedload(Transaction.category))
            .where(
                Transaction.user_id == self.user_id, Transaction.deleted_at.isnot(None)
            )
            .order_by(Transaction.deleted_at.desc(), Transaction.id.desc())
            .limit(limit)
        )
        return self.session.scalars(stmt).all()


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
        income = int(row.income or 0)
        expenses = int(row.expenses or 0)
        return baseline + income - expenses


class MetricsService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()
        self._category_breakdown_cache: dict[str, list[dict[str, object]]] = {}

    def _invalidate_period_cache(self, period: Period) -> None:
        """Invalidate cache entries for a given period."""
        # Invalidate all type-specific cache keys for this period
        period_base = f"{period.start.isoformat()}_{period.end.isoformat()}"

        # Remove specific type cache keys
        for type_suffix in ["expense", "income"]:
            period_key = f"{period_base}_{type_suffix}"
            if period_key in self._category_breakdown_cache:
                del self._category_breakdown_cache[period_key]

        # Also remove old-style cache key for backward compatibility
        old_key = period_base
        if old_key in self._category_breakdown_cache:
            del self._category_breakdown_cache[old_key]

    def kpis(self, period: Period) -> dict[str, int]:
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
                Transaction.date.between(start, end),
            )
            row = self.session.execute(stmt).one()
            return int(row.income or 0), int(row.expenses or 0)

        balance_at_end = BalanceAnchorService(self.session, self.user_id).balance_as_of(
            datetime.combine(period.end, time.max)
        )

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
            full_income = int(row.income or 0)
            full_expenses = int(row.expenses or 0)

        income = start_income + full_income + end_income
        expenses = start_expenses + full_expenses + end_expenses
        return {
            "income": income,
            "expenses": expenses,
            "balance": balance_at_end,
        }

    def kpi_sparklines(self, period: Period, *, max_points: int = 12) -> dict[str, str]:
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
                Transaction.date.between(start, end),
            )
            row = self.session.execute(stmt).one()
            return int(row.income or 0), int(row.expenses or 0)

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
        for month in months:
            bucket_start = month
            bucket_end = month_end(month)
            if bucket_start < period.start:
                bucket_start = period.start
            if bucket_end > period.end:
                bucket_end = period.end
            full_month = bucket_start == month and bucket_end == month_end(month)
            if full_month:
                rollup = rollup_map.get((month.year, month.month))
                income = rollup.income_cents if rollup else 0
                expenses = rollup.expense_cents if rollup else 0
            else:
                income, expenses = income_expense_between(bucket_start, bucket_end)
            income_series.append(income)
            expense_series.append(expenses)
            balance_series.append(
                balance_service.balance_as_of(datetime.combine(bucket_end, time.max))
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
    ) -> list[dict[str, object]]:
        # For backward compatibility, default to expense if no type specified
        if transaction_type is None:
            transaction_type = TransactionType.expense

        type_suffix = transaction_type.value if transaction_type else "expense"
        category_suffix = (
            "all"
            if not category_ids
            else "cats_" + "_".join(str(i) for i in sorted(set(category_ids)))
        )
        period_key = f"{period.start.isoformat()}_{period.end.isoformat()}_{type_suffix}_{category_suffix}"
        if period_key in self._category_breakdown_cache:
            return self._category_breakdown_cache[period_key]

        stmt = (
            select(Category.name, func.sum(Transaction.amount_cents).label("total"))
            .join(Category, Category.id == Transaction.category_id)
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == transaction_type,
                Transaction.date.between(period.start, period.end),
            )
            .group_by(Category.name)
            .order_by(func.sum(Transaction.amount_cents).desc())
        )
        if category_ids:
            stmt = stmt.where(Transaction.category_id.in_(category_ids))
        rows = self.session.execute(stmt).all()
        total = sum(row.total or 0 for row in rows)
        breakdown = []
        for row in rows:
            amount = row.total or 0
            percent = (amount / total * 100) if total else 0
            breakdown.append(
                {"name": row.name, "amount_cents": amount, "percent": percent}
            )
        self._category_breakdown_cache[period_key] = breakdown
        return breakdown


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
        dates = set()
        for row in preview_rows:
            txn_type = TransactionType(row["type"])
            txn = Transaction(
                user_id=self.user_id,
                date=row["date"],
                occurred_at=datetime.combine(row["date"], time(12, 0)),
                type=txn_type,
                amount_cents=row["amount_cents"],
                category_id=row["category_id"],
                note=row["note"],
            )
            self.session.add(txn)
            update_monthly_rollup(
                self.session,
                self.user_id,
                row["date"],
                txn_type,
                row["amount_cents"],
                increment=True,
            )
            dates.add(row["date"])
        # Invalidate donut cache for all affected periods
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
                    Transaction.date.between(options.start, options.end),
                )
                if options.transaction_type is not None:
                    stmt = stmt.where(Transaction.type == options.transaction_type)
                if options.category_ids:
                    stmt = stmt.where(Transaction.category_id.in_(options.category_ids))
                row = self.session.execute(stmt).one()
                income = int(row.income or 0)
                expenses = int(row.expenses or 0)
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
            stmt = (
                select(Transaction.date, func.sum(Transaction.amount_cents))
                .where(
                    Transaction.user_id == self.user_id,
                    Transaction.deleted_at.is_(None),
                    Transaction.type == trend_type,
                    Transaction.date.between(options.start, options.end),
                )
                .group_by(Transaction.date)
                .order_by(Transaction.date)
            )
            if options.category_ids:
                stmt = stmt.where(Transaction.category_id.in_(options.category_ids))
            rows = self.session.execute(stmt).all()
            trend = [{"date": row[0], "amount_cents": row[1] or 0} for row in rows]
            data["trend"] = trend

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
                    opening_income = int(opening_row.income or 0)
                    opening_expenses = int(opening_row.expenses or 0)
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
                    totals[key] = totals.get(key, 0) + int(txn.amount_cents or 0)
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

    def list_for_month(self, year: int, month: int) -> list[Budget]:
        stmt = (
            select(Budget)
            .options(joinedload(Budget.category))
            .where(
                Budget.user_id == self.user_id,
                Budget.year == year,
                Budget.month == month,
            )
            .order_by(Budget.category_id.is_(None).desc(), Budget.amount_cents.desc())
        )
        return self.session.scalars(stmt).all()

    def upsert(self, data: BudgetIn) -> Budget:
        stmt = select(Budget).where(
            Budget.user_id == self.user_id,
            Budget.year == data.year,
            Budget.month == data.month,
            Budget.category_id.is_(None)
            if data.category_id is None
            else Budget.category_id == data.category_id,
        )
        existing = self.session.scalar(stmt)
        if existing:
            existing.amount_cents = data.amount_cents
            self.session.commit()
            self.session.refresh(existing)
            return existing

        if data.category_id is not None:
            category = self.session.get(Category, data.category_id)
            if not category or category.user_id != self.user_id:
                raise ValueError("Category not found")

        budget = Budget(
            user_id=self.user_id,
            year=data.year,
            month=data.month,
            category_id=data.category_id,
            amount_cents=data.amount_cents,
        )
        self.session.add(budget)
        self.session.commit()
        self.session.refresh(budget)
        return budget

    def delete(self, budget_id: int) -> None:
        budget = self.session.get(Budget, budget_id)
        if not budget or budget.user_id != self.user_id:
            raise ValueError("Budget not found")
        self.session.delete(budget)
        self.session.commit()

    def progress_for_month(self, year: int, month: int) -> dict[int, dict[str, int]]:
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1) - date.resolution
        else:
            end = date(year, month + 1, 1) - date.resolution

        stmt = (
            select(
                Transaction.category_id,
                func.coalesce(func.sum(Transaction.amount_cents), 0).label("spent"),
            )
            .where(
                Transaction.user_id == self.user_id,
                Transaction.deleted_at.is_(None),
                Transaction.type == TransactionType.expense,
                Transaction.date.between(start, end),
            )
            .group_by(Transaction.category_id)
        )
        spent_by_category = {
            row.category_id: int(row.spent or 0) for row in self.session.execute(stmt)
        }
        total_spent = sum(spent_by_category.values())

        progress: dict[int, dict[str, int]] = {}
        for budget in self.list_for_month(year, month):
            spent = (
                total_spent
                if budget.category_id is None
                else spent_by_category.get(budget.category_id, 0)
            )
            progress[budget.id] = {
                "spent_cents": spent,
                "remaining_cents": budget.amount_cents - spent,
            }
        return progress
