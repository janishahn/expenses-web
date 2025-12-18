from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import Category, TransactionType
from periods import Period
from schemas import TransactionIn
from services import (
    BudgetService,
    MetricsService,
    ReimbursementService,
    TransactionService,
)


def make_session():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    return SessionLocal()


def test_reimbursement_applies_to_original_expense_period() -> None:
    session = make_session()
    income = Category(name="Income", type=TransactionType.income, order=0)
    expense = Category(name="Food", type=TransactionType.expense, order=0)
    session.add_all([income, expense])
    session.commit()
    session.refresh(income)
    session.refresh(expense)

    txns = TransactionService(session)
    reimb = ReimbursementService(session)

    dinner = txns.create(
        TransactionIn(
            date=date(2025, 1, 10),
            occurred_at=datetime(2025, 1, 10, 20, 0),
            type=TransactionType.expense,
            amount_cents=10_000,
            category_id=expense.id,
            note="Dinner for group",
        )
    )

    payback = txns.create(
        TransactionIn(
            date=date(2025, 2, 5),
            occurred_at=datetime(2025, 2, 5, 12, 0),
            type=TransactionType.income,
            is_reimbursement=True,
            amount_cents=6_000,
            category_id=income.id,
            note="Payback",
        )
    )
    reimb.upsert_allocation(payback.id, dinner.id, 6_000)

    jan = MetricsService(session).kpis(
        Period("jan", date(2025, 1, 1), date(2025, 1, 31))
    )
    assert jan["income"] == 0
    assert jan["expenses"] == 4_000

    feb = MetricsService(session).kpis(
        Period("feb", date(2025, 2, 1), date(2025, 2, 28))
    )
    assert feb["income"] == 0
    assert feb["expenses"] == 0


def test_budgets_use_net_expense() -> None:
    session = make_session()
    income = Category(name="Income", type=TransactionType.income, order=0)
    expense = Category(name="Travel", type=TransactionType.expense, order=0)
    session.add_all([income, expense])
    session.commit()
    session.refresh(income)
    session.refresh(expense)

    txns = TransactionService(session)
    reimb = ReimbursementService(session)

    booking = txns.create(
        TransactionIn(
            date=date(2025, 3, 3),
            occurred_at=datetime(2025, 3, 3, 9, 0),
            type=TransactionType.expense,
            amount_cents=20_000,
            category_id=expense.id,
            note="Hotel booking",
        )
    )
    payback = txns.create(
        TransactionIn(
            date=date(2025, 3, 10),
            occurred_at=datetime(2025, 3, 10, 10, 0),
            type=TransactionType.income,
            is_reimbursement=True,
            amount_cents=5_000,
            category_id=income.id,
            note="Hotel share",
        )
    )
    reimb.upsert_allocation(payback.id, booking.id, 5_000)

    spent = BudgetService(session).spent_by_category_for_month(2025, 3)
    assert spent[expense.id] == 15_000
    assert spent[None] == 15_000


def test_deleting_reimbursement_removes_netting() -> None:
    session = make_session()
    income = Category(name="Income", type=TransactionType.income, order=0)
    expense = Category(name="Food", type=TransactionType.expense, order=0)
    session.add_all([income, expense])
    session.commit()
    session.refresh(income)
    session.refresh(expense)

    txns = TransactionService(session)
    reimb = ReimbursementService(session)

    meal = txns.create(
        TransactionIn(
            date=date(2025, 4, 2),
            occurred_at=datetime(2025, 4, 2, 19, 0),
            type=TransactionType.expense,
            amount_cents=8_000,
            category_id=expense.id,
            note="Meal",
        )
    )
    payback = txns.create(
        TransactionIn(
            date=date(2025, 5, 1),
            occurred_at=datetime(2025, 5, 1, 9, 0),
            type=TransactionType.income,
            is_reimbursement=True,
            amount_cents=3_000,
            category_id=income.id,
            note="Payback",
        )
    )
    reimb.upsert_allocation(payback.id, meal.id, 3_000)

    april = MetricsService(session).kpis(
        Period("april", date(2025, 4, 1), date(2025, 4, 30))
    )
    assert april["expenses"] == 5_000

    txns.soft_delete(payback.id)
    april_after = MetricsService(session).kpis(
        Period("april", date(2025, 4, 1), date(2025, 4, 30))
    )
    assert april_after["expenses"] == 8_000


def test_deleting_expense_frees_reimbursement_amount() -> None:
    session = make_session()
    income = Category(name="Income", type=TransactionType.income, order=0)
    expense = Category(name="Food", type=TransactionType.expense, order=0)
    session.add_all([income, expense])
    session.commit()
    session.refresh(income)
    session.refresh(expense)

    txns = TransactionService(session)
    reimb = ReimbursementService(session)

    first = txns.create(
        TransactionIn(
            date=date(2025, 6, 1),
            occurred_at=datetime(2025, 6, 1, 12, 0),
            type=TransactionType.expense,
            amount_cents=10_000,
            category_id=expense.id,
            note="First expense",
        )
    )
    second = txns.create(
        TransactionIn(
            date=date(2025, 6, 2),
            occurred_at=datetime(2025, 6, 2, 12, 0),
            type=TransactionType.expense,
            amount_cents=10_000,
            category_id=expense.id,
            note="Second expense",
        )
    )
    payback = txns.create(
        TransactionIn(
            date=date(2025, 6, 3),
            occurred_at=datetime(2025, 6, 3, 12, 0),
            type=TransactionType.income,
            is_reimbursement=True,
            amount_cents=6_000,
            category_id=income.id,
            note="Payback",
        )
    )

    reimb.upsert_allocation(payback.id, first.id, 6_000)
    assert reimb.allocated_total_for_reimbursement(payback.id) == 6_000

    txns.soft_delete(first.id)
    assert reimb.allocated_total_for_reimbursement(payback.id) == 0

    reimb.upsert_allocation(payback.id, second.id, 6_000)
    assert reimb.allocated_total_for_reimbursement(payback.id) == 6_000
