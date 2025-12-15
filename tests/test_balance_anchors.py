from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import Category, TransactionType
from periods import Period
from schemas import BalanceAnchorIn, TransactionIn
from services import BalanceAnchorService, MetricsService, TransactionService


def make_session():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    return SessionLocal()


def test_balance_anchor_is_point_in_time_same_day() -> None:
    session = make_session()

    cat = Category(name="General", type=TransactionType.income, order=0)
    session.add(cat)
    session.commit()
    session.refresh(cat)

    txns = TransactionService(session)
    txns.create(
        TransactionIn(
            date=date(2025, 1, 1),
            occurred_at=datetime(2025, 1, 1, 10, 0),
            type=TransactionType.income,
            amount_cents=5_000,
            category_id=cat.id,
            note="Morning",
        )
    )

    anchors = BalanceAnchorService(session)
    anchors.create(
        BalanceAnchorIn(
            as_of_at=datetime(2025, 1, 1, 12, 0),
            balance_cents=10_000,
            note=None,
        )
    )

    txns.create(
        TransactionIn(
            date=date(2025, 1, 1),
            occurred_at=datetime(2025, 1, 1, 15, 0),
            type=TransactionType.income,
            amount_cents=2_000,
            category_id=cat.id,
            note="Afternoon",
        )
    )

    assert anchors.balance_as_of(datetime(2025, 1, 1, 11, 0)) == 5_000
    assert anchors.balance_as_of(datetime(2025, 1, 1, 12, 0)) == 10_000
    assert anchors.balance_as_of(datetime(2025, 1, 1, 23, 59, 59)) == 12_000

    metrics = MetricsService(session).kpis(
        Period("custom", date(2025, 1, 1), date(2025, 1, 1))
    )
    assert metrics["income"] == 7_000
    assert metrics["expenses"] == 0
    assert metrics["balance"] == 12_000


def test_multiple_anchors_per_day_last_anchor_wins() -> None:
    session = make_session()

    income = Category(name="Income", type=TransactionType.income, order=0)
    expense = Category(name="Expense", type=TransactionType.expense, order=0)
    session.add_all([income, expense])
    session.commit()
    session.refresh(income)
    session.refresh(expense)

    txns = TransactionService(session)
    txns.create(
        TransactionIn(
            date=date(2025, 1, 1),
            occurred_at=datetime(2025, 1, 1, 9, 0),
            type=TransactionType.income,
            amount_cents=3_000,
            category_id=income.id,
            note="Pay",
        )
    )
    txns.create(
        TransactionIn(
            date=date(2025, 1, 1),
            occurred_at=datetime(2025, 1, 1, 9, 30),
            type=TransactionType.expense,
            amount_cents=1_000,
            category_id=expense.id,
            note="Coffee",
        )
    )

    anchors = BalanceAnchorService(session)
    anchors.create(
        BalanceAnchorIn(
            as_of_at=datetime(2025, 1, 1, 12, 0),
            balance_cents=10_000,
            note="Noon reconcile",
        )
    )
    anchors.create(
        BalanceAnchorIn(
            as_of_at=datetime(2025, 1, 1, 18, 0),
            balance_cents=20_000,
            note="Evening reconcile",
        )
    )

    txns.create(
        TransactionIn(
            date=date(2025, 1, 1),
            occurred_at=datetime(2025, 1, 1, 20, 0),
            type=TransactionType.expense,
            amount_cents=500,
            category_id=expense.id,
            note="Snack",
        )
    )

    assert anchors.balance_as_of(datetime(2025, 1, 1, 17, 0)) == 10_000
    assert anchors.balance_as_of(datetime(2025, 1, 1, 19, 0)) == 20_000
    assert anchors.balance_as_of(datetime(2025, 1, 1, 23, 59, 59)) == 19_500

    metrics = MetricsService(session).kpis(
        Period("custom", date(2025, 1, 1), date(2025, 1, 1))
    )
    assert metrics["income"] == 3_000
    assert metrics["expenses"] == 1_500
    assert metrics["balance"] == 19_500
