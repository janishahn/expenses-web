from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from database import Base
from models import (
    Category,
    IntervalUnit,
    MonthDayPolicy,
    RecurringRule,
    Transaction,
    TransactionType,
)
from recurrence import RecurringEngine, calculate_next_date


def _rule(policy: MonthDayPolicy, skip_weekends: bool = False) -> RecurringRule:
    return RecurringRule(
        id=1,
        user_id=1,
        name="Test",
        type=TransactionType.expense,
        amount_cents=1000,
        category_id=1,
        anchor_date=date(2024, 1, 31),
        interval_unit=IntervalUnit.month,
        interval_count=1,
        next_occurrence=date(2024, 1, 31),
        end_date=None,
        auto_post=True,
        skip_weekends=skip_weekends,
        month_day_policy=policy,
    )


def test_calculate_next_date_snap_to_end():
    rule = _rule(MonthDayPolicy.snap_to_end)
    assert calculate_next_date(rule, date(2024, 1, 31)) == date(2024, 2, 29)


def test_calculate_next_date_skip_policy():
    rule = _rule(MonthDayPolicy.skip)
    assert calculate_next_date(rule, date(2024, 1, 31)) == date(2024, 3, 31)


def test_calculate_next_date_weekend_shift():
    rule = _rule(MonthDayPolicy.snap_to_end, skip_weekends=True)
    # Skip weekends ensures we nudge forward if result lands on Saturday/Sunday.
    next_date = calculate_next_date(rule, date(2024, 3, 29))
    assert next_date == date(2024, 4, 30)


def test_recurring_engine_idempotent_posts():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        category = Category(
            user_id=1,
            name="Rent",
            type=TransactionType.expense,
            color="#ffffff",
        )
        session.add(category)
        session.flush()
        rule = RecurringRule(
            user_id=1,
            name="Rent",
            type=TransactionType.expense,
            amount_cents=10000,
            category_id=category.id,
            anchor_date=date(2024, 1, 1),
            interval_unit=IntervalUnit.month,
            interval_count=1,
            next_occurrence=date(2024, 1, 1),
            auto_post=True,
            skip_weekends=False,
            month_day_policy=MonthDayPolicy.snap_to_end,
        )
        session.add(rule)
        session.commit()

    with Session(engine) as session:
        rule = session.query(RecurringRule).first()
        recurring = RecurringEngine(session)
        recurring.catch_up_rule(rule, today=date(2024, 3, 1))
        session.commit()

    with Session(engine) as session:
        rule = session.query(RecurringRule).first()
        assert rule.next_occurrence > date(2024, 3, 1)
        recurring = RecurringEngine(session)
        recurring.catch_up_rule(rule, today=date(2024, 3, 1))
        session.commit()
        txn_count = (
            session.query(Transaction)
            .filter(Transaction.origin_rule_id == rule.id)
            .count()
        )
        assert txn_count == 3
