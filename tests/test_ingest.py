from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from database import Base
from models import Category, RuleMatchType, TransactionType
from schemas import CategoryIn, IngestTransactionIn, RuleIn
from services import (
    CategoryService,
    IngestCategoryAmbiguous,
    IngestService,
    RuleService,
)


def test_ingest_creates_and_uses_uncategorized_default() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        txn = IngestService(session).ingest_expense(
            IngestTransactionIn(amount_cents=1234, note="Coffee", date=date(2025, 1, 1))
        )
        assert txn.type == TransactionType.expense
        assert txn.category.name == "Uncategorized"

        categories = session.scalars(
            select(Category).where(Category.type == txn.type)
        ).all()
        assert [c.name for c in categories] == ["Uncategorized"]


def test_ingest_matches_existing_category_case_insensitive() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        food = CategoryService(session).create(
            CategoryIn(name="Food", type=TransactionType.expense, order=0)
        )
        txn = IngestService(session).ingest_expense(
            IngestTransactionIn(
                amount_cents=500,
                note="Lunch",
                date=date(2025, 1, 2),
                category="food",
            )
        )
        assert txn.category_id == food.id
        assert txn.category.name == "Food"


def test_ingest_fuzzy_matches_within_one_edit() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        subs = CategoryService(session).create(
            CategoryIn(name="Subscriptions", type=TransactionType.expense, order=0)
        )
        txn = IngestService(session).ingest_expense(
            IngestTransactionIn(
                amount_cents=1299,
                note="Netflix",
                date=date(2025, 1, 3),
                category="Subscriptioms",
            )
        )
        assert txn.category_id == subs.id


def test_ingest_creates_category_when_not_found() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        txn = IngestService(session).ingest_expense(
            IngestTransactionIn(
                amount_cents=2500,
                note="Gym",
                date=date(2025, 1, 4),
                category="Health & Fitness",
            )
        )
        assert txn.category.name == "Health & Fitness"


def test_ingest_raises_on_ambiguous_fuzzy_match() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        CategoryService(session).create(
            CategoryIn(name="Food", type=TransactionType.expense, order=0)
        )
        CategoryService(session).create(
            CategoryIn(name="Fool", type=TransactionType.expense, order=0)
        )
        with pytest.raises(IngestCategoryAmbiguous):
            IngestService(session).ingest_expense(
                IngestTransactionIn(
                    amount_cents=100,
                    note="Test",
                    date=date(2025, 1, 5),
                    category="Foob",
                )
            )


def test_ingest_can_be_recategorized_by_rules() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        categories = CategoryService(session)
        subs = categories.create(
            CategoryIn(name="Subscriptions", type=TransactionType.expense, order=0)
        )
        RuleService(session).create(
            RuleIn(
                name="Netflix → Subscriptions",
                enabled=True,
                priority=10,
                match_type=RuleMatchType.contains,
                match_value="netflix",
                transaction_type=TransactionType.expense,
                min_amount_cents=None,
                max_amount_cents=None,
                set_category_id=subs.id,
                add_tags=[],
                budget_exclude_tag_id=None,
            )
        )

        txn = IngestService(session).ingest_expense(
            IngestTransactionIn(
                amount_cents=1299,
                note="Netflix January",
                date=date(2025, 1, 6),
            )
        )
        assert txn.category_id == subs.id
