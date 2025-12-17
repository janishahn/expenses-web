from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import pytest

from database import Base
from models import RuleMatchType, TransactionType
from schemas import CategoryIn, RuleIn, TransactionIn
from services import CategoryService, RuleService, TagService, TransactionService


def test_rule_applies_category_and_tags_on_create() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        categories = CategoryService(session)
        misc = categories.create(
            CategoryIn(name="Misc", type=TransactionType.expense, order=0)
        )
        subs = categories.create(
            CategoryIn(name="Subscriptions", type=TransactionType.expense, order=0)
        )
        hidden = TagService(session).create("Reimbursed", is_hidden_from_budget=True)

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
                add_tags=["Streaming"],
                budget_exclude_tag_id=hidden.id,
            )
        )

        txn = TransactionService(session).create(
            TransactionIn(
                date=date(2025, 1, 5),
                occurred_at=datetime(2025, 1, 5, 12, 0),
                type=TransactionType.expense,
                amount_cents=1299,
                category_id=misc.id,
                note="Netflix January",
                tags=[],
            )
        )

        assert txn.category_id == subs.id
        tag_names = {t.name for t in txn.tags}
        assert "Streaming" in tag_names
        assert "Reimbursed" in tag_names


def test_rule_does_not_set_category_across_types() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        categories = CategoryService(session)
        expense_cat = categories.create(
            CategoryIn(name="Food", type=TransactionType.expense, order=0)
        )
        income_cat = categories.create(
            CategoryIn(name="Salary", type=TransactionType.income, order=0)
        )

        with pytest.raises(ValueError, match="Category type mismatch"):
            RuleService(session).create(
                RuleIn(
                    name="Salary keyword sets income category (should not apply)",
                    enabled=True,
                    priority=10,
                    match_type=RuleMatchType.contains,
                    match_value="salary",
                    transaction_type=TransactionType.expense,
                    min_amount_cents=None,
                    max_amount_cents=None,
                    set_category_id=income_cat.id,
                    add_tags=[],
                    budget_exclude_tag_id=None,
                )
            )

        txn = TransactionService(session).create(
            TransactionIn(
                date=date(2025, 1, 1),
                occurred_at=datetime(2025, 1, 1, 9, 0),
                type=TransactionType.expense,
                amount_cents=500,
                category_id=expense_cat.id,
                note="salary test",
                tags=[],
            )
        )
        assert txn.category_id == expense_cat.id
