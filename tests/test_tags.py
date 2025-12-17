from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from database import Base
from models import TransactionType
from schemas import CategoryIn, TransactionIn
from services import CategoryService, TagService, TransactionService


def test_deleting_used_tag_clears_associations() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        category = CategoryService(session).create(
            CategoryIn(name="Food", type=TransactionType.expense, order=0)
        )
        txn = TransactionService(session).create(
            TransactionIn(
                date=date(2025, 1, 5),
                occurred_at=datetime(2025, 1, 5, 12, 0),
                type=TransactionType.expense,
                amount_cents=1299,
                category_id=category.id,
                note="Lunch",
                tags=["Dining"],
            )
        )
        tag = TagService(session).list_all()[0]

        TagService(session).delete(tag.id)

        txn_after = TransactionService(session).get(txn.id)
        assert txn_after.tags == []


def test_transaction_tag_inputs_are_deduplicated_case_insensitive() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        category = CategoryService(session).create(
            CategoryIn(name="Food", type=TransactionType.expense, order=0)
        )

        txn = TransactionService(session).create(
            TransactionIn(
                date=date(2025, 1, 5),
                occurred_at=datetime(2025, 1, 5, 12, 0),
                type=TransactionType.expense,
                amount_cents=1299,
                category_id=category.id,
                note="Lunch",
                tags=["Dining", "dining", " DINING "],
            )
        )

        assert len(txn.tags) == 1
        assert txn.tags[0].name == "Dining"
