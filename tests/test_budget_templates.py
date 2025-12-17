from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from database import Base
from models import BudgetFrequency, TransactionType
from schemas import BudgetOverrideIn, BudgetTemplateIn, CategoryIn, TransactionIn
from services import BudgetService, CategoryService, TagService, TransactionService


def test_effective_budget_prefers_override_over_template() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        categories = CategoryService(session)
        groceries = categories.create(
            CategoryIn(name="Groceries", type=TransactionType.expense, order=0)
        )

        budgets = BudgetService(session)
        budgets.upsert_template(
            BudgetTemplateIn(
                frequency=BudgetFrequency.monthly,
                category_id=groceries.id,
                amount_cents=10_000,
                starts_on=date(2025, 1, 1),
                ends_on=None,
            )
        )

        effective = budgets.effective_budgets_for_month(2025, 2)
        by_scope = {b.scope_category_id: b for b in effective}
        assert by_scope[groceries.id].amount_cents == 10_000
        assert by_scope[groceries.id].source == "template"

        budgets.upsert_override(
            BudgetOverrideIn(
                year=2025, month=2, category_id=groceries.id, amount_cents=15_000
            )
        )
        effective2 = budgets.effective_budgets_for_month(2025, 2)
        by_scope2 = {b.scope_category_id: b for b in effective2}
        assert by_scope2[groceries.id].amount_cents == 15_000
        assert by_scope2[groceries.id].source == "override"


def test_budget_progress_excludes_hidden_from_budget_tags() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        categories = CategoryService(session)
        groceries = categories.create(
            CategoryIn(name="Groceries", type=TransactionType.expense, order=0)
        )
        income = categories.create(
            CategoryIn(name="Salary", type=TransactionType.income, order=0)
        )

        TagService(session).create("Reimbursed", is_hidden_from_budget=True)

        budgets = BudgetService(session)
        budgets.upsert_override(
            BudgetOverrideIn(
                year=2025, month=1, category_id=groceries.id, amount_cents=10_000
            )
        )

        txns = TransactionService(session)
        txns.create(
            TransactionIn(
                date=date(2025, 1, 10),
                occurred_at=datetime(2025, 1, 10, 12, 0),
                type=TransactionType.expense,
                amount_cents=3_000,
                category_id=groceries.id,
                note="Groceries",
                tags=[],
            )
        )
        txns.create(
            TransactionIn(
                date=date(2025, 1, 11),
                occurred_at=datetime(2025, 1, 11, 12, 0),
                type=TransactionType.expense,
                amount_cents=4_000,
                category_id=groceries.id,
                note="Reimbursed groceries",
                tags=["Reimbursed"],
            )
        )
        txns.create(
            TransactionIn(
                date=date(2025, 1, 1),
                occurred_at=datetime(2025, 1, 1, 9, 0),
                type=TransactionType.income,
                amount_cents=100_000,
                category_id=income.id,
                note="Salary",
                tags=[],
            )
        )

        progress = budgets.progress_for_month(2025, 1)
        assert progress[groceries.id]["spent_cents"] == 3_000
        assert progress[groceries.id]["remaining_cents"] == 7_000
