import logging
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from typing import TYPE_CHECKING

from csv_utils import parse_amount
from config import get_settings
from csrf import generate_csrf_token, validate_csrf_token
from database import SessionLocal
from legacy_sqlite_import import LegacySQLiteImportService
from models import (
    Category,
    CurrencyCode,
    IntervalUnit,
    MonthDayPolicy,
    Transaction,
    TransactionType,
)
from periods import Period, resolve_period
from scheduler import SchedulerManager
from schemas import (
    BalanceAnchorIn,
    BudgetOverrideIn,
    BudgetTemplateIn,
    CategoryIn,
    IngestTransactionIn,
    IngestTransactionOut,
    RecurringRuleIn,
    ReportOptions,
    RuleIn,
    TransactionIn,
)
from services import (
    BalanceAnchorService,
    BudgetService,
    CSVService,
    CategoryService,
    IngestCategoryAmbiguous,
    IngestCategoryNotFound,
    IngestService,
    InsightsService,
    MetricsService,
    ReimbursementService,
    RecurringRuleService,
    TransactionFilters,
    TransactionService,
    ReportService,
    rebuild_monthly_rollups,
)
import services

if TYPE_CHECKING:  # pragma: no cover
    from weasyprint import HTML, CSS  # noqa: F401
    from weasyprint.text.fonts import FontConfiguration  # noqa: F401

app = FastAPI(title="Expense Tracker")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def _load_app_version() -> str:
    try:
        import tomllib
    except Exception:
        return "unknown"
    try:
        with open("pyproject.toml", "rb") as f:
            data = tomllib.load(f)
        return str(data.get("project", {}).get("version", "unknown"))
    except Exception:
        return "unknown"


APP_VERSION = _load_app_version()


def format_currency(cents: int, options: Optional[dict] = None) -> str:
    include_cents = True
    if options is not None:
        if isinstance(options, dict):
            include_cents = options.get("include_cents", True)
        else:
            include_cents = bool(getattr(options, "include_cents", True))
    if include_cents:
        return f"{cents / 100:,.2f}".replace(",", " ").replace(".", ",")
    return f"{cents / 100:,.0f}".replace(",", " ")


def format_eurodate(value) -> str:
    """Format a date or datetime as DD.MM.YYYY (European format)."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%d.%m.%Y")
    return str(value)


def format_eurodatetime(value) -> str:
    """Format a datetime as DD.MM.YYYY HH:MM (European format)."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%d.%m.%Y %H:%M")
    return str(value)


templates.env.filters["currency"] = format_currency
templates.env.filters["eurodate"] = format_eurodate
templates.env.filters["eurodatetime"] = format_eurodatetime
templates.env.globals["math"] = math
templates.env.globals["TransactionType"] = TransactionType
templates.env.globals["IntervalUnit"] = IntervalUnit
templates.env.globals["MonthDayPolicy"] = MonthDayPolicy
templates.env.globals["csrf_token"] = generate_csrf_token
templates.env.globals["today"] = lambda: date.today().isoformat()
templates.env.globals["now_local"] = (
    lambda: datetime.now(ZoneInfo(get_settings().timezone))
    .replace(tzinfo=None)
    .strftime("%Y-%m-%dT%H:%M")
)


def static_path(path: str) -> str:
    return app.url_path_for("static", path=path)


templates.env.globals["static_path"] = static_path


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


scheduler_manager = SchedulerManager()


@app.on_event("startup")
def startup_event():
    scheduler_manager.start()


@app.on_event("shutdown")
def shutdown_event():
    scheduler_manager.stop()


def period_from_request(request: Request) -> Period:
    period_slug = request.query_params.get("period")
    start = request.query_params.get("start")
    end = request.query_params.get("end")
    try:
        return resolve_period(period_slug, start, end)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def filters_from_request(request: Request) -> TransactionFilters:
    type_param = request.query_params.get("type")
    category_param = request.query_params.get("category")
    query = request.query_params.get("q")
    txn_type = None
    if type_param:
        try:
            txn_type = TransactionType(type_param)
        except ValueError:
            txn_type = None
    category_id = None
    if category_param:
        try:
            category_id = int(category_param)
        except ValueError:
            category_id = None

    tag_param = request.query_params.get("tag")
    tag_id = None
    if tag_param:
        try:
            tag_id = int(tag_param)
        except ValueError:
            tag_id = None

    return TransactionFilters(
        type=txn_type, category_id=category_id, query=query, tag_id=tag_id
    )


def render(request: Request, template: str, context: dict[str, object]) -> HTMLResponse:
    ctx = {"request": request}
    ctx.update(context)
    return templates.TemplateResponse(template, ctx)


def recurring_payload_from_form(form) -> RecurringRuleIn:
    start_date = date.fromisoformat(form["start_date"])
    next_occurrence_raw = form.get("next_occurrence")
    if next_occurrence_raw:
        next_occurrence = date.fromisoformat(next_occurrence_raw)
    else:
        next_occurrence = start_date

    return RecurringRuleIn(
        name=form.get("name"),
        type=TransactionType(form["type"]),
        currency_code=CurrencyCode(form.get("currency_code", "EUR")),
        amount_cents=parse_amount(form["amount"]),
        category_id=int(form["category_id"]),
        anchor_date=start_date,
        interval_unit=IntervalUnit(form["interval_unit"]),
        interval_count=int(form.get("interval_count", 1) or 1),
        next_occurrence=next_occurrence,
        end_date=date.fromisoformat(form["end_date"]) if form.get("end_date") else None,
        auto_post=form.get("auto_post") == "on",
        skip_weekends=form.get("skip_weekends") == "on",
        month_day_policy=MonthDayPolicy(form["month_day_policy"]),
    )


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    category_service = CategoryService(db)
    metrics_service = MetricsService(db)
    txn_service = TransactionService(db)
    categories = category_service.list_all()
    all_tags = [
        {"id": t.id, "name": t.name} for t in services.TagService(db).list_all()
    ]
    has_any_transactions = txn_service.has_any()

    donut_context: dict[str, object] = {"has_any_transactions": has_any_transactions}
    if has_any_transactions:
        if filters.type == TransactionType.expense:
            donut_context.update(
                {
                    "mode": "expense-only",
                    "expense_breakdown": metrics_service.category_breakdown(
                        period, TransactionType.expense
                    ),
                }
            )
        elif filters.type == TransactionType.income:
            donut_context.update(
                {
                    "mode": "income-only",
                    "income_breakdown": metrics_service.category_breakdown(
                        period, TransactionType.income
                    ),
                }
            )
        else:
            donut_context.update(
                {
                    "mode": "both",
                    "expense_breakdown": metrics_service.category_breakdown(
                        period, TransactionType.expense
                    ),
                    "income_breakdown": metrics_service.category_breakdown(
                        period, TransactionType.income
                    ),
                }
            )
    kpi = metrics_service.kpis(period)
    sparklines = metrics_service.kpi_sparklines(period)
    recent = txn_service.list(period, filters, limit=10)
    period_query = f"period={period.slug}&start={period.start}&end={period.end}"
    return render(
        request,
        "dashboard.html",
        {
            "period": period,
            "filters": filters,
            "categories": categories,
            "tags": all_tags,
            "kpi": kpi,
            "sparklines": sparklines,
            **donut_context,
            "recent": recent,
            "period_query": period_query,
        },
    )


@app.get("/transactions", response_class=HTMLResponse)
def transactions_page(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    page = int(request.query_params.get("page", "1"))
    page = max(page, 1)
    limit = 25
    txn_service = TransactionService(db)
    offset = (page - 1) * limit
    items = txn_service.list(period, filters, limit=limit + 1, offset=offset)
    has_more = len(items) > limit
    items = items[:limit]
    categories = CategoryService(db).list_all()
    all_tags = [
        {"id": t.id, "name": t.name} for t in services.TagService(db).list_all()
    ]
    period_query = f"period={period.slug}&start={period.start}&end={period.end}"
    from urllib.parse import urlencode

    filter_params: dict[str, str] = {}
    if filters.type:
        filter_params["type"] = filters.type.value
    if filters.category_id:
        filter_params["category"] = str(filters.category_id)
    if filters.tag_id:
        filter_params["tag"] = str(filters.tag_id)
    if filters.query:
        filter_params["q"] = filters.query
    filter_query = urlencode(filter_params)
    from urllib.parse import quote

    next_q = quote(str(request.url), safe="")
    return render(
        request,
        "transactions.html",
        {
            "period": period,
            "transactions": items,
            "categories": categories,
            "tags": all_tags,
            "filters": filters,
            "page": page,
            "has_more": has_more,
            "period_query": period_query,
            "filter_query": filter_query,
            "next_q": next_q,
        },
    )


@app.post("/transactions")
async def create_transaction(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    token = form.get("csrf_token", "")
    if not validate_csrf_token(token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        category_id = int(form["category_id"])
        category = db.get(Category, category_id)
        if not category:
            raise ValueError("Category not found")

        occurred_raw = form.get("occurred_at")
        if occurred_raw:
            occurred_at = datetime.fromisoformat(str(occurred_raw))
            txn_date = occurred_at.date()
        else:
            txn_date = date.fromisoformat(form["date"])
            occurred_at = datetime.combine(txn_date, datetime.now().time()).replace(
                second=0, microsecond=0
            )
        data = TransactionIn(
            date=txn_date,
            occurred_at=occurred_at,
            type=category.type,
            is_reimbursement=(form.get("is_reimbursement") == "on"),
            amount_cents=parse_amount(form["amount"]),
            category_id=category_id,
            note=form["note"],
            tags=[t.strip() for t in (form.get("tags") or "").split(",") if t.strip()],
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    service = TransactionService(db)
    try:
        service.create(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    headers = {"HX-Trigger": "transactions-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    return RedirectResponse(
        url=request.app.url_path_for("dashboard"), status_code=303, headers=headers
    )


@app.post("/balance-anchors")
async def create_balance_anchor(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    token = form.get("csrf_token", "")
    if not validate_csrf_token(token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    next_url = form.get("next") or request.app.url_path_for("dashboard")
    try:
        note_raw = form.get("note")
        note = (
            note_raw.strip() if isinstance(note_raw, str) and note_raw.strip() else None
        )
        data = BalanceAnchorIn(
            as_of_at=datetime.fromisoformat(str(form["as_of_at"])),
            balance_cents=parse_amount(str(form["balance"]), allow_negative=True),
            note=note,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        BalanceAnchorService(db).create(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    headers = {"HX-Trigger": "balance-anchors-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    return RedirectResponse(url=str(next_url), status_code=303, headers=headers)


@app.post("/balance-anchors/{anchor_id}/delete")
async def delete_balance_anchor(
    anchor_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        BalanceAnchorService(db).delete(anchor_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    headers = {"HX-Trigger": "balance-anchors-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=200, content="", headers=headers)
    next_url = form.get("next") or request.app.url_path_for("admin_page")
    return RedirectResponse(url=str(next_url), status_code=303, headers=headers)


@app.post("/balance-anchors/{anchor_id}/edit")
async def edit_balance_anchor(
    anchor_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    next_url = form.get("next") or request.app.url_path_for("admin_page")
    try:
        note_raw = form.get("note")
        note = (
            note_raw.strip() if isinstance(note_raw, str) and note_raw.strip() else None
        )
        data = BalanceAnchorIn(
            as_of_at=datetime.fromisoformat(str(form["as_of_at"])),
            balance_cents=parse_amount(str(form["balance"]), allow_negative=True),
            note=note,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        BalanceAnchorService(db).update(anchor_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    headers = {"HX-Trigger": "balance-anchors-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    return RedirectResponse(url=str(next_url), status_code=303, headers=headers)


@app.post("/transactions/{transaction_id}/delete")
async def delete_transaction(
    transaction_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        TransactionService(db).soft_delete(transaction_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    headers = {"HX-Trigger": "transactions-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=200, content="", headers=headers)
    next_url = request.headers.get("Referer") or request.app.url_path_for(
        "transactions_page"
    )
    return RedirectResponse(url=str(next_url), status_code=303, headers=headers)


@app.get("/categories", response_class=HTMLResponse)
def categories_page(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    category_service = CategoryService(db)
    categories = category_service.list_all(include_archived=True)
    usage_stmt = (
        select(Transaction.category_id, func.count().label("usage"))
        .where(
            Transaction.user_id == category_service.user_id,
            Transaction.deleted_at.is_(None),
            Transaction.date.between(period.start, period.end),
        )
        .group_by(Transaction.category_id)
    )
    usage_rows = db.execute(usage_stmt).all()
    usage_map = {row.category_id: row.usage for row in usage_rows}
    return render(
        request,
        "categories.html",
        {
            "categories": categories,
            "period": period,
            "category_usage": usage_map,
        },
    )


@app.post("/categories")
async def create_category(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        data = CategoryIn(
            name=form["name"],
            type=TransactionType(form["type"]),
            order=int(form.get("order", 0) or 0),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        CategoryService(db).create(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    headers = {"HX-Trigger": "categories-updated"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    return RedirectResponse(
        url=request.app.url_path_for("categories_page"),
        status_code=303,
        headers=headers,
    )


@app.post("/categories/{category_id}/archive")
async def archive_category(
    category_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        CategoryService(db).archive(category_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204, headers={"HX-Trigger": "categories-updated"})


@app.post("/categories/{category_id}/restore")
async def restore_category(
    category_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        CategoryService(db).restore(category_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204, headers={"HX-Trigger": "categories-updated"})


@app.get("/recurring", response_class=HTMLResponse)
def recurring_page(request: Request, db: Session = Depends(get_db)):
    service = RecurringRuleService(db)
    rules = service.list()
    stats = service.get_statistics()
    categories = CategoryService(db).list_all()
    return render(
        request,
        "recurring.html",
        {"rules": rules, "categories": categories, "stats": stats},
    )


@app.post("/recurring")
async def create_recurring(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        data = recurring_payload_from_form(form)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        RecurringRuleService(db).create(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(
        url=request.app.url_path_for("recurring_page"), status_code=303
    )


@app.post("/recurring/{rule_id}/toggle")
async def toggle_recurring(
    rule_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    auto_post = form.get("auto_post") == "true"
    try:
        RecurringRuleService(db).toggle_auto_post(rule_id, auto_post)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204, headers={"HX-Trigger": "recurring-updated"})


@app.post("/recurring/{rule_id}")
async def update_recurring(
    rule_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        data = recurring_payload_from_form(form)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        RecurringRuleService(db).update(rule_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RedirectResponse(
        url=request.app.url_path_for("recurring_page"), status_code=303
    )


@app.post("/recurring/{rule_id}/delete")
async def delete_recurring(
    rule_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        RecurringRuleService(db).delete(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers={"HX-Trigger": "recurring-updated"})
    return RedirectResponse(
        url=request.app.url_path_for("recurring_page"), status_code=303
    )


@app.get("/recurring/{rule_id}/occurrences", response_class=HTMLResponse)
def recurring_occurrences(
    rule_id: int, request: Request, db: Session = Depends(get_db)
):
    service = RecurringRuleService(db)
    try:
        rule = service.get(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    stmt = (
        select(Transaction)
        .where(
            Transaction.user_id == service.user_id,
            Transaction.origin_rule_id == rule_id,
            Transaction.deleted_at.is_(None),
        )
        .order_by(Transaction.occurrence_date.desc())
    )
    occurrences = db.scalars(stmt).all()

    return render(
        request,
        "recurring_occurrences.html",
        {"rule": rule, "occurrences": occurrences},
    )


@app.post("/api/recurring/preview")
async def preview_recurring_occurrences(request: Request):
    """
    Returns next N occurrences for a recurring rule configuration.
    Used for live preview in the form.
    """
    from recurrence import calculate_next_date

    try:
        data = await request.json()
    except Exception:
        return {"occurrences": [], "error": "Invalid JSON"}

    start_date_str = data.get("start_date")
    interval_unit_str = data.get("interval_unit", "month")
    interval_count = int(data.get("interval_count", 1) or 1)
    month_day_policy_str = data.get("month_day_policy", "snap_to_end")
    skip_weekends = data.get("skip_weekends", False)

    if not start_date_str:
        return {"occurrences": [], "error": "start_date required"}

    try:
        start_date = date.fromisoformat(start_date_str)
        interval_unit = IntervalUnit(interval_unit_str)
        month_day_policy = MonthDayPolicy(month_day_policy_str)
    except (ValueError, KeyError) as e:
        return {"occurrences": [], "error": str(e)}

    class TempRule:
        pass

    rule = TempRule()
    rule.anchor_date = start_date
    rule.interval_unit = interval_unit
    rule.interval_count = interval_count
    rule.month_day_policy = month_day_policy
    rule.skip_weekends = skip_weekends

    occurrences = [start_date.isoformat()]
    current_date = start_date

    for _ in range(3):
        try:
            next_date = calculate_next_date(rule, current_date)
            occurrences.append(next_date.isoformat())
            current_date = next_date
        except Exception:
            break

    return {"occurrences": occurrences}


@app.post("/api/ingest", response_model=IngestTransactionOut, status_code=201)
def api_ingest(data: IngestTransactionIn, db: Session = Depends(get_db)):
    try:
        txn = IngestService(db).ingest_expense(data)
    except IngestCategoryNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except IngestCategoryAmbiguous as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return IngestTransactionOut(
        id=txn.id,
        date=txn.date,
        occurred_at=txn.occurred_at,
        type="expense",
        amount_cents=txn.amount_cents,
        category=txn.category.name,
        note=txn.note or "",
    )


@app.get("/api/kpis")
def api_kpis(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    metrics = MetricsService(db).kpis(period)
    return metrics


@app.get("/api/category-breakdown")
def api_category_breakdown(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    data = MetricsService(db).category_breakdown(period)
    return data


@app.get("/api/transactions")
def api_transactions(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    page = int(request.query_params.get("page", "1"))
    page = max(page, 1)
    limit = int(request.query_params.get("limit", "50"))
    limit = min(max(limit, 1), 100)
    offset = (page - 1) * limit
    txn_service = TransactionService(db)
    items = txn_service.list(period, filters, limit=limit + 1, offset=offset)
    has_more = len(items) > limit
    items = items[:limit]

    return {
        "items": [
            {
                "id": txn.id,
                "date": txn.date.isoformat(),
                "occurred_at": txn.occurred_at.isoformat(),
                "type": txn.type.value,
                "amount_cents": txn.amount_cents,
                "category": txn.category.name if txn.category else None,
                "note": txn.note,
            }
            for txn in items
        ],
        "page": page,
        "limit": limit,
        "has_more": has_more,
    }


@app.get("/components/kpis", response_class=HTMLResponse)
def component_kpis(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    service = MetricsService(db)
    tag_ids = [filters.tag_id] if filters.tag_id else None
    metrics = service.kpis(period, tag_ids=tag_ids)
    sparklines = service.kpi_sparklines(period, tag_ids=tag_ids)
    deltas = None
    duration_days = (period.end - period.start).days + 1
    if period.slug != "all" and duration_days <= 370:
        prev_end = period.start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=duration_days - 1)
        prev_period = Period("prev", prev_start, prev_end)
        prev = service.kpis(prev_period, tag_ids=tag_ids)
        deltas = {
            "income": metrics["income"] - prev["income"],
            "expenses": metrics["expenses"] - prev["expenses"],
            "balance": metrics["balance"] - prev["balance"],
        }
    return render(
        request,
        "components/kpis.html",
        {"kpi": metrics, "sparklines": sparklines, "deltas": deltas},
    )


@app.get("/components/category-donut", response_class=HTMLResponse)
def component_donut(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)

    service = MetricsService(db)
    tag_ids = [filters.tag_id] if filters.tag_id else None
    if tag_ids:
        kpis = service.kpis(period, tag_ids=tag_ids)
        has_any_transactions = kpis["income"] > 0 or kpis["expenses"] > 0
    else:
        has_any_transactions = TransactionService(db).has_any()

    if not has_any_transactions:
        return render(request, "components/donut.html", {"has_any_transactions": False})

    if filters.type == TransactionType.expense:
        expense_data = service.category_breakdown(
            period, TransactionType.expense, tag_ids=tag_ids
        )
        return render(
            request,
            "components/donut.html",
            {
                "has_any_transactions": True,
                "mode": "expense-only",
                "expense_breakdown": expense_data,
            },
        )
    elif filters.type == TransactionType.income:
        income_data = service.category_breakdown(
            period, TransactionType.income, tag_ids=tag_ids
        )
        return render(
            request,
            "components/donut.html",
            {
                "has_any_transactions": True,
                "mode": "income-only",
                "income_breakdown": income_data,
            },
        )
    else:
        expense_data = service.category_breakdown(
            period, TransactionType.expense, tag_ids=tag_ids
        )
        income_data = service.category_breakdown(
            period, TransactionType.income, tag_ids=tag_ids
        )
        return render(
            request,
            "components/donut.html",
            {
                "has_any_transactions": True,
                "mode": "both",
                "expense_breakdown": expense_data,
                "income_breakdown": income_data,
            },
        )


@app.get("/components/tag-activity", response_class=HTMLResponse)
def component_tag_activity(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    if not filters.tag_id:
        raise HTTPException(status_code=400, detail="Missing tag filter")
    txns = TransactionService(db).list(period, filters, limit=50)
    return render(request, "components/tag_activity.html", {"transactions": txns})


@app.get("/components/transaction-list", response_class=HTMLResponse)
def component_transaction_list(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    txns = TransactionService(db).list(period, filters, limit=10)
    return render(request, "components/transaction_list.html", {"transactions": txns})


@app.get("/transactions/export.csv")
def export_transactions_endpoint(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    transactions = TransactionService(db).all_for_period(period, filters)
    csv_text = CSVService(db).export(transactions)
    filename = f"transactions_{period.start}_{period.end}.csv"
    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/transactions/import/preview", response_class=HTMLResponse)
async def import_preview(
    request: Request,
    csrf_token: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    content = (await file.read()).decode("utf-8")
    rows, errors = CSVService(db).preview(content)
    return render(
        request,
        "components/csv_preview.html",
        {"rows": rows, "errors": errors},
    )


@app.post("/transactions/import/commit")
async def import_commit(
    request: Request,
    csrf_token: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    content = (await file.read()).decode("utf-8")
    try:
        count = CSVService(db).commit(content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    headers = {"HX-Trigger": "transactions-changed"}
    return Response(status_code=200, content=f"Imported {count} rows.", headers=headers)


@app.post("/reports/pdf")
async def generate_pdf_report(
    request: Request,
    start: str = Form(...),
    end: str = Form(...),
    sections: List[str] = Form(
        ["summary", "category_breakdown", "recent_transactions"]
    ),
    transaction_type: Optional[str] = Form(None),
    category_ids: Optional[List[int]] = Form(None),
    transactions_sort: str = Form("newest"),
    show_running_balance: bool = Form(False),
    include_category_subtotals: bool = Form(False),
    include_cents: bool = Form(False),
    notes: Optional[str] = Form(None),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    try:
        parsed_txn_type = (
            TransactionType(transaction_type) if transaction_type else None
        )
        parsed_sort = transactions_sort.strip().lower()
        if parsed_sort not in {"newest", "oldest"}:
            raise ValueError("Invalid transactions_sort")
        options = ReportOptions(
            start=date.fromisoformat(start),
            end=date.fromisoformat(end),
            sections=[s.strip() for s in sections],
            include_cents=include_cents,
            notes=notes,
            transaction_type=parsed_txn_type,
            category_ids=category_ids or None,
            transactions_sort=parsed_sort,  # type: ignore[arg-type]
            show_running_balance=show_running_balance,
            include_category_subtotals=include_category_subtotals,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        try:
            from weasyprint import CSS, HTML
            from weasyprint.text.fonts import FontConfiguration
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail="PDF export requires WeasyPrint system dependencies; install them for your OS and retry.",
            ) from exc

        start_time = datetime.now()
        report_service = ReportService(db)
        data = report_service.gather_data(options)
        data["generated_at"] = datetime.now()
        data["app_version"] = APP_VERSION
        gather_duration = (datetime.now() - start_time).total_seconds()
        logging.info(
            f"report_generated: period={options.start}to{options.end} "
            f"sections={len(options.sections)} "
            f"data_gather_duration={gather_duration:.2f}s"
        )

        font_config = FontConfiguration()
        html = templates.env.get_template("report.html").render(**data)
        css = CSS(
            string="""
                @page {{
                    size: A4;
                    margin: 18mm 16mm 20mm 16mm;
                    @bottom-center {{
                        content: "Page " counter(page) " of " counter(pages);
                        color: #64748b;
                        font-size: 9pt;
                    }}
                }}
                :root {{
                    --text: #0f172a;
                    --muted: #64748b;
                    --border: #e2e8f0;
                    --panel: #f8fafc;
                    --panel-strong: #f1f5f9;
                    --accent: #2563eb;
                    --positive: #16a34a;
                    --negative: #dc2626;
                }}

                html, body {{
                    margin: 0;
                    padding: 0;
                }}

                body {{
                    font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI",
                        Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
                    font-size: 11pt;
                    line-height: 1.35;
                    color: var(--text);
                }}

                .report {{
                    display: block;
                }}

                .header {{
                    padding: 10mm 0 6mm 0;
                    border-bottom: 1px solid var(--border);
                    margin-bottom: 8mm;
                }}

                .header-top {{
                    display: flex;
                    align-items: flex-end;
                    justify-content: space-between;
                    gap: 12px;
                }}

                .title {{
                    font-size: 20pt;
                    font-weight: 700;
                    letter-spacing: -0.02em;
                }}

                .subtitle {{
                    margin-top: 2mm;
                    font-size: 10pt;
                    color: var(--muted);
                }}

                .meta {{
                    text-align: right;
                    font-size: 9.5pt;
                    color: var(--muted);
                    white-space: nowrap;
                }}

                .note {{
                    margin-top: 6mm;
                    border: 1px solid var(--border);
                    background: var(--panel);
                    border-radius: 10px;
                    padding: 10px 12px;
                }}

                .note-label {{
                    font-size: 9pt;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    color: var(--muted);
                    margin-bottom: 4px;
                }}

                .section {{
                    margin-top: 10mm;
                    break-inside: avoid;
                    page-break-inside: avoid;
                }}

                .section-title {{
                    font-size: 12.5pt;
                    font-weight: 700;
                    letter-spacing: -0.01em;
                    margin: 0 0 3mm 0;
                }}

                .kpi-grid {{
                    display: flex;
                    gap: 10px;
                }}

                .kpi-card {{
                    flex: 1;
                    border: 1px solid var(--border);
                    background: var(--panel);
                    border-radius: 12px;
                    padding: 10px 12px;
                }}

                .kpi-label {{
                    font-size: 9pt;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    color: var(--muted);
                    margin-bottom: 2mm;
                }}

                .kpi-value {{
                    font-size: 17pt;
                    font-weight: 800;
                    letter-spacing: -0.02em;
                    font-variant-numeric: tabular-nums;
                    white-space: nowrap;
                }}

                .positive {{
                    color: var(--positive);
                }}

                .negative {{
                    color: var(--negative);
                }}

                .neutral {{
                    color: var(--text);
                }}

                .split {{
                    display: flex;
                    align-items: flex-start;
                    gap: 12px;
                }}

                .split > .col {{
                    flex: 1;
                }}

                .bars {{
                    border: 1px solid var(--border);
                    background: #fff;
                    border-radius: 12px;
                    padding: 10px 12px;
                }}

                .bar-row {{
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    margin: 6px 0;
                }}

                .swatch {{
                    width: 10px;
                    height: 10px;
                    border-radius: 3px;
                    flex: 0 0 auto;
                }}

                .bar-name {{
                    flex: 0 0 32%;
                    font-size: 9.5pt;
                    color: var(--text);
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }}

                .bar-track {{
                    flex: 1;
                    height: 8px;
                    background: var(--panel-strong);
                    border-radius: 999px;
                    overflow: hidden;
                }}

                .bar-fill {{
                    height: 100%;
                    border-radius: 999px;
                }}

                .bar-pct {{
                    flex: 0 0 12%;
                    text-align: right;
                    font-size: 9pt;
                    color: var(--muted);
                    white-space: nowrap;
                    font-variant-numeric: tabular-nums;
                }}

                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 2mm;
                    font-size: 9.5pt;
                }}

                thead th {{
                    text-align: left;
                    font-size: 8.5pt;
                    font-weight: 800;
                    text-transform: uppercase;
                    letter-spacing: 0.06em;
                    color: var(--muted);
                    background: var(--panel-strong);
                    padding: 7px 8px;
                    border-bottom: 1px solid var(--border);
                }}

                tbody td {{
                    padding: 7px 8px;
                    border-bottom: 1px solid var(--border);
                    vertical-align: top;
                }}

                tbody tr:nth-child(even) td {{
                    background: #fbfdff;
                }}

                .cell-muted {{
                    color: var(--muted);
                }}

                .cell-right {{
                    text-align: right;
                    font-variant-numeric: tabular-nums;
                    white-space: nowrap;
                }}

                .pill {{
                    display: inline-block;
                    padding: 1px 6px;
                    border-radius: 999px;
                    font-size: 8.5pt;
                    font-weight: 700;
                    background: var(--panel-strong);
                    color: var(--muted);
                    white-space: nowrap;
                }}

                .pill-income {{
                    background: rgba(22, 163, 74, 0.12);
                    color: var(--positive);
                }}

                .pill-expense {{
                    background: rgba(220, 38, 38, 0.12);
                    color: var(--negative);
                }}

                .chart {{
                    margin-top: 2mm;
                }}

                svg {{
                    max-width: 100%;
                }}
                """,
            font_config=font_config,
        )

        start_time = datetime.now()
        pdf_bytes = HTML(string=html, base_url=str(request.base_url)).write_pdf(
            stylesheets=[css], font_config=font_config
        )
        pdf_duration = (datetime.now() - start_time).total_seconds()
        logging.info(
            f"report_generated: period={options.start}to{options.end} "
            f"pdf_size_bytes={len(pdf_bytes)} "
            f"pdf_duration={pdf_duration:.2f}s"
        )

        filename = f"expense_report_{start}_{end}.pdf"
        return StreamingResponse(
            iter([pdf_bytes]),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(pdf_bytes)),
            },
        )
    except Exception as exc:
        logging.exception("Error generating PDF report")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/reports/builder", response_class=HTMLResponse)
def report_builder(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    categories = CategoryService(db).list_all()
    return render(
        request,
        "report_builder.html",
        {
            "period": period,
            "categories": categories,
        },
    )


@app.get("/admin/download-db", response_class=StreamingResponse)
def download_database(db: Session = Depends(get_db)):
    db_path = Path("data/expenses.db")
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"expenses_backup_{timestamp}.db"

    def iter_file(path: Path, chunk_size: int = 1024 * 1024):
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(
        iter_file(db_path),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/admin/export-csv")
def export_all_transactions(
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    transactions = TransactionService(db).recent(limit=10000)
    csv_text = CSVService(db).export(transactions)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"expenses_export_{timestamp}.csv"
    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/admin/purge-deleted")
def purge_deleted_transactions(
    csrf_token: str = Form(...),
    days: int = Form(30),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    cutoff_date = datetime.utcnow() - timedelta(days=days)
    from sqlalchemy import delete
    from models import Transaction

    stmt = delete(Transaction).where(
        Transaction.user_id == 1,
        Transaction.deleted_at.isnot(None),
        Transaction.deleted_at < cutoff_date,
    )
    result = db.execute(stmt)
    db.commit()
    logging.info(
        f"Purged {result.rowcount} deleted transactions older than {days} days"
    )
    return Response(
        status_code=200,
        content=f"Purged {result.rowcount} transactions",
        headers={"HX-Trigger": "transactions-changed"},
    )


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request, db: Session = Depends(get_db)):
    db_path = Path("data/expenses.db")
    db_size_bytes = db_path.stat().st_size if db_path.exists() else 0
    db_modified = (
        datetime.fromtimestamp(db_path.stat().st_mtime) if db_path.exists() else None
    )
    balance_service = BalanceAnchorService(db)
    balance_anchors = balance_service.list_all()
    current_balance = balance_service.balance_as_of(
        datetime.now(ZoneInfo(get_settings().timezone)).replace(tzinfo=None)
    )
    return render(
        request,
        "admin.html",
        {
            "app_version": APP_VERSION,
            "environment": os.getenv("EXPENSES_ENV", "Local"),
            "db_path": str(db_path),
            "db_size_mb": round(db_size_bytes / (1024 * 1024), 2)
            if db_size_bytes
            else 0,
            "db_modified": db_modified,
            "balance_anchors": balance_anchors,
            "current_balance": current_balance,
        },
    )


@app.get("/admin/import-sqlite", response_class=HTMLResponse)
def admin_import_sqlite_page(request: Request, db: Session = Depends(get_db)):
    categories = CategoryService(db).list_all()
    return render(
        request,
        "admin_import_sqlite.html",
        {"categories": categories},
    )


@app.get("/admin/import", response_class=HTMLResponse)
def admin_import_page(request: Request, db: Session = Depends(get_db)):
    categories = CategoryService(db).list_all()
    return render(request, "admin_import.html", {"categories": categories})


@app.post("/admin/import-sqlite/preview", response_class=HTMLResponse)
async def admin_import_sqlite_preview(
    request: Request,
    csrf_token: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    if not file.filename or not file.filename.endswith(".db"):
        raise HTTPException(status_code=400, detail="Please upload a .db file")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 25 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="DB file too large (max 25MB)")

    import_dir = Path("data/imports")
    import_dir.mkdir(parents=True, exist_ok=True)
    token = os.urandom(16).hex()
    legacy_path = import_dir / f"legacy_{token}.db"
    legacy_path.write_bytes(content)

    try:
        preview = LegacySQLiteImportService(db).preview(legacy_path)
    except ValueError as exc:
        try:
            legacy_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    categories = CategoryService(db).list_all()
    return render(
        request,
        "components/sqlite_import_preview.html",
        {
            "token": token,
            "preview": preview,
            "categories": categories,
        },
    )


@app.post("/admin/import-sqlite/commit", response_class=HTMLResponse)
async def admin_import_sqlite_commit(
    request: Request,
    csrf_token: str = Form(...),
    token: str = Form(...),
    mapping_count: int = Form(...),
    import_recurring_rules: Optional[str] = Form(None),
    recurring_auto_post: Optional[str] = Form(None),
    link_recurring_transactions: Optional[str] = Form(None),
    preserve_time_in_note: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    legacy_path = Path("data/imports") / f"legacy_{token}.db"
    if not legacy_path.exists():
        raise HTTPException(
            status_code=400, detail="Import file not found; please re-upload."
        )

    form = await request.form()
    mapping_targets: dict[tuple[TransactionType, str], str] = {}
    for idx in range(mapping_count):
        legacy_type = TransactionType(str(form.get(f"legacy_type_{idx}")))
        legacy_category = str(form.get(f"legacy_category_{idx}") or "").strip()
        target = str(form.get(f"target_{idx}") or "create").strip()
        if not legacy_category:
            raise HTTPException(status_code=400, detail="Missing legacy category")
        if not target:
            raise HTTPException(status_code=400, detail="Missing mapping target")
        mapping_targets[(legacy_type, legacy_category)] = target

    try:
        result = LegacySQLiteImportService(db).commit(
            legacy_path,
            mapping_targets=mapping_targets,
            import_recurring_rules=import_recurring_rules == "on",
            recurring_auto_post=recurring_auto_post == "on",
            link_recurring_transactions=link_recurring_transactions == "on",
            preserve_time_in_note=preserve_time_in_note == "on",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        try:
            legacy_path.unlink(missing_ok=True)
        except Exception:
            pass

    return templates.TemplateResponse(
        "components/sqlite_import_result.html",
        {"request": request, "result": result},
        headers={"HX-Trigger": "transactions-changed"},
    )


@app.post("/admin/rebuild-rollups")
def admin_rebuild_rollups(
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    rebuild_monthly_rollups(db, user_id=1)
    return Response(
        status_code=200,
        content="Rollups rebuilt.",
        headers={"HX-Trigger": "transactions-changed"},
    )


@app.get("/transactions/deleted", response_class=HTMLResponse)
def deleted_transactions_page(request: Request, db: Session = Depends(get_db)):
    items = TransactionService(db).deleted(limit=200)
    return render(request, "transactions_deleted.html", {"transactions": items})


@app.post("/transactions/{transaction_id}/restore")
async def restore_transaction(
    transaction_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        TransactionService(db).restore(transaction_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    headers = {"HX-Trigger": "transactions-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    return RedirectResponse(
        url=request.app.url_path_for("deleted_transactions_page"),
        status_code=303,
        headers=headers,
    )


@app.get("/transactions/{transaction_id}/edit", response_class=HTMLResponse)
def edit_transaction_page(
    transaction_id: int, request: Request, db: Session = Depends(get_db)
):
    try:
        txn = TransactionService(db).get(transaction_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    categories = CategoryService(db).list_all()
    all_tags = [
        {"id": t.id, "name": t.name} for t in services.TagService(db).list_all()
    ]
    next_url = request.query_params.get("next")
    return render(
        request,
        "transaction_edit.html",
        {
            "transaction": txn,
            "categories": categories,
            "tags": all_tags,
            "next": next_url,
        },
    )


@app.post("/transactions/{transaction_id}/edit")
async def edit_transaction_submit(
    transaction_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    next_url = form.get("next") or request.app.url_path_for("transactions_page")
    try:
        existing = TransactionService(db).get(transaction_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    try:
        category_id = int(form["category_id"])
        category = db.get(Category, category_id)
        if not category:
            raise ValueError("Category not found")

        occurred_raw = form.get("occurred_at")
        if occurred_raw:
            occurred_at = datetime.fromisoformat(str(occurred_raw))
            txn_date = occurred_at.date()
        else:
            txn_date = date.fromisoformat(form["date"])
            occurred_at = datetime.combine(txn_date, existing.occurred_at.time())
        data = TransactionIn(
            date=txn_date,
            occurred_at=occurred_at,
            type=category.type,
            amount_cents=parse_amount(form["amount"]),
            category_id=category_id,
            note=form["note"],
            tags=[t.strip() for t in (form.get("tags") or "").split(",") if t.strip()],
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        TransactionService(db).update(transaction_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RedirectResponse(url=str(next_url), status_code=303)


@app.post("/transactions/{transaction_id}/reimbursement")
async def set_transaction_reimbursement(
    transaction_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    is_reimbursement = form.get("is_reimbursement") == "on"
    try:
        ReimbursementService(db).set_reimbursement(transaction_id, is_reimbursement)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    headers = {"HX-Trigger": "transactions-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    next_url = form.get("next") or request.app.url_path_for(
        "edit_transaction_page", transaction_id=transaction_id
    )
    return RedirectResponse(url=str(next_url), status_code=303, headers=headers)


@app.post("/reimbursements/{reimbursement_id}/allocate")
async def allocate_reimbursement(
    reimbursement_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        expense_id = int(form["expense_transaction_id"])
        amount_cents = parse_amount(str(form["amount"]))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        ReimbursementService(db).upsert_allocation(
            reimbursement_id, expense_id, amount_cents
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    headers = {"HX-Trigger": "transactions-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    next_url = form.get("next") or request.app.url_path_for(
        "edit_transaction_page", transaction_id=reimbursement_id
    )
    return RedirectResponse(url=str(next_url), status_code=303, headers=headers)


@app.post("/reimbursements/allocations/{allocation_id}/delete")
async def delete_reimbursement_allocation(
    allocation_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        ReimbursementService(db).delete_allocation(allocation_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    headers = {"HX-Trigger": "transactions-changed"}
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers=headers)
    next_url = (
        form.get("next")
        or request.headers.get("Referer")
        or request.app.url_path_for("transactions_page")
    )
    return RedirectResponse(url=str(next_url), status_code=303, headers=headers)


@app.get("/components/transactions-page-list", response_class=HTMLResponse)
def component_transactions_page_list(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    page = int(request.query_params.get("page", "1"))
    page = max(page, 1)
    limit = 25
    txn_service = TransactionService(db)
    offset = (page - 1) * limit
    items = txn_service.list(period, filters, limit=limit + 1, offset=offset)
    items = items[:limit]
    from urllib.parse import urlencode, quote

    filter_params: dict[str, str] = {}
    if filters.type:
        filter_params["type"] = filters.type.value
    if filters.category_id:
        filter_params["category"] = str(filters.category_id)
    if filters.tag_id:
        filter_params["tag"] = str(filters.tag_id)
    if filters.query:
        filter_params["q"] = filters.query
    filter_query = urlencode(filter_params)
    period_query = f"period={period.slug}&start={period.start}&end={period.end}"
    next_q = quote(str(request.url), safe="")
    return render(
        request,
        "components/transactions_page_list.html",
        {
            "transactions": items,
            "period_query": period_query,
            "filter_query": filter_query,
            "next_q": next_q,
        },
    )


@app.get("/components/transaction-reimbursements", response_class=HTMLResponse)
def component_transaction_reimbursements(
    request: Request, db: Session = Depends(get_db)
):
    txn_raw = str(request.query_params.get("transaction_id") or "").strip()
    if not txn_raw:
        raise HTTPException(status_code=400, detail="Missing transaction_id")
    try:
        transaction_id = int(txn_raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid transaction_id") from exc
    try:
        txn = TransactionService(db).get(transaction_id, include_deleted=False)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    reimbursements = ReimbursementService(db)
    context: dict[str, object] = {"transaction": txn}
    if txn.type == TransactionType.income:
        if txn.is_reimbursement:
            allocated_total = reimbursements.allocated_total_for_reimbursement(txn.id)
            context["allocated_total_cents"] = allocated_total
            context["remaining_to_allocate_cents"] = max(
                0, txn.amount_cents - allocated_total
            )
            context["allocations_out"] = reimbursements.allocations_for_reimbursement(
                txn.id
            )
    else:
        reimbursed_total = reimbursements.reimbursed_total_for_expense(txn.id)
        context["reimbursed_total_cents"] = reimbursed_total
        context["net_cost_cents"] = max(0, txn.amount_cents - reimbursed_total)
        context["allocations_in"] = reimbursements.allocations_for_expense(txn.id)

    return render(request, "components/transaction_reimbursements.html", context)


@app.get("/components/reimbursement-expense-search", response_class=HTMLResponse)
def component_reimbursement_expense_search(
    request: Request, db: Session = Depends(get_db)
):
    reimb_raw = str(request.query_params.get("reimbursement_id") or "").strip()
    if not reimb_raw:
        raise HTTPException(status_code=400, detail="Missing reimbursement_id")
    try:
        reimbursement_id = int(reimb_raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid reimbursement_id") from exc
    query = (request.query_params.get("q") or "").strip()
    try:
        results = ReimbursementService(db).search_expenses_for_reimbursement(
            reimbursement_id, query=query, limit=25
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return render(
        request,
        "components/reimbursement_expense_search.html",
        {"reimbursement_id": reimbursement_id, "query": query, "results": results},
    )


@app.get("/budgets", response_class=HTMLResponse)
def budgets_page(request: Request, db: Session = Depends(get_db)):
    today = date.today()
    view = (request.query_params.get("view") or "month").strip().lower()
    if view not in {"month", "templates", "year"}:
        view = "month"

    ym = request.query_params.get("month")
    if ym:
        year_str, month_str = ym.split("-", 1)
        year = int(year_str)
        month = int(month_str)
    else:
        year = today.year
        month = today.month

    svc = BudgetService(db)
    month_value = f"{year:04d}-{month:02d}"

    effective_budgets = []
    progress = {}
    if view == "month":
        effective_budgets = svc.effective_budgets_for_month(year, month)
        progress = svc.progress_for_month(year, month)

    templates = []
    if view == "templates":
        templates = svc.list_templates()

    year_value = int(request.query_params.get("year") or today.year)
    yearly_budgets = []
    yearly_spent = {}
    if view == "year":
        yearly_budgets = svc.yearly_budgets_for_year(year_value)
        yearly_spent = svc.spent_by_category_for_year(year_value)

    categories = CategoryService(db).list_all()
    return render(
        request,
        "budgets.html",
        {
            "view": view,
            "year": year,
            "month": month,
            "month_value": month_value,
            "budgets": effective_budgets,
            "progress": progress,
            "categories": categories,
            "templates": templates,
            "year_value": year_value,
            "yearly_budgets": yearly_budgets,
            "yearly_spent": yearly_spent,
            "default_month_template_start": f"{today.year:04d}-{today.month:02d}-01",
            "default_year_template_start": f"{today.year:04d}-01-01",
        },
    )


@app.post("/budgets/overrides")
async def upsert_budget_override(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        ym = str(form.get("month") or "")
        year_str, month_str = ym.split("-", 1)
        year = int(year_str)
        month = int(month_str)
        category_id_raw = (form.get("category_id") or "").strip()
        category_id = int(category_id_raw) if category_id_raw else None
        amount_cents = parse_amount(str(form.get("amount") or "0"))
        data = BudgetOverrideIn(
            year=year, month=month, category_id=category_id, amount_cents=amount_cents
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        BudgetService(db).upsert_override(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(
        url=f"/budgets?view=month&month={year:04d}-{month:02d}", status_code=303
    )


@app.post("/budgets/overrides/{override_id}/delete")
async def delete_budget_override(
    override_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        BudgetService(db).delete_override(override_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    ym = request.query_params.get("month")
    back = f"/budgets?view=month&month={ym}" if ym else "/budgets?view=month"
    return RedirectResponse(url=back, status_code=303)


@app.post("/budgets/templates")
async def upsert_budget_template(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        frequency = str(form.get("frequency") or "monthly").strip().lower()
        starts_on = date.fromisoformat(str(form.get("starts_on") or ""))
        ends_raw = str(form.get("ends_on") or "").strip()
        ends_on = date.fromisoformat(ends_raw) if ends_raw else None
        category_id_raw = (form.get("category_id") or "").strip()
        category_id = int(category_id_raw) if category_id_raw else None
        amount_cents = parse_amount(str(form.get("amount") or "0"))
        data = BudgetTemplateIn(
            frequency=frequency,  # type: ignore[arg-type]
            category_id=category_id,
            amount_cents=amount_cents,
            starts_on=starts_on,
            ends_on=ends_on,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        BudgetService(db).upsert_template(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url="/budgets?view=templates", status_code=303)


@app.post("/budgets/templates/{template_id}/delete")
async def delete_budget_template(
    template_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        BudgetService(db).delete_template(template_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RedirectResponse(url="/budgets?view=templates", status_code=303)


@app.get("/rules", response_class=HTMLResponse)
def rules_page(request: Request, db: Session = Depends(get_db)):
    rules = services.RuleService(db).list_all()
    categories = CategoryService(db).list_all()
    tags = services.TagService(db).list_all()
    hidden_tags = [t for t in tags if t.is_hidden_from_budget]
    return render(
        request,
        "rules.html",
        {
            "rules": rules,
            "categories": categories,
            "tags": [{"id": t.id, "name": t.name} for t in tags],
            "hidden_tags": hidden_tags,
        },
    )


def _optional_amount_cents(value: object) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    return parse_amount(raw)


@app.post("/rules")
async def create_rule(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        tx_type = str(form.get("transaction_type") or "").strip().lower()
        transaction_type = TransactionType(tx_type) if tx_type else None

        set_category_raw = str(form.get("set_category_id") or "").strip()
        set_category_id = int(set_category_raw) if set_category_raw else None

        add_tags_raw = str(form.get("add_tags") or "")
        add_tags = [t.strip() for t in add_tags_raw.split(",") if t.strip()]

        budget_exclude_raw = str(form.get("budget_exclude_tag_id") or "").strip()
        budget_exclude_tag_id = int(budget_exclude_raw) if budget_exclude_raw else None

        data = RuleIn(
            name=str(form.get("name") or "").strip(),
            enabled=str(form.get("enabled") or "") == "on",
            priority=int(str(form.get("priority") or "100")),
            match_type=str(form.get("match_type") or "contains"),  # type: ignore[arg-type]
            match_value=str(form.get("match_value") or "").strip(),
            transaction_type=transaction_type,
            min_amount_cents=_optional_amount_cents(form.get("min_amount")),
            max_amount_cents=_optional_amount_cents(form.get("max_amount")),
            set_category_id=set_category_id,
            add_tags=add_tags,
            budget_exclude_tag_id=budget_exclude_tag_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        services.RuleService(db).create(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url="/rules", status_code=303)


@app.post("/rules/{rule_id}")
async def update_rule(rule_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        tx_type = str(form.get("transaction_type") or "").strip().lower()
        transaction_type = TransactionType(tx_type) if tx_type else None

        set_category_raw = str(form.get("set_category_id") or "").strip()
        set_category_id = int(set_category_raw) if set_category_raw else None

        add_tags_raw = str(form.get("add_tags") or "")
        add_tags = [t.strip() for t in add_tags_raw.split(",") if t.strip()]

        budget_exclude_raw = str(form.get("budget_exclude_tag_id") or "").strip()
        budget_exclude_tag_id = int(budget_exclude_raw) if budget_exclude_raw else None

        data = RuleIn(
            name=str(form.get("name") or "").strip(),
            enabled=str(form.get("enabled") or "") == "on",
            priority=int(str(form.get("priority") or "100")),
            match_type=str(form.get("match_type") or "contains"),  # type: ignore[arg-type]
            match_value=str(form.get("match_value") or "").strip(),
            transaction_type=transaction_type,
            min_amount_cents=_optional_amount_cents(form.get("min_amount")),
            max_amount_cents=_optional_amount_cents(form.get("max_amount")),
            set_category_id=set_category_id,
            add_tags=add_tags,
            budget_exclude_tag_id=budget_exclude_tag_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        services.RuleService(db).update(rule_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url="/rules", status_code=303)


@app.post("/rules/{rule_id}/toggle")
async def toggle_rule(rule_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    enabled = str(form.get("enabled") or "").strip().lower() == "true"
    try:
        services.RuleService(db).toggle(rule_id, enabled)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204, headers={"HX-Trigger": "rules-updated"})


@app.post("/rules/{rule_id}/delete")
async def delete_rule(rule_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        services.RuleService(db).delete(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204, headers={"HX-Trigger": "rules-updated"})


@app.post("/rules/preview", response_class=HTMLResponse)
async def preview_rule(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    try:
        match_type = str(form.get("match_type") or "contains").strip()
        match_value = str(form.get("match_value") or "").strip()
        tx_type = str(form.get("transaction_type") or "").strip().lower()
        transaction_type = TransactionType(tx_type) if tx_type else None
        min_amount = _optional_amount_cents(form.get("min_amount"))
        max_amount = _optional_amount_cents(form.get("max_amount"))
        set_category_raw = str(form.get("set_category_id") or "").strip()
        set_category_id = int(set_category_raw) if set_category_raw else None
        add_tags_raw = str(form.get("add_tags") or "")
        add_tags = [t.strip() for t in add_tags_raw.split(",") if t.strip()]
        budget_exclude_raw = str(form.get("budget_exclude_tag_id") or "").strip()
        budget_exclude_tag_id = int(budget_exclude_raw) if budget_exclude_raw else None
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    recent = TransactionService(db).recent(limit=200)
    set_category = db.get(Category, set_category_id) if set_category_id else None
    exclude_tag = None
    if budget_exclude_tag_id:
        from models import Tag

        exclude_tag = db.get(Tag, budget_exclude_tag_id)

    def matches(txn: Transaction) -> bool:
        if transaction_type and txn.type != transaction_type:
            return False
        if min_amount is not None and txn.amount_cents < min_amount:
            return False
        if max_amount is not None and txn.amount_cents > max_amount:
            return False
        note = (txn.note or "").strip()
        if not match_value:
            return False
        nl = note.lower()
        mv = match_value.lower()
        if match_type == "contains":
            return mv in nl
        if match_type == "equals":
            return nl == mv
        if match_type == "starts_with":
            return nl.startswith(mv)
        if match_type == "regex":
            import re

            try:
                return re.search(match_value, note, flags=re.IGNORECASE) is not None
            except re.error:
                return False
        return False

    sample: list[dict[str, object]] = []
    for txn in recent:
        if not matches(txn):
            continue
        before_category = txn.category.name if txn.category else "Uncategorized"
        after_category = before_category
        if set_category and set_category.type == txn.type:
            after_category = set_category.name
        added_tags = set(add_tags)
        if exclude_tag:
            added_tags.add(exclude_tag.name)
        sample.append(
            {
                "id": txn.id,
                "note": txn.note,
                "amount_cents": txn.amount_cents,
                "type": txn.type.value,
                "before_category": before_category,
                "after_category": after_category,
                "add_tags": sorted(added_tags),
            }
        )
        if len(sample) >= 10:
            break

    return render(
        request,
        "components/rule_preview.html",
        {"matches_count": sum(1 for txn in recent if matches(txn)), "sample": sample},
    )


@app.get("/insights", response_class=HTMLResponse)
def insights_page(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    tag_ids = [filters.tag_id] if filters.tag_id else None

    insights = InsightsService(db)
    series = insights.monthly_series(period, months_back=12, tag_ids=tag_ids)
    expense_breakdown = MetricsService(db).category_breakdown(
        period, TransactionType.expense, tag_ids=tag_ids
    )
    income_breakdown = MetricsService(db).category_breakdown(
        period, TransactionType.income, tag_ids=tag_ids
    )
    deltas = insights.expense_category_deltas(period, tag_ids=tag_ids)
    top_tags = insights.top_tags(period, transaction_type=TransactionType.expense)

    all_categories = CategoryService(db).list_all()
    trend_category_raw = str(request.query_params.get("trend_category") or "").strip()
    trend_category_id = int(trend_category_raw) if trend_category_raw else None
    if not trend_category_id and all_categories:
        default = next(
            (c for c in all_categories if c.type == TransactionType.expense), None
        )
        trend_category_id = (default or all_categories[0]).id
    trend = (
        insights.category_trend(
            trend_category_id, end=period.end, months_back=12, tag_ids=tag_ids
        )
        if trend_category_id
        else []
    )

    budget_month = str(request.query_params.get("budget_month") or "")
    if not budget_month:
        budget_month = f"{period.end.year:04d}-{period.end.month:02d}"
    try:
        byear, bmonth = (int(p) for p in budget_month.split("-", 1))
    except Exception:
        byear, bmonth = period.end.year, period.end.month
        budget_month = f"{byear:04d}-{bmonth:02d}"
    budget_service = BudgetService(db)
    budget_effective = budget_service.effective_budgets_for_month(byear, bmonth)
    budget_progress = budget_service.progress_for_month(byear, bmonth)

    all_tags = services.TagService(db).list_all()
    period_query = f"period={period.slug}&start={period.start}&end={period.end}"
    return render(
        request,
        "insights.html",
        {
            "period": period,
            "filters": filters,
            "tags": [{"id": t.id, "name": t.name} for t in all_tags],
            "categories": all_categories,
            "series": series,
            "expense_breakdown": expense_breakdown,
            "income_breakdown": income_breakdown,
            "deltas": deltas,
            "top_tags": top_tags,
            "trend_category_id": trend_category_id,
            "trend": trend,
            "budget_month": budget_month,
            "budget_effective": budget_effective,
            "budget_progress": budget_progress,
            "period_query": period_query,
        },
    )


@app.get("/components/insights/monthly-series", response_class=HTMLResponse)
def component_insights_monthly_series(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    tag_ids = [filters.tag_id] if filters.tag_id else None
    series = InsightsService(db).monthly_series(period, months_back=12, tag_ids=tag_ids)
    return render(
        request, "components/insights_monthly_series.html", {"series": series}
    )


@app.get("/components/insights/top-categories", response_class=HTMLResponse)
def component_insights_top_categories(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    tag_ids = [filters.tag_id] if filters.tag_id else None
    expense_breakdown = MetricsService(db).category_breakdown(
        period, TransactionType.expense, tag_ids=tag_ids
    )
    income_breakdown = MetricsService(db).category_breakdown(
        period, TransactionType.income, tag_ids=tag_ids
    )
    return render(
        request,
        "components/insights_top_categories.html",
        {"expense_breakdown": expense_breakdown, "income_breakdown": income_breakdown},
    )


@app.get("/components/insights/deltas", response_class=HTMLResponse)
def component_insights_deltas(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    tag_ids = [filters.tag_id] if filters.tag_id else None
    deltas = InsightsService(db).expense_category_deltas(period, tag_ids=tag_ids)
    return render(request, "components/insights_deltas.html", {"deltas": deltas})


@app.get("/components/insights/top-tags", response_class=HTMLResponse)
def component_insights_top_tags(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    tag_ids = [filters.tag_id] if filters.tag_id else None
    top_tags = InsightsService(db).top_tags(
        period, transaction_type=TransactionType.expense
    )
    if tag_ids:
        # If a tag filter is active, "top tags" is not meaningful; show empty.
        top_tags = []
    return render(
        request,
        "components/insights_top_tags.html",
        {"top_tags": top_tags, "period": period},
    )


@app.get("/components/insights/category-trend", response_class=HTMLResponse)
def component_insights_category_trend(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    filters = filters_from_request(request)
    tag_ids = [filters.tag_id] if filters.tag_id else None
    category_raw = str(request.query_params.get("trend_category") or "").strip()
    category_id = int(category_raw) if category_raw else None
    trend = (
        InsightsService(db).category_trend(
            category_id, end=period.end, months_back=12, tag_ids=tag_ids
        )
        if category_id
        else []
    )
    return render(request, "components/insights_category_trend.html", {"trend": trend})


@app.get("/components/insights/budget", response_class=HTMLResponse)
def component_insights_budget(request: Request, db: Session = Depends(get_db)):
    budget_month = str(request.query_params.get("budget_month") or "").strip()
    if not budget_month:
        budget_month = f"{date.today().year:04d}-{date.today().month:02d}"
    try:
        byear, bmonth = (int(p) for p in budget_month.split("-", 1))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid budget_month")
    svc = BudgetService(db)
    effective = svc.effective_budgets_for_month(byear, bmonth)
    progress = svc.progress_for_month(byear, bmonth)
    return render(
        request,
        "components/insights_budget.html",
        {
            "budget_month": budget_month,
            "budget_effective": effective,
            "budget_progress": progress,
        },
    )


@app.get("/tags", response_class=HTMLResponse)
def tags_page(request: Request, db: Session = Depends(get_db)):
    tags = services.TagService(db).list_all()
    # TODO: Add usage counts
    return render(request, "tags.html", {"tags": tags})


@app.get("/tags/{tag_id}", response_class=HTMLResponse)
def tag_details_page(tag_id: int, request: Request, db: Session = Depends(get_db)):
    # We don't have get() yet, only get_or_create. Need to add get or use db.get directly.
    from models import Tag

    tag = db.get(Tag, tag_id)
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    period = period_from_request(request)
    # Default to all time for tags if not specified, as they are often event-based?
    # Or stick to default period logic (this month). Stick to default for consistency.

    metrics_service = MetricsService(db)
    txn_service = TransactionService(db)

    kpi = metrics_service.kpis(period, tag_ids=[tag_id])
    sparklines = metrics_service.kpi_sparklines(period, tag_ids=[tag_id])

    # Pie chart data
    expense_breakdown = metrics_service.category_breakdown(
        period, TransactionType.expense, tag_ids=[tag_id]
    )
    income_breakdown = metrics_service.category_breakdown(
        period, TransactionType.income, tag_ids=[tag_id]
    )

    donut_context = {
        "mode": "both",
        "expense_breakdown": expense_breakdown,
        "income_breakdown": income_breakdown,
        "has_any_transactions": (kpi["income"] > 0 or kpi["expenses"] > 0),
    }

    filters = TransactionFilters(tag_id=tag_id)
    txns = txn_service.list(period, filters, limit=50)

    period_query = f"period={period.slug}&start={period.start}&end={period.end}"

    return render(
        request,
        "tag_details.html",
        {
            "tag": tag,
            "period": period,
            "kpi": kpi,
            "sparklines": sparklines,
            **donut_context,
            "transactions": txns,
            "period_query": period_query,
        },
    )


@app.post("/tags")
async def create_tag(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    # No CSRF token in create_tag for now to match other quick-creates, but should add
    # Using basic validation
    try:
        name = str(form.get("name") or "").strip()
        is_hidden = form.get("is_hidden_from_budget") == "on"
        services.TagService(db).create(name, is_hidden)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url="/tags", status_code=303)


@app.post("/tags/{tag_id}/edit")
async def edit_tag(tag_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    try:
        new_name = str(form.get("name") or "").strip()
        is_hidden = form.get("is_hidden_from_budget") == "on"
        services.TagService(db).update(tag_id, new_name, is_hidden)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return RedirectResponse(url=f"/tags/{tag_id}", status_code=303)


@app.post("/tags/{tag_id}/delete")
async def delete_tag(tag_id: int, request: Request, db: Session = Depends(get_db)):
    try:
        services.TagService(db).delete(tag_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(url="/tags", status_code=303)


def main():
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()
