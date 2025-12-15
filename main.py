import logging
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List

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
from csrf import generate_csrf_token, validate_csrf_token
from database import SessionLocal
from legacy_sqlite_import import LegacySQLiteImportService
from models import (
    Category,
    IntervalUnit,
    MonthDayPolicy,
    Transaction,
    TransactionKind,
    TransactionType,
)
from periods import Period, resolve_period
from scheduler import SchedulerManager
from schemas import BudgetIn, CategoryIn, RecurringRuleIn, TransactionIn, ReportOptions
from services import (
    BudgetService,
    CSVService,
    CategoryService,
    MetricsService,
    RecurringRuleService,
    TransactionFilters,
    TransactionService,
    ReportService,
    rebuild_monthly_rollups,
)

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
    if options is None:
        include_cents = True
    elif isinstance(options, dict):
        include_cents = options.get("include_cents", True)
    else:
        include_cents = bool(getattr(options, "include_cents", True))
    if include_cents:
        return f"{cents / 100:,.2f}".replace(",", " ").replace(".", ",")
    return f"{cents / 100:,.0f}".replace(",", " ")


templates.env.filters["currency"] = format_currency
templates.env.globals["math"] = math
templates.env.globals["TransactionType"] = TransactionType
templates.env.globals["IntervalUnit"] = IntervalUnit
templates.env.globals["MonthDayPolicy"] = MonthDayPolicy
templates.env.globals["csrf_token"] = generate_csrf_token


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
    return TransactionFilters(type=txn_type, category_id=category_id, query=query)


def render(request: Request, template: str, context: dict[str, object]) -> HTMLResponse:
    ctx = {"request": request}
    ctx.update(context)
    return templates.TemplateResponse(template, ctx)


def recurring_payload_from_form(form) -> RecurringRuleIn:
    return RecurringRuleIn(
        name=form.get("name"),
        type=TransactionType(form["type"]),
        amount_cents=parse_amount(form["amount"]),
        category_id=int(form["category_id"]),
        anchor_date=date.fromisoformat(form["anchor_date"]),
        interval_unit=IntervalUnit(form["interval_unit"]),
        interval_count=int(form.get("interval_count", 1) or 1),
        next_occurrence=date.fromisoformat(form["next_occurrence"]),
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
    period_query = f"period={period.slug}&start={period.start}&end={period.end}"
    from urllib.parse import urlencode

    filter_params: dict[str, str] = {}
    if filters.type:
        filter_params["type"] = filters.type.value
    if filters.category_id:
        filter_params["category"] = str(filters.category_id)
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

        kind_value = form.get("kind", "normal")
        kind = (
            TransactionKind(kind_value)
            if kind_value in ["normal", "adjustment"]
            else TransactionKind.normal
        )
        data = TransactionIn(
            date=date.fromisoformat(form["date"]),
            type=category.type,
            kind=kind,
            amount_cents=parse_amount(form["amount"]),
            category_id=category_id,
            note=form["note"],
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
    return Response(status_code=204, headers={"HX-Trigger": "transactions-changed"})


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
            Transaction.kind == TransactionKind.normal,
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
        url=request.app.url_path_for("categories_page"), status_code=303, headers=headers
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
    categories = CategoryService(db).list_all()
    return render(
        request,
        "recurring.html",
        {"rules": rules, "categories": categories},
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

    from sqlalchemy import select
    from models import Transaction

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
                "type": txn.type.value,
                "kind": txn.kind.value,
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
    service = MetricsService(db)
    metrics = service.kpis(period)
    sparklines = service.kpi_sparklines(period)
    deltas = None
    duration_days = (period.end - period.start).days + 1
    if period.slug != "all" and duration_days <= 370:
        prev_end = period.start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=duration_days - 1)
        prev_period = Period("prev", prev_start, prev_end)
        prev = service.kpis(prev_period)
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
    has_any_transactions = TransactionService(db).has_any()
    if not has_any_transactions:
        return render(request, "components/donut.html", {"has_any_transactions": False})

    # Determine what to show based on type filter
    if filters.type == TransactionType.expense:
        expense_data = service.category_breakdown(period, TransactionType.expense)
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
        income_data = service.category_breakdown(period, TransactionType.income)
        return render(
            request,
            "components/donut.html",
            {
                "has_any_transactions": True,
                "mode": "income-only",
                "income_breakdown": income_data,
            },
        )
    else:  # All types - show both donuts
        expense_data = service.category_breakdown(period, TransactionType.expense)
        income_data = service.category_breakdown(period, TransactionType.income)
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
    sections: List[str] = Form(["summary", "category_breakdown", "recent_transactions"]),
    transaction_type: Optional[str] = Form(None),
    category_ids: Optional[List[int]] = Form(None),
    include_adjustments: bool = Form(False),
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
            include_adjustments=include_adjustments,
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
            string=f"""
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
def admin_page(request: Request):
    db_path = Path("data/expenses.db")
    db_size_bytes = db_path.stat().st_size if db_path.exists() else 0
    db_modified = (
        datetime.fromtimestamp(db_path.stat().st_mtime) if db_path.exists() else None
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
    next_url = request.query_params.get("next")
    return render(
        request,
        "transaction_edit.html",
        {"transaction": txn, "categories": categories, "next": next_url},
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
        category_id = int(form["category_id"])
        category = db.get(Category, category_id)
        if not category:
            raise ValueError("Category not found")

        kind_value = form.get("kind", "normal")
        kind = (
            TransactionKind(kind_value)
            if kind_value in ["normal", "adjustment"]
            else TransactionKind.normal
        )
        data = TransactionIn(
            date=date.fromisoformat(form["date"]),
            type=category.type,
            kind=kind,
            amount_cents=parse_amount(form["amount"]),
            category_id=category_id,
            note=form["note"],
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        TransactionService(db).update(transaction_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RedirectResponse(url=str(next_url), status_code=303)


@app.get("/budgets", response_class=HTMLResponse)
def budgets_page(request: Request, db: Session = Depends(get_db)):
    today = date.today()
    ym = request.query_params.get("month")  # YYYY-MM
    if ym:
        year_str, month_str = ym.split("-", 1)
        year = int(year_str)
        month = int(month_str)
    else:
        year = today.year
        month = today.month

    svc = BudgetService(db)
    budgets = svc.list_for_month(year, month)
    progress = svc.progress_for_month(year, month)
    categories = CategoryService(db).list_all()
    return render(
        request,
        "budgets.html",
        {
            "year": year,
            "month": month,
            "month_value": f"{year:04d}-{month:02d}",
            "budgets": budgets,
            "progress": progress,
            "categories": categories,
        },
    )


@app.post("/budgets")
async def upsert_budget(request: Request, db: Session = Depends(get_db)):
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
        data = BudgetIn(
            year=year, month=month, category_id=category_id, amount_cents=amount_cents
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        BudgetService(db).upsert(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(
        url=f"/budgets?month={year:04d}-{month:02d}", status_code=303
    )


@app.post("/budgets/{budget_id}/delete")
async def delete_budget(
    budget_id: int, request: Request, db: Session = Depends(get_db)
):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        BudgetService(db).delete(budget_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    ym = request.query_params.get("month")
    back = f"/budgets?month={ym}" if ym else "/budgets"
    return RedirectResponse(url=back, status_code=303)


def main():
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()
