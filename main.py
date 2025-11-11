from __future__ import annotations

import logging
import math
import os
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration

from csv_utils import parse_amount
from csrf import generate_csrf_token, validate_csrf_token
from database import SessionLocal
from models import IntervalUnit, MonthDayPolicy, TransactionKind, TransactionType
from periods import Period, resolve_period
from scheduler import SchedulerManager
from schemas import CategoryIn, RecurringRuleIn, TransactionIn, ReportOptions
from services import (
    CSVService,
    CategoryService,
    MetricsService,
    RecurringRuleService,
    TransactionFilters,
    TransactionService,
    ReportService,
)

app = FastAPI(title="Expense Tracker")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def format_currency(cents: int, options: Optional[dict] = None) -> str:
    include_cents = options.get("include_cents", True) if options else True
    if include_cents:
        return f"{cents / 100:,.2f}".replace(",", " ").replace(".", ",")
    return f"{cents / 100:,.0f}".replace(",", " ")


templates.env.filters["currency"] = format_currency
templates.env.globals["math"] = math
templates.env.globals["TransactionType"] = TransactionType
templates.env.globals["IntervalUnit"] = IntervalUnit
templates.env.globals["MonthDayPolicy"] = MonthDayPolicy
templates.env.globals["csrf_token"] = generate_csrf_token


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
    category_service = CategoryService(db)
    metrics_service = MetricsService(db)
    txn_service = TransactionService(db)
    categories = category_service.list_all()
    breakdown = metrics_service.category_breakdown(period)
    kpi = metrics_service.kpis(period)
    recent = txn_service.recent()
    period_query = f"period={period.slug}&start={period.start}&end={period.end}"
    return render(
        request,
        "dashboard.html",
        {
            "period": period,
            "categories": categories,
            "kpi": kpi,
            "breakdown": breakdown,
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
        },
    )


@app.post("/transactions")
async def create_transaction(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    token = form.get("csrf_token", "")
    if not validate_csrf_token(token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        kind_value = form.get("kind", "normal")
        kind = TransactionKind(kind_value) if kind_value in ["normal", "adjustment"] else TransactionKind.normal
        data = TransactionIn(
            date=date.fromisoformat(form["date"]),
            type=TransactionType(form["type"]),
            kind=kind,
            amount_cents=parse_amount(form["amount"]),
            category_id=int(form["category_id"]),
            note=form.get("note"),
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
    return RedirectResponse(url=request.url_for("dashboard"), status_code=303, headers=headers)


@app.post("/transactions/{transaction_id}/delete")
async def delete_transaction(transaction_id: int, request: Request, db: Session = Depends(get_db)):
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
    categories = CategoryService(db).list_all(include_archived=True)
    return render(request, "categories.html", {"categories": categories})


@app.post("/categories")
async def create_category(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    if not validate_csrf_token(form.get("csrf_token", "")):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")
    try:
        data = CategoryIn(
            name=form["name"],
            type=TransactionType(form["type"]),
            color=form.get("color") or None,
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
    return RedirectResponse(url=request.url_for("categories_page"), status_code=303, headers=headers)


@app.post("/categories/{category_id}/archive")
def archive_category(category_id: int, db: Session = Depends(get_db)):
    try:
        CategoryService(db).archive(category_id)
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
    return RedirectResponse(url=request.url_for("recurring_page"), status_code=303)


@app.post("/recurring/{rule_id}/toggle")
async def toggle_recurring(rule_id: int, request: Request, db: Session = Depends(get_db)):
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
async def update_recurring(rule_id: int, request: Request, db: Session = Depends(get_db)):
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
    return RedirectResponse(url=request.url_for("recurring_page"), status_code=303)


@app.get("/recurring/{rule_id}/occurrences", response_class=HTMLResponse)
def recurring_occurrences(rule_id: int, request: Request, db: Session = Depends(get_db)):
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
    items = txn_service.list(period, filters, limit=limit, offset=offset)

    return {
        "items": [
            {
                "id": txn.id,
                "date": txn.date.isoformat(),
                "type": txn.type.value,
                "kind": txn.kind.value if hasattr(txn, 'kind') else 'normal',
                "amount_cents": txn.amount_cents,
                "category": txn.category.name if txn.category else None,
                "note": txn.note,
            }
            for txn in items
        ],
        "page": page,
        "limit": limit,
        "has_more": len(items) == limit,
    }


@app.get("/components/kpis", response_class=HTMLResponse)
def component_kpis(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    metrics = MetricsService(db).kpis(period)
    return render(request, "components/kpis.html", {"kpi": metrics})


@app.get("/components/category-donut", response_class=HTMLResponse)
def component_donut(request: Request, db: Session = Depends(get_db)):
    period = period_from_request(request)
    data = MetricsService(db).category_breakdown(period)
    return render(request, "components/donut.html", {"breakdown": data})


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
    sections: List[str] = Form(["summary", "kpis", "category_breakdown", "recent_transactions"]),
    currency_symbol: str = Form("€"),
    page_size: str = Form("A4"),
    include_cents: bool = Form(True),
    recent_transactions_count: int = Form(50),
    notes: Optional[str] = Form(None),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    if not validate_csrf_token(csrf_token):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")

    try:
        options = ReportOptions(
            start=date.fromisoformat(start),
            end=date.fromisoformat(end),
            sections=[s.strip() for s in sections],
            currency_symbol=currency_symbol,
            page_size=page_size,
            include_cents=include_cents,
            recent_transactions_count=recent_transactions_count,
            notes=notes,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        start_time = datetime.now()
        report_service = ReportService(db)
        data = report_service.gather_data(options)
        gather_duration = (datetime.now() - start_time).total_seconds()
        logging.info(
            f"report_generated: period={options.start}to{options.end} "
            f"sections={len(options.sections)} "
            f"data_gather_duration={gather_duration:.2f}s"
        )

        font_config = FontConfiguration()
        html = templates.env.get_template("report.html").render(**data)
        css = CSS(string="""
            @page {
                size: A4;
                margin: 2cm;
            }
            body {
                font-family: Arial, sans-serif;
                font-size: 10pt;
                color: #333;
            }
            h1, h2, h3 {
                color: #222;
            }
            .section {
                margin-bottom: 20px;
                page-break-inside: avoid;
            }
            .kpi-grid {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 10px;
                margin-bottom: 20px;
            }
            .kpi-card {
                border: 1px solid #ddd;
                padding: 10px;
                border-radius: 4px;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 6px;
                text-align: left;
            }
            th {
                background-color: #f5f5f5;
            }
            .donut-chart {
                margin: 20px 0;
            }
        """, font_config=font_config)

        start_time = datetime.now()
        pdf_bytes = HTML(string=html, base_url=str(request.base_url)).write_pdf(
            stylesheets=[css],
            font_config=font_config
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


@app.get("/admin/download-db", response_class=StreamingResponse)
def download_database(db: Session = Depends(get_db)):
    db_path = Path("data/expenses.db")
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    def file_generator():
        with open(db_path, "rb") as f:
            shutil.copyfileobj(f, os.fdopen(os.dup(f.fileno()), "rb"))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"expenses_backup_{timestamp}.db"
    return StreamingResponse(
        iter([open(db_path, "rb").read()]),
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
        Transaction.deleted_at.isnot(None),
        Transaction.deleted_at < cutoff_date
    )
    result = db.execute(stmt)
    db.commit()
    logging.info(f"Purged {result.rowcount} deleted transactions older than {days} days")
    return Response(
        status_code=200,
        content=f"Purged {result.rowcount} transactions",
        headers={"HX-Trigger": "transactions-changed"}
    )


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    return render(request, "admin.html", {})


def main():
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()
