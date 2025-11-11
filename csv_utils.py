from __future__ import annotations

import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Sequence

from models import Transaction, TransactionKind, TransactionType
from schemas import CSVRow


def parse_date(value: str):
    value = value.strip()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return datetime.strptime(value, "%d.%m.%Y").date()


def parse_amount(value: str) -> int:
    clean = value.strip().replace("€", "").replace(" ", "")
    clean = clean.replace(",", ".")
    if clean.count(".") > 1:
        parts = clean.split(".")
        clean = "".join(parts[:-1]) + "." + parts[-1]
    try:
        amount = Decimal(clean)
    except InvalidOperation as exc:
        raise ValueError("Invalid amount") from exc
    cents = int((amount * 100).quantize(Decimal("1")))
    if cents < 0:
        raise ValueError("Amount must be positive")
    return cents


def parse_csv(content: str) -> tuple[list[CSVRow], list[str]]:
    reader = csv.DictReader(StringIO(content))
    rows: list[CSVRow] = []
    errors: list[str] = []
    for idx, raw in enumerate(reader, start=1):
        try:
            date_value = parse_date((raw.get("Date") or "").strip())
            type_raw = (raw.get("Type") or "").strip().lower()
            type_value = TransactionType(type_raw)
            kind_raw = (raw.get("Kind") or "normal").strip().lower()
            kind_value = TransactionKind(kind_raw) if kind_raw in ["normal", "adjustment"] else TransactionKind.normal
            amount_value = parse_amount(raw.get("Amount") or "0")
            category = (raw.get("Category") or "").strip()
            note_raw = raw.get("Note")
            note = note_raw.strip() if isinstance(note_raw, str) and note_raw.strip() else None
            rows.append(
                CSVRow(
                    date=date_value,
                    type=type_value,
                    kind=kind_value,
                    amount_cents=amount_value,
                    category=category,
                    note=note,
                )
            )
        except Exception as exc:
            errors.append(f"Row {idx}: {exc}")
    return rows, errors


def export_transactions(transactions: Sequence[Transaction]) -> str:
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Type", "Kind", "Amount", "Category", "Note"])
    for txn in transactions:
        writer.writerow(
            [
                txn.date.isoformat(),
                txn.type.value,
                txn.kind.value if hasattr(txn, 'kind') else 'normal',
                f"{txn.amount_cents / 100:.2f}",
                txn.category.name if txn.category else "",
                txn.note or "",
            ]
        )
    return output.getvalue()
