import csv
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Sequence

from models import Transaction, TransactionType
from schemas import CSVRow


def sanitize_csv_value(value: str) -> str:
    """
    Sanitize CSV values to prevent formula injection by prefixing dangerous patterns with tab.
    """
    if not value or value.strip() == "":
        return ""

    value = value.strip()

    formula_triggers = ("=", "+", "-", "@", "\t", "\r")

    if value.startswith(formula_triggers):
        return "\t" + value

    dangerous_patterns = [
        r"^cmd\s*",
        r"^powershell\s*",
        r"^bash\s*",
        r"^sh\s*",
        r"^\.",
        r"^http[s]?://",
    ]

    for pattern in dangerous_patterns:
        if re.match(pattern, value, re.IGNORECASE):
            return "\t" + value

    return value


def parse_date(value: str):
    value = value.strip()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return datetime.strptime(value, "%d.%m.%Y").date()


def parse_amount(value: str, *, allow_negative: bool = False) -> int:
    clean = value.strip().replace("€", "").replace("$", "").replace(" ", "")
    clean = clean.replace(",", ".")
    if clean.count(".") > 1:
        parts = clean.split(".")
        clean = "".join(parts[:-1]) + "." + parts[-1]
    try:
        amount = Decimal(clean)
    except InvalidOperation as exc:
        raise ValueError("Invalid amount") from exc
    cents = int((amount * 100).quantize(Decimal("1")))
    if cents < 0 and not allow_negative:
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
            is_reimbursement_raw = (raw.get("IsReimbursement") or "").strip().lower()
            is_reimbursement_value = is_reimbursement_raw in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }
            amount_value = parse_amount(raw.get("Amount") or "0")
            category = (raw.get("Category") or "").strip()
            note_raw = raw.get("Note") or ""
            note = note_raw.strip() if note_raw.strip() else None
            rows.append(
                CSVRow(
                    date=date_value,
                    type=type_value,
                    is_reimbursement=is_reimbursement_value,
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
    writer.writerow(["Date", "Type", "IsReimbursement", "Amount", "Category", "Note"])
    for txn in transactions:
        writer.writerow(
            [
                txn.date.isoformat(),
                txn.type.value,
                "1" if txn.is_reimbursement else "0",
                f"{txn.amount_cents / 100:.2f}",
                sanitize_csv_value(txn.category.name if txn.category else ""),
                sanitize_csv_value(txn.note or ""),
            ]
        )
    return output.getvalue()
