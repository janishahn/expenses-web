from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import sqlite3
from sqlalchemy import select
from sqlalchemy.orm import Session

from models import (
    Category,
    IntervalUnit,
    MonthDayPolicy,
    RecurringRule,
    Transaction,
    TransactionKind,
    TransactionType,
)
from recurrence import calculate_next_date
from services import rebuild_monthly_rollups, get_current_user_id


@dataclass(frozen=True)
class LegacyCategoryMappingRow:
    idx: int
    legacy_type: TransactionType
    legacy_category: str
    transaction_count: int
    suggested_category_id: Optional[int]
    suggested_category_name: Optional[str]


@dataclass(frozen=True)
class LegacyRecurringPreviewRow:
    description: str
    legacy_type: TransactionType
    legacy_category: str
    amount_cents: int
    start_date: date
    recurrence_type: str
    interval: int
    last_processed_date: Optional[date]
    computed_next_occurrence: Optional[date]


@dataclass(frozen=True)
class LegacyDBPreview:
    transactions_count: int
    recurring_count: int
    min_transaction_date: Optional[date]
    max_transaction_date: Optional[date]
    non_midnight_transaction_times: int
    mapping_rows: list[LegacyCategoryMappingRow]
    recurring_rows: list[LegacyRecurringPreviewRow]
    warnings: list[str]


def _connect_readonly(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve()}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA query_only=ON;")
    except Exception:
        pass
    return con


def _require_legacy_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute(
        "select name from sqlite_master where type='table' and name not like 'sqlite_%'"
    )
    tables = {r[0] for r in cur.fetchall()}
    required_tables = {"categories", "transactions", "recurring_transactions"}
    missing = required_tables - tables
    if missing:
        raise ValueError(f"Legacy DB missing tables: {', '.join(sorted(missing))}")

    required_cols: dict[str, set[str]] = {
        "transactions": {
            "amount",
            "category",
            "description",
            "transaction_date",
            "transaction_type",
        },
        "recurring_transactions": {
            "amount",
            "category",
            "description",
            "start_date",
            "recurrence_type",
            "interval",
            "transaction_type",
            "last_processed_date",
        },
    }
    for table, cols in required_cols.items():
        cur.execute(f"pragma table_info({table})")
        present = {row["name"] for row in cur.fetchall()}
        missing_cols = cols - present
        if missing_cols:
            raise ValueError(
                f"Legacy DB table '{table}' missing columns: {', '.join(sorted(missing_cols))}"
            )


def _parse_legacy_datetime(value: str) -> datetime:
    value = value.strip()
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ValueError(f"Invalid legacy datetime: {value}") from exc


def _parse_amount_cents(amount_text: str) -> int:
    try:
        cents = int((Decimal(amount_text) * 100).quantize(Decimal("1")))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid legacy amount: {amount_text}") from exc
    if cents < 0:
        raise ValueError("Legacy amount must be non-negative")
    return cents


def _interval_unit_from_legacy(recurrence_type: str) -> Optional[IntervalUnit]:
    rt = recurrence_type.strip().lower()
    if rt == "monthly":
        return IntervalUnit.month
    if rt == "yearly":
        return IntervalUnit.year
    return None


class LegacySQLiteImportService:
    def __init__(self, session: Session, user_id: Optional[int] = None) -> None:
        self.session = session
        self.user_id = user_id or get_current_user_id()

    def _category_lookup(self) -> dict[tuple[TransactionType, str], Category]:
        stmt = select(Category).where(
            Category.user_id == self.user_id, Category.archived_at.is_(None)
        )
        lookup: dict[tuple[TransactionType, str], Category] = {}
        for cat in self.session.scalars(stmt):
            lookup[(cat.type, cat.name.lower())] = cat
        return lookup

    def preview(self, legacy_db_path: Path) -> LegacyDBPreview:
        if not legacy_db_path.exists():
            raise ValueError("Legacy DB file not found")

        con = _connect_readonly(legacy_db_path)
        try:
            _require_legacy_schema(con)
            cur = con.cursor()

            warnings: list[str] = []

            cur.execute("select count(*) as n from transactions")
            txn_count = int(cur.fetchone()["n"])
            cur.execute("select count(*) as n from recurring_transactions")
            recurring_count = int(cur.fetchone()["n"])

            cur.execute(
                "select min(transaction_date) as min_d, max(transaction_date) as max_d from transactions"
            )
            row = cur.fetchone()
            min_d = _parse_legacy_datetime(row["min_d"]).date() if row["min_d"] else None
            max_d = _parse_legacy_datetime(row["max_d"]).date() if row["max_d"] else None

            cur.execute(
                "select count(*) as n from transactions "
                "where strftime('%H:%M:%S', transaction_date) != '00:00:00'"
            )
            non_midnight = int(cur.fetchone()["n"])
            if non_midnight:
                warnings.append(
                    f"{non_midnight} transaction(s) have a time-of-day; only the date can be stored."
                )

            cur.execute(
                "select transaction_type, category, count(*) as c "
                "from transactions group by transaction_type, category "
                "order by transaction_type, lower(category)"
            )
            grouped = cur.fetchall()

            lookup = self._category_lookup()
            mapping_rows: list[LegacyCategoryMappingRow] = []
            for idx, r in enumerate(grouped):
                legacy_type = TransactionType(str(r["transaction_type"]))
                legacy_category = str(r["category"])
                suggested = lookup.get((legacy_type, legacy_category.lower()))
                mapping_rows.append(
                    LegacyCategoryMappingRow(
                        idx=idx,
                        legacy_type=legacy_type,
                        legacy_category=legacy_category,
                        transaction_count=int(r["c"]),
                        suggested_category_id=suggested.id if suggested else None,
                        suggested_category_name=suggested.name if suggested else None,
                    )
                )

            case_collisions: dict[tuple[str, str], set[str]] = {}
            for r in mapping_rows:
                key = (r.legacy_type.value, r.legacy_category.lower())
                case_collisions.setdefault(key, set()).add(r.legacy_category)
            for (t, lower_name), variants in case_collisions.items():
                if len(variants) > 1:
                    warnings.append(
                        f"Category casing differs for type={t}: {', '.join(sorted(variants))}"
                    )

            cur.execute(
                "select description, transaction_type, category, "
                "cast(amount as text) as amount_text, start_date, recurrence_type, interval, last_processed_date "
                "from recurring_transactions order by id"
            )
            recurring_rows: list[LegacyRecurringPreviewRow] = []
            unknown_recurrence: set[str] = set()
            for rr in cur.fetchall():
                rt = str(rr["recurrence_type"])
                unit = _interval_unit_from_legacy(rt)
                if not unit:
                    unknown_recurrence.add(rt)
                amount_cents = _parse_amount_cents(str(rr["amount_text"]))
                start = date.fromisoformat(str(rr["start_date"]))
                last = (
                    date.fromisoformat(str(rr["last_processed_date"]))
                    if rr["last_processed_date"]
                    else None
                )
                computed_next = None
                if unit and last:
                    temp_rule = RecurringRule(
                        user_id=self.user_id,
                        name=str(rr["description"]),
                        type=TransactionType(str(rr["transaction_type"])),
                        amount_cents=amount_cents,
                        category_id=1,
                        anchor_date=start,
                        interval_unit=unit,
                        interval_count=int(rr["interval"] or 1),
                        next_occurrence=start,
                        end_date=None,
                        auto_post=False,
                        skip_weekends=False,
                        month_day_policy=MonthDayPolicy.snap_to_end,
                    )
                    computed_next = calculate_next_date(temp_rule, last)

                recurring_rows.append(
                    LegacyRecurringPreviewRow(
                        description=str(rr["description"]),
                        legacy_type=TransactionType(str(rr["transaction_type"])),
                        legacy_category=str(rr["category"]),
                        amount_cents=amount_cents,
                        start_date=start,
                        recurrence_type=rt,
                        interval=int(rr["interval"] or 1),
                        last_processed_date=last,
                        computed_next_occurrence=computed_next,
                    )
                )

            if unknown_recurrence:
                warnings.append(
                    f"Unknown recurrence_type(s) in legacy DB: {', '.join(sorted(unknown_recurrence))}"
                )

            return LegacyDBPreview(
                transactions_count=txn_count,
                recurring_count=recurring_count,
                min_transaction_date=min_d,
                max_transaction_date=max_d,
                non_midnight_transaction_times=non_midnight,
                mapping_rows=mapping_rows,
                recurring_rows=recurring_rows,
                warnings=warnings,
            )
        finally:
            con.close()

    def commit(
        self,
        legacy_db_path: Path,
        *,
        mapping_targets: dict[tuple[TransactionType, str], str],
        import_recurring_rules: bool,
        recurring_auto_post: bool,
        link_recurring_transactions: bool,
        preserve_time_in_note: bool,
    ) -> dict[str, int]:
        con = _connect_readonly(legacy_db_path)
        try:
            _require_legacy_schema(con)
            cur = con.cursor()

            existing_categories = self._category_lookup()
            category_id_by_legacy: dict[tuple[TransactionType, str], int] = {}
            discarded_legacy_keys: set[tuple[TransactionType, str]] = set()

            for (legacy_type, legacy_category), target in mapping_targets.items():
                legacy_key = (legacy_type, legacy_category)
                if target == "discard":
                    discarded_legacy_keys.add(legacy_key)
                    continue
                if target.startswith("existing:"):
                    category_id = int(target.split(":", 1)[1])
                    cat = self.session.get(Category, category_id)
                    if not cat or cat.user_id != self.user_id:
                        raise ValueError(f"Mapped category not found: {category_id}")
                    if cat.type != legacy_type:
                        raise ValueError(
                            f"Mapped category type mismatch for '{legacy_category}'"
                        )
                    category_id_by_legacy[legacy_key] = cat.id
                    continue

                existing = existing_categories.get((legacy_type, legacy_category.lower()))
                if existing:
                    category_id_by_legacy[legacy_key] = existing.id
                    continue

                new_cat = Category(
                    user_id=self.user_id,
                    name=legacy_category.strip(),
                    type=legacy_type,
                    order=0,
                )
                self.session.add(new_cat)
                self.session.flush()
                category_id_by_legacy[legacy_key] = new_cat.id
                existing_categories[(legacy_type, new_cat.name.lower())] = new_cat

            imported_rule_ids_by_name: dict[str, int] = {}
            discarded_rules = 0
            if import_recurring_rules:
                cur.execute(
                    "select description, transaction_type, category, "
                    "cast(amount as text) as amount_text, start_date, recurrence_type, interval, last_processed_date "
                    "from recurring_transactions order by id"
                )
                for rr in cur.fetchall():
                    rule_name = str(rr["description"])
                    legacy_type = TransactionType(str(rr["transaction_type"]))
                    legacy_category = str(rr["category"])
                    if (legacy_type, legacy_category) in discarded_legacy_keys:
                        discarded_rules += 1
                        continue
                    category_id = category_id_by_legacy.get((legacy_type, legacy_category))
                    if not category_id:
                        raise ValueError(
                            f"Missing mapping for category '{legacy_category}' ({legacy_type.value})"
                        )

                    unit = _interval_unit_from_legacy(str(rr["recurrence_type"]))
                    if not unit:
                        raise ValueError(
                            f"Unsupported recurrence_type: {rr['recurrence_type']}"
                        )
                    amount_cents = _parse_amount_cents(str(rr["amount_text"]))
                    start = date.fromisoformat(str(rr["start_date"]))
                    last = (
                        date.fromisoformat(str(rr["last_processed_date"]))
                        if rr["last_processed_date"]
                        else None
                    )

                    existing_rule = self.session.scalar(
                        select(RecurringRule.id).where(
                            RecurringRule.user_id == self.user_id,
                            RecurringRule.name == rule_name,
                            RecurringRule.type == legacy_type,
                            RecurringRule.category_id == category_id,
                            RecurringRule.anchor_date == start,
                            RecurringRule.amount_cents == amount_cents,
                        )
                    )
                    if existing_rule:
                        imported_rule_ids_by_name[rule_name] = int(existing_rule)
                        continue

                    next_occurrence = start
                    temp = RecurringRule(
                        user_id=self.user_id,
                        name=rule_name,
                        type=legacy_type,
                        amount_cents=amount_cents,
                        category_id=category_id,
                        anchor_date=start,
                        interval_unit=unit,
                        interval_count=int(rr["interval"] or 1),
                        next_occurrence=start,
                        end_date=None,
                        auto_post=recurring_auto_post,
                        skip_weekends=False,
                        month_day_policy=MonthDayPolicy.snap_to_end,
                    )
                    if last:
                        next_occurrence = calculate_next_date(temp, last)
                        temp.next_occurrence = next_occurrence

                    self.session.add(temp)
                    self.session.flush()
                    imported_rule_ids_by_name[rule_name] = temp.id

            inserted_txns = 0
            skipped_recurring_duplicates = 0
            time_appended = 0
            discarded_txns = 0

            cur.execute(
                "select id, cast(amount as text) as amount_text, category, description, transaction_date, transaction_type "
                "from transactions order by transaction_date, id"
            )
            for r in cur.fetchall():
                txn_type = TransactionType(str(r["transaction_type"]))
                legacy_category = str(r["category"])
                if (txn_type, legacy_category) in discarded_legacy_keys:
                    discarded_txns += 1
                    continue
                category_id = category_id_by_legacy.get((txn_type, legacy_category))
                if not category_id:
                    raise ValueError(
                        f"Missing mapping for category '{legacy_category}' ({txn_type.value})"
                    )

                dt = _parse_legacy_datetime(str(r["transaction_date"]))
                txn_date = dt.date()

                note = str(r["description"])
                if preserve_time_in_note and dt.time() != datetime.min.time():
                    note = f"{note} (time {dt.strftime('%H:%M:%S')})"
                    time_appended += 1

                origin_rule_id = None
                occurrence_date = None
                if (
                    import_recurring_rules
                    and link_recurring_transactions
                    and note.endswith(" (Recurring)")
                ):
                    base = note[: -len(" (Recurring)")]
                    rule_id = imported_rule_ids_by_name.get(base)
                    if rule_id:
                        origin_rule_id = rule_id
                        occurrence_date = txn_date
                        existing_txn = self.session.scalar(
                            select(Transaction.id).where(
                                Transaction.user_id == self.user_id,
                                Transaction.origin_rule_id == origin_rule_id,
                                Transaction.occurrence_date == occurrence_date,
                            )
                        )
                        if existing_txn:
                            skipped_recurring_duplicates += 1
                            continue

                txn = Transaction(
                    user_id=self.user_id,
                    date=txn_date,
                    type=txn_type,
                    kind=TransactionKind.normal,
                    amount_cents=_parse_amount_cents(str(r["amount_text"])),
                    category_id=category_id,
                    note=note,
                    origin_rule_id=origin_rule_id,
                    occurrence_date=occurrence_date,
                )
                self.session.add(txn)
                inserted_txns += 1

            self.session.commit()
            rebuild_monthly_rollups(self.session, user_id=self.user_id)

            return {
                "inserted_transactions": inserted_txns,
                "skipped_recurring_duplicates": skipped_recurring_duplicates,
                "time_appended_to_notes": time_appended,
                "discarded_transactions": discarded_txns,
                "imported_recurring_rules": len(imported_rule_ids_by_name)
                if import_recurring_rules
                else 0,
                "discarded_recurring_rules": discarded_rules if import_recurring_rules else 0,
            }
        finally:
            con.close()
