import re
from datetime import date, datetime, timedelta

from src.db import _db_conn

SCHEMA = "finance"
TBL_ACCOUNTS = f"{SCHEMA}.accounts"
TBL_EXPENSE_CATEGORIES = f"{SCHEMA}.expense_categories"
TBL_INCOME_SOURCES = f"{SCHEMA}.income_sources"
TBL_INCOMES = f"{SCHEMA}.incomes"
TBL_EXPENSES = f"{SCHEMA}.expenses"
TBL_FIXED_EXPENSES = f"{SCHEMA}.fixed_expenses"
TBL_FIXED_EXPENSE_PAYMENTS = f"{SCHEMA}.fixed_expense_payments"
TBL_INSTALLMENT_PLANS = f"{SCHEMA}.installment_plans"
TBL_ACCOUNT_CUT_EVENTS = f"{SCHEMA}.account_cut_events"
TBL_FIXED_INCOMES = f"{SCHEMA}.fixed_incomes"


def _require_month(month: str | None) -> str:
    value = (month or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise ValueError("Formato invalido para month. Usa YYYY-MM.")
    return value


def _require_date(raw: str | None, field_name: str) -> str:
    value = (raw or "").strip()
    if not value:
        raise ValueError(f"Falta {field_name}.")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"Formato invalido para {field_name}. Usa YYYY-MM-DD.") from exc


def _optional_date(raw: str | None) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None
    return _require_date(value, "purchase_date")


def _require_positive_amount(raw, field_name: str) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} invalido.") from exc
    if value < 0:
        raise ValueError(f"{field_name} no puede ser negativo.")
    return value


def _optional_day(raw, field_name: str) -> int | None:
    if raw in (None, ""):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} invalido.") from exc
    if value < 1 or value > 31:
        raise ValueError(f"{field_name} debe estar entre 1 y 31.")
    return value


def _require_day(raw, field_name: str) -> int:
    value = _optional_day(raw, field_name)
    if value is None:
        raise ValueError(f"Falta {field_name}.")
    return value


def _require_int(raw, field_name: str, minimum: int = 0) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} invalido.") from exc
    if value < minimum:
        raise ValueError(f"{field_name} debe ser mayor o igual a {minimum}.")
    return value


def _require_choice(raw: str | None, field_name: str, allowed: set[str]) -> str:
    value = (raw or "").strip().lower()
    if value not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise ValueError(f"{field_name} invalido. Usa {allowed_values}.")
    return value


def _account_type_label(account_type: str) -> str:
    labels = {
        "credit": "Tarjeta de credito",
        "store_card": "Tarjeta de tienda",
        "debit": "Debito",
        "cash": "Efectivo",
    }
    return labels.get(account_type, account_type)


def _month_range(month: str) -> tuple[str, str]:
    first_day = date.fromisoformat(f"{month}-01")
    if first_day.month == 12:
        next_month = date(first_day.year + 1, 1, 1)
    else:
        next_month = date(first_day.year, first_day.month + 1, 1)
    return first_day.isoformat(), next_month.isoformat()


def _month_due_date(month: str, due_day: int) -> str:
    first_day = date.fromisoformat(f"{month}-01")
    _, next_month = _month_range(month)
    last_day = (date.fromisoformat(next_month) - timedelta(days=1)).day
    return date(first_day.year, first_day.month, min(due_day, last_day)).isoformat()


def _shift_month(month: str, delta: int) -> str:
    first_day = date.fromisoformat(f"{month}-01")
    month_index = first_day.month - 1 + delta
    year = first_day.year + month_index // 12
    month_value = month_index % 12 + 1
    return f"{year:04d}-{month_value:02d}"


def _month_index(month: str) -> int:
    year, month_value = map(int, month.split("-"))
    return year * 12 + month_value


def _months_between_inclusive(start_month: str, end_month: str) -> int:
    return max(0, _month_index(end_month) - _month_index(start_month) + 1)


def _max_month(month_a: str, month_b: str) -> str:
    return month_a if _month_index(month_a) >= _month_index(month_b) else month_b


def _date_to_iso(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _datetime_to_iso(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _sort_expense_entries(entries: list[dict]) -> list[dict]:
    return sorted(
        entries,
        key=lambda item: (
            item.get("date") or "",
            item.get("created_at") or "",
            str(item.get("id") or ""),
        ),
        reverse=True,
    )


def _is_card_account_type(account_type: str | None) -> bool:
    return account_type in {"credit", "store_card"}


def _uses_two_month_post_cutover(account_name: str | None) -> bool:
    normalized = (account_name or "").strip().lower()
    return normalized in {"heb", "stori"}


def _load_cut_events_by_account(conn, until_date: str | None = None) -> dict[str, list[dict]]:
    where = ""
    params: list = []
    if until_date:
        where = "where ace.cut_date < %s"
        params.append(until_date)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            select a.name, ace.cut_date, ace.created_at
            from {TBL_ACCOUNT_CUT_EVENTS} ace
            join {TBL_ACCOUNTS} a on a.id = ace.account_id
            {where}
            order by a.name asc, ace.cut_date asc, ace.id asc
            """,
            tuple(params),
        )
        rows = cur.fetchall()

    grouped: dict[str, list[dict]] = {}
    for account_name, cut_date, created_at in rows:
        grouped.setdefault(account_name, []).append({
            "cut_date": _coerce_date(cut_date),
            "created_at": _coerce_datetime(created_at),
        })
    return grouped


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if hasattr(value, "date") and not isinstance(value, str):
        try:
            return value.date()
        except Exception:
            pass
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    return date.fromisoformat(str(value)[:10])


def _coerce_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            text = value.isoformat()
            return datetime.fromisoformat(text[:19])
        except Exception:
            pass
    if isinstance(value, str):
        return datetime.fromisoformat(value[:19])
    return datetime.fromisoformat(str(value)[:19])


def _installment_anchor_date(purchase_date, created_at) -> date:
    return _coerce_date(purchase_date) or _coerce_date(created_at) or date.today()


def _installment_first_payment_month(
    purchase_date,
    account_name: str,
    account_type: str | None,
    cutoff_day: int | None,
    cut_events_by_account: dict[str, list[dict]],
) -> str:
    anchor_date = _coerce_date(purchase_date) or date.today()
    purchase_month = anchor_date.strftime("%Y-%m")
    if not _is_card_account_type(account_type):
        return purchase_month

    cut_events = cut_events_by_account.get(account_name, [])
    latest_cut = None
    for event in cut_events:
        cut_date = event["cut_date"]
        if cut_date <= anchor_date:
            latest_cut = cut_date
        else:
            break

    if latest_cut and latest_cut.strftime("%Y-%m") == purchase_month and anchor_date > latest_cut:
        return _shift_month(purchase_month, 1)
    if cutoff_day and anchor_date.day > int(cutoff_day):
        return _shift_month(purchase_month, 1)
    return purchase_month


def _installment_term_months(months_total, stored_months_remaining, fallback_end_month: str | None, first_payment_month: str) -> int:
    if months_total not in (None, ""):
        return int(months_total)
    if stored_months_remaining not in (None, ""):
        return int(stored_months_remaining)
    if fallback_end_month:
        return _months_between_inclusive(first_payment_month, fallback_end_month)
    return 0


def _installment_end_month(first_payment_month: str, term_months: int, fallback_end_month: str | None = None) -> str | None:
    if term_months > 0:
        return _shift_month(first_payment_month, term_months - 1)
    return fallback_end_month


def _installment_months_remaining(first_payment_month: str, end_month: str | None, reference_month: str) -> int:
    if not end_month:
        return 0
    effective_start = _max_month(first_payment_month, reference_month)
    return _months_between_inclusive(effective_start, end_month)


def _installment_active_in_month(first_payment_month: str, end_month: str | None, month: str) -> bool:
    if not end_month:
        return False
    month_idx = _month_index(month)
    return _month_index(first_payment_month) <= month_idx <= _month_index(end_month)


def _report_month_for_expense(expense_date, expense_created_at, account_name: str, account_type: str | None, cut_events_by_account: dict[str, list[dict]]) -> str:
    expense_month = expense_date.strftime("%Y-%m")
    if not _is_card_account_type(account_type):
        return expense_month

    expense_created_dt = _coerce_datetime(expense_created_at)
    cut_events = cut_events_by_account.get(account_name, [])
    latest_cut_event = None
    for event in cut_events:
        cut_date = event["cut_date"]
        if cut_date <= expense_date:
            latest_cut_event = event
        else:
            break

    if not latest_cut_event:
        return expense_month

    latest_cut_date = latest_cut_event["cut_date"]
    latest_cut_created_at = latest_cut_event.get("created_at")
    is_same_day_post_cut = (
        expense_date == latest_cut_date
        and expense_created_dt is not None
        and latest_cut_created_at is not None
        and expense_created_dt > latest_cut_created_at
    )
    if latest_cut_date and latest_cut_date.strftime("%Y-%m") == expense_month and (expense_date > latest_cut_date or is_same_day_post_cut):
        return _shift_month(expense_month, 2 if _uses_two_month_post_cutover(account_name) else 1)
    return expense_month


def _list_regular_expense_entries(conn, month: str) -> list[dict]:
    month_start, next_month = _month_range(month)
    lookback_month = _shift_month(month, -2)
    lookback_month_start, _ = _month_range(lookback_month)
    cut_events_by_account = _load_cut_events_by_account(conn, until_date=next_month)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            select
              e.id,
              e.expense_date,
              e.amount,
              e.description,
              a.name as account_name,
              a.account_type,
              a.cutoff_day,
              a.payment_due_day,
              c.name as category_name,
              e.notes,
              e.created_at,
              a.is_virtual
            from {TBL_EXPENSES} e
            join {TBL_ACCOUNTS} a on a.id = e.account_id
            join {TBL_EXPENSE_CATEGORIES} c on c.id = e.category_id
            where e.expense_date >= %s and e.expense_date < %s
            """,
            (lookback_month_start, next_month),
        )
        rows = cur.fetchall()

    items = []
    for row in rows:
        report_month = _report_month_for_expense(row[1], row[10], row[4], row[5], cut_events_by_account)
        if report_month != month:
            continue
        items.append(
            {
                "id": row[0],
                "date": _date_to_iso(row[1]),
                "amount": float(row[2]),
                "description": row[3],
                "account_name": row[4],
                "account_type": row[5],
                "cutoff_day": row[6],
                "payment_due_day": row[7],
                "category_name": row[8],
                "notes": row[9],
                "created_at": _datetime_to_iso(row[10]),
                "entry_type": "expense",
                "fixed_expense_id": None,
                "payment_status": None,
                "paid_date": None,
                "report_month": report_month,
                "is_virtual": bool(row[11]),
            }
        )
    return items


def _list_fixed_expense_entries(conn, month: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select
              f.id,
              f.name,
              f.amount,
              f.due_day,
              a.name as account_name,
              a.account_type,
              a.cutoff_day,
              a.payment_due_day,
              c.name as category_name,
              coalesce(fp.status, 'pending') as payment_status,
              fp.paid_date,
              f.created_at,
              a.is_virtual
            from {TBL_FIXED_EXPENSES} f
            join {TBL_ACCOUNTS} a on a.id = f.account_id
            join {TBL_EXPENSE_CATEGORIES} c on c.id = f.category_id
            left join {TBL_FIXED_EXPENSE_PAYMENTS} fp
              on fp.fixed_expense_id = f.id
             and fp.payment_month = %s
            where f.is_active = true
            """,
            (month,),
        )
        rows = cur.fetchall()

    return [
        {
            "id": f"fixed-{row[0]}-{month}",
            "date": _month_due_date(month, int(row[3])),
            "amount": float(row[2]),
            "description": row[1],
            "account_name": row[4],
            "account_type": row[5],
            "cutoff_day": row[6],
            "payment_due_day": row[7],
            "category_name": row[8],
            "notes": None,
            "created_at": _datetime_to_iso(row[11]),
            "entry_type": "fixed",
            "fixed_expense_id": row[0],
            "payment_status": row[9],
            "paid_date": _date_to_iso(row[10]),
            "report_month": month,
            "is_virtual": bool(row[12]),
        }
        for row in rows
    ]


def _list_monthly_expense_entries(conn, month: str) -> list[dict]:
    return _sort_expense_entries(
        _list_regular_expense_entries(conn, month) + _list_fixed_expense_entries(conn, month)
    )


def _fetch_named_entities(table_name: str, columns: str, order_by: str = "name asc") -> list[dict]:
    with _db_conn() as conn:
        if conn is None:
            return []
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select {columns}
                from {table_name}
                where is_active = true
                order by {order_by}
                """
            )
            rows = cur.fetchall()
    column_names = [part.strip() for part in columns.split(",")]
    return [dict(zip(column_names, row)) for row in rows]


def _lookup_id_by_name(table_name: str, name: str) -> int:
    clean_name = (name or "").strip()
    if not clean_name:
        raise ValueError("Falta name.")
    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"select id from {table_name} where lower(name) = lower(%s) and is_active = true",
                (clean_name,),
            )
            row = cur.fetchone()
    if not row:
        raise ValueError(f"No existe '{clean_name}' en {table_name}.")
    return int(row[0])


def list_finance_catalogs() -> dict:
    return {
        "accounts": _fetch_named_entities(
            TBL_ACCOUNTS,
            "id, name, account_type, cutoff_day, payment_due_day, is_virtual",
        ),
        "expense_categories": _fetch_named_entities(
            TBL_EXPENSE_CATEGORIES,
            "id, name, kind",
        ),
        "income_sources": _fetch_named_entities(
            TBL_INCOME_SOURCES,
            "id, name",
        ),
    }


def create_finance_expense_category(payload: dict) -> dict:
    name = (payload.get("name") or "").strip()
    kind = (payload.get("kind") or "variable").strip().lower()

    if not name:
        raise ValueError("Falta name.")
    if kind not in {"variable", "fixed", "debt"}:
        raise ValueError("kind invalido. Usa variable, fixed o debt.")

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_EXPENSE_CATEGORIES} (name, kind)
                values (%s, %s)
                on conflict (name) do nothing
                returning id, name, kind, created_at
                """,
                (name, kind),
            )
            row = cur.fetchone()
            conn.commit()
    if not row:
        raise ValueError(f"La categoría '{name}' ya existe.")
    return {"id": row[0], "name": row[1], "kind": row[2], "created_at": _datetime_to_iso(row[3])}


def deactivate_finance_expense_category(payload: dict) -> dict:
    name = (payload.get("name") or "").strip()
    if not name:
        raise ValueError("Falta name.")

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                update {TBL_EXPENSE_CATEGORIES}
                set is_active = false
                where lower(name) = lower(%s) and is_active = true
                returning id, name
                """,
                (name,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No existe la categoría activa '{name}'.")
            conn.commit()
    return {"id": row[0], "name": row[1], "is_active": False}


def list_finance_expenses(month: str | None = None, limit: int = 100) -> list[dict]:
    limit_value = max(1, min(int(limit or 100), 500))
    if month:
        month_value = _require_month(month)
        with _db_conn() as conn:
            if conn is None:
                return []
            return _list_monthly_expense_entries(conn, month_value)[:limit_value]

    with _db_conn() as conn:
        if conn is None:
            return []
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                  e.id,
                  e.expense_date,
                  e.amount,
                  e.description,
                  a.name as account_name,
                  a.account_type,
                  a.cutoff_day,
                  a.payment_due_day,
                  c.name as category_name,
                  e.notes,
                  e.created_at
                from {TBL_EXPENSES} e
                join {TBL_ACCOUNTS} a on a.id = e.account_id
                join {TBL_EXPENSE_CATEGORIES} c on c.id = e.category_id
                order by e.expense_date desc, e.id desc
                limit %s
                """,
                (limit_value,),
            )
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "date": _date_to_iso(row[1]),
            "amount": float(row[2]),
            "description": row[3],
            "account_name": row[4],
            "account_type": row[5],
            "cutoff_day": row[6],
            "payment_due_day": row[7],
            "category_name": row[8],
            "notes": row[9],
            "created_at": _datetime_to_iso(row[10]),
            "entry_type": "expense",
            "fixed_expense_id": None,
            "payment_status": None,
            "paid_date": None,
        }
        for row in rows
    ]


def create_finance_expense(payload: dict) -> dict:
    expense_date = _require_date(payload.get("date"), "date")
    amount = _require_positive_amount(payload.get("amount"), "amount")
    description = (payload.get("description") or "").strip()
    account_name = (payload.get("account_name") or "").strip()
    category_name = (payload.get("category_name") or "").strip()
    notes = (payload.get("notes") or "").strip() or None

    if not description:
        raise ValueError("Falta description.")
    if not account_name:
        raise ValueError("Falta account_name.")
    if not category_name:
        raise ValueError("Falta category_name.")

    account_id = _lookup_id_by_name(TBL_ACCOUNTS, account_name)
    category_id = _lookup_id_by_name(TBL_EXPENSE_CATEGORIES, category_name)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_EXPENSES}
                (expense_date, amount, description, account_id, category_id, notes)
                values (%s, %s, %s, %s, %s, %s)
                returning id, expense_date, amount, description, created_at
                """,
                (expense_date, amount, description, account_id, category_id, notes),
            )
            row = cur.fetchone()
            conn.commit()
    return {
        "id": row[0],
        "date": _date_to_iso(row[1]),
        "amount": float(row[2]),
        "description": row[3],
        "account_name": account_name,
        "category_name": category_name,
        "created_at": _datetime_to_iso(row[4]),
    }


def update_finance_expense(expense_id: int, payload: dict) -> dict:
    expense_date = _require_date(payload.get("date"), "date")
    amount = _require_positive_amount(payload.get("amount"), "amount")
    description = (payload.get("description") or "").strip()
    account_name = (payload.get("account_name") or "").strip()
    category_name = (payload.get("category_name") or "").strip()
    notes = (payload.get("notes") or "").strip() or None

    if not description:
        raise ValueError("Falta description.")
    if not account_name:
        raise ValueError("Falta account_name.")
    if not category_name:
        raise ValueError("Falta category_name.")

    account_id = _lookup_id_by_name(TBL_ACCOUNTS, account_name)
    category_id = _lookup_id_by_name(TBL_EXPENSE_CATEGORIES, category_name)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                update {TBL_EXPENSES}
                set expense_date = %s,
                    amount = %s,
                    description = %s,
                    account_id = %s,
                    category_id = %s,
                    notes = %s
                where id = %s
                returning id, expense_date, amount, description, created_at
                """,
                (expense_date, amount, description, account_id, category_id, notes, expense_id),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Gasto {expense_id} no encontrado.")
            conn.commit()
    return {
        "id": row[0],
        "date": _date_to_iso(row[1]),
        "amount": float(row[2]),
        "description": row[3],
        "account_name": account_name,
        "category_name": category_name,
        "created_at": _datetime_to_iso(row[4]),
    }


def list_finance_incomes(month: str | None = None, limit: int = 100) -> list[dict]:
    limit_value = max(1, min(int(limit or 100), 500))
    where = ""
    params: list = []
    if month:
        month_value = _require_month(month)
        month_start, next_month = _month_range(month_value)
        where = "where i.income_date >= %s and i.income_date < %s"
        params.extend([month_start, next_month])
    params.append(limit_value)

    with _db_conn() as conn:
        if conn is None:
            return []
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                  i.id,
                  i.income_date,
                  i.amount,
                  i.description,
                  s.name as source_name,
                  i.notes,
                  i.created_at
                from {TBL_INCOMES} i
                join {TBL_INCOME_SOURCES} s on s.id = i.income_source_id
                {where}
                order by i.income_date desc, i.id desc
                limit %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "date": _date_to_iso(row[1]),
            "amount": float(row[2]),
            "description": row[3],
            "source_name": row[4],
            "notes": row[5],
            "created_at": _datetime_to_iso(row[6]),
        }
        for row in rows
    ]


def create_finance_income(payload: dict) -> dict:
    income_date = _require_date(payload.get("date"), "date")
    amount = _require_positive_amount(payload.get("amount"), "amount")
    description = (payload.get("description") or "").strip()
    source_name = (payload.get("source_name") or "").strip()
    notes = (payload.get("notes") or "").strip() or None

    if not description:
        raise ValueError("Falta description.")
    if not source_name:
        raise ValueError("Falta source_name.")

    source_id = _lookup_id_by_name(TBL_INCOME_SOURCES, source_name)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_INCOMES}
                (income_date, amount, description, income_source_id, notes)
                values (%s, %s, %s, %s, %s)
                returning id, income_date, amount, description, created_at
                """,
                (income_date, amount, description, source_id, notes),
            )
            row = cur.fetchone()
            conn.commit()
    return {
        "id": row[0],
        "date": _date_to_iso(row[1]),
        "amount": float(row[2]),
        "description": row[3],
        "source_name": source_name,
        "created_at": _datetime_to_iso(row[4]),
    }


def list_finance_fixed_incomes(active_only: bool = True) -> list[dict]:
    where = "where fi.is_active = true" if active_only else ""
    with _db_conn() as conn:
        if conn is None:
            return []
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select fi.id, fi.name, fi.amount, s.name as source_name, fi.is_active, fi.created_at, fi.kind, fi.account_id, a.name as account_name
                from {TBL_FIXED_INCOMES} fi
                join {TBL_INCOME_SOURCES} s on s.id = fi.income_source_id
                left join {TBL_ACCOUNTS} a on a.id = fi.account_id
                {where}
                order by fi.name asc
                """,
            )
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "name": row[1],
            "amount": float(row[2]),
            "source_name": row[3],
            "is_active": row[4],
            "created_at": _datetime_to_iso(row[5]),
            "kind": row[6],
            "account_id": row[7],
            "account_name": row[8],
        }
        for row in rows
    ]


def create_finance_fixed_income(payload: dict) -> dict:
    name = (payload.get("name") or "").strip()
    amount = _require_positive_amount(payload.get("amount"), "amount")
    source_name = (payload.get("source_name") or "").strip()
    kind = (payload.get("kind") or "cash").strip()
    if kind not in ("cash", "in_kind"):
        kind = "cash"
    account_name = (payload.get("account_name") or "").strip() or None

    if not name:
        raise ValueError("Falta name.")
    if not source_name:
        raise ValueError("Falta source_name.")

    source_id = _lookup_id_by_name(TBL_INCOME_SOURCES, source_name)
    account_id = _lookup_id_by_name(TBL_ACCOUNTS, account_name) if account_name else None

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_FIXED_INCOMES} (name, amount, income_source_id, kind, account_id)
                values (%s, %s, %s, %s, %s)
                returning id, name, amount, created_at
                """,
                (name, amount, source_id, kind, account_id),
            )
            row = cur.fetchone()
            conn.commit()
    return {
        "id": row[0],
        "name": row[1],
        "amount": float(row[2]),
        "source_name": source_name,
        "kind": kind,
        "account_id": account_id,
        "account_name": account_name,
        "created_at": _datetime_to_iso(row[3]),
    }


def deactivate_finance_fixed_income(payload: dict) -> dict:
    fixed_income_id = _require_int(payload.get("fixed_income_id"), "fixed_income_id", minimum=1)
    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"update {TBL_FIXED_INCOMES} set is_active = false where id = %s returning id",
                (fixed_income_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Ingreso fijo {fixed_income_id} no encontrado.")
            conn.commit()
    return {"id": row[0]}


def update_finance_income(income_id: int, payload: dict) -> dict:
    income_date = _require_date(payload.get("date"), "date")
    amount = _require_positive_amount(payload.get("amount"), "amount")
    description = (payload.get("description") or "").strip()
    source_name = (payload.get("source_name") or "").strip()
    notes = (payload.get("notes") or "").strip() or None

    if not description:
        raise ValueError("Falta description.")
    if not source_name:
        raise ValueError("Falta source_name.")

    source_id = _lookup_id_by_name(TBL_INCOME_SOURCES, source_name)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                update {TBL_INCOMES}
                set income_date = %s,
                    amount = %s,
                    description = %s,
                    income_source_id = %s,
                    notes = %s
                where id = %s
                returning id, income_date, amount, description, created_at
                """,
                (income_date, amount, description, source_id, notes, income_id),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Ingreso {income_id} no encontrado.")
            conn.commit()
    return {
        "id": row[0],
        "date": _date_to_iso(row[1]),
        "amount": float(row[2]),
        "description": row[3],
        "source_name": source_name,
        "created_at": _datetime_to_iso(row[4]),
    }


def list_finance_fixed_expenses(month: str | None = None) -> list[dict]:
    params: list = []
    payment_join = ""
    payment_columns = """
                  null::char(7) as payment_month,
                  'pending' as payment_status,
                  null::date as paid_date,
    """
    if month:
        month_value = _require_month(month)
        payment_join = f"""
                left join {TBL_FIXED_EXPENSE_PAYMENTS} fp
                  on fp.fixed_expense_id = f.id
                 and fp.payment_month = %s
        """
        payment_columns = """
                  fp.payment_month,
                  coalesce(fp.status, 'pending') as payment_status,
                  fp.paid_date,
        """
        params.append(month_value)

    with _db_conn() as conn:
        if conn is None:
            return []
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                  f.id,
                  f.name,
                  f.amount,
                  f.due_day,
                  a.name as account_name,
                  c.name as category_name,
                  {payment_columns}
                  f.is_active,
                  f.created_at
                from {TBL_FIXED_EXPENSES} f
                join {TBL_ACCOUNTS} a on a.id = f.account_id
                join {TBL_EXPENSE_CATEGORIES} c on c.id = f.category_id
                {payment_join}
                where f.is_active = true
                order by f.due_day asc, f.name asc
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "name": row[1],
            "amount": float(row[2]),
            "due_day": row[3],
            "account_name": row[4],
            "category_name": row[5],
            "payment_month": row[6],
            "payment_status": row[7],
            "paid_date": _date_to_iso(row[8]),
            "is_active": bool(row[9]),
            "created_at": _datetime_to_iso(row[10]),
        }
        for row in rows
    ]


def create_finance_fixed_expense(payload: dict) -> dict:
    name = (payload.get("name") or "").strip()
    amount = _require_positive_amount(payload.get("amount"), "amount")
    due_day = _require_day(payload.get("due_day"), "due_day")
    account_name = (payload.get("account_name") or "").strip()
    category_name = (payload.get("category_name") or "").strip()

    if not name:
        raise ValueError("Falta name.")
    if not account_name:
        raise ValueError("Falta account_name.")
    if not category_name:
        raise ValueError("Falta category_name.")

    account_id = _lookup_id_by_name(TBL_ACCOUNTS, account_name)
    category_id = _lookup_id_by_name(TBL_EXPENSE_CATEGORIES, category_name)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_FIXED_EXPENSES}
                (name, amount, due_day, account_id, category_id)
                values (%s, %s, %s, %s, %s)
                returning id, created_at
                """,
                (name, amount, due_day, account_id, category_id),
            )
            row = cur.fetchone()
            conn.commit()
    return {
        "id": row[0],
        "name": name,
        "amount": amount,
        "due_day": due_day,
        "account_name": account_name,
        "category_name": category_name,
        "created_at": _datetime_to_iso(row[1]),
    }


def deactivate_finance_fixed_expense(payload: dict) -> dict:
    fixed_expense_id = _require_int(payload.get("fixed_expense_id"), "fixed_expense_id", minimum=1)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                update {TBL_FIXED_EXPENSES}
                set is_active = false
                where id = %s and is_active = true
                returning id, name
                """,
                (fixed_expense_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No existe el gasto fijo activo {fixed_expense_id}.")
            conn.commit()

    return {
        "id": row[0],
        "name": row[1],
        "is_active": False,
    }


def list_finance_account_cut_events(account_name: str | None = None, limit: int = 50) -> list[dict]:
    limit_value = max(1, min(int(limit or 50), 200))
    where = ""
    params: list = []
    if account_name:
        where = "where lower(a.name) = lower(%s)"
        params.append(account_name.strip())
    params.append(limit_value)

    with _db_conn() as conn:
        if conn is None:
            return []
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                  ace.id,
                  a.name,
                  ace.cut_date,
                  ace.created_at
                from {TBL_ACCOUNT_CUT_EVENTS} ace
                join {TBL_ACCOUNTS} a on a.id = ace.account_id
                {where}
                order by ace.cut_date desc, ace.id desc
                limit %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()

    return [
        {
            "id": row[0],
            "account_name": row[1],
            "cut_date": _date_to_iso(row[2]),
            "created_at": _datetime_to_iso(row[3]),
        }
        for row in rows
    ]


def create_finance_account_cut_event(payload: dict) -> dict:
    account_name = (payload.get("account_name") or "").strip()
    cut_date = _require_date(payload.get("cut_date") or date.today().isoformat(), "cut_date")

    if not account_name:
        raise ValueError("Falta account_name.")

    account_id = _lookup_id_by_name(TBL_ACCOUNTS, account_name)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_ACCOUNT_CUT_EVENTS}
                (account_id, cut_date)
                values (%s, %s)
                on conflict (account_id, cut_date)
                do update set cut_date = excluded.cut_date
                returning id, cut_date, created_at
                """,
                (account_id, cut_date),
            )
            row = cur.fetchone()
            conn.commit()

    return {
        "id": row[0],
        "account_name": account_name,
        "cut_date": _date_to_iso(row[1]),
        "created_at": _datetime_to_iso(row[2]),
    }


def upsert_finance_fixed_expense_payment(payload: dict) -> dict:
    fixed_expense_id = _require_int(payload.get("fixed_expense_id"), "fixed_expense_id", minimum=1)
    payment_month = _require_month(payload.get("payment_month"))
    payment_status = _require_choice(payload.get("status"), "status", {"pending", "paid"})
    paid_date = _optional_date(payload.get("paid_date"))

    if payment_status == "paid" and paid_date is None:
        paid_date = date.today().isoformat()
    if payment_status == "pending":
        paid_date = None

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select id
                from {TBL_FIXED_EXPENSES}
                where id = %s and is_active = true
                """,
                (fixed_expense_id,),
            )
            if cur.fetchone() is None:
                raise ValueError(f"No existe el gasto fijo {fixed_expense_id}.")

            cur.execute(
                f"""
                insert into {TBL_FIXED_EXPENSE_PAYMENTS}
                (fixed_expense_id, payment_month, status, paid_date, updated_at)
                values (%s, %s, %s, %s, current_timestamp)
                on conflict (fixed_expense_id, payment_month)
                do update
                   set status = excluded.status,
                       paid_date = excluded.paid_date,
                       updated_at = current_timestamp
                returning fixed_expense_id, payment_month, status, paid_date, updated_at
                """,
                (fixed_expense_id, payment_month, payment_status, paid_date),
            )
            row = cur.fetchone()
            conn.commit()

    return {
        "fixed_expense_id": row[0],
        "payment_month": row[1],
        "status": row[2],
        "paid_date": _date_to_iso(row[3]),
        "updated_at": _datetime_to_iso(row[4]),
    }


def _fetch_installment_plan_rows(conn, active_only: bool = False):
    where = "where p.status = 'active'" if active_only else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select
              p.id,
              p.purchase_name,
              a.name as account_name,
              p.monthly_payment,
              p.months_total,
              p.months_remaining,
              p.pending_total,
              p.purchase_date,
              p.status,
              p.created_at,
              p.updated_at,
              p.end_month,
              c.name as category_name,
              a.account_type,
              a.cutoff_day
            from {TBL_INSTALLMENT_PLANS} p
            join {TBL_ACCOUNTS} a on a.id = p.account_id
            left join {TBL_EXPENSE_CATEGORIES} c on c.id = p.category_id
            {where}
            order by p.status asc, p.created_at desc
            """
        )
        rows = cur.fetchall()
    return rows


def _serialize_installment_plan_row(
    row,
    cut_events_by_account: dict[str, list[date]],
    reference_month: str | None = None,
) -> dict:
    current_month = reference_month or date.today().strftime("%Y-%m")
    anchor_date = _installment_anchor_date(row[7], row[9])
    first_payment_month = _installment_first_payment_month(
        anchor_date,
        row[2],
        row[13],
        row[14],
        cut_events_by_account,
    )
    term_months = _installment_term_months(row[4], row[5], row[11], first_payment_month)
    end_month = _installment_end_month(first_payment_month, term_months, row[11])
    months_remaining = (
        _installment_months_remaining(first_payment_month, end_month, current_month)
        if row[8] == "active"
        else 0
    )
    return {
        "id": row[0],
        "purchase_name": row[1],
        "account_name": row[2],
        "monthly_payment": float(row[3]),
        "months_total": term_months or None,
        "months_remaining": months_remaining,
        "pending_total": float(row[3]) * months_remaining,
        "purchase_date": _date_to_iso(anchor_date),
        "status": row[8],
        "created_at": _datetime_to_iso(row[9]),
        "updated_at": _datetime_to_iso(row[10]),
        "end_month": end_month,
        "first_payment_month": first_payment_month,
        "category_name": row[12],
    }


def list_finance_installment_plans(active_only: bool = False) -> list[dict]:
    with _db_conn() as conn:
        if conn is None:
            return []
        rows = _fetch_installment_plan_rows(conn, active_only=active_only)
        cut_events_by_account = _load_cut_events_by_account(conn)

    return [
        _serialize_installment_plan_row(row, cut_events_by_account)
        for row in rows
    ]


def _resolve_installment_end_month(
    account_name: str,
    account_type: str | None,
    cutoff_day: int | None,
    purchase_date,
    created_at,
    term_months: int,
    cut_events_by_account: dict[str, list[date]],
) -> tuple[str | None, str]:
    anchor_date = _installment_anchor_date(purchase_date, created_at)
    first_payment_month = _installment_first_payment_month(
        anchor_date,
        account_name,
        account_type,
        cutoff_day,
        cut_events_by_account,
    )
    end_month = _installment_end_month(first_payment_month, term_months)
    return end_month, first_payment_month


def _compute_end_month(account_name: str, months_remaining: int) -> str:
    """Calcula end_month según el cutoff_day de la cuenta.

    Si hoy ya pasó el corte, el primer cargo cae el mes siguiente;
    si aún no ha cortado, cae este mismo mes.
    """
    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"select cutoff_day from {TBL_ACCOUNTS} where lower(name) = lower(%s) and is_active = true",
                (account_name,),
            )
            row = cur.fetchone()
    cutoff_day: int | None = int(row[0]) if row and row[0] else None

    today = date.today()
    if cutoff_day is not None and today.day > cutoff_day:
        # La tarjeta ya cortó → la compra cae en el siguiente estado de cuenta,
        # que se paga el mes después → primer pago = 2 meses adelante
        m1 = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
        first_payment = (m1 + timedelta(days=32)).replace(day=1)
    else:
        first_payment = today.replace(day=1)

    # end_month = primer_pago + (months_remaining - 1) meses
    month_offset = months_remaining - 1
    year = first_payment.year + (first_payment.month - 1 + month_offset) // 12
    month = (first_payment.month - 1 + month_offset) % 12 + 1
    return f"{year:04d}-{month:02d}"


def update_finance_installment_plan(plan_id: int, payload: dict) -> dict:
    fields: list[str] = []
    values: list = []

    if "purchase_name" in payload:
        v = (payload["purchase_name"] or "").strip()
        if not v:
            raise ValueError("purchase_name no puede estar vacío.")
        fields.append("purchase_name = %s")
        values.append(v)

    if "monthly_payment" in payload:
        fields.append("monthly_payment = %s")
        values.append(_require_positive_amount(payload["monthly_payment"], "monthly_payment"))

    if "months_remaining" in payload:
        fields.append("months_remaining = %s")
        values.append(_require_int(payload["months_remaining"], "months_remaining", minimum=0))

    if "months_total" in payload:
        mt = payload["months_total"]
        fields.append("months_total = %s")
        values.append(_require_int(mt, "months_total", minimum=1) if mt not in (None, "") else None)

    if "pending_total" in payload:
        fields.append("pending_total = %s")
        values.append(_require_positive_amount(payload["pending_total"], "pending_total"))

    if "end_month" in payload:
        fields.append("end_month = %s")
        values.append(_require_month(payload["end_month"]))

    if "status" in payload:
        s = (payload["status"] or "").strip().lower()
        if s not in {"active", "closed"}:
            raise ValueError("status invalido. Usa active o closed.")
        fields.append("status = %s")
        values.append(s)

    if "purchase_date" in payload:
        fields.append("purchase_date = %s")
        values.append(_optional_date(payload["purchase_date"]))

    if "category_name" in payload:
        cat = (payload["category_name"] or "").strip() or None
        category_id = _lookup_id_by_name(TBL_EXPENSE_CATEGORIES, cat) if cat else None
        fields.append("category_id = %s")
        values.append(category_id)

    if not fields:
        raise ValueError("No hay campos para actualizar.")

    fields.append("updated_at = now()")
    values.append(plan_id)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"update {TBL_INSTALLMENT_PLANS} set {', '.join(fields)} where id = %s returning id",
                values,
            )
            if cur.fetchone() is None:
                raise ValueError(f"Plan {plan_id} no encontrado.")
            conn.commit()

    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select p.id, p.purchase_name, a.name, p.monthly_payment, p.months_total,
                       p.months_remaining, p.pending_total, p.purchase_date, p.status,
                       p.created_at, p.updated_at, p.end_month, c.name
                from {TBL_INSTALLMENT_PLANS} p
                join {TBL_ACCOUNTS} a on a.id = p.account_id
                left join {TBL_EXPENSE_CATEGORIES} c on c.id = p.category_id
                where p.id = %s
                """,
                (plan_id,),
            )
            row = cur.fetchone()

    from datetime import date as _date
    today = _date.today()
    current_ym = today.year * 12 + today.month

    def _dyn_remaining(end_month):
        if not end_month:
            return 0
        try:
            ey, em = map(int, end_month.split("-"))
            return max(0, (ey * 12 + em) - current_ym + 1)
        except Exception:
            return 0

    return {
        "id": row[0],
        "purchase_name": row[1],
        "account_name": row[2],
        "monthly_payment": float(row[3]),
        "months_total": row[4],
        "months_remaining": _dyn_remaining(row[11]),
        "pending_total": float(row[3]) * _dyn_remaining(row[11]),
        "end_month": row[11],
        "purchase_date": _date_to_iso(row[7]),
        "status": row[8],
        "created_at": _datetime_to_iso(row[9]),
        "updated_at": _datetime_to_iso(row[10]),
        "category_name": row[12],
    }


def create_finance_installment_plan(payload: dict) -> dict:
    purchase_name = (payload.get("purchase_name") or "").strip()
    account_name = (payload.get("account_name") or "").strip()
    monthly_payment = _require_positive_amount(payload.get("monthly_payment"), "monthly_payment")
    months_remaining = _require_int(payload.get("months_remaining"), "months_remaining", minimum=0)
    pending_total = _require_positive_amount(payload.get("pending_total"), "pending_total")
    category_name = (payload.get("category_name") or "").strip() or None
    months_total = payload.get("months_total")
    purchase_date = _optional_date(payload.get("purchase_date"))
    status = (payload.get("status") or "active").strip().lower()

    if not purchase_name:
        raise ValueError("Falta purchase_name.")
    if not account_name:
        raise ValueError("Falta account_name.")
    if status not in {"active", "closed"}:
        raise ValueError("status invalido. Usa active o closed.")

    months_total_value = None
    if months_total not in (None, ""):
        months_total_value = _require_int(months_total, "months_total", minimum=1)

    account_id = _lookup_id_by_name(TBL_ACCOUNTS, account_name)
    category_id = _lookup_id_by_name(TBL_EXPENSE_CATEGORIES, category_name) if category_name else None

    # end_month se calcula automáticamente según el cutoff_day de la cuenta;
    # si el cliente lo manda explícitamente se respeta.
    raw_end_month = (payload.get("end_month") or "").strip()
    end_month = _require_month(raw_end_month) if raw_end_month else _compute_end_month(account_name, months_remaining)

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_INSTALLMENT_PLANS}
                (purchase_name, account_id, monthly_payment, months_total, months_remaining, pending_total, purchase_date, status, end_month, category_id)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning id, created_at, updated_at
                """,
                (
                    purchase_name,
                    account_id,
                    monthly_payment,
                    months_total_value,
                    months_remaining,
                    pending_total,
                    purchase_date,
                    status,
                    end_month,
                    category_id,
                ),
            )
            row = cur.fetchone()
            conn.commit()
    return {
        "id": row[0],
        "purchase_name": purchase_name,
        "account_name": account_name,
        "monthly_payment": monthly_payment,
        "months_total": months_total_value,
        "months_remaining": months_remaining,
        "pending_total": pending_total,
        "end_month": end_month,
        "purchase_date": purchase_date,
        "status": status,
        "created_at": _datetime_to_iso(row[1]),
        "updated_at": _datetime_to_iso(row[2]),
    }


def create_finance_installment_plan(payload: dict) -> dict:
    purchase_name = (payload.get("purchase_name") or "").strip()
    account_name = (payload.get("account_name") or "").strip()
    monthly_payment = _require_positive_amount(payload.get("monthly_payment"), "monthly_payment")
    months_remaining = _require_int(payload.get("months_remaining"), "months_remaining", minimum=0)
    category_name = (payload.get("category_name") or "").strip() or None
    months_total = payload.get("months_total")
    purchase_date = _optional_date(payload.get("purchase_date")) or date.today().isoformat()
    status = (payload.get("status") or "active").strip().lower()

    if not purchase_name:
        raise ValueError("Falta purchase_name.")
    if not account_name:
        raise ValueError("Falta account_name.")
    if status not in {"active", "closed"}:
        raise ValueError("status invalido. Usa active o closed.")

    months_total_value = _require_int(months_total, "months_total", minimum=1) if months_total not in (None, "") else months_remaining

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        account_id = _lookup_id_by_name(TBL_ACCOUNTS, account_name)
        category_id = _lookup_id_by_name(TBL_EXPENSE_CATEGORIES, category_name) if category_name else None

        with conn.cursor() as cur:
            cur.execute(
                f"""
                select a.name, a.account_type, a.cutoff_day
                from {TBL_ACCOUNTS} a
                where a.id = %s
                """,
                (account_id,),
            )
            account_row = cur.fetchone()

        cut_events_by_account = _load_cut_events_by_account(conn)
        end_month, _ = _resolve_installment_end_month(
            account_row[0],
            account_row[1],
            account_row[2],
            purchase_date,
            None,
            months_total_value,
            cut_events_by_account,
        )
        pending_total = monthly_payment * months_total_value

        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into {TBL_INSTALLMENT_PLANS}
                (purchase_name, account_id, monthly_payment, months_total, months_remaining, pending_total, purchase_date, status, end_month, category_id)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning id
                """,
                (
                    purchase_name,
                    account_id,
                    monthly_payment,
                    months_total_value,
                    months_total_value,
                    pending_total,
                    purchase_date,
                    status,
                    end_month,
                    category_id,
                ),
            )
            plan_id = cur.fetchone()[0]
            conn.commit()

        row = next(item for item in _fetch_installment_plan_rows(conn, active_only=False) if item[0] == plan_id)
        return _serialize_installment_plan_row(row, cut_events_by_account)


def update_finance_installment_plan(plan_id: int, payload: dict) -> dict:
    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")

        current_row = next((row for row in _fetch_installment_plan_rows(conn, active_only=False) if row[0] == plan_id), None)
        if current_row is None:
            raise ValueError(f"Plan {plan_id} no encontrado.")

        purchase_name = (payload.get("purchase_name") or current_row[1]).strip()
        if not purchase_name:
            raise ValueError("purchase_name no puede estar vacio.")

        monthly_payment = (
            _require_positive_amount(payload["monthly_payment"], "monthly_payment")
            if "monthly_payment" in payload
            else float(current_row[3])
        )
        months_total_value = current_row[4]
        if "months_total" in payload:
            mt = payload["months_total"]
            months_total_value = _require_int(mt, "months_total", minimum=1) if mt not in (None, "") else None

        months_remaining_value = (
            _require_int(payload["months_remaining"], "months_remaining", minimum=0)
            if "months_remaining" in payload
            else int(current_row[5] or 0)
        )
        term_months = months_total_value or months_remaining_value
        if term_months < 0:
            raise ValueError("months_remaining invalido.")

        purchase_date = (
            _optional_date(payload["purchase_date"])
            if "purchase_date" in payload
            else _date_to_iso(current_row[7])
        ) or _date_to_iso(_installment_anchor_date(current_row[7], current_row[9]))
        status = (payload.get("status") or current_row[8] or "active").strip().lower()
        if status not in {"active", "closed"}:
            raise ValueError("status invalido. Usa active o closed.")

        category_name = current_row[12]
        if "category_name" in payload:
            category_name = (payload["category_name"] or "").strip() or None
        category_id = _lookup_id_by_name(TBL_EXPENSE_CATEGORIES, category_name) if category_name else None

        cut_events_by_account = _load_cut_events_by_account(conn)
        end_month, _ = _resolve_installment_end_month(
            current_row[2],
            current_row[13],
            current_row[14],
            purchase_date,
            current_row[9],
            term_months,
            cut_events_by_account,
        )
        pending_total = monthly_payment * term_months

        with conn.cursor() as cur:
            cur.execute(
                f"""
                update {TBL_INSTALLMENT_PLANS}
                set purchase_name = %s,
                    monthly_payment = %s,
                    months_total = %s,
                    months_remaining = %s,
                    pending_total = %s,
                    purchase_date = %s,
                    status = %s,
                    end_month = %s,
                    category_id = %s,
                    updated_at = now()
                where id = %s
                returning id
                """,
                (
                    purchase_name,
                    monthly_payment,
                    term_months,
                    term_months,
                    pending_total,
                    purchase_date,
                    status,
                    end_month,
                    category_id,
                    plan_id,
                ),
            )
            cur.fetchone()
            conn.commit()

        row = next(item for item in _fetch_installment_plan_rows(conn, active_only=False) if item[0] == plan_id)
        return _serialize_installment_plan_row(row, cut_events_by_account)


def upsert_finance_account_settings(payload: dict) -> dict:
    account_name = (payload.get("account_name") or "").strip()
    account_type = (payload.get("account_type") or "").strip().lower()
    cutoff_day = _optional_day(payload.get("cutoff_day"), "cutoff_day")
    payment_due_day = _optional_day(payload.get("payment_due_day"), "payment_due_day")

    if not account_name:
        raise ValueError("Falta account_name.")
    if account_type not in {"credit", "store_card", "debit", "cash"}:
        raise ValueError("account_type invalido.")
    if account_type in {"debit", "cash"}:
        cutoff_day = None
        payment_due_day = None

    with _db_conn() as conn:
        if conn is None:
            raise ValueError("DATABASE_URL not set")
        with conn.cursor() as cur:
            cur.execute(
                f"""
                update {TBL_ACCOUNTS}
                set account_type = %s,
                    cutoff_day = %s,
                    payment_due_day = %s,
                    updated_at = now()
                where lower(name) = lower(%s)
                returning id, name, account_type, cutoff_day, payment_due_day, updated_at
                """,
                (account_type, cutoff_day, payment_due_day, account_name),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No existe la cuenta '{account_name}'.")
            conn.commit()
    return {
        "id": row[0],
        "name": row[1],
        "account_type": row[2],
        "account_type_label": _account_type_label(row[2]),
        "cutoff_day": row[3],
        "payment_due_day": row[4],
        "updated_at": _datetime_to_iso(row[5]),
    }


def get_finance_dashboard(month: str) -> dict:
    month_value = _require_month(month)
    month_start, next_month = _month_range(month_value)
    with _db_conn() as conn:
        if conn is None:
            return {
                "month": month_value,
                "totals": {
                    "income": 0.0,
                    "expense": 0.0,
                    "fixed_commitment": 0.0,
                    "installment_commitment": 0.0,
                    "balance": 0.0,
                },
                "msi": {"active_count": 0, "remaining_months": 0, "pending_total": 0.0},
                "expense_by_account": [],
                "expense_by_category": [],
                "recent_expenses": [],
            }
        monthly_expenses = _list_monthly_expense_entries(conn, month_value)
        cut_events_by_account = _load_cut_events_by_account(conn)
        active_installment_plans = [
            _serialize_installment_plan_row(row, cut_events_by_account, reference_month=month_value)
            for row in _fetch_installment_plan_rows(conn, active_only=True)
        ]

        with conn.cursor() as cur:
            cur.execute(
                f"""
                select coalesce(sum(amount), 0)
                from {TBL_INCOMES}
                where income_date >= %s and income_date < %s
                """,
                (month_start, next_month),
            )
            total_income = float(cur.fetchone()[0] or 0)

            cur.execute(
                f"select coalesce(sum(amount), 0) from {TBL_FIXED_INCOMES} where is_active = true and kind = 'cash'",
            )
            total_fixed_income = float(cur.fetchone()[0] or 0)
            total_income += total_fixed_income

    total_expense = sum(item["amount"] for item in monthly_expenses if not item.get("is_virtual"))
    fixed_commitment = sum(
        item["amount"]
        for item in monthly_expenses
        if item.get("entry_type") == "fixed" and item.get("payment_status") != "paid" and not item.get("is_virtual")
    )
    installment_commitment = sum(
        float(item["monthly_payment"])
        for item in active_installment_plans
        if _installment_active_in_month(item["first_payment_month"], item["end_month"], month_value)
    )
    msi_row = (
        len(active_installment_plans),
        sum(int(item["months_remaining"] or 0) for item in active_installment_plans),
        sum(float(item["pending_total"] or 0) for item in active_installment_plans),
    )

    expense_by_account_totals: dict[str, float] = {}
    msi_by_account_totals: dict[str, float] = {}
    expense_by_category_totals: dict[str, float] = {}
    virtual_accounts: set[str] = set()
    for item in monthly_expenses:
        expense_by_account_totals[item["account_name"]] = expense_by_account_totals.get(item["account_name"], 0.0) + float(item["amount"])
        expense_by_category_totals[item["category_name"]] = expense_by_category_totals.get(item["category_name"], 0.0) + float(item["amount"])
        if item.get("is_virtual"):
            virtual_accounts.add(item["account_name"])

    for plan in active_installment_plans:
        if not _installment_active_in_month(plan["first_payment_month"], plan["end_month"], month_value):
            continue
        msi_by_account_totals[plan["account_name"]] = msi_by_account_totals.get(plan["account_name"], 0.0) + float(plan["monthly_payment"] or 0)

    all_accounts = set(expense_by_account_totals) | set(msi_by_account_totals)
    expense_by_account = [
        {
            "account_name": name,
            "amount": expense_by_account_totals.get(name, 0.0) + msi_by_account_totals.get(name, 0.0),
            "expense_amount": expense_by_account_totals.get(name, 0.0),
            "msi_amount": msi_by_account_totals.get(name, 0.0),
            "is_virtual": name in virtual_accounts,
        }
        for name in sorted(all_accounts, key=lambda n: (-(expense_by_account_totals.get(n, 0.0) + msi_by_account_totals.get(n, 0.0)), n))
    ]
    expense_by_category = [
        {"category_name": name, "amount": amount}
        for name, amount in sorted(expense_by_category_totals.items(), key=lambda item: (-item[1], item[0]))
    ]
    recent_captured_expenses = sorted(
        (item for item in monthly_expenses if item.get("entry_type") == "expense"),
        key=lambda item: (
            item.get("created_at") or "",
            str(item.get("id") or ""),
        ),
        reverse=True,
    )
    recent_expenses = [
        {
            "id": item["id"],
            "date": item["date"],
            "created_at": item.get("created_at"),
            "amount": item["amount"],
            "description": item["description"],
            "account_name": item["account_name"],
            "category_name": item["category_name"],
            "entry_type": item.get("entry_type"),
            "payment_status": item.get("payment_status"),
        }
        for item in recent_captured_expenses[:10]
    ]

    balance = total_income - total_expense - installment_commitment
    return {
        "month": month_value,
        "totals": {
            "income": total_income,
            "expense": total_expense,
            "fixed_commitment": fixed_commitment,
            "installment_commitment": installment_commitment,
            "balance": balance,
        },
        "msi": {
            "active_count": int(msi_row[0] or 0),
            "remaining_months": int(msi_row[1] or 0),
            "pending_total": float(msi_row[2] or 0),
        },
        "expense_by_account": expense_by_account,
        "expense_by_category": expense_by_category,
        "recent_expenses": recent_expenses,
    }


def get_finance_yearly_summary(year: int) -> dict:
    months = [f"{year:04d}-{m:02d}" for m in range(1, 13)]
    with _db_conn() as conn:
        if conn is None:
            return {"year": year, "months": []}
        cut_events_by_account = _load_cut_events_by_account(conn)
        active_installment_plans = [
            _serialize_installment_plan_row(row, cut_events_by_account, reference_month=months[-1])
            for row in _fetch_installment_plan_rows(conn, active_only=True)
        ]
        with conn.cursor() as cur:
            # Fixed incomes (same amount every month)
            cur.execute(
                f"select coalesce(sum(amount), 0) from {TBL_FIXED_INCOMES} where is_active = true and kind = 'cash'",
            )
            monthly_fixed_income = float(cur.fetchone()[0] or 0)

            # Incomes per month
            cur.execute(
                f"""
                select to_char(income_date, 'YYYY-MM') as month,
                       coalesce(sum(amount), 0)
                from {TBL_INCOMES}
                where income_date >= %s and income_date < %s
                group by 1
                """,
                (f"{year}-01-01", f"{year + 1}-01-01"),
            )
            income_by_month = {row[0]: float(row[1]) for row in cur.fetchall()}

        expense_by_month: dict[str, float] = {}
        for month in months:
            monthly_expenses = _list_monthly_expense_entries(conn, month)
            expense_by_month[month] = sum(
                float(item["amount"]) for item in monthly_expenses if not item.get("is_virtual")
            )
        msi_by_month = {
            month: sum(
                float(plan["monthly_payment"] or 0)
                for plan in active_installment_plans
                if _installment_active_in_month(plan["first_payment_month"], plan["end_month"], month)
            )
            for month in months
        }

    result = []
    cumulative_balance = 0.0
    for month in months:
        income = income_by_month.get(month, 0.0) + monthly_fixed_income
        expense = expense_by_month.get(month, 0.0)
        msi = msi_by_month.get(month, 0.0)
        balance = income - expense - msi
        cumulative_balance += balance
        result.append({
            "month": month,
            "income": income,
            "expense": expense,
            "msi": msi,
            "balance": balance,
            "cumulative": cumulative_balance,
        })

    totals_income = sum(r["income"] for r in result)
    totals_expense = sum(r["expense"] for r in result)
    totals_msi = sum(r["msi"] for r in result)
    return {
        "year": year,
        "months": result,
        "totals": {
            "income": totals_income,
            "expense": totals_expense,
            "msi": totals_msi,
            "balance": totals_income - totals_expense - totals_msi,
        },
    }
