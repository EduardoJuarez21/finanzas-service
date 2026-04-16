import re
from datetime import date, timedelta

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


def _load_cut_events_by_account(conn, until_date: str | None = None) -> dict[str, list[date]]:
    where = ""
    params: list = []
    if until_date:
        where = "where ace.cut_date < %s"
        params.append(until_date)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            select a.name, ace.cut_date
            from {TBL_ACCOUNT_CUT_EVENTS} ace
            join {TBL_ACCOUNTS} a on a.id = ace.account_id
            {where}
            order by a.name asc, ace.cut_date asc, ace.id asc
            """,
            tuple(params),
        )
        rows = cur.fetchall()

    grouped: dict[str, list[date]] = {}
    for account_name, cut_date in rows:
        grouped.setdefault(account_name, []).append(cut_date)
    return grouped


def _report_month_for_expense(expense_date, account_name: str, account_type: str | None, cut_events_by_account: dict[str, list[date]]) -> str:
    expense_month = expense_date.strftime("%Y-%m")
    if not _is_card_account_type(account_type):
        return expense_month

    cut_dates = cut_events_by_account.get(account_name, [])
    latest_cut = None
    for cut_date in cut_dates:
        if cut_date <= expense_date:
            latest_cut = cut_date
        else:
            break

    if latest_cut and latest_cut.strftime("%Y-%m") == expense_month and expense_date > latest_cut:
        return _shift_month(expense_month, 1)
    return expense_month


def _list_regular_expense_entries(conn, month: str) -> list[dict]:
    month_start, next_month = _month_range(month)
    previous_month = _shift_month(month, -1)
    previous_month_start, _ = _month_range(previous_month)
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
            (previous_month_start, next_month),
        )
        rows = cur.fetchall()

    items = []
    for row in rows:
        report_month = _report_month_for_expense(row[1], row[4], row[5], cut_events_by_account)
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


def list_finance_installment_plans(active_only: bool = False) -> list[dict]:
    where = "where p.status = 'active'" if active_only else ""
    with _db_conn() as conn:
        if conn is None:
            return []
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
                  c.name as category_name
                from {TBL_INSTALLMENT_PLANS} p
                join {TBL_ACCOUNTS} a on a.id = p.account_id
                left join {TBL_EXPENSE_CATEGORIES} c on c.id = p.category_id
                {where}
                order by p.status asc, p.created_at desc
                """
            )
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "purchase_name": row[1],
            "account_name": row[2],
            "monthly_payment": float(row[3]),
            "months_total": row[4],
            "months_remaining": row[5],
            "pending_total": float(row[6]),
            "purchase_date": _date_to_iso(row[7]),
            "status": row[8],
            "created_at": _datetime_to_iso(row[9]),
            "updated_at": _datetime_to_iso(row[10]),
            "end_month": row[11],
            "category_name": row[12],
        }
        for row in rows
    ]


def create_finance_installment_plan(payload: dict) -> dict:
    purchase_name = (payload.get("purchase_name") or "").strip()
    account_name = (payload.get("account_name") or "").strip()
    monthly_payment = _require_positive_amount(payload.get("monthly_payment"), "monthly_payment")
    months_remaining = _require_int(payload.get("months_remaining"), "months_remaining", minimum=0)
    pending_total = _require_positive_amount(payload.get("pending_total"), "pending_total")
    end_month = _require_month(payload.get("end_month") or "")
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

            cur.execute(
                f"""
                select coalesce(sum(monthly_payment), 0)
                from {TBL_INSTALLMENT_PLANS}
                where status = 'active'
                  and months_remaining > 0
                  and end_month >= %s
                """,
                (month_value,),
            )
            installment_commitment = float(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                select count(*), coalesce(sum(months_remaining), 0), coalesce(sum(pending_total), 0)
                from {TBL_INSTALLMENT_PLANS}
                where status = 'active'
                  and months_remaining > 0
                  and end_month >= %s
                """,
                (month_value,),
            )
            msi_row = cur.fetchone()

    total_expense = sum(item["amount"] for item in monthly_expenses if not item.get("is_virtual"))
    fixed_commitment = sum(
        item["amount"]
        for item in monthly_expenses
        if item.get("entry_type") == "fixed" and item.get("payment_status") != "paid" and not item.get("is_virtual")
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

    with conn.cursor() as cur:
        cur.execute(
            f"""
            select a.name, coalesce(sum(p.monthly_payment), 0) as monthly_total
            from {TBL_INSTALLMENT_PLANS} p
            join {TBL_ACCOUNTS} a on a.id = p.account_id
            where p.status = 'active'
              and p.months_remaining > 0
              and p.end_month >= %s
              and to_char(date_trunc('month', p.created_at), 'YYYY-MM') <= %s
            group by a.name
            """,
            (month_value, month_value),
        )
        installment_by_account_rows = cur.fetchall()

    for account_name, monthly_total in installment_by_account_rows:
        msi_by_account_totals[account_name] = float(monthly_total or 0)

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
    recent_expenses = [
        {
            "id": item["id"],
            "date": item["date"],
            "amount": item["amount"],
            "description": item["description"],
            "account_name": item["account_name"],
            "category_name": item["category_name"],
            "entry_type": item.get("entry_type"),
            "payment_status": item.get("payment_status"),
        }
        for item in monthly_expenses[:10]
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

            # MSI commitment per month
            cur.execute(
                f"""
                select end_month_label, coalesce(sum(monthly_payment), 0)
                from (
                    select monthly_payment,
                           generate_series(
                               date_trunc('month', created_at)::date,
                               (end_month || '-01')::date,
                               '1 month'
                           )::date as month_date,
                           end_month as end_month_label
                    from {TBL_INSTALLMENT_PLANS}
                    where status = 'active' and months_remaining > 0
                ) sub
                where to_char(month_date, 'YYYY') = %s::text
                group by to_char(month_date, 'YYYY-MM'), end_month_label
                """,
                (year,),
            )
            # Re-query simpler: sum MSI per calendar month of that year
            cur.execute(
                f"""
                select m.month_label, coalesce(sum(p.monthly_payment), 0)
                from (
                    select to_char(generate_series(
                        make_date(%s, 1, 1),
                        make_date(%s, 12, 1),
                        '1 month'
                    ), 'YYYY-MM') as month_label
                ) m
                left join {TBL_INSTALLMENT_PLANS} p
                  on p.status = 'active'
                 and p.months_remaining > 0
                 and p.end_month >= m.month_label
                 and to_char(date_trunc('month', p.created_at), 'YYYY-MM') <= m.month_label
                group by m.month_label
                order by m.month_label
                """,
                (year, year),
            )
            msi_by_month = {row[0]: float(row[1]) for row in cur.fetchall()}

        expense_by_month: dict[str, float] = {}
        for month in months:
            monthly_expenses = _list_monthly_expense_entries(conn, month)
            expense_by_month[month] = sum(
                float(item["amount"]) for item in monthly_expenses if not item.get("is_virtual")
            )

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
