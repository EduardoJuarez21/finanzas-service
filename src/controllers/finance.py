from datetime import date

from flask import Blueprint, jsonify, request

from src.finance_services import (
    create_finance_account_cut_event,
    create_finance_expense,
    update_finance_expense,
    create_finance_fixed_expense,
    create_finance_fixed_income,
    create_finance_income,
    create_finance_installment_plan,
    deactivate_finance_fixed_expense,
    deactivate_finance_fixed_income,
    get_finance_dashboard,
    get_finance_yearly_summary,
    list_finance_catalogs,
    list_finance_account_cut_events,
    list_finance_expenses,
    list_finance_fixed_expenses,
    list_finance_fixed_incomes,
    list_finance_incomes,
    list_finance_installment_plans,
    update_finance_income,
    upsert_finance_account_settings,
    upsert_finance_fixed_expense_payment,
)

finance_bp = Blueprint("finance", __name__)


def _json_payload() -> dict:
    return request.get_json(silent=True) or {}


@finance_bp.get("/finance/catalogs")
def get_finance_catalogs():
    return jsonify({"status": "ok", **list_finance_catalogs()}), 200


@finance_bp.get("/finance/dashboard")
def get_dashboard():
    month = (request.args.get("month") or date.today().strftime("%Y-%m")).strip()
    try:
        payload = get_finance_dashboard(month)
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", **payload}), 200


@finance_bp.route("/finance/expenses", methods=["GET", "POST"])
def finance_expenses():
    if request.method == "GET":
        month = (request.args.get("month") or "").strip() or None
        limit = request.args.get("limit", 100)
        try:
            items = list_finance_expenses(month=month, limit=limit)
        except ValueError as exc:
            return jsonify({"status": "error", "error": str(exc)}), 400
        return jsonify({"status": "ok" if items else "empty", "items": items}), 200

    try:
        item = create_finance_expense(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 201


@finance_bp.route("/finance/incomes", methods=["GET", "POST"])
def finance_incomes():
    if request.method == "GET":
        month = (request.args.get("month") or "").strip() or None
        limit = request.args.get("limit", 100)
        try:
            items = list_finance_incomes(month=month, limit=limit)
        except ValueError as exc:
            return jsonify({"status": "error", "error": str(exc)}), 400
        return jsonify({"status": "ok" if items else "empty", "items": items}), 200

    try:
        item = create_finance_income(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 201


@finance_bp.route("/finance/fixed-expenses", methods=["GET", "POST"])
def finance_fixed_expenses():
    if request.method == "GET":
        month = (request.args.get("month") or "").strip() or None
        try:
            items = list_finance_fixed_expenses(month=month)
        except ValueError as exc:
            return jsonify({"status": "error", "error": str(exc)}), 400
        return jsonify({"status": "ok" if items else "empty", "items": items}), 200

    try:
        item = create_finance_fixed_expense(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 201


@finance_bp.route("/finance/account-cuts", methods=["GET", "POST"])
def finance_account_cuts():
    if request.method == "GET":
        account_name = (request.args.get("account_name") or "").strip() or None
        limit = request.args.get("limit", 50)
        try:
            items = list_finance_account_cut_events(account_name=account_name, limit=limit)
        except ValueError as exc:
            return jsonify({"status": "error", "error": str(exc)}), 400
        return jsonify({"status": "ok" if items else "empty", "items": items}), 200

    try:
        item = create_finance_account_cut_event(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 201


@finance_bp.post("/finance/fixed-expenses/status")
def finance_fixed_expense_status():
    try:
        item = upsert_finance_fixed_expense_payment(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 200


@finance_bp.post("/finance/fixed-expenses/delete")
def finance_fixed_expense_delete():
    try:
        item = deactivate_finance_fixed_expense(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 200


@finance_bp.route("/finance/installment-plans", methods=["GET", "POST"])
def finance_installment_plans():
    if request.method == "GET":
        active_only = (request.args.get("active_only") or "").strip().lower() in {"1", "true", "yes"}
        items = list_finance_installment_plans(active_only=active_only)
        return jsonify({"status": "ok" if items else "empty", "items": items}), 200

    try:
        item = create_finance_installment_plan(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 201


@finance_bp.post("/finance/expenses/<int:expense_id>/update")
def finance_expense_update(expense_id: int):
    try:
        item = update_finance_expense(expense_id, _json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 200


@finance_bp.route("/finance/fixed-incomes", methods=["GET", "POST"])
def finance_fixed_incomes():
    if request.method == "GET":
        active_only = (request.args.get("active_only") or "").strip().lower() in {"1", "true", "yes"}
        items = list_finance_fixed_incomes(active_only=active_only)
        return jsonify({"status": "ok" if items else "empty", "items": items}), 200

    try:
        item = create_finance_fixed_income(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 201


@finance_bp.post("/finance/fixed-incomes/delete")
def finance_fixed_income_delete():
    try:
        item = deactivate_finance_fixed_income(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 200


@finance_bp.post("/finance/incomes/<int:income_id>/update")
def finance_income_update(income_id: int):
    try:
        item = update_finance_income(income_id, _json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 200


@finance_bp.get("/finance/yearly-summary")
def finance_yearly_summary():
    try:
        year = int(request.args.get("year") or date.today().year)
    except ValueError:
        return jsonify({"status": "error", "error": "year invalido"}), 400
    payload = get_finance_yearly_summary(year)
    return jsonify({"status": "ok", **payload}), 200


@finance_bp.post("/finance/accounts/settings")
def finance_account_settings():
    try:
        item = upsert_finance_account_settings(_json_payload())
    except ValueError as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    return jsonify({"status": "ok", "item": item}), 200
