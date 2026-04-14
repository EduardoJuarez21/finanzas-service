import hmac
import os

from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

from src.controllers import finance_bp

load_dotenv()

app = Flask(__name__)
API_AUTH_HEADER = (os.getenv("API_AUTH_HEADER", "X-API-Token") or "X-API-Token").strip()
API_AUTH_TOKEN = (os.getenv("API_AUTH_TOKEN", "") or "").strip()

CORS(
    app,
    resources={r"/*": {"origins": os.getenv("CORS_ORIGINS", "*").split(",")}},
    supports_credentials=True,
)

app.register_blueprint(finance_bp)


def _extract_token() -> str:
    token = (request.headers.get(API_AUTH_HEADER) or "").strip()
    if token:
        return token
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


@app.before_request
def _require_api_token():
    if request.method == "OPTIONS":
        return None

    if not API_AUTH_TOKEN:
        return jsonify({"status": "error", "error": "API auth token not configured"}), 500

    provided = _extract_token()
    if not provided or not hmac.compare_digest(provided, API_AUTH_TOKEN):
        return jsonify({"status": "error", "error": "unauthorized"}), 401
    return None


@app.get("/")
def root():
    return {"ok": True, "service": "finance-api"}


@app.after_request
def _add_cors_headers(resp):
    origin = os.getenv("CORS_ALLOW_ORIGIN", "*")
    resp.headers.setdefault("Access-Control-Allow-Origin", origin)
    allow_headers = f"Content-Type, Authorization, {API_AUTH_HEADER}"
    resp.headers.setdefault("Access-Control-Allow-Headers", allow_headers)
    resp.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    return resp


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
