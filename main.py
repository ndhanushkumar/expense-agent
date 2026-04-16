
import hashlib
import hmac
import json
import logging
import os
import secrets
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

from agent import chat_agent
from agent.agent import run
from db.store import get_connection, initialize_db
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, status
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import id_token as google_id_token
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from pydantic import BaseModel

load_dotenv()

scheduler = BackgroundScheduler()
JOB_MAX_EMAILS = int(os.getenv("JOB_MAX_EMAILS", "50"))
JOB_SCHEDULE_HOUR_IST = int(os.getenv("JOB_SCHEDULE_HOUR_IST", "22"))
JOB_SCHEDULE_MINUTE_IST = int(os.getenv("JOB_SCHEDULE_MINUTE_IST", "0"))
JOB_TIMEZONE = ZoneInfo("Asia/Kolkata")
SESSION_COOKIE_NAME = "expense_session"
GOOGLE_OAUTH_STATE_COOKIE = "expense_google_oauth_state"
GOOGLE_OAUTH_CODE_VERIFIER_COOKIE = "expense_google_oauth_verifier"
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "14"))
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
GOOGLE_PASSWORD_PLACEHOLDER = "google-oauth"
GOOGLE_CREDENTIALS_FILE = Path(__file__).resolve().parent / "credentials.json"
GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/gmail.readonly",
]
logger = logging.getLogger("uvicorn.error")
manual_job_status_by_user: dict[int, dict[str, Any]] = {}
manual_job_lock = threading.Lock()


# ── helpers ────────────────────────────────────────────────────────────────

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_email(email: str) -> str:
    return email.strip().lower()


def is_valid_email(email: str) -> bool:
    if "@" not in email:
        return False
    local, _, domain = email.partition("@")
    return bool(local and domain and "." in domain)


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_session(conn, user_id: int) -> str:
    token = secrets.token_urlsafe(48)
    token_hash = hash_session_token(token)
    created_at = now_utc_iso()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)).isoformat()
    conn.execute(
        """
        INSERT INTO sessions (user_id, token_hash, expires_at, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (user_id, token_hash, expires_at, created_at),
    )
    return token


def get_user_from_request(request: Request) -> dict[str, Any] | None:
    raw_token = request.cookies.get(SESSION_COOKIE_NAME)
    if not raw_token:
        return None

    token_hash = hash_session_token(raw_token)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT u.id, u.email, s.expires_at
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token_hash = ?
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            return None

        expires_at = datetime.fromisoformat(row["expires_at"])
        if expires_at <= datetime.now(timezone.utc):
            conn.execute("DELETE FROM sessions WHERE token_hash = ?", (token_hash,))
            conn.commit()
            return None

        return {"id": row["id"], "email": row["email"]}


def require_auth(request: Request) -> dict[str, Any]:
    user = get_user_from_request(request)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    return user


def set_session_cookie(response: JSONResponse | RedirectResponse, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite="lax",
        max_age=SESSION_TTL_DAYS * 24 * 60 * 60,
        path="/",
    )


def get_google_redirect_uri(request: Request) -> str:
    configured = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
    if configured:
        return configured

    with GOOGLE_CREDENTIALS_FILE.open("r", encoding="utf-8") as f:
        parsed = json.load(f)
    client_config = parsed.get("web") or parsed.get("installed") or {}
    redirect_uris = client_config.get("redirect_uris") or []
    if redirect_uris:
        return redirect_uris[0]

    return str(request.url_for("google_auth_callback"))


def build_google_flow(
    request: Request,
    state: str | None = None,
    code_verifier: str | None = None,
) -> Flow:
    redirect_uri = get_google_redirect_uri(request)
    if redirect_uri.startswith("http://localhost") or redirect_uri.startswith("http://127.0.0.1"):
        os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")
    os.environ.setdefault("OAUTHLIB_RELAX_TOKEN_SCOPE", "1")

    flow = Flow.from_client_secrets_file(
        str(GOOGLE_CREDENTIALS_FILE),
        scopes=GOOGLE_SCOPES,
        state=state,
        code_verifier=code_verifier,
    )
    flow.redirect_uri = redirect_uri
    return flow


def get_google_client_id() -> str:
    env_client_id = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
    if env_client_id:
        return env_client_id

    with GOOGLE_CREDENTIALS_FILE.open("r", encoding="utf-8") as f:
        parsed = json.load(f)

    client_config = parsed.get("web") or parsed.get("installed") or {}
    if client_config.get("client_id"):
        return client_config["client_id"]

    raise ValueError("client_id not found in OAuth credentials file")


def extract_google_email_from_credentials(creds) -> str:
    if not creds.id_token:
        raise ValueError("missing_id_token")

    id_info = google_id_token.verify_oauth2_token(
        creds.id_token,
        GoogleAuthRequest(),
        get_google_client_id(),
    )
    email = normalize_email(id_info.get("email", ""))
    if not is_valid_email(email):
        raise ValueError("missing_email")
    return email


def upsert_gmail_token(conn, user_id: int, token_json: str) -> None:
    conn.execute(
        """
        INSERT INTO gmail_tokens (user_id, token_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          token_json = excluded.token_json,
          updated_at = excluded.updated_at
        """,
        (user_id, token_json, now_utc_iso()),
    )


def backfill_unowned_transactions_for_first_user(conn, user_id: int) -> None:
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if user_count == 1:
        conn.execute(
            "UPDATE transactions SET user_id = ? WHERE user_id IS NULL",
            (user_id,),
        )


def _default_manual_job_status() -> dict[str, Any]:
    return {
        "status": "idle",
        "queued_at": None,
        "started_at": None,
        "completed_at": None,
        "last_error": None,
    }


def get_manual_job_status(user_id: int) -> dict[str, Any]:
    with manual_job_lock:
        state = manual_job_status_by_user.get(user_id)
        if not state:
            return _default_manual_job_status()
        return dict(state)


def _set_manual_job_status(user_id: int, **updates: Any) -> dict[str, Any]:
    with manual_job_lock:
        state = manual_job_status_by_user.get(user_id, _default_manual_job_status())
        state.update(updates)
        manual_job_status_by_user[user_id] = state
        return dict(state)


def _run_manual_job_for_user(user_id: int) -> None:
    _set_manual_job_status(
        user_id,
        status="running",
        started_at=now_utc_iso(),
        completed_at=None,
        last_error=None,
    )
    try:
        run(max_emails=JOB_MAX_EMAILS, user_id=user_id)
        _set_manual_job_status(
            user_id,
            status="idle",
            completed_at=now_utc_iso(),
            last_error=None,
        )
    except Exception as exc:
        logger.exception("Manual job failed for user %s: %s", user_id, exc)
        _set_manual_job_status(
            user_id,
            status="idle",
            completed_at=now_utc_iso(),
            last_error=str(exc),
        )


# ── lifespan ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_db()
    trigger = CronTrigger(
        hour=JOB_SCHEDULE_HOUR_IST,
        minute=JOB_SCHEDULE_MINUTE_IST,
        timezone=JOB_TIMEZONE,
    )
    scheduler.add_job(
        run,
        trigger,
        id="daily_expense_job",
        replace_existing=True,
        kwargs={"max_emails": JOB_MAX_EMAILS}
    )
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)

app = FastAPI(lifespan=lifespan)


# ── pages ──────────────────────────────────────────────────────────────────

@app.get("/")
def read_root(request: Request):
    if get_user_from_request(request):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)


@app.get("/login")
def login_page(request: Request):
    if get_user_from_request(request):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return FileResponse("static/login.html")


@app.get("/register")
def register_page(request: Request):
    if get_user_from_request(request):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)


# ── google oauth ───────────────────────────────────────────────────────────

@app.get("/auth/google/start")
def google_auth_start(request: Request):
    flow = build_google_flow(request)
    code_verifier = secrets.token_urlsafe(64)
    flow.code_verifier = code_verifier
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        code_challenge_method="S256",
    )
    response = RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)
    response.set_cookie(key=GOOGLE_OAUTH_STATE_COOKIE, value=state, httponly=True, secure=SESSION_COOKIE_SECURE, samesite="lax", max_age=10 * 60, path="/")
    response.set_cookie(key=GOOGLE_OAUTH_CODE_VERIFIER_COOKIE, value=code_verifier, httponly=True, secure=SESSION_COOKIE_SECURE, samesite="lax", max_age=10 * 60, path="/")
    return response


@app.get("/auth/google/callback", name="google_auth_callback")
def google_auth_callback(request: Request):
    expected_state = request.cookies.get(GOOGLE_OAUTH_STATE_COOKIE)
    got_state = request.query_params.get("state")
    code_verifier = request.cookies.get(GOOGLE_OAUTH_CODE_VERIFIER_COOKIE)
    if not expected_state or not got_state or not hmac.compare_digest(expected_state, got_state):
        response = RedirectResponse(url="/login?error=invalid_oauth_state", status_code=status.HTTP_302_FOUND)
        response.delete_cookie(key=GOOGLE_OAUTH_STATE_COOKIE, path="/")
        response.delete_cookie(key=GOOGLE_OAUTH_CODE_VERIFIER_COOKIE, path="/")
        return response

    if not code_verifier:
        response = RedirectResponse(url="/login?error=missing_oauth_verifier", status_code=status.HTTP_302_FOUND)
        response.delete_cookie(key=GOOGLE_OAUTH_STATE_COOKIE, path="/")
        response.delete_cookie(key=GOOGLE_OAUTH_CODE_VERIFIER_COOKIE, path="/")
        return response

    error = request.query_params.get("error")
    if error:
        response = RedirectResponse(url=f"/login?error={error}", status_code=status.HTTP_302_FOUND)
        response.delete_cookie(key=GOOGLE_OAUTH_STATE_COOKIE, path="/")
        response.delete_cookie(key=GOOGLE_OAUTH_CODE_VERIFIER_COOKIE, path="/")
        return response

    try:
        code = request.query_params.get("code")
        if not code:
            raise ValueError("missing_auth_code")

        flow = build_google_flow(request, state=got_state, code_verifier=code_verifier)
        flow.fetch_token(code=code)
        creds = flow.credentials

        email = extract_google_email_from_credentials(creds)

        with get_connection() as conn:
            row = conn.execute("SELECT id, email FROM users WHERE email = ?", (email,)).fetchone()
            if row:
                user_id = row["id"]
            else:
                cursor = conn.execute(
                    "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                    (email, GOOGLE_PASSWORD_PLACEHOLDER, now_utc_iso()),
                )
                user_id = cursor.lastrowid

            upsert_gmail_token(conn, user_id, creds.to_json())
            backfill_unowned_transactions_for_first_user(conn, user_id)
            token = create_session(conn, user_id)
            conn.commit()
    except Exception as exc:
        logger.exception("Google OAuth callback failed: %s", exc)
        detail = str(exc).strip().replace("\n", " ")[:160] or "no_detail"
        safe_error = quote(f"google_auth_failed:{exc.__class__.__name__}:{detail}")
        response = RedirectResponse(url=f"/login?error={safe_error}", status_code=status.HTTP_302_FOUND)
        response.delete_cookie(key=GOOGLE_OAUTH_STATE_COOKIE, path="/")
        response.delete_cookie(key=GOOGLE_OAUTH_CODE_VERIFIER_COOKIE, path="/")
        return response

    response = RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    set_session_cookie(response, token)
    response.delete_cookie(key=GOOGLE_OAUTH_STATE_COOKIE, path="/")
    response.delete_cookie(key=GOOGLE_OAUTH_CODE_VERIFIER_COOKIE, path="/")
    return response


# ── auth endpoints ─────────────────────────────────────────────────────────

@app.post("/auth/logout")
def logout(request: Request):
    raw_token = request.cookies.get(SESSION_COOKIE_NAME)
    if raw_token:
        token_hash = hash_session_token(raw_token)
        with get_connection() as conn:
            conn.execute("DELETE FROM sessions WHERE token_hash = ?", (token_hash,))
            conn.commit()

    response = JSONResponse({"message": "Logged out"})
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")
    return response


@app.get("/auth/me")
def auth_me(current_user: dict[str, Any] = Depends(require_auth)):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT updated_at FROM gmail_tokens WHERE user_id = ?",
            (current_user["id"],),
        ).fetchone()

    gmail = {
        "connected": bool(row),
        "updated_at": row["updated_at"] if row else None,
    }
    return {"user": current_user, "gmail": gmail}


# ── job status ─────────────────────────────────────────────────────────────

@app.get("/job/status")
def job_status(current_user: dict[str, Any] = Depends(require_auth)):
    job = scheduler.get_job("daily_expense_job")
    with get_connection() as conn:
        connected_users = conn.execute("SELECT COUNT(*) FROM gmail_tokens").fetchone()[0]
    manual_status = get_manual_job_status(current_user["id"])
    return {
        "scheduler_running": scheduler.running,
        "job_id": job.id if job else None,
        "next_run_time": job.next_run_time.isoformat() if job and job.next_run_time else None,
        "schedule": {
            "hour": JOB_SCHEDULE_HOUR_IST,
            "minute": JOB_SCHEDULE_MINUTE_IST,
            "timezone": str(JOB_TIMEZONE),
        },
        "max_emails_per_run": JOB_MAX_EMAILS,
        "gmail_connected_users": connected_users,
        "manual_run": manual_status,
    }


@app.post("/job/run-now")
def run_job_now(
    background_tasks: BackgroundTasks,
    current_user: dict[str, Any] = Depends(require_auth),
):
    user_id = current_user["id"]

    with get_connection() as conn:
        connected = conn.execute(
            "SELECT 1 FROM gmail_tokens WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    if not connected:
        raise HTTPException(status_code=400, detail="Connect Gmail before running the job")

    state = get_manual_job_status(user_id)
    if state["status"] in {"queued", "running"}:
        return {
            "accepted": False,
            "status": state["status"],
            "message": "A manual job is already in progress",
            "queued_at": state.get("queued_at"),
            "started_at": state.get("started_at"),
        }

    queued_at = now_utc_iso()
    _set_manual_job_status(
        user_id,
        status="queued",
        queued_at=queued_at,
        started_at=None,
        completed_at=None,
        last_error=None,
    )
    background_tasks.add_task(_run_manual_job_for_user, user_id)

    return {
        "accepted": True,
        "status": "queued",
        "queued_at": queued_at,
    }


@app.get("/job/run-now/status")
def run_job_now_status(current_user: dict[str, Any] = Depends(require_auth)):
    return get_manual_job_status(current_user["id"])


# ── transactions ───────────────────────────────────────────────────────────

@app.get("/transactions")
def list_transactions(
    current_user: dict[str, Any] = Depends(require_auth),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, email_id, amount, type, merchant, upi_ref, date, account, category, payment_mode
            FROM transactions
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (current_user["id"], limit, offset),
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE user_id = ?",
            (current_user["id"],),
        ).fetchone()[0]

    return {
        "total": total,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "items": [dict(row) for row in rows],
    }


class TransactionUpdate(BaseModel):
    amount: Optional[float] = None
    merchant: Optional[str] = None
    type: Optional[str] = None
    category: Optional[str] = None
    date: Optional[str] = None
    payment_mode: Optional[str] = None


class TransactionCreate(BaseModel):
    amount: float
    type: str
    date: str
    merchant: Optional[str] = None
    upi_ref: Optional[str] = None
    account: Optional[str] = None
    category: Optional[str] = "other"
    email_id: Optional[str] = None
    payment_mode: Optional[str] = None


class NLQueryRequest(BaseModel):
    q: str
    thread_id: Optional[str] = None


class BudgetUpsert(BaseModel):
    category: str
    monthly_limit: float


def normalize_payment_mode(value: str | None, upi_ref: str | None = None) -> str:
    raw = (value or "").strip().lower()
    if raw in {"upi", "credit_card", "debit_card"}:
        return raw
    if not raw and upi_ref:
        return "upi"
    if not raw:
        return "debit_card"
    raise HTTPException(status_code=400, detail="payment_mode must be upi, credit_card, or debit_card")


def normalize_category(value: str | None) -> str:
    return (value or "").strip().lower() or "other"


@app.post("/transactions")
def create_transaction(
    body: TransactionCreate,
    current_user: dict[str, Any] = Depends(require_auth),
):
    tx_type = (body.type or "").strip().lower()
    if tx_type not in {"debited", "credited"}:
        raise HTTPException(status_code=400, detail="type must be debited or credited")
    if body.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be greater than 0")

    email_id = (body.email_id or "").strip()
    if not email_id:
        email_id = f"manual-{current_user['id']}-{secrets.token_hex(8)}"

    merchant = (body.merchant or "").strip() or None
    upi_ref = (body.upi_ref or "").strip() or None
    account = (body.account or "").strip() or None
    category = normalize_category(body.category)
    payment_mode = normalize_payment_mode(body.payment_mode, upi_ref)
    date = (body.date or "").strip()
    if not date:
        raise HTTPException(status_code=400, detail="date is required")

    with get_connection() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO transactions (user_id, email_id, amount, type, merchant, upi_ref, date, account, category, payment_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    current_user["id"],
                    email_id,
                    body.amount,
                    tx_type,
                    merchant,
                    upi_ref,
                    date,
                    account,
                    category,
                    payment_mode,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="transaction already exists") from exc

        tx_id = cursor.lastrowid
        row = conn.execute(
            """
            SELECT id, email_id, amount, type, merchant, upi_ref, date, account, category, payment_mode
            FROM transactions
            WHERE id = ? AND user_id = ?
            """,
            (tx_id, current_user["id"]),
        ).fetchone()
        conn.commit()

    return dict(row)

@app.post("/transactions/query")
def nl_query(body: NLQueryRequest, current_user: dict[str, Any] = Depends(require_auth)):
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="query parameter 'q' is required")

    try:
        result = chat_agent.invoke(q, current_user["id"],current_user["email"], thread_id=body.thread_id)
        if isinstance(result, dict):
            summary = str(result.get("summary") or "").strip()
            rows = result.get("rows") if isinstance(result.get("rows"), list) else []
            stats = result.get("stats") if isinstance(result.get("stats"), list) else []

            payload: dict[str, Any] = {
                "summary": summary or ("Done." if not rows else ""),
                "rows": rows,
                "stats": stats,
            }
            if result.get("sql") is not None:
                payload["sql"] = result.get("sql")
            return payload

        return {"summary": str(result), "rows": [], "stats": []}
    except Exception as exc:
        logger.exception("NL query failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to process query") from exc



@app.patch("/transactions/{tx_id}")
def update_transaction(
    tx_id: int,
    body: TransactionUpdate,
    current_user: dict[str, Any] = Depends(require_auth),
):
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    if "payment_mode" in fields:
        fields["payment_mode"] = normalize_payment_mode(fields["payment_mode"])
    if "category" in fields:
        fields["category"] = normalize_category(fields["category"])

    with get_connection() as conn:
        # verify ownership
        row = conn.execute(
            "SELECT id FROM transactions WHERE id = ? AND user_id = ?",
            (tx_id, current_user["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Transaction not found")

        set_clause = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(
            f"UPDATE transactions SET {set_clause} WHERE id = ? AND user_id = ?",
            (*fields.values(), tx_id, current_user["id"]),
        )
        conn.commit()

    return {"ok": True}


@app.delete("/transactions/{tx_id}")
def delete_transaction(
    tx_id: int,
    current_user: dict[str, Any] = Depends(require_auth),
):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM transactions WHERE id = ? AND user_id = ?",
            (tx_id, current_user["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Transaction not found")

        conn.execute(
            "DELETE FROM transactions WHERE id = ? AND user_id = ?",
            (tx_id, current_user["id"]),
        )
        conn.commit()

    return {"ok": True}


@app.get("/budgets")
def list_budgets(current_user: dict[str, Any] = Depends(require_auth)):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT category, monthly_limit, updated_at
            FROM budgets
            WHERE user_id = ?
            ORDER BY category ASC
            """,
            (current_user["id"],),
        ).fetchall()
    return {"count": len(rows), "items": [dict(row) for row in rows]}


@app.put("/budgets")
def upsert_budget(
    body: BudgetUpsert,
    current_user: dict[str, Any] = Depends(require_auth),
):
    monthly_limit = float(body.monthly_limit)
    if monthly_limit <= 0:
        raise HTTPException(status_code=400, detail="monthly_limit must be greater than 0")

    category = normalize_category(body.category)
    now = now_utc_iso()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO budgets (user_id, category, monthly_limit, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, category) DO UPDATE SET
              monthly_limit = excluded.monthly_limit,
              updated_at = excluded.updated_at
            """,
            (current_user["id"], category, monthly_limit, now, now),
        )
        row = conn.execute(
            """
            SELECT category, monthly_limit, updated_at
            FROM budgets
            WHERE user_id = ? AND category = ?
            """,
            (current_user["id"], category),
        ).fetchone()
        conn.commit()
    return dict(row)


# ── static + dashboard ─────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/dashboard")
def dashboard(request: Request):
    if not get_user_from_request(request):
        return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)
    return FileResponse("static/dashboard.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
