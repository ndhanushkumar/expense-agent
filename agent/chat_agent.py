
import os
import sys

from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain.messages import HumanMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import BaseModel, ConfigDict, Field, model_validator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db.store import get_connection
load_dotenv()

_BLOCKED_KEYWORDS = {"drop", "delete", "update", "insert", "alter", "attach", "detach", "pragma", "vacuum"}
_STRIP_COLS = {"id", "user_id", "email_id"}


class StatItem(BaseModel):
    label: str = Field(min_length=1)
    value: str = Field(min_length=1)


class RowItem(BaseModel):
    # Common transaction columns. Extra fields are allowed for aggregate/grouped rows.
    amount: Optional[float] = None
    type: Optional[str] = None
    merchant: Optional[str] = None
    upi_ref: Optional[str] = None
    payment_mode: Optional[str] = None
    date: Optional[str] = None
    account: Optional[str] = None
    category: Optional[str] = None

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def _validate_not_empty(self) -> "RowItem":
        if not self.model_dump(exclude_none=True):
            raise ValueError("Each row must include at least one field")
        return self


class DashboardPayload(BaseModel):
    """Structured payload expected by the dashboard chat UI."""
    summary: str = ""
    stats: list[StatItem] = Field(default_factory=list)
    rows: list[RowItem] = Field(default_factory=list)
    sql: Optional[str] = None


@tool
def run_query(sql: str, user_id: int) -> list[dict[str, Any]]:
    """Execute a SELECT SQL query on the transactions table and return rows."""
    normalized = " ".join((sql or "").strip().split()).lower()

    if not normalized.startswith("select"):
        raise ValueError("Only SELECT queries are allowed")

    for kw in _BLOCKED_KEYWORDS:
        if kw in normalized:
            raise ValueError(f"Blocked keyword in query: {kw}")

    # strip multiple statements
    safe_sql = sql.split(";")[0].strip()
    print(f"Executing SQL for user {user_id}: {safe_sql}")
    with get_connection() as conn:
        rows = conn.execute(safe_sql).fetchall()
        return [{k: v for k, v in dict(row).items() if k not in _STRIP_COLS} for row in rows]
llm = ChatGoogleGenerativeAI(
    model="gemini-3.1-flash-lite-preview",
    temperature=0,
    api_key=os.getenv("GEMINI_API_KEY")
)

_SYSTEM_PROMPT = (
    "You are a personal expense assistant. "
    "Always call run_query to fetch real data — never guess or fabricate numbers. "
    "The transactions table has columns: id, user_id, amount, type, merchant, upi_ref, payment_mode, date, account, category. "
    "date is stored as DD-MM-YY string (e.g. 05-04-26). "
    "type is either 'debited' or 'credited'. "
    "Refer to the user as 'you', never mention user_id or internal IDs. "
    "Use case-insensitive queries with LOWER() or UPPER() for merchants and categories. "
    "After calling run_query, return ONLY valid JSON: "
    '{"summary": "...", "stats": [{"label": "...", "value": "..."}], "rows": [...]}. '
    "Map rows exactly from run_query output. Do not alter keys, drop rows, or emit empty row objects. "
    "For transaction lists, rows should include fields such as amount, type, merchant, upi_ref, payment_mode, date, account, category. "
    "If run_query returns N rows, rows must contain exactly N row objects. "
    "Do not emit empty stats objects. Every stats item must include non-empty label and value. "
    "No markdown, no explanations. Just the JSON object. "
    "Valid categories: food, entertainment, utilities, shopping, other, persons, transport. "
)


def _coerce_dashboard_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, BaseModel):
        payload = payload.model_dump(exclude_none=True)

    if not isinstance(payload, dict):
        raise ValueError("Structured payload must be a dict")

    summary = str(payload.get("summary", "")).strip()

    rows_raw = payload.get("rows")
    if not isinstance(rows_raw, list):
        raise ValueError("rows must be a list")

    normalized_rows: list[dict[str, Any]] = []
    for row in rows_raw:
        if isinstance(row, BaseModel):
            row = row.model_dump(exclude_none=True)
        if not isinstance(row, dict) or not row:
            raise ValueError("rows must contain non-empty objects")
        cleaned = {k: v for k, v in row.items() if k not in _STRIP_COLS}
        if not cleaned:
            raise ValueError("rows cannot contain only stripped columns")
        normalized_rows.append(cleaned)

    stats_raw = payload.get("stats")
    if not isinstance(stats_raw, list):
        raise ValueError("stats must be a list")

    normalized_stats: list[dict[str, str]] = []
    for item in stats_raw:
        if isinstance(item, BaseModel):
            item = item.model_dump(exclude_none=True)
        if not isinstance(item, dict):
            raise ValueError("stats must contain objects")
        label = str(item.get("label", "")).strip()
        value = str(item.get("value", "")).strip()
        if not label or not value:
            raise ValueError("stats items require non-empty label and value")
        normalized_stats.append({"label": label, "value": value})

    result: dict[str, Any] = {
        "summary": summary,
        "stats": normalized_stats,
        "rows": normalized_rows,
    }
    if payload.get("sql") is not None:
        result["sql"] = str(payload["sql"])
    return result

agent = create_agent(
        model=llm,
        tools=[run_query],
        system_prompt=_SYSTEM_PROMPT,
        checkpointer=InMemorySaver(),
        response_format=DashboardPayload
    )

def invoke(input: str, user_id: int, email: str, thread_id: Optional[str] = None) -> dict[str, Any]:
    response = agent.invoke({
        "messages": [
            HumanMessage(content=f"Question: {input}")
        ]
},{"configurable": {"thread_id": thread_id}})

    if not isinstance(response, dict):
        raise ValueError("Agent returned unexpected response type")

    structured = response.get("structured_response")
    if structured is None:
        raise ValueError("Agent did not return structured_response")

    return _coerce_dashboard_payload(structured)
