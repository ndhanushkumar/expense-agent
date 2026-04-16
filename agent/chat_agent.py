
import json
import os
import re
import sys

from functools import partial
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama
from langchain.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db.store import get_connection
load_dotenv()

_BLOCKED_KEYWORDS = {"drop", "delete", "update", "insert", "alter", "attach", "detach", "pragma", "vacuum"}
_STRIP_COLS = {"id", "user_id", "email_id"}



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


ollama_llm = ChatOllama(
    model="gpt-oss:120b",
    host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
    api_key=os.getenv("OLLAMA_API_KEY")
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
    "No markdown, no explanations. Just the JSON object. "
    "Valid categories: food, entertainment, utilities, shopping, other, persons, transport. "
)


def _extract_json_from_text(text: str) -> dict[str, Any] | None:
    body = (text or "").strip()
    if not body:
        return None

    try:
        parsed = json.loads(body)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    fence_match = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", body)
    if fence_match:
        try:
            parsed = json.loads(fence_match.group(1))
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _coerce_dashboard_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        text = str(payload).strip()
        return {"summary": text or "Done.", "stats": [], "rows": []}

    summary_raw = payload.get("summary", "")
    summary = str(summary_raw).strip() if summary_raw is not None else ""

    rows_raw = payload.get("rows", [])
    rows = rows_raw if isinstance(rows_raw, list) else []
    normalized_rows: list[dict[str, Any]] = []
    _strip_cols = {"id", "user_id", "email_id"}
    for row in rows:
        if isinstance(row, dict):
            normalized_rows.append({k: v for k, v in row.items() if k not in _strip_cols})

    stats_raw = payload.get("stats", [])
    normalized_stats: list[dict[str, str]] = []
    if isinstance(stats_raw, dict):
        for key, value in stats_raw.items():
            normalized_stats.append({"label": str(key), "value": str(value)})
    elif isinstance(stats_raw, list):
        for item in stats_raw:
            if isinstance(item, dict):
                label = str(item.get("label", "")).strip()
                value = str(item.get("value", "")).strip()
                if label or value:
                    normalized_stats.append({"label": label, "value": value})

    result: dict[str, Any] = {
        "summary": summary or ("Done." if not normalized_rows else ""),
        "stats": normalized_stats,
        "rows": normalized_rows,
    }
    if payload.get("sql") is not None:
        result["sql"] = str(payload["sql"])
    return result

agent = create_agent(
        model=ollama_llm,
        tools=[run_query],
        system_prompt=_SYSTEM_PROMPT,
        checkpointer=InMemorySaver(),
    )

def invoke(input: str, user_id: int, email: str, thread_id: Optional[str] = None) -> dict[str, Any]:
    response = agent.invoke({
        "messages": [
            HumanMessage(content=f"Question: {input}")
        ]
},{"configurable": {"thread_id": thread_id}})
    if isinstance(response, dict):
        messages = response.get("messages") or []
        if messages:
            content = messages[-1].content
            if isinstance(content, str):
                parsed = _extract_json_from_text(content)
                if parsed:
                    return _coerce_dashboard_payload(parsed)
                return _coerce_dashboard_payload({"summary": content, "rows": [], "stats": []})
            if isinstance(content, list):
                text_parts = [part.get("text", "") for part in content if isinstance(part, dict)]
                merged = "\n".join(p for p in text_parts if p).strip()
                parsed = _extract_json_from_text(merged)
                if parsed:
                    return _coerce_dashboard_payload(parsed)
                return _coerce_dashboard_payload({"summary": merged, "rows": [], "stats": []})

    return _coerce_dashboard_payload(response)
