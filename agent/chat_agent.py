
import json
import os
import re
import sys

from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain.messages import HumanMessage
from langchain_ollama import ChatOllama
from langchain.tools import tool

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from db.store import get_connection
load_dotenv()



ollama_llm = ChatOllama(
    model="gpt-oss:120b",
    host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
    api_key=os.getenv("OLLAMA_API_KEY")
)
@tool
def run_query(query: str) -> list[dict[str, Any]]:
    """Run a read-only SQL query against transactions and return rows as objects."""
    normalized = " ".join((query or "").strip().split())
    if not normalized.lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed")

    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]



chat_agent = create_agent(
    model=ollama_llm,
    tools=[run_query],
    system_prompt=(
        "You are a helpful assistant for analyzing bank transactions. "
        "table has columns: id, user_id, amount, type, merchant, upi_ref, payment_mode, date, account, email_id, category. "
        "Use run_query for SQL access to the transactions table. "
        "Return ONLY JSON matching schema: {summary:string, stats:[{label,value}], rows:[object], sql?:string}. "
        "Do not use markdown or code fences."
    ),
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
    for row in rows:
        if isinstance(row, dict):
            normalized_rows.append(row)

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


def invoke(input: str, user_id: int) -> dict[str, Any]:
    response = chat_agent.invoke({
        "messages": [
            HumanMessage(
                content=(
                    f"User {user_id} asked: {input}\n\n"
                    "Important: only use data where user_id matches this user. "
                    f"Always filter SQL with user_id = {user_id}."
                )
            )
        ]
    })

    if isinstance(response, dict):
        structured = response.get("structured_response")
        if isinstance(structured, dict):
            return _coerce_dashboard_payload(structured)

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
