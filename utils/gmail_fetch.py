import base64
import os
from utils.gmail_auth import get_gmail_service

def fetch_hdfc_emails(max_results=10):
    service = get_gmail_service()
    query = os.getenv(
        "GMAIL_SEARCH_QUERY",
        "from:alerts@hdfcbank.bank.in newer_than:1d"
    )

    result = service.users().messages().list(
        userId="me",
        q=query,
        maxResults=max_results
    ).execute()

    messages = result.get("messages", [])

    emails = []
    for msg in messages:
        full_msg = service.users().messages().get(
            userId="me",
            id=msg["id"],
            format="full"
        ).execute()

        body = extract_body(full_msg)
        if body:
            emails.append({
                "id": msg["id"],
                "body": body
            })

    return emails


def extract_body(msg):
    payload = msg.get("payload", {})

    # single part
    if "body" in payload and payload["body"].get("data"):
        return _decode(payload["body"]["data"])

    # multipart — try plain first, fallback to html
    parts = payload.get("parts", [])
    
    html_fallback = None
    for part in parts:
        mime = part.get("mimeType", "")
        data = part.get("body", {}).get("data")
        if not data:
            continue
        if mime == "text/plain":
            return _decode(data)
        if mime == "text/html":
            html_fallback = _decode(data)

    return html_fallback


def _decode(data):
    return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")