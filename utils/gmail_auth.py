import json
from datetime import datetime, timezone
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path
import pickle

from db.store import get_connection

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOKEN_FILE = PROJECT_ROOT / "token.pickle"
CREDENTIALS_FILE = PROJECT_ROOT / "credentials.json"

def get_gmail_service():
    creds = None
    if TOKEN_FILE.exists():
        with TOKEN_FILE.open("rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        with TOKEN_FILE.open("wb") as f:
            pickle.dump(creds, f)

    from googleapiclient.discovery import build
    return build("gmail", "v1", credentials=creds)


def get_gmail_service_for_user(user_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT token_json FROM gmail_tokens WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            raise ValueError("No Gmail token found for this user")

        creds_info = json.loads(row["token_json"])
        creds = Credentials.from_authorized_user_info(creds_info, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            conn.execute(
                "UPDATE gmail_tokens SET token_json = ?, updated_at = ? WHERE user_id = ?",
                (creds.to_json(), datetime.now(timezone.utc).isoformat(), user_id),
            )
            conn.commit()

    from googleapiclient.discovery import build
    return build("gmail", "v1", credentials=creds)