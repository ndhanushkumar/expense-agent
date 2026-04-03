from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path
import pickle

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