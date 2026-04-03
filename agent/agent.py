import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_ollama import ChatOllama

# Ensure sibling top-level packages (models/db/utils) are importable
# even when this module is launched from inside the agent directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.transaction import Transaction
from db.store import get_connection, initialize_db
from utils.gmail_fetch import fetch_hdfc_emails

load_dotenv()

llm = ChatGoogleGenerativeAI(
    model="gemini-3.1-flash-lite-preview",
    temperature=0,
    api_key=os.getenv("GEMINI_API_KEY")
)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a bank transaction email parser for HDFC Bank India.

Extract transaction details from the HTML email and return ONLY a valid JSON object.

Rules:
- amount: extract the rupee amount as a float (e.g. 2300.00)
- type: "credited" if money came in, "debited" if money went out
- merchant: the VPA name or person name (e.g. "Mr VETRIVEL A" or "8111021439@axl")
- upi_ref: the UPI transaction reference number (numeric string)
- date: as-is from the email (e.g. "03-04-26")
- account: last 4 digits of account number (e.g. "0540")
- email_id: use exactly what is passed in

Return this exact JSON, nothing else:
{{
  "email_id": "{email_id}",
  "amount": 2300.00,
  "type": "credited",
  "merchant": "Mr VETRIVEL A",
  "upi_ref": "118962893190",
  "date": "03-04-26",
  "account": "0540"
}}

No markdown. No explanation. No code block. Just the JSON."""),
    ("human", "email_id: {email_id}\n\nEmail HTML:\n{body}")
])

chain = prompt | llm | JsonOutputParser()

def save_transaction(data: dict):
    with get_connection() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO transactions
            (email_id, amount, type, merchant, upi_ref, date, account)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data["email_id"], data["amount"], data["type"],
            data.get("merchant"), data.get("upi_ref"),
            data["date"], data.get("account")
        ))
        conn.commit()

def run(max_emails=1):
    initialize_db()
    emails = fetch_hdfc_emails(max_results=max_emails)
    print(f"{len(emails)} emails fetched")

    for email in emails:
        try:
            result = chain.invoke({
                "email_id": email["id"],
                "body": email["body"]
            })
            result["email_id"] = email["id"]
            Transaction(**result)   # validate
            save_transaction(result)
            print(f"Saved: {result['type']} Rs.{result['amount']} | {result['merchant']} | {result['date']}")
        except Exception as e:
            print(f"Failed {email['id']}: {e}")