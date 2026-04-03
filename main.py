

import os
from contextlib import asynccontextmanager

from agent.agent import run
from db.store import get_connection, initialize_db
from fastapi import FastAPI, Query
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

load_dotenv()

scheduler = BackgroundScheduler()
JOB_INTERVAL_HOURS = int(os.getenv("JOB_INTERVAL_HOURS", "24"))
JOB_MAX_EMAILS = int(os.getenv("JOB_MAX_EMAILS", "50"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_db()
    trigger = IntervalTrigger(hours=JOB_INTERVAL_HOURS)
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

@app.get("/")
def read_root():
    return {"message": "Expense Agent is running!"}


@app.get("/job/status")
def job_status():
    job = scheduler.get_job("daily_expense_job")
    return {
        "scheduler_running": scheduler.running,
        "job_id": job.id if job else None,
        "next_run_time": job.next_run_time.isoformat() if job and job.next_run_time else None,
        "interval_hours": JOB_INTERVAL_HOURS,
        "max_emails_per_run": JOB_MAX_EMAILS,
    }


@app.get("/transactions")
def list_transactions(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, email_id, amount, type, merchant, upi_ref, date, account
            FROM transactions
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

    return {
        "total": total,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "items": [dict(row) for row in rows],
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    


    


