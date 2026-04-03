

from contextlib import asynccontextmanager

from agent.agent import run
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    trigger = IntervalTrigger(hours=24)  # run once every 24 hours
    scheduler.add_job(run, trigger, id="daily_expense_job", replace_existing=True)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Expense Agent is running!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    


    


