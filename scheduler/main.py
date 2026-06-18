"""
Runs every agent in AGENTS on the clock hour (1:00, 2:00, 3:00, ...)
and exposes a small API to force an immediate rerun of a single agent
without disturbing its hourly schedule.

Start with:
    AGENT_API_KEY=some-secret uvicorn main:app --host 0.0.0.0 --port 8000

Trigger a manual rerun with:
    curl -X POST http://localhost:8000/agents/inventory/rerun \
         -H "X-API-Key: some-secret"
"""

import os
import time
from contextlib import asynccontextmanager
from typing import Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from fastapi import FastAPI, Header, HTTPException

from agents_registry import AGENTS
from runner import is_running, logger, run_agent

# Shared secret required on the rerun endpoint. Set this via the
# environment in real deployments -- the default here is only a
# placeholder so the service doesn't crash if it's unset.
API_KEY = os.environ.get("AGENT_API_KEY", "change-me")

scheduler = AsyncIOScheduler()


def _on_job_event(event) -> None:
    if event.exception:
        logger.error("Job '%s' raised an exception: %s", event.job_id, event.exception)
    else:
        logger.info("Job '%s' completed", event.job_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for name in AGENTS:
        scheduler.add_job(
            run_agent,
            trigger=CronTrigger(minute=0),  # fires on every clock hour
            args=[name],
            id=name,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True,
        )

    scheduler.add_listener(_on_job_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.start()
    logger.info("Scheduler started. Hourly jobs registered for: %s", list(AGENTS))

    yield

    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


app = FastAPI(title="Agent Scheduler", lifespan=lifespan)


def _check_api_key(x_api_key: Optional[str]) -> None:
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


@app.post("/agents/{agent_name}/rerun")
async def rerun_agent(agent_name: str, x_api_key: Optional[str] = Header(default=None)):
    """Force an immediate one-off run of a single agent.

    This does not touch that agent's hourly job -- its next scheduled
    run stays exactly where it was. If the agent is already running
    (scheduled or manual), this returns 409 instead of queuing a
    second run.
    """
    _check_api_key(x_api_key)

    if agent_name not in AGENTS:
        raise HTTPException(status_code=404, detail=f"Unknown agent '{agent_name}'")

    if is_running(agent_name):
        raise HTTPException(status_code=409, detail=f"Agent '{agent_name}' is already running")

    job_id = f"{agent_name}_manual_{int(time.time())}"
    scheduler.add_job(run_agent, trigger=DateTrigger(), args=[agent_name], id=job_id)

    return {"status": "scheduled", "agent": agent_name, "job_id": job_id}


@app.get("/agents")
async def list_agents():
    """Lists known agents and when each is next due to run on its
    regular hourly schedule."""
    next_runs = {}
    for job in scheduler.get_jobs():
        if job.id in AGENTS:
            next_runs[job.id] = job.next_run_time.isoformat() if job.next_run_time else None

    return {"agents": list(AGENTS.keys()), "next_scheduled_run": next_runs}
