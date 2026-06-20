"""
Pricing Dashboard API
Dynamic Pricing POC - read-only FastAPI bridge between the 4 Kafka topics
and a frontend dashboard.

This is a NEW, independent service. It does not replace or modify the
inventory agent, competitor agent, or pricing orchestrator - all three keep
running exactly as they are. This service just subscribes to the same 3
topics they already publish to, using its own consumer group
("pricing_dashboard_api") so it can never interfere with the orchestrator's
own consumer offsets.

On every incoming message it:
  1. updates an in-memory cache (latest message + bounded history per SKU
     per topic), and
  2. broadcasts the message to any connected WebSocket clients.

So a frontend can either poll the REST endpoints for a snapshot, or open
the WebSocket for a live feed of everything flowing through the pipeline.

State is intentionally NOT persisted anywhere by this service - every
restart replays each topic from the beginning (auto.offset.reset=earliest,
auto-commit disabled) and rebuilds the cache from scratch in a few seconds.
That keeps this service simple and stateless; the durable audit trail
already lives in proposals.jsonl / final_prices.jsonl on each agent.

This file also folds in the agent scheduler that previously ran as its own
standalone service (scheduler/main.py). On startup it registers an hourly
cron job for every agent in scheduler/agents_registry.py's AGENTS dict, and
exposes a couple of small endpoints (under /agents) to inspect that schedule
and to force an immediate one-off rerun of a single agent without disturbing
its hourly slot. That part requires scheduler/ to sit next to this file as
an importable package (an empty scheduler/__init__.py is enough).

Run (as a standing service, in its own terminal, alongside the 3 agents):
    AGENT_API_KEY=some-secret uvicorn pricing_dashboard_api:app --reload --port 8000

Trigger a manual agent rerun with:
    curl -X POST http://localhost:8000/agents/inventory/rerun \
         -H "X-API-Key: some-secret"

Interactive API docs once running: http://localhost:8000/docs
"""

import asyncio
import json
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from confluent_kafka import Consumer, OFFSET_BEGINNING
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client

# main.py (this file's source for the agent-scheduler functionality) lived in
# the scheduler/ subfolder and imported these two modules directly, since it
# ran from inside that folder. This file lives one level up, in the parent
# folder, with scheduler/ as a subfolder beneath it - hence the package-qualified
# imports below instead of bare `import agents_registry` / `import runner`.
# This requires scheduler/ to be an importable package (an empty __init__.py
# in scheduler/ is enough) or to be on PYTHONPATH as a namespace package.
from scheduler.agents_registry import AGENTS
from scheduler.runner import is_running, logger as agent_logger, run_agent

# -- Kafka config -----------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ["inventory-agent", "competitor-agent", "final-prices", "inventory-detailed", "competitor-detailed"]
CONSUMER_GROUP_ID = "pricing_dashboard_api"
HISTORY_LIMIT = 20  # entries kept per (topic, sku), for timeline/trend views

# -- Agent scheduler config ---------------------------------------------------
# Runs every agent in AGENTS on the clock hour (1:00, 2:00, 3:00, ...) and
# exposes endpoints to force an immediate rerun of a single agent without
# disturbing its hourly schedule. Folded in from the standalone scheduler
# service (scheduler/main.py) so both pieces run as one process.
#
# Trigger a manual rerun with:
#     curl -X POST http://localhost:8000/agents/inventory/rerun \
#          -H "X-API-Key: some-secret"
#
# Shared secret required on the rerun endpoint. Set this via the
# environment in real deployments -- the default here is only a
# placeholder so the service doesn't crash if it's unset.
AGENT_API_KEY = os.environ.get("AGENT_API_KEY", "change-me")

scheduler = AsyncIOScheduler()

supabase = create_client(
    os.getenv("SUPABASE_URL","https://gmqstxrmaloqymmvirce.supabase.co"),
    os.getenv("SUPABASE_SERVICE_KEY","eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdtcXN0eHJtYWxvcXltbXZpcmNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MTcxNDQ0MSwiZXhwIjoyMDk3MjkwNDQxfQ.1HacsMZoUYM3Dzuix858BYatmyPDwQh3lnZSQhG9ViU")
)


def _on_job_event(event) -> None:
    if event.exception:
        agent_logger.error("Job '%s' raised an exception: %s", event.job_id, event.exception)
    else:
        agent_logger.info("Job '%s' completed", event.job_id)


def _check_api_key(x_api_key: Optional[str]) -> None:
    if x_api_key != AGENT_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# -- Inventory agent dashboard data source -----------------------------------
# The "/agents/inventory/*" view below reads from the "inventory-detailed"
# Kafka topic rather than a JSONL file. The consumer thread for this topic
# populates the same in-memory cache (_latest / _history) used by all other
# REST endpoints, so the inventory-detail section can serve SKU-level product
# data (product_name, stock_on_hand, cost_price, etc.) without any file I/O.

# -- In-memory state ----------------------------------------------------------
# Guarded by one lock: the Kafka consumer runs in its own background thread
# while FastAPI serves requests on the asyncio event loop, so both sides
# touch these dicts concurrently.
_lock = threading.Lock()
_latest: Dict[str, Dict[str, dict]] = {t: {} for t in TOPICS}           # topic -> sku -> latest message
_history: Dict[str, Dict[str, Deque[dict]]] = {t: {} for t in TOPICS}   # topic -> sku -> recent messages
_topic_stats: Dict[str, dict] = {t: {"message_count": 0, "last_message_at": None} for t in TOPICS}

_ws_clients: List[WebSocket] = []
_broadcast_queue: Optional[asyncio.Queue] = None
_event_loop: Optional[asyncio.AbstractEventLoop] = None


# -- Response models ----------------------------------------------------------
class TopicStats(BaseModel):
    message_count: int
    last_message_at: Optional[str]


class HealthResponse(BaseModel):
    status: str
    topics: Dict[str, TopicStats]


class SKUSummary(BaseModel):
    sku: str
    final_status: Optional[str] = None
    final_action: Optional[str] = None
    final_modifier: Optional[float] = None
    final_confidence: Optional[float] = None
    needs_review: bool = False
    inventory_action: Optional[str] = None
    competitor_modifier: Optional[float] = None


class SKUDetail(BaseModel):
    sku: str
    inventory: Optional[dict] = None
    competitor: Optional[dict] = None
    final_price: Optional[dict] = None


class MetricsResponse(BaseModel):
    topics: Dict[str, TopicStats]
    total_skus: int
    fallback_count: int
    completed_count: int
    action_breakdown: Dict[str, int]


# -- Kafka consumer (runs in a background thread, never on the event loop) --
def _ingest(topic: str, payload: dict, received_at: Optional[str] = None) -> None:
    """Updates the in-memory cache for one incoming message. Called from the consumer thread."""
    if not isinstance(payload, dict):
        print(f"[dashboard-consumer] [WARNING] Skipping non-dict message on topic '{topic}': {type(payload).__name__}")
        return
    sku = (
        payload.get("sku")
        or payload.get("sku_id")
        or (payload.get("metrics_evaluated") or {}).get("sku")
        or "UNKNOWN"
    )
    received_at = received_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _lock:
        _latest[topic][sku] = payload
        _history[topic].setdefault(sku, deque(maxlen=HISTORY_LIMIT)).append(
            {"received_at": received_at, "payload": payload}
        )
        _topic_stats[topic]["message_count"] += 1
        _topic_stats[topic]["last_message_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Hand off to the asyncio side for WebSocket fan-out without blocking this thread
    if _event_loop is not None and _broadcast_queue is not None:
        envelope = {"topic": topic, "sku": sku, "payload": payload}
        _event_loop.call_soon_threadsafe(_broadcast_queue.put_nowait, envelope)


def _on_assign(consumer: Consumer, partitions: list) -> None:
    """
    Fires on every partition (re)assignment, including the very first one
    right after startup. Forces every partition to start at the true
    beginning, overriding whatever offset this group.id may have
    committed in a past life (a stray manual consumer reusing this group
    name, an earlier version of this service, a debugging session, etc).

    This matters because "auto.offset.reset": "earliest" is only a
    fallback used when Kafka has NO committed offset on record for the
    group - it does not guarantee a fresh replay on every restart. If a
    commit ever happened under this group id, the consumer would silently
    resume from there instead, permanently hiding any SKU whose messages
    sit before that offset (showing up as 404s for data that genuinely
    exists on the topic). Explicitly seeking to OFFSET_BEGINNING here
    makes the "every restart replays everything" behavior actually true,
    independent of broker-side history.
    """
    for p in partitions:
        p.offset = OFFSET_BEGINNING
    consumer.assign(partitions)
    print(f"[dashboard-consumer] Assigned {len(partitions)} partition(s), seeking to beginning")


def _consume_loop(stop_event: threading.Event, ready_event: threading.Event) -> None:
    """
    Polls all 5 topics into the cache until stop_event is set.

    ready_event is set once the initial backlog has been fully drained
    (the first poll() that comes back empty after assignment). The app's
    lifespan waits on this before serving traffic, so a request landing
    in the first second or two after startup can't 404 on a SKU that
    genuinely has data simply because this thread hasn't caught up yet.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "enable.auto.commit": False,  # never persist offsets - replay everything on every restart
    })
    consumer.subscribe(TOPICS, on_assign=_on_assign)
    print(f"[dashboard-consumer] Subscribed to: {TOPICS}")

    caught_up = False
    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                if not caught_up:
                    caught_up = True
                    ready_event.set()
                    print("[dashboard-consumer] Initial backlog drained, cache is warm")
                continue
            if msg.error():
                print(f"[dashboard-consumer] [ERROR] {msg.error()}")
                continue
            try:
                raw = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"[dashboard-consumer] [WARNING] Skipping malformed message: {e}")
                continue

            # Accept both a single dict and a list of dicts
            payloads = raw if isinstance(raw, list) else [raw]

            ts_type, ts_ms = msg.timestamp()
            received_at = (
                datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                if ts_type != 0 else None
            )
            for payload in payloads:
                _ingest(msg.topic(), payload, received_at)
    finally:
        consumer.close()


async def _broadcaster() -> None:
    """Drains the broadcast queue and fans each message out to connected WebSocket clients."""
    while True:
        envelope = await _broadcast_queue.get()
        dead = []
        for ws in _ws_clients:
            try:
                await ws.send_json(envelope)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in _ws_clients:
                _ws_clients.remove(ws)


# -- App lifecycle --------------------------------------------------------------
_stop_event = threading.Event()
_consumer_ready = threading.Event()
CONSUMER_READY_TIMEOUT = 30.0  # seconds to wait for the initial backlog to drain before serving anyway


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _broadcast_queue, _event_loop
    _event_loop = asyncio.get_running_loop()
    _broadcast_queue = asyncio.Queue()

    consumer_thread = threading.Thread(
        target=_consume_loop, args=(_stop_event, _consumer_ready), daemon=True
    )
    consumer_thread.start()
    broadcaster_task = asyncio.create_task(_broadcaster())

    # Hold off serving traffic until the consumer has drained the existing
    # backlog on all 5 topics, so an early request can't 404 on a SKU that
    # genuinely has data simply because the cache hasn't caught up yet.
    became_ready = await _event_loop.run_in_executor(
        None, _consumer_ready.wait, CONSUMER_READY_TIMEOUT
    )
    if became_ready:
        agent_logger.info("Kafka consumer caught up - cache is warm")
    else:
        agent_logger.warning(
            "Kafka consumer did not catch up within %.0fs - serving traffic anyway; "
            "early requests may 404 until it catches up",
            CONSUMER_READY_TIMEOUT,
        )

    # Agent scheduler startup (folded in from scheduler/main.py)
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
    agent_logger.info("Scheduler started. Hourly jobs registered for: %s", list(AGENTS))

    yield

    _stop_event.set()
    broadcaster_task.cancel()

    # Agent scheduler shutdown
    scheduler.shutdown(wait=False)
    agent_logger.info("Scheduler stopped")


app = FastAPI(title="Pricing Dashboard API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your actual frontend origin before shipping past a POC
    allow_methods=["*"],
    allow_headers=["*"],
)


# -- Endpoints ------------------------------------------------------------------
@app.get("/")
def root():
    return {"service": "pricing-dashboard-api", "topics": TOPICS, "docs": "/docs"}


@app.get("/health", response_model=HealthResponse)
def get_health():
    with _lock:
        return HealthResponse(status="ok", topics={t: TopicStats(**_topic_stats[t]) for t in TOPICS})

@app.get("/kpis")
def get_kpis():
    try:
        response = (
            supabase
            .table("kpi_values")
            .select("*")
            .execute()
        )

        return {
            "success": True,
            "count": len(response.data),
            "data": response.data
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/kpis/dashboard")
def get_dashboard_kpis():
    try:
        response = (
            supabase
            .table("dashboard_kpis")
            .select("*")
            .execute()
        )

        if not response.data:
            raise HTTPException(
                status_code=404,
                detail="No dashboard KPI data found"
            )

        return {
            "success": True,
            "data": response.data[0]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
        
@app.get("/kpis/{sku_id}")
def get_kpi_by_sku(sku_id: str):
    try:
        response = (
            supabase
            .table("kpi_values")
            .select("*")
            .eq("sku_id", sku_id)
            .execute()
        )

        return {
            "success": True,
            "data": response.data
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
# -- Agent scheduler endpoints --------------------------------------------------
# Folded in from the standalone scheduler service (scheduler/main.py).
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


@app.get("/skus", response_model=List[SKUSummary])
def list_skus():
    with _lock:
        all_skus = set()
        for t in TOPICS:
            all_skus.update(_latest[t].keys())

        summaries = []
        for sku in sorted(all_skus):
            final = _latest["final-prices"].get(sku)
            inventory = _latest["inventory-agent"].get(sku)
            competitor = _latest["competitor-agent"].get(sku)
            final_rec = (final or {}).get("final_recommendation")

            summaries.append(SKUSummary(
                sku=sku,
                final_status=(final or {}).get("status"),
                final_action=(final_rec or {}).get("action"),
                final_modifier=(final_rec or {}).get("suggested_modifier"),
                final_confidence=(final_rec or {}).get("confidence"),
                needs_review=(final or {}).get("status") == "FALLBACK",
                inventory_action=((inventory or {}).get("recommendation") or {}).get("action"),
                competitor_modifier=((competitor or {}).get("recommendation") or {}).get("suggested_modifier"),
            ))
        return summaries


@app.get("/skus/{sku}", response_model=SKUDetail)
def get_sku_detail(sku: str):
    with _lock:
        if sku not in (
            set(_latest["inventory-agent"]) | set(_latest["competitor-agent"]) | set(_latest["final-prices"])
        ):
            raise HTTPException(status_code=404, detail=f"No data yet for sku={sku}")
        return SKUDetail(
            sku=sku,
            inventory=_latest["inventory-agent"].get(sku),
            competitor=_latest["competitor-agent"].get(sku),
            final_price=_latest["final-prices"].get(sku),
        )


@app.get("/skus/{sku}/history")
def get_sku_history(sku: str, topic: str = "final-prices", limit: int = 20):
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail=f"topic must be one of {TOPICS}")
    with _lock:
        hist = list(_history[topic].get(sku, []))
    return hist[-limit:]


@app.get("/metrics", response_model=MetricsResponse)
def get_metrics():
    with _lock:
        final_messages = list(_latest["final-prices"].values())
        fallback_count = sum(1 for m in final_messages if m.get("status") == "FALLBACK")
        completed_count = sum(1 for m in final_messages if m.get("status") == "COMPLETED")

        action_breakdown: Dict[str, int] = {}
        for m in final_messages:
            action = (m.get("final_recommendation") or {}).get("action", "UNKNOWN")
            action_breakdown[action] = action_breakdown.get(action, 0) + 1

        all_skus = set()
        for t in TOPICS:
            all_skus.update(_latest[t].keys())

        return MetricsResponse(
            topics={t: TopicStats(**_topic_stats[t]) for t in TOPICS},
            total_skus=len(all_skus),
            fallback_count=fallback_count,
            completed_count=completed_count,
            action_breakdown=action_breakdown,
        )


@app.websocket("/ws")
async def websocket_feed(websocket: WebSocket):
    """Live feed: every message landing on any of the 3 topics, pushed as {topic, sku, payload}."""
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()  # keeps the connection open; client needn't send anything meaningful
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in _ws_clients:
            _ws_clients.remove(websocket)


# -- Inventory agent dashboard view -------------------------------------------
# A dedicated, denormalized view for the inventory-agent screen, served from
# the "inventory-detailed" Kafka topic. The topic payload includes product-
# level detail (product_name, stock_on_hand, cost_price, etc.) that the old
# "inventory-agent" topic's message lacked, so we now resolve data directly
# from the Kafka stream rather than from a JSONL file.

def _risk_tier(units_at_risk: float, stock_on_hand: float, days_to_expiry: float) -> str:
    """Coarse High/Medium/Low label for the dashboard's waste-risk badge."""
    risk_ratio = (units_at_risk / stock_on_hand) if stock_on_hand else 0.0
    if days_to_expiry <= 1 or risk_ratio >= 0.7:
        return "HIGH"
    if days_to_expiry <= 3 or risk_ratio >= 0.4:
        return "MEDIUM"
    return "LOW"


def _combine_rationale(justification: dict) -> Optional[str]:
    """Joins headline + detailed_reasoning into one readable string, mirroring agent.py's _build_rationale."""
    headline = (justification.get("headline") or "").strip()
    detailed = (justification.get("detailed_reasoning") or "").strip()
    if not headline and not detailed:
        return None
    if headline and headline[-1] not in ".!?":
        headline += "."
    return f"{headline} {detailed}".strip()


def _weekly_depletion_curve(records: List[dict], current_stock: float, avg_daily_units_sold: float) -> List[dict]:
    """
    Buckets a SKU's logged runs by ISO week (using each record's timestamp)
    and takes the last stock_on_hand seen per week, oldest first, then
    appends one naive forward projection.

    Caveat: this is only as good as how many messages the "inventory-detailed"
    topic has accumulated for this SKU so far. If the inventory agent has
    only run a few times today, this returns fewer bars rather than
    fabricating ones that don't exist.
    """
    weekly: Dict[str, float] = {}
    for rec in records:
        try:
            dt = datetime.strptime(rec["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        except (KeyError, ValueError, TypeError):
            continue
        iso_year, iso_week, _ = dt.isocalendar()
        stock = (rec.get("metrics_evaluated") or {}).get("stock_on_hand")
        if stock is not None:
            weekly[f"{iso_year}-W{iso_week:02d}"] = stock  # last entry in the bucket wins (records are append-ordered)

    bars = [{"label": k, "stock_on_hand": weekly[k], "is_projected": False} for k in sorted(weekly.keys())]
    projected = max(0.0, current_stock - avg_daily_units_sold * 7)
    bars.append({"label": "Projected", "stock_on_hand": round(projected, 1), "is_projected": True})
    return bars


@app.get("/agents/inventory/skus")
def list_inventory_skus():
    """SKU + display name pairs, for a dropdown selector."""
    with _lock:
        return [
            {"sku": sku, "product_name": (rec.get("metrics_evaluated") or {}).get("product_name", sku)}
            for sku, rec in sorted(_latest.get("inventory-detailed", {}).items())
        ]


@app.get("/agents/inventory/{sku}")
def get_inventory_detail(sku: str):
    with _lock:
        latest = _latest.get("inventory-detailed", {}).get(sku)
        if latest is None:
            raise HTTPException(status_code=404, detail=f"No inventory data yet for sku={sku}")

        history_entries = list(_history.get("inventory-detailed", {}).get(sku, []))
        records = [entry["payload"] for entry in history_entries]

        metrics = latest.get("metrics_evaluated", {})
        proposal = latest.get("proposal", {})
        justification_in = latest.get("justification", {})

        stock_on_hand = metrics.get("stock_on_hand", 0.0)
        units_at_risk = metrics.get("units_at_risk", 0.0)
        days_to_expiry = metrics.get("days_to_expiry", 0.0)
        avg_daily_units_sold = metrics.get("avg_daily_units_sold", 0.0)
        cost_price = metrics.get("cost_price")
        price_modifier = proposal.get("price_modifier", 1.0)
        risk_tier = _risk_tier(units_at_risk, stock_on_hand, days_to_expiry)

        oldest_known_stock = records[0].get("metrics_evaluated", {}).get("stock_on_hand", stock_on_hand) if records else stock_on_hand
        stock_coverage_pct = round(100 * stock_on_hand / oldest_known_stock, 1) if oldest_known_stock else None

        required_velocity = round(units_at_risk / days_to_expiry, 1) if days_to_expiry > 0 else units_at_risk

        return {
            "sku": sku,
            "product_name": metrics.get("product_name", sku),
            "category": metrics.get("category"),
            "unit": metrics.get("unit"),
            "alert": {
                "severity": risk_tier,
                "units_remaining": stock_on_hand,
                "days_to_expiry": days_to_expiry,
                "recommended_action": proposal.get("suggested_action"),
            },
            "metrics": {
                "units_remaining": stock_on_hand,
                "original_stock_estimate": oldest_known_stock,
                "original_stock_is_estimated": True,
                "stock_coverage_pct": stock_coverage_pct,
                "days_to_expiry": days_to_expiry,
                "expiry_date": None,
                "markdown_pct": round((price_modifier - 1.0) * 100, 1),
                "cost_price": cost_price,
            },
            "justification": {
                "waste_risk_tier": risk_tier,
                "units_at_risk": units_at_risk,
                "cost_basis_value_at_risk": round(units_at_risk * cost_price, 2) if cost_price is not None else None,
                "daily_velocity": avg_daily_units_sold,
                "units_to_clear": units_at_risk,
                "required_velocity": required_velocity,
            },
            "depletion_curve": _weekly_depletion_curve(records, stock_on_hand, avg_daily_units_sold),
            "reasoning": _combine_rationale(justification_in),
            "confidence": proposal.get("confidence_score"),
            "fallback_used": latest.get("status") == "FALLBACK",
        }


# -- Competitor agent dashboard view -------------------------------------------
# A dedicated, denormalized view for the competitor-agent screen, served from
# the "competitor-detailed" Kafka topic (rich payload with metrics_evaluated,
# proposal, justification) and falling back to the "competitor-agent" topic
# (compact payload with recommendation + rationale) for the SKU listing.


@app.get("/agents/competitor/skus")
def list_competitor_skus():
    """SKU + competitor pricing pairs, for a dropdown selector."""
    with _lock:
        # Prefer the detailed topic when available; fall back to basic topic
        seen = set()
        result = []
        for sku, rec in sorted(_latest.get("competitor-detailed", {}).items()):
            seen.add(sku)
            metrics = rec.get("metrics_evaluated", {})
            result.append({
                "sku": sku,
                "our_current_price": metrics.get("our_current_price"),
                "competitor_price": metrics.get("competitor_price"),
            })
        for sku, rec in sorted(_latest.get("competitor-agent", {}).items()):
            if sku not in seen:
                result.append({
                    "sku": sku,
                    "our_current_price": None,
                    "competitor_price": None,
                })
        return result


@app.get("/agents/competitor/{sku}")
def get_competitor_detail(sku: str):
    with _lock:
        latest = _latest.get("competitor-detailed", {}).get(sku)

        # Fall back to the simpler "competitor-agent" topic when the
        # detailed topic has no entry for this SKU yet.
        if latest is None:
            basic = _latest.get("competitor-agent", {}).get(sku)
            if basic is None:
                raise HTTPException(status_code=404, detail=f"No competitor data yet for sku={sku}")
            rec = basic.get("recommendation", {})
            return {
                "sku": sku,
                "our_current_price": None,
                "competitor_price": None,
                "price_difference_pct": None,
                "status": None,
                "timestamp": None,
                "alert": {
                    "suggested_action": None,
                    "modifier_pct": round((rec.get("suggested_modifier", 0.0)) * 100, 1),
                    "confidence_score": rec.get("confidence"),
                },
                "metrics": {
                    "our_current_price": None,
                    "competitor_price": None,
                    "price_difference_pct": None,
                },
                "justification": None,
                "reasoning": basic.get("rationale"),
                "confidence": rec.get("confidence"),
                "fallback_used": True,
            }

        history_entries = list(_history.get("competitor-detailed", {}).get(sku, []))
        records = [entry["payload"] for entry in history_entries]

        metrics = latest.get("metrics_evaluated") or {}
        proposal = latest.get("proposal") or {}
        justification_in = latest.get("justification") or {}

        our_price = metrics.get("our_current_price")
        comp_price = metrics.get("competitor_price")
        price_diff_pct = round(100 * (comp_price - our_price) / our_price, 1) if our_price and comp_price else None
        price_modifier = proposal.get("price_modifier", 1.0)

        return {
            "sku": sku,
            "our_current_price": our_price,
            "competitor_price": comp_price,
            "price_difference_pct": price_diff_pct,
            "status": latest.get("status"),
            "timestamp": latest.get("timestamp"),
            "alert": {
                "suggested_action": proposal.get("suggested_action"),
                "modifier_pct": round((price_modifier - 1.0) * 100, 1),
                "confidence_score": proposal.get("confidence_score"),
            },
            "metrics": {
                "our_current_price": our_price,
                "competitor_price": comp_price,
                "price_difference_pct": price_diff_pct,
            },
            "justification": {
                "headline": justification_in.get("headline"),
                "detailed_reasoning": justification_in.get("detailed_reasoning"),
            },
            "reasoning": _combine_rationale(justification_in),
            "confidence": proposal.get("confidence_score"),
            "fallback_used": latest.get("status") == "FALLBACK",
        }
