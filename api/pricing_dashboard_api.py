"""
Pricing Dashboard API
Dynamic Pricing POC - read-only FastAPI bridge between the 3 Kafka topics
and a frontend dashboard.

This is a NEW, independent service. It does not replace or modify the
inventory agent, competitor agent, or pricing orchestrator - all three keep
running exactly as they are. This service just subscribes to the same 3
topics each of them already publishes to, using its own consumer group
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

Run (as a standing service, in its own terminal, alongside the 3 agents):
    uvicorn pricing_dashboard_api:app --reload --port 8000

Interactive API docs once running: http://localhost:8000/docs
"""

import asyncio
import json
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional

from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# -- Kafka config -----------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ["inventory-agent", "competitor-agent", "final-prices"]
CONSUMER_GROUP_ID = "pricing_dashboard_api"
HISTORY_LIMIT = 20  # entries kept per (topic, sku), for timeline/trend views

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
    sku = payload.get("sku", "UNKNOWN")
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


def _consume_loop(stop_event: threading.Event) -> None:
    """Polls all 3 topics into the cache until stop_event is set."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # never persist offsets - replay everything on every restart
    })
    consumer.subscribe(TOPICS)
    print(f"[dashboard-consumer] Subscribed to: {TOPICS}")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[dashboard-consumer] [ERROR] {msg.error()}")
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"[dashboard-consumer] [WARNING] Skipping malformed message: {e}")
                continue

            ts_type, ts_ms = msg.timestamp()
            received_at = (
                datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                if ts_type != 0 else None
            )
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _broadcast_queue, _event_loop
    _event_loop = asyncio.get_running_loop()
    _broadcast_queue = asyncio.Queue()

    consumer_thread = threading.Thread(target=_consume_loop, args=(_stop_event,), daemon=True)
    consumer_thread.start()
    broadcaster_task = asyncio.create_task(_broadcaster())

    yield

    _stop_event.set()
    broadcaster_task.cancel()


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
# A dedicated, denormalized view for the inventory-agent screen specifically.
# Built from the same cache as everything above - no separate consumer, just
# a different shape, computed from the `metrics` block agent.py now publishes.

def _risk_tier(units_at_risk: float, stock_on_hand: float, days_to_expiry: float) -> str:
    """Coarse High/Medium/Low label for the dashboard's waste-risk badge."""
    risk_ratio = (units_at_risk / stock_on_hand) if stock_on_hand else 0.0
    if days_to_expiry <= 1 or risk_ratio >= 0.7:
        return "HIGH"
    if days_to_expiry <= 3 or risk_ratio >= 0.4:
        return "MEDIUM"
    return "LOW"


def _weekly_depletion_curve(history: List[dict], current_stock: float, avg_daily_units_sold: float) -> List[dict]:
    """
    Buckets cached history by ISO week (using each message's received_at)
    and takes the last stock_on_hand seen per week, oldest first, then
    appends one naive forward projection.

    Caveat: this is only as good as how far back the cache reaches
    (HISTORY_LIMIT entries). If the inventory agent runs more than a few
    times a day, the cache may not span multiple distinct weeks yet - this
    returns fewer bars in that case rather than fabricating ones that don't
    exist.
    """
    weekly: Dict[str, float] = {}
    for entry in history:
        try:
            dt = datetime.strptime(entry["received_at"], "%Y-%m-%dT%H:%M:%SZ")
        except (KeyError, ValueError, TypeError):
            continue
        iso_year, iso_week, _ = dt.isocalendar()
        stock = (entry.get("payload") or {}).get("metrics", {}).get("stock_on_hand")
        if stock is not None:
            weekly[f"{iso_year}-W{iso_week:02d}"] = stock  # last entry in the bucket wins (history is append-ordered)

    bars = [{"label": k, "stock_on_hand": weekly[k], "is_projected": False} for k in sorted(weekly.keys())]
    projected = max(0.0, current_stock - avg_daily_units_sold * 7)
    bars.append({"label": "Projected", "stock_on_hand": round(projected, 1), "is_projected": True})
    return bars


@app.get("/agents/inventory/skus")
def list_inventory_skus():
    """SKU + display name pairs, for a dropdown selector."""
    with _lock:
        return [
            {"sku": sku, "product_name": (msg.get("metrics") or {}).get("product_name", sku)}
            for sku, msg in sorted(_latest["inventory-agent"].items())
        ]


@app.get("/agents/inventory/{sku}")
def get_inventory_detail(sku: str):
    with _lock:
        msg = _latest["inventory-agent"].get(sku)
        if msg is None:
            raise HTTPException(status_code=404, detail=f"No inventory-agent data yet for sku={sku}")
        history = list(_history["inventory-agent"].get(sku, []))

    metrics = msg.get("metrics", {})
    rec = msg.get("recommendation", {})
    stock_on_hand = metrics.get("stock_on_hand", 0.0)
    units_at_risk = metrics.get("units_at_risk", 0.0)
    days_to_expiry = metrics.get("days_to_expiry", 0.0)
    avg_daily_units_sold = metrics.get("avg_daily_units_sold", 0.0)
    cost_price = metrics.get("cost_price")
    risk_tier = _risk_tier(units_at_risk, stock_on_hand, days_to_expiry)

    # "Original stock" has no real input source today (see InventoryMetrics'
    # docstring in agent.py) - estimated as the earliest stock_on_hand this
    # cache has actually observed for the SKU, flagged as an estimate rather
    # than presented as a hard figure.
    oldest_known_stock = stock_on_hand
    if history:
        first_metrics = (history[0].get("payload") or {}).get("metrics", {})
        oldest_known_stock = first_metrics.get("stock_on_hand", stock_on_hand)
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
            "recommended_action": rec.get("action"),
        },
        "metrics": {
            "units_remaining": stock_on_hand,
            "original_stock_estimate": oldest_known_stock,
            "original_stock_is_estimated": True,
            "stock_coverage_pct": stock_coverage_pct,
            "days_to_expiry": days_to_expiry,
            "expiry_date": metrics.get("expiry_date"),
            "markdown_pct": round(rec.get("suggested_modifier", 0.0) * 100, 1),
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
        "depletion_curve": _weekly_depletion_curve(history, stock_on_hand, avg_daily_units_sold),
        "reasoning": msg.get("rationale"),
        "confidence": rec.get("confidence"),
        "fallback_used": metrics.get("fallback_used", False),
    }