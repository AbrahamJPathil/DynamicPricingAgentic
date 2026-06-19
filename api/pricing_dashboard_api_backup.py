"""
Pricing Dashboard API — Static Backup

Read-only FastAPI server that serves static data from JSON files instead of
Kafka topics. Identical REST endpoints and response shapes as the live
pricing_dashboard_api.py, for use as a fallback when Kafka is unavailable.

Run:
    uvicorn api.pricing_dashboard_api_backup:app --reload --port 8001

The data directory defaults to <this-file-dir>/backup_data/.  Override with:
    PRICING_BACKUP_DATA_DIR=/path/to/data
"""

import json
import os
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

HISTORY_LIMIT = 20

TOPICS = ["inventory-agent", "competitor-agent", "final-prices", "inventory-detailed", "competitor-detailed"]

_lock = threading.Lock()
_latest: Dict[str, Dict[str, dict]] = {t: {} for t in TOPICS}
_history: Dict[str, Dict[str, Deque[dict]]] = {t: {} for t in TOPICS}
_topic_stats: Dict[str, dict] = {t: {"message_count": 0, "last_message_at": None} for t in TOPICS}

BACKUP_DATA_DIR = os.environ.get(
    "PRICING_BACKUP_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "backup_data"),
)

TOPIC_TO_FILE = {
    "inventory-agent": "inventory_agent.json",
    "competitor-agent": "competitor_agent.json",
    "final-prices": "final_prices.json",
    "inventory-detailed": "inventory_detailed.json",
    "competitor-detailed": "competitor_detailed.json",
}


def _load_data() -> None:
    for topic, filename in TOPIC_TO_FILE.items():
        filepath = os.path.join(BACKUP_DATA_DIR, filename)
        try:
            with open(filepath) as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"[backup] WARNING: data file not found: {filepath}")
            continue

        raw_latest = data.get("latest", {})
        raw_history = data.get("history", {})

        for sku, payload in raw_latest.items():
            _latest[topic][sku] = payload

            entries = []
            for h in raw_history.get(sku, []):
                entries.append({"received_at": h.get("received_at", ""), "payload": h.get("payload", {})})
            _history[topic][sku] = deque(entries, maxlen=HISTORY_LIMIT)

            _topic_stats[topic]["message_count"] += 1
            _topic_stats[topic]["last_message_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"[backup] Loaded {len(raw_latest)} SKUs from {filename}")


_load_data()

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


app = FastAPI(title="Pricing Dashboard API — Static Backup")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# -- Endpoints ------------------------------------------------------------------

@app.get("/")
def root():
    return {"service": "pricing-dashboard-api-backup", "topics": TOPICS, "docs": "/docs"}


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


# -- Inventory agent dashboard view -------------------------------------------

def _risk_tier(units_at_risk: float, stock_on_hand: float, days_to_expiry: float) -> str:
    risk_ratio = (units_at_risk / stock_on_hand) if stock_on_hand else 0.0
    if days_to_expiry <= 1 or risk_ratio >= 0.7:
        return "HIGH"
    if days_to_expiry <= 3 or risk_ratio >= 0.4:
        return "MEDIUM"
    return "LOW"


def _combine_rationale(justification: dict) -> Optional[str]:
    headline = (justification.get("headline") or "").strip()
    detailed = (justification.get("detailed_reasoning") or "").strip()
    if not headline and not detailed:
        return None
    if headline and headline[-1] not in ".!?":
        headline += "."
    return f"{headline} {detailed}".strip()


def _weekly_depletion_curve(records: List[dict], current_stock: float, avg_daily_units_sold: float) -> List[dict]:
    weekly: Dict[str, float] = {}
    for rec in records:
        try:
            dt = datetime.strptime(rec["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        except (KeyError, ValueError, TypeError):
            continue
        iso_year, iso_week, _ = dt.isocalendar()
        stock = (rec.get("metrics_evaluated") or {}).get("stock_on_hand")
        if stock is not None:
            weekly[f"{iso_year}-W{iso_week:02d}"] = stock

    bars = [{"label": k, "stock_on_hand": weekly[k], "is_projected": False} for k in sorted(weekly.keys())]
    projected = max(0.0, current_stock - avg_daily_units_sold * 7)
    bars.append({"label": "Projected", "stock_on_hand": round(projected, 1), "is_projected": True})
    return bars


@app.get("/agents/inventory/skus")
def list_inventory_skus():
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

@app.get("/agents/competitor/skus")
def list_competitor_skus():
    with _lock:
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
