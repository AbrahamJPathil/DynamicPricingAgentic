"""
Graph node functions and conditional edge routers for the Inventory & Perishability Agent.
"""

import json
import os
from datetime import datetime, timezone

import requests
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import ValidationError

from langgraph.graph import END

from core.config import (
    _PROMPT_COST_PER_1M,
    _COMPLETION_COST_PER_1M,
    PROPOSAL_LOG,
    VALIDATION_LOG,
    URGENCY_RANK,
    SYSTEM_PROMPT,
)
from core.models import AgentState, LLMProposal
from kafka_publisher import publish_proposal, publish_fall_reasoning
from utils.history import _write_log, _load_recent_history, _format_history
from utils.kafka_utils import build_kafka_payload
from utils.llm_handler import parse_llm_response, fallback_proposal


def load_csv_node(state: AgentState) -> AgentState:
    """Reads the product data and loads all rows into state."""
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
        if not supabase_url or not supabase_key:
            raise EnvironmentError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment")

        url = f"{supabase_url.rstrip('/')}/rest/v1/products_sku"
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Accept": "application/json"
        }
        params = {"select": "*"}

        resp = requests.get(url, headers=headers, params=params, timeout=10, verify=False)
        resp.raise_for_status()
        rows = resp.json()
        if not isinstance(rows, list):
            raise ValueError("Unexpected response from Supabase: expected a list of rows")

        state["rows"] = rows
    except Exception as exc:
        print(f"[load_csv] Failed to load rows from Supabase: {exc}")
        state["rows"] = []

    state["row_index"] = 0
    state["results"] = []
    state["urgency_queue"] = []
    state["all_token_usage"] = []
    print(f"[load_csv] Loaded {len(state['rows'])} row(s)  run_id={state['run_id']}")
    return state


def _precompute_urgency(row: dict) -> tuple[str, float]:
    """
    Lightweight pre-scan for a single row - no LLM, pure Python math.
    Returns (urgency_label, loss_if_no_action) for sorting purposes.
    Rows that are non-perishable or have no units at risk return ("SKIP", 0.0).
    """
    is_perishable = row.get("is_perishable")
    if isinstance(is_perishable, bool):
        if not is_perishable:
            return "SKIP", 0.0
    else:
        if str(is_perishable).strip().upper() != "TRUE":
            return "SKIP", 0.0

    try:
        now = datetime.now(timezone.utc)
        expiry = datetime.fromisoformat(row["expiry_datetime"].replace("Z", "+00:00"))
        days_to_expiry = max((expiry - now).total_seconds() / 86400, 0)
        avg_daily = float(row["avg_daily_units_sold"])
        stock = float(row["stock_on_hand"])
        units_at_risk = stock - (avg_daily * days_to_expiry)

        if units_at_risk <= 0:
            return "SKIP", 0.0

        buyback = float(row["producer_buyback_rate"])
        repurposing = float(row["repurposing_recovery_rate"])
        cost_price = float(row["cost_price"])
        expiry_loss_rate = 1.0 - buyback - repurposing
        loss_if_no_action = round(units_at_risk * cost_price * expiry_loss_rate, 2)

        urgency = (
            "IMMEDIATE"
            if days_to_expiry <= 1
            else "HIGH"
            if days_to_expiry <= 3
            else "MEDIUM"
        )
        return urgency, loss_if_no_action

    except (KeyError, ValueError):
        return "SKIP", 0.0


def sort_by_urgency_node(state: AgentState) -> AgentState:
    """
    Pre-scans ALL rows using pure Python math (no LLM).
    Sorts by:
      1. Urgency tier  - IMMEDIATE -> HIGH -> MEDIUM  (primary)
      2. loss_if_no_action descending                (tiebreaker)
    Prints the full processing queue before any LLM call is made.
    Replaces state["rows"] with the sorted order so the downstream
    row-by-row loop picks them up in priority sequence.
    """
    scored = []
    for row in state["rows"]:
        urgency, loss = _precompute_urgency(row)
        scored.append(
            {
                "sku_id": row.get("sku_id", "UNKNOWN"),
                "product_name": row.get("product_name", ""),
                "urgency": urgency,
                "loss_if_no_action": loss,
                "row": row,
            }
        )

    scored.sort(key=lambda x: (URGENCY_RANK[x["urgency"]], -x["loss_if_no_action"]))

    state["rows"] = [s["row"] for s in scored]

    state["urgency_queue"] = [
        {k: v for k, v in s.items() if k != "row"} for s in scored
    ]

    URGENCY_ICONS = {"IMMEDIATE": "[!]", "HIGH": "[H]", "MEDIUM": "[M]", "SKIP": "[ ]"}
    separator = "-" * 62

    print(f"\n[sort_by_urgency] {separator}")
    print(f"[sort_by_urgency]  PROCESSING QUEUE  ({len(scored)} SKU(s) total)")
    print(f"[sort_by_urgency] {separator}")
    print(
        f"[sort_by_urgency]  {'#':<4} {'SKU':<10} {'URGENCY':<11} "
        f"{'LOSS IF NO ACTION':<20} PRODUCT"
    )
    print(f"[sort_by_urgency] {separator}")

    for i, s in enumerate(scored, 1):
        icon = URGENCY_ICONS[s["urgency"]]
        loss = f"${s['loss_if_no_action']:.2f}" if s["urgency"] != "SKIP" else "-"
        print(
            f"[sort_by_urgency]  {i:<4} {s['sku_id']:<10} "
            f"{icon} {s['urgency']:<9} {loss:<20} {s['product_name']}"
        )

    print(f"[sort_by_urgency] {separator}")

    actionable = [s for s in scored if s["urgency"] != "SKIP"]
    skipped = [s for s in scored if s["urgency"] == "SKIP"]
    print(
        f"[sort_by_urgency]  Actionable: {len(actionable)}   "
        f"Skipped (no risk): {len(skipped)}"
    )
    print(f"[sort_by_urgency] {separator}\n")

    return state


def check_perishable_node(state: AgentState) -> AgentState:
    """
    Picks the current row and checks whether it is perishable.
    Writes True/False to is_perishable - the conditional edge
    uses this to skip non-perishable SKUs immediately.
    """
    row = state["rows"][state["row_index"]]
    state["current_row"] = row
    is_perishable = row.get("is_perishable")
    if isinstance(is_perishable, bool):
        state["is_perishable"] = is_perishable
    else:
        state["is_perishable"] = str(is_perishable).strip().upper() == "TRUE"
    sku = row.get("sku_id", "UNKNOWN")
    print(f"[check_perishable] [{sku}] is_perishable={state['is_perishable']}")
    return state


def skip_non_perishable_node(state: AgentState) -> AgentState:
    """
    Reached when is_perishable is False - there is no expiry to compute and
    no pricing action is needed. No LLM call is made, but a deterministic
    HOLD message is still published to Kafka so every SKU in the run has a
    record on the topic, not just the perishable ones that made it past
    check_perishable.
    """
    row = state["current_row"]
    sku = row.get("sku_id", "UNKNOWN")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    llm = {
        "suggested_action": "HOLD",
        "price_modifier": 1.0,
        "confidence_score": 1.0,
        "urgency": "MEDIUM",
        "headline": "No action needed - SKU is not perishable",
        "detailed_reasoning": (
            f"{sku} is flagged is_perishable=False, so there is no expiry "
            f"risk to evaluate and the price is held."
        ),
    }

    # Same shape build_output_node produces, so this entry is indistinguishable
    # in structure from a fully-evaluated proposal once it lands in results /
    # the inventory-detailed topic.
    output = {
        "agent_id": "inventory_perishability",
        "sku_id": sku,
        "status": "NOT_PERISHABLE",
        "timestamp": timestamp,
        "metrics_evaluated": {
            "product_name": row.get("product_name", ""),
            "category": row.get("category", ""),
            "unit": row.get("unit", ""),
            "stock_on_hand": int(row["stock_on_hand"]) if row.get("stock_on_hand") is not None else None,
            "days_to_expiry": None,
            "avg_daily_units_sold": float(row["avg_daily_units_sold"]) if row.get("avg_daily_units_sold") is not None else None,
            "units_sold_last_24h": int(row["units_sold_last_24h"]) if row.get("units_sold_last_24h") is not None else None,
            "units_at_risk": None,
            "cost_price": float(row["cost_price"]) if row.get("cost_price") is not None else None,
            "expiry_loss_rate": None,
            "loss_if_no_action": 0.0,
        },
        "proposal": {
            "suggested_action": llm["suggested_action"],
            "price_modifier": llm["price_modifier"],
            "confidence_score": llm["confidence_score"],
            "urgency": llm["urgency"],
        },
        "justification": {
            "headline": llm["headline"],
            "detailed_reasoning": llm["detailed_reasoning"],
        },
    }
    state["results"].append(output)

    _write_log(
        {
            "log_type": "PROPOSAL",
            "run_id": state["run_id"],
            "sku_id": sku,
            "timestamp": timestamp,
            "status": "NOT_PERISHABLE",
            "urgency": llm["urgency"],
            "suggested_action": llm["suggested_action"],
            "price_modifier": llm["price_modifier"],
            "confidence_score": llm["confidence_score"],
            "loss_if_no_action": 0.0,
            "units_at_risk": None,
            "days_to_expiry": None,
            "recovery_floor": round(
                float(row.get("producer_buyback_rate", 0.0) or 0.0)
                + float(row.get("repurposing_recovery_rate", 0.0) or 0.0),
                4,
            ),
            "fallback_used": False,
        },
        PROPOSAL_LOG,
    )

    try:
        kafka_payload = build_kafka_payload(row, llm, "inventory_perishability")
        publish_proposal(kafka_payload, key=sku)
    except ValidationError as e:
        print(
            f"[skip_non_perishable] [{sku}] [WARNING]  "
            f"Kafka payload failed schema validation - not published"
        )
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")

    print(f"[skip_non_perishable] [{sku}] Not perishable - HOLD published, no LLM call made")
    return state


def compute_expiry_node(state: AgentState) -> AgentState:
    """
    Computes days_to_expiry and units_at_risk from the current row.
    If units_at_risk <= 0 the row does not need intervention;
    the conditional edge will skip ahead to advance_row.
    """
    row = state["current_row"]
    sku = row.get("sku_id", "UNKNOWN")

    now = datetime.now(timezone.utc)
    expiry = datetime.fromisoformat(row["expiry_datetime"].replace("Z", "+00:00"))
    days_to_expiry = max((expiry - now).total_seconds() / 86400, 0)

    avg_daily = float(row["avg_daily_units_sold"])
    stock = float(row["stock_on_hand"])
    units_at_risk = stock - (avg_daily * days_to_expiry)

    state["days_to_expiry"] = round(days_to_expiry, 2)
    state["units_at_risk"] = round(units_at_risk, 2)
    print(
        f"[compute_expiry] [{sku}] days_to_expiry={state['days_to_expiry']}  "
        f"units_at_risk={state['units_at_risk']}"
    )
    return state


def skip_no_risk_node(state: AgentState) -> AgentState:
    """
    Reached when units_at_risk <= 0 - the SKU has enough days_to_expiry relative
    to its sales velocity that no pricing action is needed. No LLM call is made
    (there's nothing to decide), but a deterministic HOLD message is still
    published to Kafka so every perishable SKU has a record on the topic, not
    just the ones that ended up at risk.
    """
    row = state["current_row"]
    sku = row.get("sku_id", "UNKNOWN")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    llm = {
        "suggested_action": "HOLD",
        "price_modifier": 1.0,
        "confidence_score": 1.0,
        "urgency": "MEDIUM",
        "headline": "No action needed - stock clears before expiry",
        "detailed_reasoning": (
            f"{row['avg_daily_units_sold']} units/day average sales against "
            f"{row['stock_on_hand']} units on hand with {state['days_to_expiry']} "
            f"days to expiry leaves no units at risk, so the price is held."
        ),
    }

    # Same shape build_output_node produces, so this entry is indistinguishable
    # in structure from a fully-evaluated proposal once it lands in results /
    # the inventory-detailed topic.
    output = {
        "agent_id": "inventory_perishability",
        "sku_id": sku,
        "status": "NO_ACTION_NEEDED",
        "timestamp": timestamp,
        "metrics_evaluated": {
            "product_name": row["product_name"],
            "category": row["category"],
            "unit": row["unit"],
            "stock_on_hand": int(row["stock_on_hand"]),
            "days_to_expiry": state["days_to_expiry"],
            "avg_daily_units_sold": float(row["avg_daily_units_sold"]),
            "units_sold_last_24h": int(row["units_sold_last_24h"]),
            "units_at_risk": state["units_at_risk"],
            "cost_price": float(row["cost_price"]),
            "expiry_loss_rate": None,
            "loss_if_no_action": 0.0,
        },
        "proposal": {
            "suggested_action": llm["suggested_action"],
            "price_modifier": llm["price_modifier"],
            "confidence_score": llm["confidence_score"],
            "urgency": llm["urgency"],
        },
        "justification": {
            "headline": llm["headline"],
            "detailed_reasoning": llm["detailed_reasoning"],
        },
    }
    state["results"].append(output)

    _write_log(
        {
            "log_type": "PROPOSAL",
            "run_id": state["run_id"],
            "sku_id": sku,
            "timestamp": timestamp,
            "status": "NO_ACTION_NEEDED",
            "urgency": llm["urgency"],
            "suggested_action": llm["suggested_action"],
            "price_modifier": llm["price_modifier"],
            "confidence_score": llm["confidence_score"],
            "loss_if_no_action": 0.0,
            "units_at_risk": state["units_at_risk"],
            "days_to_expiry": state["days_to_expiry"],
            "recovery_floor": round(
                float(row.get("producer_buyback_rate", 0.0))
                + float(row.get("repurposing_recovery_rate", 0.0)),
                4,
            ),
            "fallback_used": False,
        },
        PROPOSAL_LOG,
    )

    try:
        kafka_payload = build_kafka_payload(row, llm, "inventory_perishability")
        publish_proposal(kafka_payload, key=sku)
    except ValidationError as e:
        print(
            f"[skip_no_risk] [{sku}] [WARNING]  "
            f"Kafka payload failed schema validation - not published"
        )
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")

    print(f"[skip_no_risk] [{sku}] No units at risk - HOLD published, no LLM call made")
    return state


def compute_loss_node(state: AgentState) -> AgentState:
    """
    Computes the net expiry loss rate (after producer buyback and
    repurposing recovery) and the total dollar loss if no action is taken.
    """
    row = state["current_row"]
    sku = row.get("sku_id", "UNKNOWN")

    buyback = float(row["producer_buyback_rate"])
    repurposing = float(row["repurposing_recovery_rate"])
    cost_price = float(row["cost_price"])

    expiry_loss_rate = 1.0 - buyback - repurposing
    loss_if_no_action = state["units_at_risk"] * cost_price * expiry_loss_rate

    state["expiry_loss_rate"] = round(expiry_loss_rate, 4)
    state["loss_if_no_action"] = round(loss_if_no_action, 2)
    print(
        f"[compute_loss] [{sku}] expiry_loss_rate={state['expiry_loss_rate']}  "
        f"loss_if_no_action=${state['loss_if_no_action']}"
    )
    return state


def assign_urgency_node(state: AgentState) -> AgentState:
    """
    Assigns an urgency label based on days_to_expiry.
    IMMEDIATE (<= 1 day), HIGH (<= 3 days), MEDIUM (> 3 days).
    """
    d = state["days_to_expiry"]
    state["urgency"] = "IMMEDIATE" if d <= 1 else "HIGH" if d <= 3 else "MEDIUM"
    sku = state["current_row"].get("sku_id", "UNKNOWN")
    print(f"[assign_urgency] [{sku}] urgency={state['urgency']}")
    return state


def call_llm_node(state: AgentState) -> AgentState:
    """
    Builds the prompt, calls Gemini, validates via Pydantic, and writes
    a validation log record (including token counts) to validations.jsonl.
    Falls back to a rule-based proposal if parsing or validation fails.
    """
    row = state["current_row"]
    sku = row.get("sku_id", "UNKNOWN")

    history_block = _format_history(_load_recent_history(sku))

    prompt = f"""Product: {row['product_name']}
Category: {row['category']}
Unit: {row['unit']}
Stock on hand: {row['stock_on_hand']}
Days to expiry: {state['days_to_expiry']}
Avg daily units sold: {row['avg_daily_units_sold']}
Units sold last 24h: {row['units_sold_last_24h']}
Units at risk of expiry: {state['units_at_risk']}
Cost price (floor): ${float(row['cost_price'])}
Expiry loss rate: {state['expiry_loss_rate']}
Loss if no action taken: ${state['loss_if_no_action']}

{history_block}

Recommend a price modifier to clear stock before expiry."""

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        project=state["gcp_project"],
        location=os.getenv("GCP_LOCATION", "us-central1"),
        temperature=0.2,
    )
    messages = [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=prompt)]
    response = llm.invoke(messages)

    usage = response.usage_metadata or {}
    prompt_tokens = usage.get("input_tokens", 0)
    completion_tokens = usage.get("output_tokens", 0)
    total_tokens = usage.get("total_tokens", 0)
    cached_tokens = (usage.get("input_token_details") or {}).get("cache_read", 0)
    est_cost = round(
        (prompt_tokens / 1_000_000) * _PROMPT_COST_PER_1M
        + (completion_tokens / 1_000_000) * _COMPLETION_COST_PER_1M,
        8
    )

    token_record = {
        "sku_id": sku,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cached_tokens": cached_tokens,
        "estimated_cost_usd": est_cost,
    }
    state["all_token_usage"].append(token_record)

    proposal = parse_llm_response(response.content)
    failure_reason = None
    if proposal is not None and (proposal.suggested_action == "DISCOUNT") != (state["days_to_expiry"] < 3):
        failure_reason = (
            f"suggested_action={proposal.suggested_action} violates the days_to_expiry "
            f"rule (days_to_expiry={state['days_to_expiry']}) - discarding"
        )
        proposal = None
    fallback_used = proposal is None

    if fallback_used:
        raw = response.content.strip()
        if raw.startswith("```json"):
            raw = raw[7:]
        elif raw.startswith("```"):
            raw = raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()
        try:
            LLMProposal(**json.loads(raw))
        except Exception as exc:
            failure_reason = str(exc)[:300]

        print(f"[call_llm] [{sku}] [WARNING]  Validation failed - using fallback proposal")
        state["llm_response"] = fallback_proposal(state)
    else:
        state["llm_response"] = proposal.model_dump()
        print(
            f"[call_llm] [{sku}] [OK] modifier={state['llm_response']['price_modifier']}  "
            f"confidence={state['llm_response']['confidence_score']}"
        )

    validation_record = {
        "log_type": "VALIDATION",
        "run_id": state["run_id"],
        "sku_id": sku,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "validation_passed": not fallback_used,
        "fallback_triggered": fallback_used,
        "failure_reason": failure_reason,
        "model": "gemini-2.5-flash",
        "temperature": 0.2,
        "tokens": {
            "prompt": prompt_tokens,
            "completion": completion_tokens,
            "total": total_tokens,
            "cached": cached_tokens,
        },
        "estimated_cost_usd": est_cost,
    }
    _write_log(validation_record, VALIDATION_LOG)

    if fallback_used:
        publish_fall_reasoning(validation_record, key=sku)

    return state


def build_output_node(state: AgentState) -> AgentState:
    """Assembles the final JSON proposal, appends to results, and writes proposal log."""
    row = state["current_row"]
    llm = state["llm_response"]

    is_fallback = llm["confidence_score"] == 0.0
    status = "FALLBACK" if is_fallback else "COMPLETED"

    output = {
        "agent_id": "inventory_perishability",
        "sku_id": row["sku_id"],
        "status": status,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metrics_evaluated": {
            "product_name": row["product_name"],
            "category": row["category"],
            "unit": row["unit"],
            "stock_on_hand": int(row["stock_on_hand"]),
            "days_to_expiry": state["days_to_expiry"],
            "avg_daily_units_sold": float(row["avg_daily_units_sold"]),
            "units_sold_last_24h": int(row["units_sold_last_24h"]),
            "units_at_risk": state["units_at_risk"],
            "cost_price": float(row["cost_price"]),
            "expiry_loss_rate": state["expiry_loss_rate"],
            "loss_if_no_action": state["loss_if_no_action"],
        },
        "proposal": {
            "suggested_action": llm["suggested_action"],
            "price_modifier": llm["price_modifier"],
            "confidence_score": llm["confidence_score"],
            "urgency": state["urgency"],
        },
        "justification": {
            "headline": llm["headline"],
            "detailed_reasoning": llm["detailed_reasoning"],
        },
    }
    state["results"].append(output)

    _write_log(
        {
            "log_type": "PROPOSAL",
            "run_id": state["run_id"],
            "sku_id": row["sku_id"],
            "timestamp": output["timestamp"],
            "status": status,
            "urgency": state["urgency"],
            "suggested_action": llm["suggested_action"],
            "price_modifier": llm["price_modifier"],
            "confidence_score": llm["confidence_score"],
            "loss_if_no_action": state["loss_if_no_action"],
            "units_at_risk": state["units_at_risk"],
            "days_to_expiry": state["days_to_expiry"],
            "recovery_floor": round(
                float(row.get("producer_buyback_rate", 0.0))
                + float(row.get("repurposing_recovery_rate", 0.0)),
                4,
            ),
            "fallback_used": is_fallback,
        },
        PROPOSAL_LOG,
    )

    try:
        kafka_payload = build_kafka_payload(row, llm, output["agent_id"])
        publish_proposal(kafka_payload, key=row["sku_id"])
    except ValidationError as e:
        print(
            f"[build_output] [{row['sku_id']}] [WARNING]  "
            f"Kafka payload failed schema validation - not published"
        )
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")

    flag = " [WARNING]  [FALLBACK - human review required]" if is_fallback else ""
    print(f"[build_output] [{row['sku_id']}] Proposal ready{flag}")
    return state


def advance_row_node(state: AgentState) -> AgentState:
    """Increments the row cursor to move to the next SKU."""
    state["row_index"] += 1
    return state


def route_perishable(state: AgentState) -> str:
    return "compute_expiry" if state["is_perishable"] else "skip_non_perishable"


def route_units_at_risk(state: AgentState) -> str:
    return "compute_loss" if state["units_at_risk"] > 0 else "skip_no_risk"


def route_more_rows(state: AgentState) -> str:
    return "check_perishable" if state["row_index"] < len(state["rows"]) else END