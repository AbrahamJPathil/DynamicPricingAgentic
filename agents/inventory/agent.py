"""
Inventory & Perishability Agent
Dynamic Pricing POC - LangGraph + Gemini

Graph nodes:
    load_csv          -> check_perishable -> compute_expiry -> compute_loss
                      -> assign_urgency   -> call_llm       -> build_output
                      -> advance_row      -> (loop or END)

Run:
    python inventory_agent.py --api-key YOUR_KEY --csv products.csv
"""

import csv
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal, Optional, TypedDict

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from kafka_publisher import publish_proposal, flush as kafka_flush

# -- Audit log paths ------------------------------------------------------------
PROPOSAL_LOG = "proposals.jsonl"
VALIDATION_LOG = "validations.jsonl"

# Gemini 2.5 Flash pricing (USD per 1M tokens, as of June 2026)
_PROMPT_COST_PER_1M = 0.075
_COMPLETION_COST_PER_1M = 0.30


def _write_log(record: dict, path: str) -> None:
    """Appends a single JSON record to a JSONL audit file."""
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# -- System prompt --------------------------------------------------------------
SYSTEM_PROMPT = """You are a pricing agent for a retail grocery store.
You will be given inventory data for a perishable product that is at risk of expiring unsold.
Your job is to recommend an ideal selling price that:
1. Aggressively clears units before expiry
2. Never goes below the cost_price
3. Minimises total loss compared to loss_if_no_action

Respond ONLY with a valid JSON object using exactly this schema:
{
  "suggested_action": "DISCOUNT",
  "price_modifier": <float between 0.10 and 1.0, e.g. 0.65 means 65% of current price>,
  "confidence_score": <float between 0.0 and 1.0>,
  "urgency": <"IMMEDIATE" | "HIGH" | "MEDIUM">,
  "headline": "<one line summary, minimum 10 characters>",
  "detailed_reasoning": "<two to three sentence explanation, minimum 30 characters>"
}
No preamble, no markdown fences, only the JSON object."""


# -- Pydantic output schema (internal LLM contract) ------------------------------
class LLMProposal(BaseModel):
    suggested_action: Literal["DISCOUNT", "HOLD", "SURCHARGE"]
    price_modifier: float = Field(ge=0.10, le=1.5)
    confidence_score: float = Field(ge=0.0, le=1.0)
    urgency: Literal["IMMEDIATE", "HIGH", "MEDIUM"]
    headline: str = Field(min_length=10)
    detailed_reasoning: str = Field(min_length=30)

    # -- Field-level validators -------------------------------------------------

    @field_validator("price_modifier")
    @classmethod
    def modifier_not_suspiciously_low(cls, v: float) -> float:
        """Catches catastrophically low modifiers that are almost certainly hallucinations."""
        if v < 0.20:
            raise ValueError(
                f"price_modifier {v} is below 0.20 - likely a hallucination. "
                f"Minimum realistic discount is 20% off current price."
            )
        return v

    @field_validator("detailed_reasoning")
    @classmethod
    def reasoning_references_inventory(cls, v: str) -> str:
        """
        Catches responses where the LLM produced valid JSON but the reasoning
        is completely disconnected from the inventory metrics passed in the prompt.
        """
        keywords = ["expir", "days", "units", "stock", "loss", "clear", "risk", "cost"]
        if not any(kw in v.lower() for kw in keywords):
            raise ValueError(
                "detailed_reasoning does not reference any inventory metrics - "
                "likely a generic or hallucinated response."
            )
        return v

    # -- Cross-field validators -------------------------------------------------

    @model_validator(mode="after")
    def urgency_modifier_consistency(self) -> "LLMProposal":
        """
        An IMMEDIATE SKU with a modifier above 0.85 implies only a 15% discount -
        logically contradictory for a same-day expiry situation.
        """
        if self.urgency == "IMMEDIATE" and self.price_modifier > 0.85:
            raise ValueError(
                f"IMMEDIATE urgency but price_modifier={self.price_modifier} implies "
                f"only a {round((1 - self.price_modifier) * 100)}% discount - "
                f"insufficient for a same-day expiry SKU."
            )
        return self

    @model_validator(mode="after")
    def action_modifier_consistency(self) -> "LLMProposal":
        """
        A SURCHARGE should never have modifier < 1.0.
        A DISCOUNT should never have modifier >= 1.0.
        """
        if self.suggested_action == "SURCHARGE" and self.price_modifier < 1.0:
            raise ValueError(
                "suggested_action is SURCHARGE but price_modifier < 1.0 - contradictory."
            )
        if self.suggested_action == "DISCOUNT" and self.price_modifier >= 1.0:
            raise ValueError(
                "suggested_action is DISCOUNT but price_modifier >= 1.0 - contradictory."
            )
        return self


# -- Strict Kafka output schema (external contract) ------------------------------
# This is what actually gets published to the "inventory-agent" topic. It is
# deliberately much thinner than LLMProposal - the rich metrics (loss figures,
# urgency, days_to_expiry, etc.) stay in proposals.jsonl/validations.jsonl for
# debugging; only agent_id/sku/recommendation/rationale go out on the wire,
# since that's the contract downstream consumers (and the UI) actually depend on.
class KafkaRecommendation(BaseModel):
    suggested_modifier: float = Field(
        ge=-1.0,
        le=1.0,
        description=(
            "Signed fractional price change, e.g. -0.05 for a 5% discount, "
            "+0.10 for a 10% surcharge."
        ),
    )
    confidence: float = Field(ge=0.0, le=1.0)


class KafkaProposal(BaseModel):
    agent_id: str
    sku: str
    recommendation: KafkaRecommendation
    rationale: str = Field(min_length=10)


# -- LLM response parser --------------------------------------------------------
def parse_llm_response(raw: str) -> Optional[LLMProposal]:
    """
    Cleans markdown fences, parses JSON, and validates against LLMProposal schema.
    Returns None on any failure - caller must invoke fallback_proposal().
    """
    # Step 1: strip markdown fences correctly
    raw = raw.strip()
    if raw.startswith("```json"):
        raw = raw[7:]
    elif raw.startswith("```"):
        raw = raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3]
    raw = raw.strip()

    # Step 2: parse JSON
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse_llm_response] JSON parse failed: {e}")
        print(f"[parse_llm_response] Raw output: {raw[:200]}")
        return None

    # Step 3: validate against Pydantic schema
    try:
        return LLMProposal(**data)
    except ValidationError as e:
        print("[parse_llm_response] Schema validation failed:")
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")
        return None


# -- Rule-based fallback proposal -----------------------------------------------
def fallback_proposal(state: "AgentState") -> dict:
    """
    Emitted when LLM output fails parsing or Pydantic validation.
    Uses a deterministic risk-ratio formula for price_modifier.

    Floor is derived from the SKU's actual recovery rates:
        recovery_floor = producer_buyback_rate + repurposing_recovery_rate
    This means SKUs with zero recovery (e.g. deli, bakery with no buyback
    and no repurposing) get a steeper floor than SKUs with meaningful
    supplier credit or repurposing options. The floor represents the
    minimum modifier at which selling is still better than expiry.

    confidence_score=0.0 signals to the orchestrator that this proposal
    was not LLM-generated and requires human review before applying.
    """
    row = state["current_row"]
    d = state["days_to_expiry"]
    stock = float(row["stock_on_hand"])
    risk_ratio = state["units_at_risk"] / stock if stock > 0 else 1.0

    # Dynamic floor: sum of what the store recovers per unit if it expires anyway.
    # Selling at this modifier = breaking even vs doing nothing.
    # Never allow floor to be 0.0 - always push for some revenue over pure loss.
    producer_buyback = float(row.get("producer_buyback_rate", 0.0))
    repurposing = float(row.get("repurposing_recovery_rate", 0.0))
    recovery_floor = round(producer_buyback + repurposing, 4)

    # Higher risk ratio -> steeper discount, floored at recovery_floor
    base_modifier = round(max(1.0 - (risk_ratio * 0.6), recovery_floor), 2)

    return {
        "suggested_action": "DISCOUNT",
        "price_modifier": base_modifier,
        "confidence_score": 0.0,
        "urgency": state["urgency"],
        "headline": "Fallback proposal - LLM output failed validation",
        "detailed_reasoning": (
            f"Rule-based fallback applied. {state['units_at_risk']} units at risk "
            f"with {d} days to expiry. Modifier {base_modifier} derived from "
            f"risk ratio {risk_ratio:.2f}, floored at recovery rate "
            f"{recovery_floor} (buyback={producer_buyback} + "
            f"repurposing={repurposing}). Manual review required."
        ),
    }


# -- Shared state ---------------------------------------------------------------
class AgentState(TypedDict):
    # inputs
    csv_path: str
    api_key: str
    # run identifier - shared across all log records in one pipeline execution
    run_id: str
    # populated by load_csv_node
    rows: List[dict]
    # current row being processed
    current_row: Optional[dict]
    # populated by check_perishable_node
    is_perishable: Optional[bool]
    # populated by compute_expiry_node
    days_to_expiry: Optional[float]
    units_at_risk: Optional[float]
    # populated by compute_loss_node
    expiry_loss_rate: Optional[float]
    loss_if_no_action: Optional[float]
    # populated by assign_urgency_node
    urgency: Optional[str]
    # populated by call_llm_node
    llm_response: Optional[dict]
    # accumulated across all rows
    results: List[dict]
    all_token_usage: List[dict]  # one entry per SKU with an LLM call
    # internal cursor
    row_index: int
    # populated by sort_by_urgency_node - full sorted processing queue
    urgency_queue: List[dict]


# -- Node 1: load_csv -----------------------------------------------------------
def load_csv_node(state: AgentState) -> AgentState:
    """Reads the CSV and loads all rows into state."""
    with open(state["csv_path"], newline="") as f:
        state["rows"] = list(csv.DictReader(f))
    state["row_index"] = 0
    state["results"] = []
    state["urgency_queue"] = []
    state["all_token_usage"] = []
    print(f"[load_csv] Loaded {len(state['rows'])} row(s)  run_id={state['run_id']}")
    return state


# -- Node 1b: sort_by_urgency ---------------------------------------------------
# Urgency tier weights - lower number = processed first
URGENCY_RANK = {"IMMEDIATE": 0, "HIGH": 1, "MEDIUM": 2, "SKIP": 3}


def _precompute_urgency(row: dict) -> tuple[str, float]:
    """
    Lightweight pre-scan for a single row - no LLM, pure Python math.
    Returns (urgency_label, loss_if_no_action) for sorting purposes.
    Rows that are non-perishable or have no units at risk return ("SKIP", 0.0).
    """
    if row.get("is_perishable", "").strip().upper() != "TRUE":
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

    # Sort: urgency tier first (ascending rank), loss descending as tiebreaker
    scored.sort(key=lambda x: (URGENCY_RANK[x["urgency"]], -x["loss_if_no_action"]))

    # Replace rows with sorted order so the loop processes them in priority order
    state["rows"] = [s["row"] for s in scored]

    # Build the urgency queue - used only for display
    state["urgency_queue"] = [
        {k: v for k, v in s.items() if k != "row"} for s in scored
    ]

    # -- Print the full processing queue before any LLM call -------------------
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


# -- Node 2: check_perishable ---------------------------------------------------
def check_perishable_node(state: AgentState) -> AgentState:
    """
    Picks the current row and checks whether it is perishable.
    Writes True/False to is_perishable - the conditional edge
    uses this to skip non-perishable SKUs immediately.
    """
    row = state["rows"][state["row_index"]]
    state["current_row"] = row
    state["is_perishable"] = row.get("is_perishable", "").strip().upper() == "TRUE"
    sku = row.get("sku_id", "UNKNOWN")
    print(f"[check_perishable] [{sku}] is_perishable={state['is_perishable']}")
    return state


# -- Node 3: compute_expiry -----------------------------------------------------
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


# -- Node 4: compute_loss -------------------------------------------------------
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


# -- Node 5: assign_urgency -----------------------------------------------------
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


# -- Node 6: call_llm -----------------------------------------------------------
def call_llm_node(state: AgentState) -> AgentState:
    """
    Builds the prompt, calls Gemini, validates via Pydantic, and writes
    a validation log record (including token counts) to validations.jsonl.
    Falls back to a rule-based proposal if parsing or validation fails.
    """
    row = state["current_row"]
    sku = row.get("sku_id", "UNKNOWN")

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

Recommend a price modifier to clear stock before expiry."""

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=state["api_key"],
        temperature=0.2,
    )
    messages = [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=prompt)]
    response = llm.invoke(messages)

    # -- Extract token usage from response.usage_metadata ---------------------
    # LangChain wraps Gemini token counts in response.usage_metadata (not
    # response.response_metadata). Keys are: input_tokens, output_tokens,
    # total_tokens, input_token_details (dict with cache_read key).
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

    # -- Parse and validate via Pydantic ---------------------------------------
    proposal = parse_llm_response(response.content)
    fallback_used = proposal is None
    failure_reason = None

    if fallback_used:
        # Collect the first validation error message for the log
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

    # -- Write validation log ---------------------------------------------------
    _write_log(
        {
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
        },
        VALIDATION_LOG,
    )

    return state


# -- Kafka payload builder --------------------------------------------------------
def _modifier_to_delta(price_modifier: float) -> float:
    """
    Converts the internal multiplier representation (0.65 = 65% of current
    price, i.e. a 35% discount) into the signed delta the Kafka schema
    expects (-0.35). A multiplier of 1.0 maps to a delta of 0.0 - no change.
    """
    return round(price_modifier - 1.0, 2)


def _build_rationale(headline: str, reasoning: str) -> str:
    """Joins the short headline and the longer reasoning into one readable string for the UI."""
    headline = headline.strip()
    if headline and headline[-1] not in ".!?":
        headline += "."
    return f"{headline} {reasoning.strip()}"


def build_kafka_payload(row: dict, llm: dict, agent_id: str) -> dict:
    """
    Builds and validates the strict external payload published to Kafka:
        {agent_id, sku, recommendation: {suggested_modifier, confidence}, rationale}

    Raises pydantic.ValidationError if the proposal can't be mapped onto the
    external contract - acts as a final safety net before anything leaves the
    process, on top of the LLMProposal/fallback_proposal checks that already
    ran upstream in call_llm_node.
    """
    payload = KafkaProposal(
        agent_id=agent_id,
        sku=row["sku_id"],
        recommendation=KafkaRecommendation(
            suggested_modifier=_modifier_to_delta(llm["price_modifier"]),
            confidence=round(llm["confidence_score"], 2),
        ),
        rationale=_build_rationale(llm["headline"], llm["detailed_reasoning"]),
    )
    return payload.model_dump()


# -- Node 7: build_output -------------------------------------------------------
def build_output_node(state: AgentState) -> AgentState:
    """Assembles the final JSON proposal, appends to results, and writes proposal log."""
    row = state["current_row"]
    llm = state["llm_response"]

    # If confidence_score is 0.0 it is a fallback proposal - mark status accordingly
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

    # -- Write proposal log (full internal detail, for audit/debugging) ---------
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

    # -- Publish to Kafka (inventory-agent topic) --------------------------------
    # Only the strict external contract goes on the wire - the rich internal
    # metrics above stay in proposals.jsonl. Non-blocking - the message is
    # handed to the producer's internal buffer and sent asynchronously;
    # flush() in main() guarantees delivery before exit.
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


# -- Node 8: advance_row --------------------------------------------------------
def advance_row_node(state: AgentState) -> AgentState:
    """Increments the row cursor to move to the next SKU."""
    state["row_index"] += 1
    return state


# -- Conditional edge: is this SKU perishable? ----------------------------------
def route_perishable(state: AgentState) -> str:
    return "compute_expiry" if state["is_perishable"] else "advance_row"


# -- Conditional edge: are any units at risk? -----------------------------------
def route_units_at_risk(state: AgentState) -> str:
    return "compute_loss" if state["units_at_risk"] > 0 else "advance_row"


# -- Conditional edge: are there more rows? -------------------------------------
def route_more_rows(state: AgentState) -> str:
    return "check_perishable" if state["row_index"] < len(state["rows"]) else END


# -- Build graph ----------------------------------------------------------------
def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    graph.add_node("load_csv", load_csv_node)
    graph.add_node("sort_by_urgency", sort_by_urgency_node)
    graph.add_node("check_perishable", check_perishable_node)
    graph.add_node("compute_expiry", compute_expiry_node)
    graph.add_node("compute_loss", compute_loss_node)
    graph.add_node("assign_urgency", assign_urgency_node)
    graph.add_node("call_llm", call_llm_node)
    graph.add_node("build_output", build_output_node)
    graph.add_node("advance_row", advance_row_node)

    graph.set_entry_point("load_csv")
    graph.add_edge("load_csv", "sort_by_urgency")
    graph.add_edge("sort_by_urgency", "check_perishable")

    graph.add_conditional_edges(
        "check_perishable",
        route_perishable,
        {"compute_expiry": "compute_expiry", "advance_row": "advance_row"},
    )

    graph.add_conditional_edges(
        "compute_expiry",
        route_units_at_risk,
        {"compute_loss": "compute_loss", "advance_row": "advance_row"},
    )

    graph.add_edge("compute_loss", "assign_urgency")
    graph.add_edge("assign_urgency", "call_llm")
    graph.add_edge("call_llm", "build_output")
    graph.add_edge("build_output", "advance_row")

    graph.add_conditional_edges(
        "advance_row",
        route_more_rows,
        {"check_perishable": "check_perishable", END: END},
    )

    return graph.compile()


# -- Entry point ----------------------------------------------------------------
def main():
    # -- Load environment variables from .env -----------------------------------
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY not found in environment. "
            "Ensure a .env file exists at the project root with:\n"
            "  GEMINI_API_KEY=your_key_here"
        )

    # -- Hardcoded CSV path relative to this file -------------------------------
    csv_path = (
        Path(__file__).resolve().parent.parent.parent
        / "data"
        / "inputs"
        / "inventory-agent"
        / "products_inv.csv"
    )
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Inventory CSV not found at expected path:\n  {csv_path}\n"
            "Ensure products_inv.csv exists under data/inputs/inventory-agent/"
        )

    app = build_graph()
    run_id = str(uuid.uuid4())[:8]
    print(f"\n[main] Starting run  run_id={run_id}")
    print(f"[main] CSV           -> {csv_path}")

    initial_state: AgentState = {
        "csv_path": str(csv_path),
        "api_key": api_key,
        "run_id": run_id,
        "rows": [],
        "current_row": None,
        "is_perishable": None,
        "days_to_expiry": None,
        "units_at_risk": None,
        "expiry_loss_rate": None,
        "loss_if_no_action": None,
        "urgency": None,
        "llm_response": None,
        "results": [],
        "all_token_usage": [],
        "row_index": 0,
        "urgency_queue": [],
    }

    final_state = app.invoke(initial_state)

    # -- Token summary ----------------------------------------------------------
    usage = final_state["all_token_usage"]
    sep = "-" * 62
    print(f"\n[main] {sep}")
    print(f"[main]  TOKEN SUMMARY  run_id={run_id}")
    print(f"[main] {sep}")
    print(
        f"[main]  {'SKU':<10} {'PROMPT':>8} {'COMPLETION':>12} {'TOTAL':>8} {'COST (USD)':>12}"
    )
    print(f"[main] {sep}")
    grand_prompt = grand_completion = grand_total = grand_cost = 0
    for t in usage:
        print(
            f"[main]  {t['sku_id']:<10} {t['prompt_tokens']:>8} "
            f"{t['completion_tokens']:>12} {t['total_tokens']:>8} "
            f"{t['estimated_cost_usd']:>12.6f}"
        )
        grand_prompt += t["prompt_tokens"]
        grand_completion += t["completion_tokens"]
        grand_total += t["total_tokens"]
        grand_cost += t["estimated_cost_usd"]
    print(f"[main] {sep}")
    print(
        f"[main]  {'TOTAL':<10} {grand_prompt:>8} {grand_completion:>12} "
        f"{grand_total:>8} {grand_cost:>12.6f}"
    )
    print(f"[main] {sep}")
    print(f"[main]  Proposal log   -> {PROPOSAL_LOG}")
    print(f"[main]  Validation log -> {VALIDATION_LOG}")
    print(f"[main] {sep}\n")

    # -- Ensure every buffered Kafka message is actually sent before exiting ---
    kafka_flush()

    print(f"[OK] Done - {len(final_state['results'])} proposal(s) generated\n")
    for output in final_state["results"]:
        print(json.dumps(output, indent=2))
        print()


if __name__ == "__main__":
    main()