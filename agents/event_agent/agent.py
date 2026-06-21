"""
Event & Festivity Pricing Agent  (formerly "Calendar Agent")
Dynamic Pricing POC - LangGraph + Gemini + Kafka + Supabase

Rewritten to match the Inventory agent's architecture and unified contract:
  - The pricing decision (action / price_modifier / confidence_score) is now
    computed deterministically in Python (determine_decision_node), never by
    the LLM. The LLM's only job is to write headline + detailed_reasoning,
    which is schema-validated (Pydantic) with a deterministic fallback - so
    an odd/malformed Gemini response can no longer crash the whole run (the
    old version had zero error handling around the LLM call at all).
  - price_modifier is a SIGNED FRACTIONAL DELTA everywhere (0.0857 = +8.57%,
    -0.05 = -5%, 0.0 = no change), not a multiplier - so every action's
    modifier stays comfortably under 1.0, instead of only DISCOUNT landing
    there naturally while SURCHARGE/HOLD broke that pattern under the old
    multiplier representation.
  - Token usage + cost is tracked and logged exactly like the inventory agent.
  - Two LOCAL audit logs are written every run: event_agent_proposals.jsonl
    and event_agent_validations.jsonl (same shape/spirit as the inventory
    agent's proposals.jsonl/validations.jsonl, kept under separate filenames
    so the two agents don't clobber each other's logs if run from the same
    cwd). These are distinct from the Supabase event_proposals TABLE below -
    one's a local file, one's a remote table, deliberately not given the
    identical name.
  - Every proposal is published to FOUR places:
      1. Kafka, on the "event-agent" topic (kafka_publisher.py) - the slim
         external contract.
      2. Supabase, as a row in the "event_proposals" table
         (supabase_client.py) - same slim external contract.
      3. Kafka, on the "calendar-detailed" topic (kafka_publisher.py) - the
         FULL internal `output` dict (metrics_evaluated + proposal +
         justification), for consumers that want everything this agent
         computed for the SKU rather than just the cross-agent shape.
      4. Kafka, on the "calendar-agent" topic (kafka_publisher.py) - the
         SAME slim payload as (1), republished under this agent's own
         topic name for consumers that want this agent's proposals
         specifically rather than the shared cross-agent "event-agent"
         stream.
    The slim contract used for (1), (2), and (4) is the SAME shape as the
    inventory agent (agent_id / sku / recommendation{action,
    suggested_modifier, confidence} / rationale), built once and validated
    once (build_proposal_payload), then published to each destination
    independently - if one fails (broker down, Supabase unreachable), the
    others still go through. (3) is published separately, straight from the
    `output` dict, with no dependency on that schema's validation passing.
    HOLD_EXEMPT collapses to HOLD on the wire; the exemption itself stays
    visible in metrics_evaluated.surcharge_exempt_triggered for audit/UI use.
  - The product catalog is fetched from Supabase's products_sku table
    (supabase_client.fetch_products) instead of a CSV file.
  - SKUs with no festival AND no public holiday in the lookahead window skip
    the LLM call entirely (skip_justification_node) and get a deterministic
    HOLD, mirroring the inventory agent's skip_no_risk_node - this will be
    the majority of rows on most days, so it meaningfully cuts LLM cost.
  - Removed dead code (unused route_has_festival, MOCK_FESTIVAL_CALENDAR,
    bootstrap_festival_json, unused imports) and the /mnt/user-data/uploads
    CSV-copy hack from main().
  - today_str now defaults to the real current UTC date (overridable via the
    CALENDAR_AGENT_TODAY env var for backtesting), instead of being
    hardcoded to one specific date.
  - GCP_PROJECT_ID is read inside main() instead of at import time, so
    importing build_graph() (e.g. from an orchestrator) no longer crashes
    the process if the env var isn't set yet.
  - The festival calendar and the `holidays` library's calendar are loaded
    ONCE per run (load_catalog_node) instead of being re-read/recomputed on
    every single row.

Graph nodes:
    load_catalog -> check_next_sku -> scan_festivals -> check_holidays
                 -> compute_lift -> determine_decision -> assign_urgency
                 -> [skip_justification | call_llm] -> build_output
                 -> advance_row -> (loop or END)

Run:
    python agent.py
"""



from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Literal, Optional, TypedDict

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field, ValidationError, field_validator

from supabase_client import fetch_products, insert_proposal
from kafka_publisher import publish_proposal as kafka_publish, flush as kafka_flush

try:
    import holidays as hol_lib
    _HOLIDAYS_AVAILABLE = True
except ImportError:
    hol_lib = None
    _HOLIDAYS_AVAILABLE = False

# -- Lookahead / decay constants --------------------------------------------
LOOKAHEAD_DAYS = 21
PROX_WINDOW_DAYS = 7

# -- Audit log paths (separate filenames from the inventory agent on purpose,
#    so both agents can run from the same working directory without
#    clobbering each other's logs. Deliberately NOT named "event_proposals" -
#    that name belongs to the Supabase table; these are local JSONL files,
#    a different artifact, so they get a distinguishing event_agent_ prefix
#    instead of an identical name to avoid "which one do you mean" confusion
#    when debugging.) --------------------------------------------------------
PROPOSAL_LOG = "event_agent_proposals.jsonl"
VALIDATION_LOG = "event_agent_validations.jsonl"

# Gemini 2.5 Flash pricing (USD per 1M tokens, as of June 2026)
_PROMPT_COST_PER_1M = 0.075
_COMPLETION_COST_PER_1M = 0.30


def _write_log(record: dict, path: str) -> None:
    """Appends a single JSON record to a JSONL audit file."""
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def _festival_date_for_year(festival: dict, year: int) -> Optional[date]:
    """Resolves a festival entry to a concrete date for the given year."""
    if festival.get("date_hint"):
        try:
            return date.fromisoformat(festival["date_hint"]).replace(year=year)
        except ValueError:
            pass
    if festival.get("month") and festival.get("day"):
        try:
            return date(year, festival["month"], festival["day"])
        except ValueError:
            return None
    return None


# -- System prompt ------------------------------------------------------------
# Deliberately narrow scope: the LLM is given the decision that was already
# made deterministically in Python and is asked only to explain it in plain
# language. It is never asked to choose the action or compute a number -
# the same "never asked to perform arithmetic" philosophy as the inventory
# agent, taken one step further since here even the categorical decision is
# pre-computed.
SYSTEM_PROMPT = """You are a festival & calendar pricing agent for a retail grocery store.

A pricing decision (suggested_action, price_modifier, confidence_score) has
ALREADY been made deterministically in Python from the metrics below. Do not
change it, recompute it, or second-guess it. Your only job is to write a
short headline and a 2-3 sentence detailed_reasoning that explains WHY that
decision makes sense, grounded in the specific numbers you were given
(festival name, days_to_event, demand_lift_factor, public_holiday,
surcharge_exempt_triggered, decided_action, decided_price_modifier, etc).

IMPORTANT - decided_price_modifier is a SIGNED FRACTIONAL CHANGE, not a
multiplier: 0.0857 means "raise the price by 8.57%", -0.05 means "drop the
price by 5%", 0.0 means "no change". Describe it that way (e.g. "an 8.57%
surcharge") - do not describe it as if it were the new price's multiplier.

Be concrete, never vague. Do not use hand-wavy phrases like "significant
risk", "moderate confidence", "various factors", "some units", "a portion
of", or "relatively high/low". Every sentence must cite at least one
specific number you were actually given.

Respond ONLY with a valid JSON object - no markdown fences, no preamble:
{
  "headline": "<one-line summary, minimum 10 characters>",
  "detailed_reasoning": "<2-3 sentence explanation, minimum 30 characters>"
}"""

# -- Phrases that signal hand-wavy, non-numeric reasoning - shared blocklist --
_VAGUE_PHRASES = [
    "significant risk", "moderate confidence", "some units", "a portion of",
    "various factors", "a number of", "relatively high", "relatively low",
    "a certain amount", "quite a bit", "fairly significant", "somewhat",
]


# -- Pydantic schema for the LLM's (narrow) output ----------------------------
class CalendarJustification(BaseModel):
    headline: str = Field(min_length=10)
    detailed_reasoning: str = Field(min_length=30)

    @field_validator("detailed_reasoning")
    @classmethod
    def references_metrics(cls, v: str) -> str:
        """Rejects reasoning that doesn't engage with any calendar metric at all."""
        keywords = [
            "festival", "holiday", "lift", "day", "demand", "exempt",
            "surcharge", "discount", "hold", "price", "event",
        ]
        if not any(kw in v.lower() for kw in keywords):
            raise ValueError(
                "detailed_reasoning does not reference any calendar metrics - "
                "likely a generic or hallucinated response."
            )
        return v

    @field_validator("detailed_reasoning")
    @classmethod
    def not_vague(cls, v: str) -> str:
        """Deterministic backstop behind the system prompt's anti-vagueness instruction."""
        lowered = v.lower()
        hit = next((p for p in _VAGUE_PHRASES if p in lowered), None)
        if hit:
            raise ValueError(
                f'detailed_reasoning contains vague language ("{hit}") instead of '
                f"citing specific numbers from the data provided."
            )
        return v


# -- Strict Kafka output schema (external contract) ---------------------------
# Identical shape to the inventory agent's contract so downstream consumers
# handle every agent's proposals the same way, regardless of source.
class KafkaRecommendation(BaseModel):
    action: Literal["DISCOUNT", "HOLD", "SURCHARGE"]
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


# Internal 4-way action collapses to the 3-way external contract on the wire.
# The exemption itself is still visible internally via surcharge_exempt_triggered.
_ACTION_TO_KAFKA: dict[str, str] = {
    "DISCOUNT": "DISCOUNT",
    "HOLD": "HOLD",
    "SURCHARGE": "SURCHARGE",
    "HOLD_EXEMPT": "HOLD",
}


def _build_rationale(headline: str, reasoning: str) -> str:
    """Joins headline + reasoning into one readable string for the UI."""
    headline = headline.strip()
    if headline and headline[-1] not in ".!?":
        headline += "."
    return f"{headline} {reasoning.strip()}"


def build_proposal_payload(row: dict, decision: dict, justification: dict, agent_id: str) -> dict:
    """
    Builds and validates the strict external payload published to BOTH
    transports (Kafka topic + Supabase table) - identical shape either way,
    so this one validated dict feeds both publish calls in build_output_node.

    decision["price_modifier"] is already a signed delta (see
    determine_decision_node), so it's passed straight through - no
    multiplier-to-delta conversion needed here anymore. (Previously this
    function subtracted 1.0 via _modifier_to_delta() to convert a multiplier
    like 1.0857 into a delta like 0.0857; now that determine_decision_node
    produces the delta directly, doing that subtraction again here would
    silently produce a wrong, garbage value - e.g. 0.0857 - 1.0 = -0.9143.)

    Raises pydantic.ValidationError if it can't be mapped onto the external
    contract - a final safety net before anything leaves the process,
    and gates BOTH downstream publishes (if the payload's invalid, neither
    Kafka nor Supabase should get it).
    """
    payload = KafkaProposal(
        agent_id=agent_id,
        sku=row["sku_id"],
        recommendation=KafkaRecommendation(
            action=_ACTION_TO_KAFKA[decision["suggested_action"]],
            suggested_modifier=round(decision["price_modifier"], 4),
            confidence=round(decision["confidence_score"], 2),
        ),
        rationale=_build_rationale(justification["headline"], justification["detailed_reasoning"]),
    )
    return payload.model_dump()


# -- LLM response parser -------------------------------------------------------
def _extract_text(raw_content: Any) -> str:
    """Gemini occasionally returns a list of content blocks instead of a plain
    string - normalise either shape down to plain text."""
    if isinstance(raw_content, list):
        text = ""
        for block in raw_content:
            if isinstance(block, dict) and "text" in block:
                text += block["text"]
            elif isinstance(block, str):
                text += block
        return text
    return str(raw_content)


def parse_llm_justification(raw_content: Any) -> Optional[CalendarJustification]:
    """Strips markdown fences, parses JSON, validates against CalendarJustification.
    Returns None on any failure - caller must invoke fallback_justification()."""
    raw = _extract_text(raw_content).strip()
    if raw.startswith("```json"):
        raw = raw[7:]
    elif raw.startswith("```"):
        raw = raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3]
    raw = raw.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parse_llm_justification] JSON parse failed: {e}")
        print(f"[parse_llm_justification] Raw output: {raw[:200]}")
        return None

    try:
        return CalendarJustification(**data)
    except ValidationError as e:
        print("[parse_llm_justification] Schema validation failed:")
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")
        return None


def fallback_justification(state: "AgentState") -> dict:
    """Deterministic headline/reasoning emitted when the LLM output fails
    parsing or validation. confidence_score itself is unaffected since it's
    computed deterministically elsewhere - this only replaces the narrative."""
    row = state["current_row"]
    decision = state["decision"]
    if state["festival_name"]:
        context = (
            f"{state['festival_name']} is {state['days_to_event']} day(s) away, "
            f"giving a computed demand_lift_factor of {state['demand_lift_factor']}."
        )
    else:
        context = (
            f"No festival was found in the next {LOOKAHEAD_DAYS} days, so "
            f"demand_lift_factor stayed at {state['demand_lift_factor']}."
        )
    return {
        "headline": f"Fallback justification for {row['sku_id']} - LLM output failed validation",
        "detailed_reasoning": (
            f"Rule-based fallback applied. {context} Suggested action is "
            f"{decision['suggested_action']} with a price change of "
            f"{decision['price_modifier'] * 100:.2f}% and confidence "
            f"{decision['confidence_score']}, both computed deterministically "
            f"in Python. Manual review recommended."
        ),
    }


# -- Shared state ---------------------------------------------------------------
class AgentState(TypedDict):
    # inputs
    festival_json_path: str
    gcp_project: str
    run_id: str
    today_str: str
    # loaded once in load_catalog_node
    rows: List[dict]
    festival_calendar: List[dict]
    holidays_calendar: Optional[Any]
    # cursor
    row_index: int
    current_row: Optional[dict]
    # scan_festivals_node
    festival_name: Optional[str]
    days_to_event: int
    base_lift: float
    categories_affected: List[str]
    surcharge_exempt_triggered: bool
    # check_holidays_node
    public_holiday: Optional[str]
    holiday_days_away: int
    # compute_lift_node
    demand_lift_factor: float
    days_to_nearest_event: int
    # determine_decision_node
    decision: Optional[dict]
    # assign_urgency_node
    urgency: Optional[str]
    # call_llm_node / skip_justification_node
    llm_response: Optional[dict]
    justification_is_fallback: bool
    # accumulated
    results: List[dict]
    all_token_usage: List[dict]


# -- Node 1: load_catalog --------------------------------------------------------
def load_catalog_node(state: AgentState) -> AgentState:
    """Fetches the product catalog from Supabase (products_sku table) and reads
    the festival calendar from disk, both ONCE per run. (if available)
    pre-builds the `holidays` library's calendar for the relevant years."""
    state["rows"] = fetch_products()
    state["row_index"] = 0
    state["results"] = []
    state["all_token_usage"] = []

    with open(state["festival_json_path"]) as fh:
        state["festival_calendar"] = json.load(fh)

    if _HOLIDAYS_AVAILABLE:
        today = date.fromisoformat(state["today_str"])
        state["holidays_calendar"] = hol_lib.US(years=[today.year, today.year + 1])
    else:
        state["holidays_calendar"] = None
        print("[load_catalog] 'holidays' package not installed - public holiday checks will be skipped")

    print(f"[load_catalog] Loaded {len(state['rows'])} SKU(s) from Supabase and "
          f"{len(state['festival_calendar'])} festival(s)  run_id={state['run_id']}")
    return state


# -- Node 2: check_next_sku ------------------------------------------------------
def check_next_sku_node(state: AgentState) -> AgentState:
    """Picks the row at row_index into current_row."""
    state["current_row"] = state["rows"][state["row_index"]]
    row = state["current_row"]
    print(f"[check_next_sku] Processing SKU {row['sku_id']} - {row['product_name']}")
    return state


# -- Node 3: scan_festivals -------------------------------------------------------
def scan_festivals_node(state: AgentState) -> AgentState:
    """Scans the festival calendar (loaded once in load_catalog_node) for the
    next LOOKAHEAD_DAYS, matching on the current SKU's category."""
    today = date.fromisoformat(state["today_str"])
    category = str(state["current_row"].get("category", "")).lower()

    best_festival: Optional[dict] = None
    best_days_to: int = LOOKAHEAD_DAYS + 1

    for fest in state["festival_calendar"]:
        for yr in (today.year, today.year + 1):
            fest_date = _festival_date_for_year(fest, yr)
            if fest_date is None:
                continue
            days_to = (fest_date - today).days
            if 0 <= days_to <= LOOKAHEAD_DAYS and category in [c.lower() for c in fest["categories"]]:
                if days_to < best_days_to:
                    best_festival = fest
                    best_days_to = days_to

    if best_festival:
        is_exempt = category in [c.lower() for c in best_festival.get("surcharge_exempt", [])]
        state["festival_name"] = best_festival["name"]
        state["days_to_event"] = best_days_to
        state["categories_affected"] = best_festival["categories"]
        state["surcharge_exempt_triggered"] = is_exempt
        state["base_lift"] = 1.0 if is_exempt else best_festival["base_lift"]
        print(f"[scan_festivals] Found: {best_festival['name']} in {best_days_to} day(s) -> "
              f"{'EXEMPT' if is_exempt else 'AFFECTED'}")
    else:
        state["festival_name"] = None
        state["days_to_event"] = -1
        state["base_lift"] = 1.0
        state["categories_affected"] = []
        state["surcharge_exempt_triggered"] = False
        print(f"[scan_festivals] No relevant festival in next {LOOKAHEAD_DAYS} days")

    return state


# -- Node 4: check_holidays --------------------------------------------------------
def check_holidays_node(state: AgentState) -> AgentState:
    """Checks the pre-built US holiday calendar for the next LOOKAHEAD_DAYS."""
    today = date.fromisoformat(state["today_str"])
    public_holiday_name: Optional[str] = None
    holiday_days_away: int = -1

    us_holidays = state.get("holidays_calendar")
    if us_holidays is not None:
        for delta in range(LOOKAHEAD_DAYS + 1):
            check_date = today + timedelta(days=delta)
            if check_date in us_holidays:
                public_holiday_name = us_holidays[check_date]
                holiday_days_away = delta
                break

    state["public_holiday"] = public_holiday_name
    state["holiday_days_away"] = holiday_days_away

    if public_holiday_name:
        print(f"[check_holidays] {public_holiday_name} in {holiday_days_away} day(s)")
    else:
        print(f"[check_holidays] No public holidays in next {LOOKAHEAD_DAYS} days")
    return state


# -- Node 5: compute_lift -----------------------------------------------------------
def compute_lift_node(state: AgentState) -> AgentState:
    """
    Time-decay formula:
        decay = max(0, 1 - (days_to_event / PROX_WINDOW_DAYS))
        lift  = 1.0 + (base_lift - 1.0) * decay
    Lift = 1.0 at day 7+ (curve hasn't started). Lift = base_lift at day 0.
    No festival -> lift stays at 1.0.

    Also tracks days_to_nearest_event = the closer of (festival, public
    holiday), used purely to size confidence_score deterministically in
    determine_decision_node - it does not feed into the lift/price math.
    """
    days_to = state["days_to_event"]
    base_lift = state["base_lift"]

    if days_to >= 0:
        decay = max(0.0, 1.0 - (days_to / PROX_WINDOW_DAYS))
        lift = round(1.0 + (base_lift - 1.0) * decay, 4)
    else:
        lift = 1.0

    candidates = [d for d in (state["days_to_event"], state["holiday_days_away"]) if d >= 0]
    state["days_to_nearest_event"] = min(candidates) if candidates else -1
    state["demand_lift_factor"] = lift
    print(f"[compute_lift] days_to_event={days_to}  base_lift={base_lift}  -> lift={lift}")
    return state


# -- Node 6: determine_decision -----------------------------------------------------
def determine_decision_node(state: AgentState) -> AgentState:
    """
    Deterministic pricing decision - NOT delegated to the LLM. Mirrors the
    inventory agent's philosophy of keeping every number/category decision in
    Python and only asking the LLM to narrate it.

    price_modifier is a SIGNED FRACTIONAL DELTA, not a multiplier - e.g.
    0.0857 means "raise the price by 8.57%", -0.08 means "drop the price by
    8%", 0.0 means "no change". This is deliberately the same convention
    build_proposal_payload() publishes externally (it used to convert to
    this via _modifier_to_delta(), now removed since it's no longer needed)
    - keeping one convention everywhere means every suggested_action's
    modifier stays comfortably under 1.0 for any realistic price swing,
    instead of only DISCOUNT naturally landing there while SURCHARGE
    (1.0857) and HOLD (1.0) broke the pattern under the old multiplier
    representation.

    Action:
        surcharge_exempt_triggered      -> HOLD_EXEMPT, modifier forced to 0.0
        demand_lift_factor > 1.05       -> SURCHARGE, modifier = lift - 1.0
        demand_lift_factor < 0.95       -> DISCOUNT,  modifier = lift - 1.0
        otherwise                       -> HOLD,       modifier forced to 0.0

    Confidence (keyed off whichever event - festival or holiday - is closer):
        0-2 days   -> 0.95
        3-5 days   -> 0.80
        6-14 days  -> 0.65
        15-21 days -> 0.50
        no event   -> 0.40
    """
    exempt = state["surcharge_exempt_triggered"]
    lift = state["demand_lift_factor"]
    nearest = state["days_to_nearest_event"]

    if exempt:
        action, modifier = "HOLD_EXEMPT", 0.0
    elif lift > 1.05:
        action, modifier = "SURCHARGE", round(lift - 1.0, 4)
    elif lift < 0.95:
        action, modifier = "DISCOUNT", round(lift - 1.0, 4)
    else:
        # True HOLD means no price change - 0.0 delta, not the old "force to
        # 1.0" multiplier convention. Without this, a lift sitting inside
        # the deadband would publish a non-zero suggested_modifier on the
        # wire while the action says "HOLD" - contradictory for any
        # downstream consumer, and it was visibly confusing the LLM's own
        # justification text too.
        action, modifier = "HOLD", 0.0

    if nearest < 0:
        confidence = 0.40
    elif nearest <= 2:
        confidence = 0.95
    elif nearest <= 5:
        confidence = 0.80
    elif nearest <= 14:
        confidence = 0.65
    else:
        confidence = 0.50

    state["decision"] = {
        "suggested_action": action,
        "price_modifier": modifier,
        "confidence_score": confidence,
    }
    sku = state["current_row"].get("sku_id", "UNKNOWN")
    print(f"[determine_decision] [{sku}] action={action}  modifier={modifier}  confidence={confidence}")
    return state


# -- Node 7: assign_urgency ----------------------------------------------------------
def assign_urgency_node(state: AgentState) -> AgentState:
    """
    Urgency rules (deterministic, keyed off festival proximity only):
        0-1  days  -> IMMEDIATE
        2-4  days  -> HIGH
        5-10 days  -> MEDIUM
        11+ days / no festival -> LOW
    """
    days = state["days_to_event"]
    if days < 0:
        urgency = "LOW"
    elif days <= 1:
        urgency = "IMMEDIATE"
    elif days <= 4:
        urgency = "HIGH"
    elif days <= 10:
        urgency = "MEDIUM"
    else:
        urgency = "LOW"

    state["urgency"] = urgency
    sku = state["current_row"].get("sku_id", "UNKNOWN")
    print(f"[assign_urgency] [{sku}] urgency={urgency}")
    return state


# -- Node 8a: skip_justification (no event at all - skip the LLM call) ---------------
def skip_justification_node(state: AgentState) -> AgentState:
    """
    Reached when there is no festival AND no public holiday in the lookahead
    window. The narrative would be entirely generic in this case, so the LLM
    call is skipped outright to save cost - mirroring the inventory agent's
    skip_no_risk_node. Still routes through build_output_node like any other
    row, so logging/Kafka publishing stay identical either way.
    """
    row = state["current_row"]
    state["llm_response"] = {
        "headline": f"No festival or holiday within {LOOKAHEAD_DAYS} days - price held",
        "detailed_reasoning": (
            f"No festival was found within the next {LOOKAHEAD_DAYS} days and no US "
            f"public holiday falls within that window either, so demand_lift_factor "
            f"remains at {state['demand_lift_factor']} and the price is held unchanged. "
            f"LLM call skipped to save cost since there is nothing event-driven to explain."
        ),
    }
    state["justification_is_fallback"] = False
    print(f"[skip_justification] [{row['sku_id']}] No event nearby - LLM call skipped")
    return state


# -- Node 8b: call_llm ------------------------------------------------------------------
def call_llm_node(state: AgentState) -> AgentState:
    """
    Sends the already-decided metrics to Gemini and asks ONLY for a headline
    and detailed_reasoning. Validates via Pydantic and writes a validation log
    record (including token counts), mirroring the inventory agent. Falls
    back to a deterministic narrative if parsing/validation fails - the run
    never crashes because of a malformed LLM response.
    """
    row = state["current_row"]
    sku = row.get("sku_id", "UNKNOWN")
    decision = state["decision"]

    user_payload = json.dumps({
        "sku_id": sku,
        "product_name": row["product_name"],
        "category": row["category"],
        "today": state["today_str"],
        "festival_name": state["festival_name"],
        "days_to_event": state["days_to_event"],
        "base_lift": state["base_lift"],
        "demand_lift_factor": state["demand_lift_factor"],
        "public_holiday": state["public_holiday"],
        "holiday_days_away": state["holiday_days_away"],
        "categories_affected": state["categories_affected"],
        "surcharge_exempt_triggered": state["surcharge_exempt_triggered"],
        "decided_action": decision["suggested_action"],
        "decided_price_modifier": decision["price_modifier"],
        "decided_confidence": decision["confidence_score"],
    }, indent=2)

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        project=state["gcp_project"],
        location=os.getenv("GCP_LOCATION", "us-central1"),
        temperature=0.2,
    )
    messages = [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=user_payload)]
    response = llm.invoke(messages)

    # -- Token usage (same extraction pattern as the inventory agent) ----------
    usage = response.usage_metadata or {}
    prompt_tokens = usage.get("input_tokens", 0)
    completion_tokens = usage.get("output_tokens", 0)
    total_tokens = usage.get("total_tokens", 0)
    cached_tokens = (usage.get("input_token_details") or {}).get("cache_read", 0)
    est_cost = round(
        (prompt_tokens / 1_000_000) * _PROMPT_COST_PER_1M
        + (completion_tokens / 1_000_000) * _COMPLETION_COST_PER_1M,
        8,
    )
    state["all_token_usage"].append({
        "sku_id": sku,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cached_tokens": cached_tokens,
        "estimated_cost_usd": est_cost,
    })

    # -- Parse and validate ------------------------------------------------------
    justification = parse_llm_justification(response.content)
    fallback_used = justification is None
    failure_reason = None

    if fallback_used:
        try:
            CalendarJustification(**json.loads(_extract_text(response.content).strip()))
        except Exception as exc:
            failure_reason = str(exc)[:300]
        print(f"[call_llm] [{sku}] [WARNING]  Validation failed - using fallback justification")
        state["llm_response"] = fallback_justification(state)
    else:
        state["llm_response"] = justification.model_dump()
        print(f"[call_llm] [{sku}] [OK] headline={state['llm_response']['headline'][:60]!r}")

    state["justification_is_fallback"] = fallback_used

    _write_log({
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
    }, VALIDATION_LOG)

    return state


# -- Node 9: build_output ----------------------------------------------------------------
def build_output_node(state: AgentState) -> AgentState:
    """Assembles the final JSON proposal, appends to results, writes the local
    audit log, and publishes to BOTH the event-agent Kafka topic and the
    event_proposals Supabase table on the same unified external contract."""
    row = state["current_row"]
    decision = state["decision"]
    justification = state["llm_response"]
    is_fallback = state["justification_is_fallback"]
    status = "FALLBACK" if is_fallback else "COMPLETED"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    agent_id = "event_festivity"

    output = {
        "agent_id": agent_id,
        "sku_id": row["sku_id"],
        "status": status,
        "timestamp": timestamp,
        "metrics_evaluated": {
            "product_name": row["product_name"],
            "category": row["category"],
            "festival_name": state["festival_name"],
            "days_to_event": state["days_to_event"],
            "demand_lift_factor": state["demand_lift_factor"],
            "public_holiday": state["public_holiday"],
            "holiday_days_away": state["holiday_days_away"],
            "categories_affected": state["categories_affected"],
            "surcharge_exempt_triggered": state["surcharge_exempt_triggered"],
        },
        "proposal": {
            "suggested_action": decision["suggested_action"],
            "price_modifier": decision["price_modifier"],
            "confidence_score": decision["confidence_score"],
            "urgency": state["urgency"],
        },
        "justification": {
            "headline": justification["headline"],
            "detailed_reasoning": justification["detailed_reasoning"],
        },
    }
    state["results"].append(output)

    _write_log({
        "log_type": "PROPOSAL",
        "run_id": state["run_id"],
        "sku_id": row["sku_id"],
        "timestamp": timestamp,
        "status": status,
        "urgency": state["urgency"],
        "suggested_action": decision["suggested_action"],
        "price_modifier": decision["price_modifier"],
        "confidence_score": decision["confidence_score"],
        "festival_name": state["festival_name"],
        "days_to_event": state["days_to_event"],
        "public_holiday": state["public_holiday"],
        "surcharge_exempt_triggered": state["surcharge_exempt_triggered"],
        "fallback_used": is_fallback,
    }, PROPOSAL_LOG)

    # -- Publish the FULL detailed JSON (metrics_evaluated + proposal +
    # justification, i.e. the same `output` dict appended to state["results"]
    # above) to its own "calendar-detailed" Kafka topic. This is deliberately
    # separate from the slim agent_id/sku/recommendation/rationale contract
    # published below: that one is the cross-agent shape every consumer must
    # be able to parse identically, this one is the full internal picture for
    # this agent specifically (debugging, analytics, audit UIs, etc). Guarded
    # independently so a failure here never blocks the slim-contract
    # publish/insert that follows.
    try:
        kafka_publish(output, key=row["sku_id"], topic="calendar-detailed")
    except Exception as e:
        print(f"[build_output] [{row['sku_id']}] [WARNING]  Kafka publish "
              f"(calendar-detailed) failed: {e}")

    # -- Build the validated external payload ONCE - shared by both transports.
    # If it fails schema validation, neither Kafka nor Supabase gets it.
    try:
        proposal_payload = build_proposal_payload(row, decision, justification, agent_id)
    except ValidationError as e:
        print(f"[build_output] [{row['sku_id']}] [WARNING]  Proposal payload failed schema "
              f"validation - not published to Kafka or Supabase")
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")
        proposal_payload = None

    if proposal_payload is not None:
        # -- Publish to Kafka (own topic) - guarded independently so a broker
        # outage doesn't prevent the Supabase insert below from running.
        try:
            kafka_publish(proposal_payload, key=row["sku_id"])
        except Exception as e:
            print(f"[build_output] [{row['sku_id']}] [WARNING]  Kafka publish failed: {e}")

        # -- Insert into Supabase (event_proposals table) - guarded
        # independently so a Supabase outage doesn't prevent the Kafka
        # publish above from running.
        try:
            insert_proposal(proposal_payload, key=row["sku_id"])
        except Exception as e:
            print(f"[build_output] [{row['sku_id']}] [WARNING]  Supabase insert failed: {e}")

        # -- Publish the SAME concise payload to the "calendar-agent" topic
        # too - identical agent_id/sku/recommendation/rationale shape as the
        # "event-agent" publish above, just under a second topic name for
        # consumers that key off this agent specifically rather than the
        # shared cross-agent "event-agent" topic. Guarded independently so a
        # failure here never blocks the two publishes above.
        try:
            kafka_publish(proposal_payload, key=row["sku_id"], topic="calendar-agent")
        except Exception as e:
            print(f"[build_output] [{row['sku_id']}] [WARNING]  Kafka publish "
                  f"(calendar-agent) failed: {e}")

    flag = " [FALLBACK - human review recommended]" if is_fallback else ""
    print(f"[build_output] [{row['sku_id']}] Proposal ready{flag}")
    return state


# -- Node 10: advance_row -----------------------------------------------------------------
def advance_row_node(state: AgentState) -> AgentState:
    """Increments row_index to move to the next SKU."""
    state["row_index"] += 1
    return state


# -- Conditional edges ---------------------------------------------------------------------
def route_more_rows(state: AgentState) -> str:
    return "check_next_sku" if state["row_index"] < len(state["rows"]) else END


def route_skip_llm(state: AgentState) -> str:
    """Skips the LLM call when there's no festival AND no public holiday nearby."""
    if state["festival_name"] is None and state["public_holiday"] is None:
        return "skip_justification"
    return "call_llm"


# -- Build graph ------------------------------------------------------------------------------
def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    graph.add_node("load_catalog", load_catalog_node)
    graph.add_node("check_next_sku", check_next_sku_node)
    graph.add_node("scan_festivals", scan_festivals_node)
    graph.add_node("check_holidays", check_holidays_node)
    graph.add_node("compute_lift", compute_lift_node)
    graph.add_node("determine_decision", determine_decision_node)
    graph.add_node("assign_urgency", assign_urgency_node)
    graph.add_node("skip_justification", skip_justification_node)
    graph.add_node("call_llm", call_llm_node)
    graph.add_node("build_output", build_output_node)
    graph.add_node("advance_row", advance_row_node)

    graph.set_entry_point("load_catalog")
    graph.add_edge("load_catalog", "check_next_sku")
    graph.add_edge("check_next_sku", "scan_festivals")
    graph.add_edge("scan_festivals", "check_holidays")
    graph.add_edge("check_holidays", "compute_lift")
    graph.add_edge("compute_lift", "determine_decision")
    graph.add_edge("determine_decision", "assign_urgency")

    graph.add_conditional_edges(
        "assign_urgency",
        route_skip_llm,
        {"skip_justification": "skip_justification", "call_llm": "call_llm"},
    )

    graph.add_edge("skip_justification", "build_output")
    graph.add_edge("call_llm", "build_output")
    graph.add_edge("build_output", "advance_row")

    graph.add_conditional_edges(
        "advance_row",
        route_more_rows,
        {"check_next_sku": "check_next_sku", END: END},
    )

    return graph.compile()


# -- Entry point --------------------------------------------------------------------------------
def main():
    load_dotenv()
    gcp_project = os.getenv("GCP_PROJECT_ID")
    if not gcp_project:
        raise EnvironmentError(
            "GCP_PROJECT_ID not found in environment. "
            "Ensure a .env file exists at the project root with:\n"
            "  GCP_PROJECT_ID=your-gcp-project-id\n"
            "Also set GOOGLE_APPLICATION_CREDENTIALS to point to your service account key JSON file."
        )

    # -- Hardcoded festival path relative to this file, mirroring the inventory
    # agent's layout. Product catalog no longer needs a path - it's fetched
    # from Supabase's products_sku table inside load_catalog_node instead.
    root = Path(__file__).resolve().parent.parent.parent
    festival_json_path = root / "data" / "inputs" / "event_agent" / "festival_calendar.json"

    if not festival_json_path.exists():
        raise FileNotFoundError(
            f"Festival calendar not found at expected path:\n  {festival_json_path}\n"
            "Ensure festival_calendar.json exists under data/inputs/event_agent/"
        )

    # Reference date defaults to "today" in UTC; override for backtesting.
    today_str = os.getenv("CALENDAR_AGENT_TODAY") or datetime.now(timezone.utc).date().isoformat()

    app = build_graph()
    run_id = str(uuid.uuid4())[:8]
    print(f"\n[main] Starting run  run_id={run_id}")
    print(f"[main] Product catalog   -> Supabase: products_sku")
    print(f"[main] Festival calendar -> {festival_json_path}")
    print(f"[main] Reference date    -> {today_str}")

    initial_state: AgentState = {
        "festival_json_path": str(festival_json_path),
        "gcp_project": gcp_project,
        "run_id": run_id,
        "today_str": today_str,
        "rows": [],
        "festival_calendar": [],
        "holidays_calendar": None,
        "row_index": 0,
        "current_row": None,
        "festival_name": None,
        "days_to_event": -1,
        "base_lift": 1.0,
        "categories_affected": [],
        "surcharge_exempt_triggered": False,
        "public_holiday": None,
        "holiday_days_away": -1,
        "demand_lift_factor": 1.0,
        "days_to_nearest_event": -1,
        "decision": None,
        "urgency": None,
        "llm_response": None,
        "justification_is_fallback": False,
        "results": [],
        "all_token_usage": [],
    }

    final_state = app.invoke(initial_state)

    # -- Token summary (same format as the inventory agent) ----------------------
    usage = final_state["all_token_usage"]
    sep = "-" * 62
    print(f"\n[main] {sep}")
    print(f"[main]  TOKEN SUMMARY  run_id={run_id}")
    print(f"[main] {sep}")
    print(f"[main]  {'SKU':<10} {'PROMPT':>8} {'COMPLETION':>12} {'TOTAL':>8} {'COST (USD)':>12}")
    print(f"[main] {sep}")
    grand_prompt = grand_completion = grand_total = grand_cost = 0
    for t in usage:
        print(f"[main]  {t['sku_id']:<10} {t['prompt_tokens']:>8} "
              f"{t['completion_tokens']:>12} {t['total_tokens']:>8} "
              f"{t['estimated_cost_usd']:>12.6f}")
        grand_prompt += t["prompt_tokens"]
        grand_completion += t["completion_tokens"]
        grand_total += t["total_tokens"]
        grand_cost += t["estimated_cost_usd"]
    print(f"[main] {sep}")
    print(f"[main]  {'TOTAL':<10} {grand_prompt:>8} {grand_completion:>12} "
          f"{grand_total:>8} {grand_cost:>12.6f}")
    print(f"[main] {sep}")
    print(f"[main]  Proposal log   -> {PROPOSAL_LOG}")
    print(f"[main]  Validation log -> {VALIDATION_LOG}")
    print(f"[main] {sep}\n")

    kafka_flush()

    print(f"[OK] Done - {len(final_state['results'])} proposal(s) generated\n")
    for output in final_state["results"]:
        print(json.dumps(output, indent=2))
        print()


if __name__ == "__main__":
    main()
