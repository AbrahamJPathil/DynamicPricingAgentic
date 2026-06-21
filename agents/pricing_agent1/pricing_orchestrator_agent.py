"""
Pricing Orchestrator Agent
Dynamic Pricing POC - LangGraph + Gemini + Kafka consumer

Unlike the inventory and competitor agents (which run once over a CSV and
exit), this agent is a long-running Kafka CONSUMER. It subscribes to both
upstream topics, keeps only the latest known recommendation per (sku,
agent_id) in memory, and synthesizes a single final pricing decision per SKU
- not once per incoming message. Concretely:
  - If both upstream agents have reported for a SKU, it runs immediately
    using each agent's latest message (collapsing any backlog of older
    messages for that SKU into a single, current synthesis).
  - If only one upstream agent has reported for a SKU, it waits up to
    PARTIAL_DATA_WAIT_SECONDS (1.5 minutes) for the other agent to catch up
    before running on partial data, instead of firing on every single
    one-sided update.

Graph nodes (run once per synthesis, not once per Kafka message):
    fetch_price -> call_llm -> build_output -> update_price -> END

Run (as a standing service, in its own terminal):
    python pricing_orchestrator_agent.py

Stop with Ctrl+C - the consumer and producer are both closed/flushed cleanly.
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Literal, Optional, TypedDict

import requests
from confluent_kafka import Consumer
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from final_prices_kafka_publisher import publish_proposal as publish_final_price
from final_prices_kafka_publisher import flush as kafka_flush

# -- Kafka config -----------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPICS = ["inventory-agent", "competitor-agent"]
CONSUMER_GROUP_ID = "pricing_orchestrator"

# -- Upstream agent debounce config --------------------------------------------
INVENTORY_AGENT_ID = "inventory_perishability"
COMPETITOR_AGENT_ID = "competitor_pricing"
REQUIRED_AGENT_IDS = {INVENTORY_AGENT_ID, COMPETITOR_AGENT_ID}

# How long to wait for the second upstream agent before synthesizing on
# partial (single-agent) data for a SKU.
PARTIAL_DATA_WAIT_SECONDS = 90  # 1.5 minutes

# -- Supabase config ----------------------------------------------------------
SUPABASE_TABLE = "products_sku"
SKU_COLUMN = "sku_id"
PRICE_COLUMN = "our_price"


# -- Audit log ----------------------------------------------------------------
FINAL_LOG = "final_prices.jsonl"


def _write_log(record: dict, path: str) -> None:
    """Appends a single JSON record to a JSONL audit file."""
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# -- Supabase price lookup/update ------------------------------------------------
def _get_supabase_config() -> tuple[str, str]:
    """Reads Supabase connection details from the environment, mirroring load_csv_node's approach."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not supabase_url or not supabase_key:
        raise EnvironmentError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment")
    return supabase_url.rstrip("/"), supabase_key


def _supabase_headers(supabase_key: str, *, writing: bool = False) -> dict:
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    if writing:
        headers["Content-Type"] = "application/json"
        headers["Prefer"] = "return=minimal"
    return headers


def _fetch_current_price(sku: str) -> Optional[float]:
    """Looks up our_price for this sku_id from the products_sku Supabase table."""
    try:
        supabase_url, supabase_key = _get_supabase_config()
    except EnvironmentError as exc:
        print(f"[fetch_price] [{sku}] {exc}")
        return None

    url = f"{supabase_url}/rest/v1/{SUPABASE_TABLE}"
    params = {"select": PRICE_COLUMN, SKU_COLUMN: f"eq.{sku}"}

    try:
        # NOTE: verify=False (mirrors load_csv_node) skips TLS certificate
        # verification - fine against a local/dev Supabase instance, but
        # should be removed (or set verify=True) against production.
        resp = requests.get(url, headers=_supabase_headers(supabase_key), params=params, timeout=10, verify=False)
        resp.raise_for_status()
        rows = resp.json()
    except Exception as exc:
        print(f"[fetch_price] [{sku}] Failed to fetch {PRICE_COLUMN} from Supabase: {exc}")
        return None

    if not rows:
        print(f"[fetch_price] [{sku}] No row found in {SUPABASE_TABLE} where {SKU_COLUMN}={sku}")
        return None

    try:
        return float(rows[0][PRICE_COLUMN])
    except (KeyError, TypeError, ValueError) as exc:
        print(f"[fetch_price] [{sku}] {PRICE_COLUMN} missing/invalid on matched row: {exc}")
        return None


def _update_price(sku: str, new_price: float) -> bool:
    """Writes new_price back to the our_price column for this sku_id in Supabase."""
    try:
        supabase_url, supabase_key = _get_supabase_config()
    except EnvironmentError as exc:
        print(f"[update_price] [{sku}] {exc}")
        return False

    url = f"{supabase_url}/rest/v1/{SUPABASE_TABLE}"
    params = {SKU_COLUMN: f"eq.{sku}"}
    body = {PRICE_COLUMN: new_price}

    try:
        resp = requests.patch(
            url,
            headers=_supabase_headers(supabase_key, writing=True),
            params=params,
            json=body,
            timeout=10,
            verify=False,
        )
        resp.raise_for_status()
    except Exception as exc:
        print(f"[update_price] [{sku}] Failed to update {PRICE_COLUMN} in Supabase: {exc}")
        return False

    print(f"[update_price] [{sku}] {PRICE_COLUMN} updated to {new_price}")
    return True


# -- Decision history (read from the same JSONL audit log build_output_node writes to) --
def _load_recent_history(sku: str, limit: int = 3) -> list[dict]:
    """
    Reads final_prices.jsonl and returns up to the last `limit` prior final
    decisions for this SKU, oldest first. Purely a local-file read with no
    dependency on Kafka, so it works unchanged regardless of which broker/
    topic build_output_node is publishing to.
    """
    if not os.path.exists(FINAL_LOG):
        return []
    history = []
    with open(FINAL_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("sku") == sku:
                history.append(record)
    return history[-limit:]


def _format_decision_history(history: list[dict]) -> str:
    """Renders prior final decisions as a prompt block, or states plainly that none exist."""
    if not history:
        return "No prior final decision exists for this SKU - this is the first synthesis."
    lines = []
    for r in history:
        rec = r["final_recommendation"]
        lines.append(
            f"- {r['timestamp']}: action={rec['action']}, modifier={rec['suggested_modifier']:+.4f}, "
            f"confidence={rec['confidence']}, status={r['status']}"
        )
    return "Prior final decisions for this SKU, most recent last:\n" + "\n".join(lines)


# -- System prompt --------------------------------------------------------------
SYSTEM_PROMPT = """You are a pricing orchestration agent for a retail grocery store.
You receive recommendations from up to two upstream specialist agents for the
same SKU:

1. An inventory & perishability agent - recommends discounts to clear stock
   before it expires unsold.
2. A competitor pricing agent - recommends matching or beating a competitor's
   shelf price.

Each recommendation is a signed fractional price modifier (e.g. -0.05 means a
5% discount, +0.10 means a 10% surcharge), a confidence score (0.0 to 1.0),
and a short rationale. One of the two inputs may be missing (NO DATA
AVAILABLE) if that agent hasn't reported on this SKU yet.

Your job is to synthesize these into ONE final pricing decision:
- If both inputs are present and point the same direction, blend them,
  weighting more heavily by whichever has higher confidence.
- If they conflict (e.g. inventory wants a discount but competitor wants a
  surcharge), resolve it explicitly in your reasoning. As a default rule,
  prioritize clearing perishable inventory at risk of becoming a total loss
  over matching competitor pricing - but use judgment based on the
  confidence scores you were given, and say so in your rationale.
- If only one input is present, you may use it directly, but say so in your
  rationale and moderate your confidence accordingly (a single signal should
  rarely justify confidence above 0.85).
- Do not recommend a final modifier more extreme than the most extreme
  individual input you were given.

You will also be given this SKU's recent final-decision history, if any
exists. If your prior decision for this SKU pointed the same direction and
nothing material has changed, say so plainly instead of restating generic
reasoning. If no history is given, state that this is the first synthesis
for this SKU rather than inventing a trend that doesn't exist.

Be concrete, never vague. Do not use hand-wavy phrases like "significant
risk", "moderate confidence", "various factors", "a portion of", or
"relatively high/low". Every sentence in rationale must cite at least one
specific number you were actually given - a modifier, a confidence score,
or a value from the history block.

Respond ONLY with a valid JSON object using exactly this schema:
{
  "action": "DISCOUNT" | "HOLD" | "SURCHARGE",
  "suggested_modifier": <float, signed fraction, e.g. -0.20 for a 20% discount, +0.10 for a 10% surcharge>,
  "confidence": <float between 0.0 and 1.0>,
  "rationale": "<two to three sentence explanation referencing the input signal(s) used, minimum 30 characters>"
}
No preamble, no markdown fences, only the JSON object."""


# -- Phrases that signal hand-wavy, non-numeric reasoning - shared blocklist ----
_VAGUE_PHRASES = [
    "significant risk", "moderate confidence", "some units", "a portion of",
    "various factors", "a number of", "relatively high", "relatively low",
    "a certain amount", "quite a bit", "fairly significant", "somewhat",
]


# -- Pydantic schema for the LLM's synthesis output ------------------------------
class FinalLLMProposal(BaseModel):
    action: Literal["DISCOUNT", "HOLD", "SURCHARGE"]
    suggested_modifier: float = Field(ge=-1.0, le=5.0)
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = Field(min_length=30)

    @model_validator(mode="after")
    def action_modifier_consistency(self) -> "FinalLLMProposal":
        if self.action == "SURCHARGE" and self.suggested_modifier < 0:
            raise ValueError("action SURCHARGE but suggested_modifier is negative - contradictory.")
        if self.action == "DISCOUNT" and self.suggested_modifier > 0:
            raise ValueError("action DISCOUNT but suggested_modifier is positive - contradictory.")
        if self.action == "HOLD" and abs(self.suggested_modifier) > 0.02:
            raise ValueError("action HOLD but suggested_modifier is non-trivial - contradictory.")
        return self

    @field_validator("rationale")
    @classmethod
    def rationale_not_vague(cls, v: str) -> str:
        """
        Deterministic backstop behind the system prompt's anti-vagueness
        instruction - rejects known hand-wavy phrases so the rule-based
        fallback synthesis kicks in instead of letting vague language
        reach build_output_node.
        """
        lowered = v.lower()
        hit = next((p for p in _VAGUE_PHRASES if p in lowered), None)
        if hit:
            raise ValueError(
                f'rationale contains vague language ("{hit}") instead of '
                f"citing specific numbers from the inputs provided."
            )
        return v


def _parse_llm_response(raw: str) -> Optional[FinalLLMProposal]:
    """Cleans markdown fences, parses JSON, and validates against FinalLLMProposal."""
    raw = raw.strip()
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
        print(f"[parse_llm_response] JSON parse failed: {e}")
        print(f"[parse_llm_response] Raw output: {raw[:200]}")
        return None

    try:
        return FinalLLMProposal(**data)
    except ValidationError as e:
        print("[parse_llm_response] Schema validation failed:")
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")
        return None


# -- Rule-based fallback synthesis -----------------------------------------------
def _fallback_synthesis(inventory_data: Optional[dict], competitor_data: Optional[dict]) -> dict:
    """
    Emitted when the LLM's synthesis fails parsing or Pydantic validation.
    Confidence-weighted average of whichever inputs are present.
    confidence=0.0 signals this was not LLM-generated and needs human review,
    same convention used by the upstream agents' own fallback proposals.
    """
    sources = []
    if inventory_data:
        rec = inventory_data["recommendation"]
        sources.append((inventory_data["agent_id"], rec["suggested_modifier"], rec["confidence"]))
    if competitor_data:
        rec = competitor_data["recommendation"]
        sources.append((competitor_data["agent_id"], rec["suggested_modifier"], rec["confidence"]))

    total_conf = sum(c for _, _, c in sources)
    if total_conf > 0:
        final_modifier = sum(m * c for _, m, c in sources) / total_conf
    elif sources:
        final_modifier = sum(m for _, m, _ in sources) / len(sources)
    else:
        final_modifier = 0.0

    final_modifier = round(final_modifier, 4)
    action = (
        "DISCOUNT" if final_modifier < -0.01
        else "SURCHARGE" if final_modifier > 0.01
        else "HOLD"
    )

    breakdown = ", ".join(
        f"{name}={modifier:+.2%} (confidence {conf:.2f})" for name, modifier, conf in sources
    )
    rationale = (
        "Rule-based fallback applied because the LLM synthesis failed validation. "
        f"Confidence-weighted average across {len(sources)} signal(s): {breakdown}."
    )

    return {
        "action": action,
        "suggested_modifier": final_modifier,
        "confidence": 0.0,
        "rationale": rationale,
    }


def _clamp_to_input_range(modifier: float, inputs: list[float]) -> float:
    """
    Safety net mirroring the system prompt's "never more extreme than the
    inputs" instruction - LLMs don't always follow instructions perfectly.
    """
    if not inputs:
        return round(modifier, 4)
    lo, hi = min(inputs) - 0.01, max(inputs) + 0.01
    return round(max(lo, min(hi, modifier)), 4)


def _format_source(label: str, data: Optional[dict]) -> str:
    """Renders one upstream agent's latest known message as a prompt line, or notes it's missing."""
    if data is None:
        return f"{label}: NO DATA AVAILABLE for this SKU yet."
    rec = data["recommendation"]
    return (
        f"{label}: suggested_modifier={rec['suggested_modifier']:+.4f}, "
        f"confidence={rec['confidence']:.2f}, rationale=\"{data['rationale']}\""
    )


# -- Shared state (one invocation per incoming Kafka message) -------------------
class AgentState(TypedDict):
    sku: str
    gcp_project: str
    current_price: Optional[float]   # our_price looked up from Supabase for this sku_id
    inventory_data: Optional[dict]   # latest known inventory-agent message for this SKU, or None
    competitor_data: Optional[dict]  # latest known competitor-agent message for this SKU, or None
    llm_response: Optional[dict]
    final_output: Optional[dict]


# -- Node 1: fetch_price -----------------------------------------------------------
def fetch_price_node(state: AgentState) -> AgentState:
    """Looks up this SKU's current our_price from Supabase before synthesis runs."""
    sku = state["sku"]
    state["current_price"] = _fetch_current_price(sku)
    if state["current_price"] is not None:
        print(f"[fetch_price] [{sku}] current {PRICE_COLUMN}={state['current_price']}")
    return state


# -- Node 2: call_llm -------------------------------------------------------------
def call_llm_node(state: AgentState) -> AgentState:
    """Builds the synthesis prompt from whatever upstream data is available and calls Gemini."""
    sku = state["sku"]
    inventory_data = state["inventory_data"]
    competitor_data = state["competitor_data"]

    decision_history_block = _format_decision_history(_load_recent_history(sku))

    prompt = f"""SKU: {sku}

{_format_source("Inventory & Perishability Agent", inventory_data)}
{_format_source("Competitor Pricing Agent", competitor_data)}

{decision_history_block}

Synthesize a single final pricing decision for this SKU."""

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        project=state["gcp_project"],
        location=os.getenv("GCP_LOCATION", "us-central1"),
        temperature=0.2,
    )
    messages = [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=prompt)]
    response = llm.invoke(messages)

    proposal = _parse_llm_response(response.content)

    input_modifiers = [
        d["recommendation"]["suggested_modifier"]
        for d in (inventory_data, competitor_data)
        if d is not None
    ]

    if proposal is None:
        print(f"[call_llm] [{sku}] [WARNING]  Validation failed - using fallback synthesis")
        state["llm_response"] = _fallback_synthesis(inventory_data, competitor_data)
    else:
        clamped_modifier = _clamp_to_input_range(proposal.suggested_modifier, input_modifiers)
        state["llm_response"] = {
            "action": proposal.action,
            "suggested_modifier": clamped_modifier,
            "confidence": proposal.confidence,
            "rationale": proposal.rationale,
        }
        print(
            f"[call_llm] [{sku}] [OK] action={proposal.action}  "
            f"modifier={clamped_modifier}  confidence={proposal.confidence}"
        )

    return state


# -- Node 3: build_output ----------------------------------------------------------
def build_output_node(state: AgentState) -> AgentState:
    """Assembles the final payload, logs it, and publishes it to the final-prices topic."""
    sku = state["sku"]
    llm = state["llm_response"]
    inventory_data = state["inventory_data"]
    competitor_data = state["competitor_data"]

    contributing_agents = []
    if inventory_data:
        rec = inventory_data["recommendation"]
        contributing_agents.append({
            "agent_id": inventory_data["agent_id"],
            "suggested_modifier": rec["suggested_modifier"],
            "confidence": rec["confidence"],
        })
    if competitor_data:
        rec = competitor_data["recommendation"]
        contributing_agents.append({
            "agent_id": competitor_data["agent_id"],
            "suggested_modifier": rec["suggested_modifier"],
            "confidence": rec["confidence"],
        })

    is_fallback = llm["confidence"] == 0.0

    current_price = state.get("current_price")
    updated_price = (
        round(current_price * (1 + llm["suggested_modifier"]), 2)
        if current_price is not None
        else None
    )

    output = {
        "agent_id": "pricing_orchestrator",
        "sku": sku,
        "status": "FALLBACK" if is_fallback else "COMPLETED",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "previous_price": current_price,
        "updated_price": updated_price,
        "final_recommendation": {
            "action": llm["action"],
            "suggested_modifier": llm["suggested_modifier"],
            "confidence": llm["confidence"],
        },
        "rationale": llm["rationale"],
        "contributing_agents": contributing_agents,
    }
    state["final_output"] = output

    _write_log(output, FINAL_LOG)
    publish_final_price(output, key=sku)

    flag = " [WARNING]  [FALLBACK - human review required]" if is_fallback else ""
    print(f"[build_output] [{sku}] Final price published{flag}")
    return state


# -- Node 4: update_price ----------------------------------------------------------
def update_price_node(state: AgentState) -> AgentState:
    """Writes the newly computed price back to our_price in Supabase for this SKU."""
    sku = state["sku"]
    updated_price = state["final_output"].get("updated_price")

    if updated_price is None:
        print(
            f"[update_price] [{sku}] [WARNING]  No current_price was available from Supabase - "
            f"skipping {PRICE_COLUMN} update"
        )
        return state

    _update_price(sku, updated_price)
    return state


# -- Build graph ----------------------------------------------------------------
def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)
    graph.add_node("fetch_price", fetch_price_node)
    graph.add_node("call_llm", call_llm_node)
    graph.add_node("build_output", build_output_node)
    graph.add_node("update_price", update_price_node)
    graph.set_entry_point("fetch_price")
    graph.add_edge("fetch_price", "call_llm")
    graph.add_edge("call_llm", "build_output")
    graph.add_edge("build_output", "update_price")
    graph.set_finish_point("update_price")
    return graph.compile()


# -- Synthesis trigger (builds AgentState from the cache and runs the graph) ----
def _run_for_sku(sku: str, app, gcp_project: str, sku_cache: Dict[str, Dict[str, dict]]) -> None:
    """Invokes the graph once for a SKU using whatever is currently cached for it."""
    initial_state: AgentState = {
        "sku": sku,
        "gcp_project": gcp_project,
        "current_price": None,
        "inventory_data": sku_cache[sku].get(INVENTORY_AGENT_ID),
        "competitor_data": sku_cache[sku].get(COMPETITOR_AGENT_ID),
        "llm_response": None,
        "final_output": None,
    }
    app.invoke(initial_state)


# -- Entry point: long-running Kafka consumer ------------------------------------
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

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        # earliest -> on first run this backfills from every message either
        # upstream agent has ever published, so the cache starts "warm" even
        # if this agent is started after the others have already run.
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(SOURCE_TOPICS)

    app = build_graph()

    # sku -> {agent_id -> latest message dict for that agent}
    sku_cache: Dict[str, Dict[str, dict]] = {}

    # sku -> epoch timestamp after which we synthesize on partial data even if
    # the second upstream agent still hasn't reported. A SKU only ever has ONE
    # entry here at a time: it's set the moment the SKU first becomes
    # "incomplete" and cleared as soon as it either completes or fires.
    sku_pending_deadline: Dict[str, float] = {}

    print(f"\n[main] Pricing orchestrator running")
    print(f"[main] Subscribed to: {SOURCE_TOPICS}")
    print(f"[main] Publishing to: final-prices")
    print(f"[main] Partial-data wait: {PARTIAL_DATA_WAIT_SECONDS:.0f}s")
    print(f"[main] Waiting for messages ... (Ctrl+C to stop)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is not None:
                if msg.error():
                    print(f"[main] [ERROR]  {msg.error()}")
                else:
                    try:
                        payload = json.loads(msg.value().decode("utf-8"))
                    except json.JSONDecodeError as e:
                        print(f"[main] [WARNING]  Could not decode message on {msg.topic()}: {e}")
                        payload = None

                    if payload is not None:
                        sku = payload.get("sku")
                        agent_id = payload.get("agent_id")
                        if not sku or not agent_id:
                            print(f"[main] [WARNING]  Message missing sku/agent_id, skipping: {payload}")
                        else:
                            # Overwriting by agent_id means only the latest message per
                            # agent per SKU is ever kept - older messages from a backlog
                            # (e.g. a second inventory update) are superseded here and
                            # never separately trigger their own synthesis below.
                            sku_cache.setdefault(sku, {})[agent_id] = payload
                            print(f"[main] Cache updated: sku={sku}  from={agent_id} (topic={msg.topic()})")

                            if REQUIRED_AGENT_IDS.issubset(sku_cache[sku].keys()):
                                # Both upstream agents have now reported for this SKU -
                                # run immediately on their latest outputs and drop any
                                # partial-data timer that was ticking for it.
                                sku_pending_deadline.pop(sku, None)
                                print(f"[main] [{sku}] Both agents reported - synthesizing now")
                                _run_for_sku(sku, app, gcp_project, sku_cache)
                            elif sku not in sku_pending_deadline:
                                # Only one agent has reported so far, and we're not
                                # already waiting on this SKU - start the grace period.
                                sku_pending_deadline[sku] = now + PARTIAL_DATA_WAIT_SECONDS
                                missing = REQUIRED_AGENT_IDS - sku_cache[sku].keys()
                                print(
                                    f"[main] [{sku}] Only {agent_id} has reported so far - "
                                    f"waiting up to {PARTIAL_DATA_WAIT_SECONDS:.0f}s for "
                                    f"{sorted(missing)} before synthesizing on partial data"
                                )
                            # else: already waiting on this SKU; the cache update above
                            # is enough - no need to touch or extend the timer.

            # Checked every loop iteration (including idle poll timeouts) so a
            # SKU's grace period elapses even if no further messages arrive.
            expired_skus = [sku for sku, deadline in sku_pending_deadline.items() if now >= deadline]
            for sku in expired_skus:
                sku_pending_deadline.pop(sku, None)
                missing = REQUIRED_AGENT_IDS - sku_cache.get(sku, {}).keys()
                print(
                    f"[main] [{sku}] [WARNING]  {sorted(missing)} still hasn't reported after "
                    f"{PARTIAL_DATA_WAIT_SECONDS:.0f}s - synthesizing on partial data"
                )
                _run_for_sku(sku, app, gcp_project, sku_cache)

    except KeyboardInterrupt:
        print("\n[main] Stopping pricing orchestrator ...")
    finally:
        consumer.close()
        kafka_flush()
        print("[main] Consumer closed, producer flushed. Bye.")


if __name__ == "__main__":
    main()