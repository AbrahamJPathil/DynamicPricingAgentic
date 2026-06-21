"""
Pricing Orchestrator Agent
Dynamic Pricing POC - LangGraph + Gemini + Kafka consumer

Unlike the inventory and competitor agents (which run once over a CSV and
exit), this agent is a long-running Kafka CONSUMER. It subscribes to both
upstream topics, keeps the latest known recommendation per (sku, agent_id)
in memory, and every time either upstream agent publishes something new for
a SKU, it re-synthesizes a single final pricing decision for that SKU and
publishes it to the "final-prices" topic.

Graph nodes (run once per incoming Kafka message):
    call_llm -> build_output -> END

Run (as a standing service, in its own terminal):
    python pricing_orchestrator_agent.py

Stop with Ctrl+C - the consumer and producer are both closed/flushed cleanly.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, Literal, Optional, TypedDict

# from confluent_kafka import Consumer  # Kafka disabled - no broker on this machine
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

# from final_prices_kafka_publisher import publish_proposal as publish_final_price  # Kafka disabled
# from final_prices_kafka_publisher import flush as kafka_flush                      # Kafka disabled

# -- Kafka config (unused while Kafka is disabled) --------------------------
# KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPICS = ["inventory-agent", "competitor-agent"]
# CONSUMER_GROUP_ID = "pricing_orchestrator"

# -- LOCAL TESTING MODE ------------------------------------------------------
# No Kafka broker on this machine. The two upstream agents now write their
# strict-schema payloads to local JSONL files instead of Kafka topics (see
# the matching change in inventory_agent.py / competitor_agent.py). This
# agent reads those same files in place of consumer.poll(), and
# publish_final_price()/kafka_flush() below are drop-in stand-ins with
# identical names/signatures to the real Kafka producer - they just write to
# a local "final-prices" file instead. To restore real Kafka, delete this
# block, uncomment the imports/config above, and restore the consumer loop
# in main() (see the commented-out version there).
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_MOCK_KAFKA_DIR = os.getenv("MOCK_KAFKA_DIR", os.path.join(_ROOT, "data", "mock_kafka"))
os.makedirs(_MOCK_KAFKA_DIR, exist_ok=True)
_FINAL_PRICES_PATH = os.path.join(_MOCK_KAFKA_DIR, "final-prices.jsonl")


def publish_final_price(payload: dict, key: str = None) -> None:
    """Local stand-in for the Kafka producer - appends to a JSONL file acting as a mock topic."""
    with open(_FINAL_PRICES_PATH, "a") as f:
        f.write(json.dumps(payload) + "\n")


def kafka_flush() -> None:
    """No-op - the write above is synchronous, so there's nothing buffered to flush."""
    pass


# -- Audit log ----------------------------------------------------------------
FINAL_LOG = "final_prices.jsonl"


def _write_log(record: dict, path: str) -> None:
    """Appends a single JSON record to a JSONL audit file."""
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# -- Decision history (read from the same JSONL audit log build_output_node writes to) --
def _load_recent_history(sku: str, limit: int = 3) -> list[dict]:
    """
    Reads final_prices.jsonl and returns up to the last `limit` prior final
    decisions for this SKU, oldest first. Purely a local-file read with no
    dependency on Kafka/mock-topics, so it's unaffected either way once
    Kafka is restored.
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
    inventory_data: Optional[dict]   # latest known inventory-agent message for this SKU, or None
    competitor_data: Optional[dict]  # latest known competitor-agent message for this SKU, or None
    llm_response: Optional[dict]
    final_output: Optional[dict]


# -- Node 1: call_llm -------------------------------------------------------------
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


# -- Node 2: build_output ----------------------------------------------------------
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

    output = {
        "agent_id": "pricing_orchestrator",
        "sku": sku,
        "status": "FALLBACK" if is_fallback else "COMPLETED",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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


# -- Build graph ----------------------------------------------------------------
def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)
    graph.add_node("call_llm", call_llm_node)
    graph.add_node("build_output", build_output_node)
    graph.set_entry_point("call_llm")
    graph.add_edge("call_llm", "build_output")
    graph.set_finish_point("build_output")
    return graph.compile()


# -- Entry point: local mock-topic replay (Kafka consumer disabled) --------------
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

    # consumer = Consumer({                                              # Kafka disabled
    #     "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,                   # Kafka disabled
    #     "group.id": CONSUMER_GROUP_ID,                                  # Kafka disabled
    #     "auto.offset.reset": "earliest",                                # Kafka disabled
    # })                                                                  # Kafka disabled
    # consumer.subscribe(SOURCE_TOPICS)                                  # Kafka disabled

    app = build_graph()

    # sku -> {agent_id -> latest message dict for that agent}
    sku_cache: Dict[str, Dict[str, dict]] = {}

    print(f"\n[main] Pricing orchestrator running - LOCAL FILE REPLAY (Kafka disabled)")
    print(f"[main] Reading mock topics from: {_MOCK_KAFKA_DIR}")
    print(f"[main] Writing final prices to: {_FINAL_PRICES_PATH}\n")

    # -- Pass 1: read every line from each mock topic file and build the
    #    final sku_cache state. No LLM calls here - each new message just
    #    overwrites sku_cache[sku][agent_id], so only the LAST entry per
    #    (sku, agent_id) ever matters. Synthesizing on every intermediate
    #    message during backfill would just burn tokens on results that get
    #    immediately superseded by the next message for that same SKU.
    for topic_name in SOURCE_TOPICS:
        path = os.path.join(_MOCK_KAFKA_DIR, f"{topic_name}.jsonl")
        if not os.path.exists(path):
            print(f"[main] [WARNING]  {path} not found yet - has that agent run?")
            continue
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"[main] [WARNING]  Could not decode message on {topic_name}: {e}")
                    continue

                sku = payload.get("sku")
                agent_id = payload.get("agent_id")
                if not sku or not agent_id:
                    print(f"[main] [WARNING]  Message missing sku/agent_id, skipping: {payload}")
                    continue

                sku_cache.setdefault(sku, {})[agent_id] = payload

    print(f"[main] Cache built for {len(sku_cache)} SKU(s): {sorted(sku_cache.keys())}\n")

    # -- Pass 2: synthesize once per SKU, using only the latest known data
    #    from each agent - one Gemini call per SKU, not per message.
    for sku in sorted(sku_cache.keys()):
        sources = sku_cache[sku]
        print(
            f"[main] Synthesizing sku={sku}  "
            f"(inventory={'yes' if 'inventory_perishability' in sources else 'no'}, "
            f"competitor={'yes' if 'competitor_pricing' in sources else 'no'})"
        )

        initial_state: AgentState = {
            "sku": sku,
            "gcp_project": gcp_project,
            "inventory_data": sources.get("inventory_perishability"),
            "competitor_data": sources.get("competitor_pricing"),
            "llm_response": None,
            "final_output": None,
        }
        app.invoke(initial_state)

    # consumer.close()  # Kafka disabled - no consumer object exists in this mode
    kafka_flush()
    print(f"\n[main] Done. Final prices written to: {_FINAL_PRICES_PATH}")


if __name__ == "__main__":
    main()