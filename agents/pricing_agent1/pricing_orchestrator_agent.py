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

from confluent_kafka import Consumer
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph
from pydantic import BaseModel, Field, ValidationError, model_validator

from final_prices_kafka_publisher import publish_proposal as publish_final_price
from final_prices_kafka_publisher import flush as kafka_flush

# -- Kafka config -----------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPICS = ["inventory-agent", "competitor-agent"]
CONSUMER_GROUP_ID = "pricing_orchestrator"

# -- Audit log ----------------------------------------------------------------
FINAL_LOG = "final_prices.jsonl"


def _write_log(record: dict, path: str) -> None:
    """Appends a single JSON record to a JSONL audit file."""
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


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

Respond ONLY with a valid JSON object using exactly this schema:
{
  "action": "DISCOUNT" | "HOLD" | "SURCHARGE",
  "suggested_modifier": <float, signed fraction, e.g. -0.20 for a 20% discount, +0.10 for a 10% surcharge>,
  "confidence": <float between 0.0 and 1.0>,
  "rationale": "<two to three sentence explanation referencing the input signal(s) used, minimum 30 characters>"
}
No preamble, no markdown fences, only the JSON object."""


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
    api_key: str
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

    prompt = f"""SKU: {sku}

{_format_source("Inventory & Perishability Agent", inventory_data)}
{_format_source("Competitor Pricing Agent", competitor_data)}

Synthesize a single final pricing decision for this SKU."""

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=state["api_key"],
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


# -- Entry point: long-running Kafka consumer ------------------------------------
def main():
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY not found in environment. "
            "Ensure a .env file exists at the project root with:\n"
            "  GEMINI_API_KEY=your_key_here"
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

    print(f"\n[main] Pricing orchestrator running")
    print(f"[main] Subscribed to: {SOURCE_TOPICS}")
    print(f"[main] Publishing to: final-prices")
    print(f"[main] Waiting for messages ... (Ctrl+C to stop)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[main] [ERROR]  {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"[main] [WARNING]  Could not decode message on {msg.topic()}: {e}")
                continue

            sku = payload.get("sku")
            agent_id = payload.get("agent_id")
            if not sku or not agent_id:
                print(f"[main] [WARNING]  Message missing sku/agent_id, skipping: {payload}")
                continue

            sku_cache.setdefault(sku, {})[agent_id] = payload
            print(f"[main] Cache updated: sku={sku}  from={agent_id} (topic={msg.topic()})")

            initial_state: AgentState = {
                "sku": sku,
                "api_key": api_key,
                "inventory_data": sku_cache[sku].get("inventory_perishability"),
                "competitor_data": sku_cache[sku].get("competitor_pricing"),
                "llm_response": None,
                "final_output": None,
            }
            app.invoke(initial_state)

    except KeyboardInterrupt:
        print("\n[main] Stopping pricing orchestrator ...")
    finally:
        consumer.close()
        kafka_flush()
        print("[main] Consumer closed, producer flushed. Bye.")


if __name__ == "__main__":
    main()