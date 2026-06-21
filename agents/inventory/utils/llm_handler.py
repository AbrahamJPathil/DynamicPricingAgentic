"""
LLM response parsing and rule-based fallback proposal for the Inventory & Perishability Agent.
"""

import json
from typing import Optional

from pydantic import ValidationError

from core.config import _VAGUE_PHRASES
from core.models import LLMProposal, AgentState


def parse_llm_response(raw: str) -> Optional[LLMProposal]:
    """
    Cleans markdown fences, parses JSON, and validates against LLMProposal schema.
    Returns None on any failure - caller must invoke fallback_proposal().
    """
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
        return LLMProposal(**data)
    except ValidationError as e:
        print("[parse_llm_response] Schema validation failed:")
        for error in e.errors():
            print(f"  field={error['loc']}  msg={error['msg']}")
        return None


def fallback_proposal(state: AgentState) -> dict:
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

    producer_buyback = float(row.get("producer_buyback_rate", 0.0))
    repurposing = float(row.get("repurposing_recovery_rate", 0.0))
    recovery_floor = round(producer_buyback + repurposing, 4)

    base_modifier = round(max(1.0 - (risk_ratio * 0.6), recovery_floor), 2)

    action = "DISCOUNT" if d < 3 else "HOLD"
    modifier = base_modifier if d < 3 else 1.0

    return {
        "suggested_action": action,
        "price_modifier": modifier,
        "confidence_score": 0.0,
        "urgency": state["urgency"],
        "headline": "Fallback proposal - LLM output failed validation",
        "detailed_reasoning": (
            f"Rule-based fallback applied. {state['units_at_risk']} units at risk "
            f"with {d} days to expiry. "
            + (
                f"Modifier {modifier} derived from risk ratio {risk_ratio:.2f}, "
                f"floored at recovery rate {recovery_floor} (buyback={producer_buyback} + "
                f"repurposing={repurposing})."
                if d < 3
                else "days_to_expiry >= 3 so no markdown applied per policy; price held unchanged."
            )
            + " Manual review required."
        ),
    }
