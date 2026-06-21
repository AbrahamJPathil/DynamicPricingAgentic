"""
Kafka payload builder utilities for the Inventory & Perishability Agent.
"""

from core.models import KafkaProposal, KafkaRecommendation


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
        {agent_id, sku, recommendation: {action, suggested_modifier, confidence}, rationale}

    Raises pydantic.ValidationError if the proposal can't be mapped onto the
    external contract - acts as a final safety net before anything leaves the
    process, on top of the LLMProposal/fallback_proposal checks that already
    ran upstream in call_llm_node.
    """
    payload = KafkaProposal(
        agent_id=agent_id,
        sku=row["sku_id"],
        recommendation=KafkaRecommendation(
            action=llm["suggested_action"],
            suggested_modifier=_modifier_to_delta(llm["price_modifier"]),
            confidence=round(llm["confidence_score"], 2),
        ),
        rationale=_build_rationale(llm["headline"], llm["detailed_reasoning"]),
    )
    return payload.model_dump()
