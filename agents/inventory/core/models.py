"""
Pydantic models and state type definitions for the Inventory & Perishability Agent.
"""

from typing import List, Literal, Optional, TypedDict

from pydantic import BaseModel, Field, field_validator, model_validator

from .config import _VAGUE_PHRASES


class LLMProposal(BaseModel):
    suggested_action: Literal["DISCOUNT", "HOLD", "SURCHARGE"]
    price_modifier: float = Field(ge=0.10, le=1.5)
    confidence_score: float = Field(ge=0.0, le=1.0)
    urgency: Literal["IMMEDIATE", "HIGH", "MEDIUM"]
    headline: str = Field(min_length=10)
    detailed_reasoning: str = Field(min_length=30)

    @field_validator("price_modifier")
    @classmethod
    def modifier_not_suspiciously_low(cls, v: float) -> float:
        if v < 0.20:
            raise ValueError(
                f"price_modifier {v} is below 0.20 - likely a hallucination. "
                f"Minimum realistic discount is 20% off current price."
            )
        return v

    @field_validator("detailed_reasoning")
    @classmethod
    def reasoning_references_inventory(cls, v: str) -> str:
        keywords = ["expir", "days", "units", "stock", "loss", "clear", "risk", "cost"]
        if not any(kw in v.lower() for kw in keywords):
            raise ValueError(
                "detailed_reasoning does not reference any inventory metrics - "
                "likely a generic or hallucinated response."
            )
        return v

    @field_validator("detailed_reasoning")
    @classmethod
    def reasoning_not_vague(cls, v: str) -> str:
        lowered = v.lower()
        hit = next((p for p in _VAGUE_PHRASES if p in lowered), None)
        if hit:
            raise ValueError(
                f'detailed_reasoning contains vague language ("{hit}") instead of '
                f"citing specific numbers from the data provided."
            )
        return v

    @model_validator(mode="after")
    def urgency_modifier_consistency(self) -> "LLMProposal":
        if self.urgency == "IMMEDIATE" and self.price_modifier > 0.85:
            raise ValueError(
                f"IMMEDIATE urgency but price_modifier={self.price_modifier} implies "
                f"only a {round((1 - self.price_modifier) * 100)}% discount - "
                f"insufficient for a same-day expiry SKU."
            )
        return self

    @model_validator(mode="after")
    def action_modifier_consistency(self) -> "LLMProposal":
        if self.suggested_action == "SURCHARGE" and self.price_modifier <= 1.0:
            raise ValueError(
                "suggested_action is SURCHARGE but price_modifier <= 1.0 - contradictory."
            )
        if self.suggested_action == "DISCOUNT" and self.price_modifier >= 1.0:
            raise ValueError(
                "suggested_action is DISCOUNT but price_modifier >= 1.0 - contradictory."
            )
        if self.suggested_action == "HOLD" and self.price_modifier != 1.0:
            raise ValueError(
                f"suggested_action is HOLD but price_modifier={self.price_modifier} != 1.0 - "
                f"contradictory."
            )
        return self


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


class AgentState(TypedDict):
    # inputs
    gcp_project: str
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
