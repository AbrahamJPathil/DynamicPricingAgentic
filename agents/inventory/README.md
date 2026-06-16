# Inventory & Perishability Agent

**Author:** Abraham J  
**Version:** 1.0.0  
**Stack:** LangGraph + Gemini 2.5 Flash + Pydantic

---

## Overview

A dynamic pricing agent for retail grocery inventory. It scans perishable SKUs, identifies units at risk of expiring unsold, quantifies the financial exposure, and recommends a price modifier to clear stock before expiry — prioritising the most urgent items first.

The agent is built as a LangGraph `StateGraph` where every business concern lives in its own node, making each step independently testable and extensible.

---

## Business Logic

**Step 1 — Load inventory**  
Reads all rows from a CSV file into shared agent state.

**Step 2 — Sort by urgency**  
Before any LLM call is made, every row is pre-scanned using pure Python to compute `days_to_expiry`, `units_at_risk`, and `loss_if_no_action`. Rows are sorted by urgency tier first (IMMEDIATE before HIGH before MEDIUM), with `loss_if_no_action` as the tiebreaker within the same tier. The full processing queue is printed to console before the pipeline begins.

**Step 3 — Perishability gate**  
Non-perishable SKUs are skipped immediately. No further computation is performed on them.

**Step 4 — Expiry computation**  
`days_to_expiry` is computed at runtime from `expiry_datetime`, never read from a stored column. `units_at_risk = stock_on_hand - (avg_daily_units_sold x days_to_expiry)`. SKUs where `units_at_risk <= 0` are skipped — natural velocity will clear them before expiry.

**Step 5 — Loss computation**  
`expiry_loss_rate = 1.0 - producer_buyback_rate - repurposing_recovery_rate`  
`loss_if_no_action = units_at_risk x cost_price x expiry_loss_rate`

Both `producer_buyback_rate` and `repurposing_recovery_rate` are stored as independent columns so either can be updated without recalculating the derived rate.

**Step 6 — Urgency assignment**  
Assigned deterministically in Python, never by the LLM:

| days_to_expiry | urgency   |
|----------------|-----------|
| <= 1 day       | IMMEDIATE |
| <= 3 days      | HIGH      |
| > 3 days       | MEDIUM    |

**Step 7 — LLM call**  
Gemini 2.5 Flash is called with the pre-computed metrics. The LLM is asked only to recommend a `price_modifier` and generate a human-readable `headline` and `detailed_reasoning`. It is never asked to perform arithmetic — all numeric inputs are pre-computed in Python.

Output is validated against a Pydantic schema (`LLMProposal`) enforcing:
- `price_modifier` in range `[0.10, 1.0]`
- `suggested_action` as one of `DISCOUNT`, `HOLD`, `SURCHARGE`
- Cross-field consistency — an `IMMEDIATE` SKU with a modifier above `0.85` is rejected as contradictory
- `detailed_reasoning` must reference at least one inventory keyword

If validation fails, a deterministic fallback proposal is emitted using a risk-ratio formula. The fallback floor is `producer_buyback_rate + repurposing_recovery_rate` — representing what the store recovers per unit if the item expires anyway.

**Step 8 — Output assembly**  
The final proposal is assembled in the agreed inter-agent JSON schema. `status` is set to `COMPLETED` for LLM-generated proposals and `FALLBACK` for rule-based ones. `confidence_score = 0.0` in fallback proposals signals the orchestrator to route them for human review.

**Step 9 — Audit logging**  
Two JSONL log files are written on every run:
- `proposals.jsonl` — one record per proposal, written by `build_output_node`
- `validations.jsonl` — one record per LLM call, including token counts (`prompt`, `completion`, `total`, `cached`) and `estimated_cost_usd`, written by `call_llm_node`

Every log record carries a `run_id` so any historical run can be fully reconstructed.

---

## CSV Schema

| Column | Type | Used by |
|---|---|---|
| `sku_id` | string | All nodes |
| `product_name` | string | LLM, output |
| `category` | string | LLM, output |
| `unit` | string | LLM, output |
| `stock_on_hand` | int | Expiry computation |
| `reorder_level` | int | Reserved |
| `batch_received_at` | datetime | Reserved |
| `expiry_datetime` | datetime | Expiry computation |
| `is_perishable` | bool | Perishability gate |
| `avg_daily_units_sold` | float | Expiry computation |
| `units_sold_last_24h` | int | LLM context |
| `cost_price` | float | Loss computation |
| `producer_buyback_rate` | float | Loss computation, fallback floor |
| `repurposing_recovery_rate` | float | Loss computation, fallback floor |

---

## Getting Started

**Install dependencies**

```bash
pip install langgraph langchain-google-genai langchain-core pydantic
```

**Prepare your inventory CSV**

Name the file `products.csv` and ensure it contains the columns listed in the schema above. A sample dataset with four SKUs (two meat, one bakery, one deli) is included in the repository.

**Run the agent**

```bash
python inventory_agent.py --api-key YOUR_GEMINI_API_KEY --csv products.csv
```

Both arguments are required. `--csv` defaults to `products.csv` if omitted.

**Inspect the audit logs**

After the run completes, two files are created in the working directory:

```bash
# View all proposals from the last run
cat proposals.jsonl | python -m json.tool

# View validation outcomes and token costs
cat validations.jsonl | python -m json.tool

# Filter only fallback proposals
grep '"status": "FALLBACK"' proposals.jsonl
```

---

## Output Schema

```json
{
  "agent_id": "inventory_perishability",
  "sku_id": "MEA001",
  "status": "COMPLETED",
  "timestamp": "2026-06-14T10:45:00Z",
  "metrics_evaluated": {
    "product_name": "Rotisserie Chicken",
    "category": "meat",
    "unit": "2.25lb",
    "stock_on_hand": 38,
    "days_to_expiry": 1.0,
    "avg_daily_units_sold": 14.0,
    "units_sold_last_24h": 6,
    "units_at_risk": 24.0,
    "cost_price": 4.0,
    "expiry_loss_rate": 0.42,
    "loss_if_no_action": 40.32
  },
  "proposal": {
    "suggested_action": "DISCOUNT",
    "price_modifier": 0.62,
    "confidence_score": 0.91,
    "urgency": "IMMEDIATE"
  },
  "justification": {
    "headline": "24 units at critical expiry risk — loss exposure $40.32 if unsold",
    "detailed_reasoning": "38 units on hand with only 1 day to expiry..."
  }
}
```

---

## Project Structure

```
inventory_agent.py    — agent source code
products.csv          — inventory dataset
proposals.jsonl       — proposal audit log (generated at runtime)
validations.jsonl     — validation and token audit log (generated at runtime)
README.md             — this file
```

---

*Author: Abraham J*
