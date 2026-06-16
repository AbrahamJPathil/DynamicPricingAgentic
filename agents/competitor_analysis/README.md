# Competitor Pricing Agent
**Author:** Sruthi S Menon 
**Version:** 1.0.0  
**Stack:** LangGraph + Kroger API + Pandas

---

## Overview
A competitive pricing agent for retail grocery. It scans SKUs from an inventory CSV, fetches live shelf prices from the Kroger API, compares them against our current prices, and recommends a price modifier to stay competitive in the market — prioritising the most impactful pricing decisions first.

The agent is built as a LangGraph `StateGraph` where every business concern lives in its own node, making each step independently testable and extensible.

---

## Business Logic

**Step 1 — Load inventory**  
Reads all rows from a CSV file into shared agent state. Validates that all required columns are present before proceeding.

**Step 2 — Authenticate with Kroger**  
Obtains a short-lived OAuth2 bearer token from the Kroger API using client credentials. If authentication fails, the specific HTTP error code is captured and propagated into every SKU's output as `"status": "ERROR"` — no silent failures.

**Step 3 — Fetch competitor prices**  
For each SKU, the Kroger Products API is queried by product name and unit. The promotional price is preferred when available; regular shelf price is used as a fallback. Each fetch captures either a price or a typed error reason (`AUTH_ERROR`, `FETCH_ERROR: HTTP 404`, `FETCH_ERROR: Request timed out`) — never a silent `None`.

**Step 4 — Analyse and build proposals**  
Our price is compared against the Kroger price using deterministic rules. The LLM is not involved in arithmetic — all numeric decisions are made in Python:

| Condition | Action | Price Modifier |
|---|---|---|
| `our_price == kroger_price` | `HOLD` | `1.0` |
| `our_price > kroger_price` | `DISCOUNT` | `kroger_price / our_price` |
| `our_price < kroger_price` | `SURCHARGE` | `kroger_price / our_price` |

The `price_modifier` is always `competitor_price / our_price` — multiplying our price by this value lands it exactly at the Kroger price.

**Step 5 — Compile report**  
The final proposals are written to a JSON file. A formatted summary table is printed to the console. All non-fatal errors encountered during the run are logged as warnings.

---

## Error Handling

Every failure path produces an explicit `"status": "ERROR"` entry in the output JSON — the agent never silently drops a SKU or produces an empty result.

| Failure Point | Status | Error visible in output? |
|---|---|---|
| CSV missing / malformed | `ERROR` | ✅ Full error in `detailed_reasoning` |
| Missing API credentials | `ERROR` | ✅ `AUTH_ERROR` propagated per SKU |
| Kroger API HTTP error | `ERROR` | ✅ Status code in `detailed_reasoning` |
| Request timeout | `ERROR` | ✅ `FETCH_ERROR: Request timed out` |
| No product match found | `ERROR` | ✅ Search term included in message |
| Price fetched successfully | `COMPLETED` | — |

---

## CSV Schema

| Column | Type | Used by |
|---|---|---|
| `sku_id` | string | All nodes |
| `product_name` | string | Kroger search, output |
| `category` | string | Output |
| `unit` | string | Kroger search, output |
| `our_price` | float | Price comparison, output |

---

## Getting Started

**Install dependencies**
```bash
pip install langgraph langchain-core requests pandas python-dotenv
```

**Configure credentials**  
Create a `.env` file in the project root:
```env
KROGER_CLIENT_ID=your_client_id
KROGER_CLIENT_SECRET=your_client_secret
KROGER_LOCATION_ID=01400943
```

**Prepare your inventory CSV**  
Name the file `data.csv` and place it at `data/inputs/competitor/data.csv`. Ensure it contains the columns listed in the schema above.

**Run the agent**
```bash
python competitor_pricing_agent.py
```

**Inspect the output**
```bash
# View the full pricing report
cat data/outputs/pricing_report.json | python -m json.tool

# Filter only ERROR entries
python -c "import json; [print(json.dumps(r, indent=2)) for r in json.load(open('data/outputs/pricing_report.json')) if r['status'] == 'ERROR']"

# Filter only COMPLETED entries
python -c "import json; [print(json.dumps(r, indent=2)) for r in json.load(open('data/outputs/pricing_report.json')) if r['status'] == 'COMPLETED']"
```

---

## Output Schema

```json
{
  "agent_id": "competitor_pricing",
  "status": "COMPLETED",
  "timestamp": "2026-06-14T10:44:41Z",
  "metrics_evaluated": {
    "sku": "MILK-CET-01",
    "our_current_price": 4.00,
    "competitor_price": 3.96
  },
  "proposal": {
    "suggested_action": "DISCOUNT",
    "price_modifier": 0.99,
    "confidence_score": 0.85
  },
  "justification": {
    "headline": "Competitive Pressure Detected",
    "detailed_reasoning": "Our price ($4.00) exceeds Kroger's ($3.96) by 1.0%. Applying a 1.0% discount brings us level with the competitor and protects market share."
  }
}
```

---

## Project Structure

```
agents/
  competitor_pricing/
    agent.py                  — agent source code
data/
  inputs/
    competitor/
      data.csv                — inventory dataset
  outputs/
    pricing_report.json       — pricing proposals (generated at runtime)
.env                          — API credentials (never commit this)
README.md                     — this file
```

---

**Author:** Sruthi S Menon