# Calendar & Festivity Agent

**Stack:** LangGraph + Gemini 2.5 Flash + Pydantic + Kafka

---

## What changed in this rewrite

The previous version had a few issues that this rewrite addresses, to bring
it in line with the inventory agent's architecture and Kafka contract:

1. **No error handling around the LLM call.** `json.loads(clean)` was called
   directly on Gemini's raw output with no try/except and no schema
   validation. Any malformed response (extra text, wrong field, a markdown
   fence in an unexpected place) would throw an uncaught exception and kill
   the entire batch, including every SKU still left in the queue. The LLM
   call is now wrapped in `parse_llm_justification()` + Pydantic validation
   with a deterministic fallback (`fallback_justification()`), so a bad
   response degrades to a generic-but-numeric explanation instead of crashing.
2. **The LLM was doing arithmetic it shouldn't have been.** It was asked to
   pick `suggested_action`, copy back `price_modifier` (= `demand_lift_factor`),
   and compute `confidence_score` from a lookup table — three things a model
   can get wrong for no benefit, since none of them require judgment. All
   three are now computed deterministically in Python
   (`determine_decision_node`). The LLM's only remaining job is to write
   `headline` + `detailed_reasoning` explaining the decision it was handed.
3. **Dead code removed:** `route_has_festival` (built but never wired into
   the graph), `MOCK_FESTIVAL_CALENDAR`, `bootstrap_festival_json()`, and
   unused imports (`create_agent`, `tool`, `re`, `sys`).
4. **Hardcoded paths/dates removed:** the `/mnt/user-data/uploads` CSV-copy
   hack and the hardcoded `today_str = "2026-05-26"` are gone. Paths now
   follow the inventory agent's layout
   (`data/inputs/calendar-agent/calendar_data.csv` and `festival_calendar.json`
   under the project root), and `today_str` defaults to the real current UTC
   date, overridable via `CALENDAR_AGENT_TODAY=YYYY-MM-DD` for backtesting.
5. **`GCP_PROJECT_ID` is read inside `main()`**, not at module import time —
   importing `build_graph` from an orchestrator no longer crashes the process
   if the env var isn't set yet.
6. **Cost optimisation:** SKUs with no festival *and* no public holiday in
   the 21-day lookahead window now skip the LLM call entirely
   (`skip_justification_node`) and get a deterministic HOLD — mirroring the
   inventory agent's `skip_no_risk_node`. This will be the majority of rows
   on any given day.
7. **Unified Kafka contract.** Every proposal is now published to the
   `calendar-agent` topic using the exact same external schema as the
   inventory agent: `agent_id` / `sku` / `recommendation` (`action`,
   `suggested_modifier`, `confidence`) / `rationale`. The internal
   `HOLD_EXEMPT` action collapses to `HOLD` on the wire; the exemption itself
   stays visible in `metrics_evaluated.surcharge_exempt_triggered` for the
   rich audit JSON.
8. **Audit logging + token cost tracking added**, matching the inventory
   agent: `calendar_proposals.jsonl` and `calendar_validations.jsonl` (kept
   under separate filenames from the inventory agent's logs so the two don't
   clobber each other when run from the same working directory).

**Known limitation, unchanged from before:** festival entries carry a
`region` field (`"global"` vs `"US"`), but neither the old code nor this
rewrite filters on it — a US-only holiday like Thanksgiving will currently
surface for any store regardless of location. Flagging this rather than
guessing at a fix, since there's no store-location field in the CSV to
filter against yet.

---

## Business logic

**Step 1 — Load catalog.** Reads the product CSV and the festival calendar
once per run (not once per row).

**Step 2 — Scan festivals.** For the current SKU's category, finds the
nearest festival in the next 21 days whose `categories` list includes it.

**Step 3 — Check holidays.** Checks the `holidays` library's US calendar for
the next 21 days (built once per run).

**Step 4 — Compute lift.** Time-decay curve:
`lift = 1.0 + (base_lift - 1.0) * max(0, 1 - days_to_event / 7)`. Lift is
`1.0` with no festival, and ramps up to `base_lift` as the festival
approaches.

**Step 5 — Determine decision (deterministic, Python only).**
`surcharge_exempt_triggered` → `HOLD_EXEMPT` (modifier forced to `1.0`);
`lift > 1.15` → `SURCHARGE`; `lift < 0.95` → `DISCOUNT`; otherwise `HOLD`.
`confidence_score` is a lookup table keyed on whichever event (festival or
holiday) is closer.

**Step 6 — Assign urgency (deterministic).** Keyed on `days_to_event`:
`<=1` IMMEDIATE, `<=4` HIGH, `<=10` MEDIUM, else LOW.

**Step 7 — LLM call (or skip).** If there's no festival and no holiday
nearby, the LLM call is skipped and a generic deterministic HOLD narrative is
used. Otherwise Gemini is given the decision that was already made and asked
only for `headline` + `detailed_reasoning`, validated via Pydantic with a
deterministic fallback on failure.

**Step 8 — Build output + publish.** Assembles the rich internal JSON,
writes it to `calendar_proposals.jsonl`, and publishes the thinner external
contract to the `calendar-agent` Kafka topic.

---

## Expected input files (already present, not regenerated by this rewrite)

- `data/inputs/calendar-agent/calendar_data.csv` — columns used:
  `sku_id`, `product_name`, `category` (others are ignored by this agent).
- `data/inputs/calendar-agent/festival_calendar.json` — array of
  `{name, month, day, date_hint, base_lift, categories, surcharge_exempt, region}`.

---

## Getting started

```bash
pip install -r requirements_agent.txt
```

Add to your project-root `.env`:

```
GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json
```

Run:

```bash
cd agents/calendar
python agent.py
```

Optional backtest override:

```bash
CALENDAR_AGENT_TODAY=2026-06-05 python agent.py
```

Inspect logs:

```bash
cat calendar_proposals.jsonl | python -m json.tool
cat calendar_validations.jsonl | python -m json.tool
grep '"status": "FALLBACK"' calendar_proposals.jsonl
```

---

## Output schema

```json
{
  "agent_id": "calendar_festivity",
  "sku_id": "MEA003",
  "status": "COMPLETED",
  "timestamp": "2026-06-15T10:45:00Z",
  "metrics_evaluated": {
    "product_name": "Premium Lamb Chops",
    "category": "meat",
    "festival_name": "Eid al-Adha",
    "days_to_event": 1,
    "demand_lift_factor": 1.75,
    "public_holiday": null,
    "holiday_days_away": -1,
    "categories_affected": ["meat", "sweets", "dairy", "bakery", "spices"],
    "surcharge_exempt_triggered": false
  },
  "proposal": {
    "suggested_action": "SURCHARGE",
    "price_modifier": 1.75,
    "confidence_score": 0.95,
    "urgency": "IMMEDIATE"
  },
  "justification": {
    "headline": "Eid al-Adha 1 day out drives a 75% demand surge for lamb",
    "detailed_reasoning": "..."
  }
}
```
