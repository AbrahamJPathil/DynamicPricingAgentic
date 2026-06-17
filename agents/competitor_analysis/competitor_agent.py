import os, json, math, requests, pandas as pd
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import TypedDict, Optional
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from langgraph.graph import StateGraph
from pydantic import BaseModel, Field, ValidationError

# from competitor_kafka_publisher import publish_proposal, flush as kafka_flush  # Kafka disabled - no broker on this machine

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# ---------------------------------------------
#  CONFIGURATION  (set your creds here or via env vars)
# ---------------------------------------------
KROGER_CLIENT_ID = os.getenv("KROGER_CLIENT_ID")
KROGER_CLIENT_SECRET = os.getenv("KROGER_CLIENT_SECRET")
KROGER_LOCATION_ID   = os.getenv("KROGER_LOCATION_ID",   "01400943")   # default: a Kroger store

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CSV_PATH = os.path.join(ROOT, "data", "inputs", "competitor", "data.csv")
OUTPUT_PATH = os.path.join(ROOT, "data", "outputs", "pricing_report.json")
# pricing_report.json is overwritten every run, so it can't hold history -
# this JSONL log is append-only and is what _load_recent_history() reads.
COMPETITOR_PROPOSAL_LOG = os.path.join(ROOT, "data", "outputs", "competitor_proposals.jsonl")


def _write_log(record: dict, path: str) -> None:
    """Appends a single JSON record to a JSONL audit file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def _load_recent_history(sku: str, limit: int = 3) -> list:
    """
    Reads competitor_proposals.jsonl and returns up to the last `limit`
    prior entries for this SKU, oldest first. Purely a local-file read -
    no dependency on Kafka/mock-topics either way.
    """
    if not os.path.exists(COMPETITOR_PROPOSAL_LOG):
        return []
    history = []
    with open(COMPETITOR_PROPOSAL_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("metrics_evaluated", {}).get("sku") == sku:
                history.append(record)
    return history[-limit:]

KROGER_TOKEN_URL   = "https://api.kroger.com/v1/connect/oauth2/token"
KROGER_PRODUCT_URL = "https://api.kroger.com/v1/products"

# -- LOCAL TESTING MODE ------------------------------------------------------
# No Kafka broker on this machine. publish_proposal()/kafka_flush() below are
# drop-in stand-ins with identical names/signatures to the real ones in
# competitor_kafka_publisher.py - they write to a local JSONL file acting as
# a mock "competitor-agent" topic instead of producing to Kafka. Nothing else
# in this file needs to change. To restore real Kafka, delete this block and
# uncomment the import near the top of the file.
_MOCK_KAFKA_DIR = os.getenv("MOCK_KAFKA_DIR", os.path.join(ROOT, "data", "mock_kafka"))
os.makedirs(_MOCK_KAFKA_DIR, exist_ok=True)
_MOCK_TOPIC_PATH = os.path.join(_MOCK_KAFKA_DIR, "competitor-agent.jsonl")


def publish_proposal(payload: dict, key: str = None) -> None:
    """Local stand-in for the Kafka producer - appends to a JSONL file acting as a mock topic."""
    with open(_MOCK_TOPIC_PATH, "a") as f:
        f.write(json.dumps(payload) + "\n")


def kafka_flush() -> None:
    """No-op - the write above is synchronous, so there's nothing buffered to flush."""
    pass


# ---------------------------------------------
#  AGENT STATE
# ---------------------------------------------
class AgentState(TypedDict):
    # Inputs
    csv_path:      str
    location_id:   str

    # Intermediate
    products:      list          # rows from CSV as dicts
    access_token:  Optional[str]
    auth_error:    Optional[str]
    raw_prices:    dict          # sku_id -> {"price": float|None, "error": str|None}

    # Outputs
    results:       list          # final list of report dicts
    report_path:   Optional[str]
    errors:        list          # non-fatal per-SKU errors


# ---------------------------------------------
#  NODE 1 - LOAD CSV
# ---------------------------------------------
def load_csv(state: AgentState) -> AgentState:
    """Read products.csv -> list of product dicts."""
    logger.info("[NODE 1/5]   Loading product catalogue from CSV")
    try:
        df = pd.read_csv(state["csv_path"])
        required = {"sku_id", "product_name", "unit", "our_price"}
        missing  = required - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing columns: {missing}")

        products = df.to_dict(orient="records")
        logger.info(
            f"Loaded {len(products)} SKU(s): {[p['sku_id'] for p in products]}"
        )
        return {**state, "products": products, "errors": []}

    except Exception as exc:
        logger.error(f"CSV error: {exc}")
        return {**state, "products": [], "errors": [str(exc)]}


# ---------------------------------------------
#  NODE 2 - AUTH KROGER(GET TOKEN)
# ---------------------------------------------
def auth_kroger(state: AgentState) -> AgentState:
    """Obtain a client-credentials bearer token from Kroger."""
    logger.info("[NODE 2/5]   Authenticating with Kroger API ...")

    if not KROGER_CLIENT_ID or not KROGER_CLIENT_SECRET:
        msg = "Missing Kroger API credentials in .env file."
        logger.error(msg)
        return {**state, "access_token": None, "auth_error": msg}

    try:
        resp = requests.post(
            KROGER_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "scope":         "product.compact",
            },
            auth=(KROGER_CLIENT_ID, KROGER_CLIENT_SECRET),
            timeout=10,
            verify=False
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        logger.info("Token obtained successfully.")
        return {**state, "access_token": token, "auth_error": None}

    except Exception as exc:
        msg = f"Kroger auth failed: {exc}"
        logger.error(msg)
        return {**state, "access_token": None, "auth_error": msg}


# ---------------------------------------------
#  NODE 3 - FETCH COMPETITOR PRICES
# ---------------------------------------------

def _fetch_kroger_price(product: dict, token: str, location_id: str) -> tuple[Optional[float], Optional[str]]:
    """Search Kroger's catalogue by product name and return the first shelf price."""
    headers = {"Authorization": f"Bearer {token}"}
    search_term = f"{product['product_name']} {product['unit']}"

    params = {
        "filter.term": search_term,
        "filter.locationId": location_id,
        "filter.limit": 1,
    }
    try:
        resp = requests.get(KROGER_PRODUCT_URL, headers=headers,
                            params=params, timeout=10, verify=False)
        resp.raise_for_status()
        items = resp.json().get("data", [])

        logger.info(f"Search Term: {search_term}")

        for i, item in enumerate(items):
            logger.info(
                f"Result {i+1}: {item.get('description')}"
            )

        if not items:
            return None, "FETCH_ERROR: No Kroger items found"

        item = items[0]
        prices = item.get("items", [{}])[0].get("price", {})
        price = float(prices.get("promo") or prices.get("regular") or 0) or None
        if price is None:
            return None, "FETCH_ERROR: No price found in Kroger response"

        return price, None
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        return None, f"FETCH_ERROR: HTTP {status} from Kroger API"
    except requests.Timeout:
        return None, "FETCH_ERROR: Request timed out"
    except Exception as exc:
        return None, f"FETCH_ERROR: {exc}"


def fetch_prices(state: AgentState) -> AgentState:
    """For each product, query Kroger and store the price."""
    logger.info("[NODE 3/5]   Fetching Kroger competitor prices ...")

    token      = state.get("access_token")
    auth_error = state.get("auth_error")
    raw_prices = {}
    errors     = list(state.get("errors", []))

    for product in state["products"]:
        sku  = product["sku_id"]
        
        if not token:
            raw_prices[sku] = {"price": None, "error": auth_error or "AUTH_ERROR: No access token"}
            errors.append(f"{sku}: {auth_error}")
            continue
        else:
            price, error = _fetch_kroger_price(
                product,
                token,
                state["location_id"]
            )
            raw_prices[sku] = {"price": price, "error": error}

        if price is not None:
            logger.info(f"{sku} -> Kroger Price: ${price:.2f} [LIVE]")
        else:
            logger.warning(f"{sku} -> N/A [{error}]")
            errors.append(f"{sku}: {error}")

    return {**state, "raw_prices": raw_prices, "errors": errors}


# ---------------------------------------------
#  NODE 4 - ANALYSE & BUILD PROPOSALS
# ---------------------------------------------
def _build_proposal(our_price: float, comp_price: float):
    """
    Compare prices and return (action, modifier, confidence, reasoning).

    modifier  = comp_price / our_price  -> multiply our_price by modifier
                to land exactly at competitor price.
    """
    diff_pct = abs(our_price - comp_price) / comp_price * 100

    if math.isclose(our_price, comp_price, rel_tol=1e-3):
        action     = "HOLD"
        modifier   = 1.0
        confidence = 0.95
        reasoning  = (
            f"Our price (${our_price:.2f}) matches Kroger's (${comp_price:.2f}). "
            "No price change needed - maintain current positioning."
        )
    elif our_price > comp_price:
        action     = "DISCOUNT"
        modifier   = round(comp_price / our_price, 4)
        confidence = round(min(0.99, 0.70 + diff_pct / 100), 2)
        reasoning  = (
            f"Our price (${our_price:.2f}) exceeds Kroger's (${comp_price:.2f}) "
            f"by {diff_pct:.1f}%. Applying a {(1-modifier)*100:.1f}% discount "
            "brings us level with the competitor and protects market share."
        )
    else:  # our_price < comp_price
        action     = "SURCHARGE"
        modifier   = round(comp_price / our_price, 4)
        confidence = round(min(0.99, 0.60 + diff_pct / 100), 2)
        reasoning  = (
            f"Kroger prices (${comp_price:.2f}) are higher than ours (${our_price:.2f}) "
            f"by {diff_pct:.1f}%. A {(modifier-1)*100:.1f}% surcharge captures "
            "margin while remaining competitive."
        )

    return action, modifier, confidence, reasoning


HEADLINE_MAP = {
    "HOLD":      "Price Parity Achieved",
    "DISCOUNT":  "Competitive Pressure Detected",
    "SURCHARGE": "Margin Expansion Opportunity",
}


def _describe_trend(history: list, current_action: str, current_comp_price: float) -> str:
    """
    Builds a concrete, numbers-based trend sentence from this SKU's logged
    history, appended to the template reasoning above. There's no LLM here
    to prompt against vagueness, so the grounding has to come from actually
    citing the prior numbers rather than from instructing a model.
    """
    if not history:
        return "This is the first competitor price check on record for this SKU."

    last = history[-1]
    last_comp_price = last["metrics_evaluated"].get("competitor_price")

    if last_comp_price is not None:
        delta = round(current_comp_price - last_comp_price, 2)
        if delta > 0:
            trend = (
                f"Kroger's price rose ${delta:.2f} since the last check "
                f"(${last_comp_price:.2f} -> ${current_comp_price:.2f})."
            )
        elif delta < 0:
            trend = (
                f"Kroger's price dropped ${abs(delta):.2f} since the last check "
                f"(${last_comp_price:.2f} -> ${current_comp_price:.2f})."
            )
        else:
            trend = f"Kroger's price is unchanged at ${current_comp_price:.2f} since the last check."
    else:
        trend = "The last check could not retrieve a competitor price for comparison."

    streak = 0
    for h in reversed(history):
        if (h.get("proposal") or {}).get("suggested_action") == current_action:
            streak += 1
        else:
            break
    if streak > 0:
        trend += f" This is the {streak + 1} consecutive check recommending {current_action}."

    return trend


def _describe_unavailable_trend(history: list) -> str:
    """Same idea as _describe_trend(), for the case where this check also has no competitor price."""
    if not history:
        return "This is the first competitor price check on record for this SKU."

    streak = 0
    for h in reversed(history):
        if h.get("status") == "ERROR":
            streak += 1
        else:
            break
    if streak > 0:
        return f"This is the {streak + 1} consecutive check unable to retrieve a Kroger price for this SKU."

    last_price = history[-1]["metrics_evaluated"].get("competitor_price")
    return (
        f"The last check did retrieve a competitor price of ${last_price:.2f}, "
        f"but this check could not."
    )


def _failed_entry(sku: str, our_price: float, headline: str, reasoning: str) -> dict:
    return {
        "agent_id":          "competitor_pricing",
        "status":            "ERROR",
        "timestamp":         datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metrics_evaluated": {
            "sku":               sku,
            "our_current_price": round(our_price, 2),
            "competitor_price":  None,
        },
        "proposal":    None,
        "justification": {
            "headline":           headline,
            "detailed_reasoning": reasoning,
        },
    }


# ---------------------------------------------
#  STRICT KAFKA OUTPUT SCHEMA  (external contract)
# ---------------------------------------------
# This is what actually gets published to the "competitor-agent" topic. It's
# deliberately thinner than the report entry above - metrics_evaluated and
# the full justification stay in pricing_report.json; only agent_id / sku /
# recommendation / rationale go out on the wire.
class KafkaRecommendation(BaseModel):
    suggested_modifier: float = Field(
        ge=-1.0,
        le=5.0,
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


def _modifier_to_delta(price_modifier: float) -> float:
    """
    Converts the multiplier representation (comp_price / our_price, e.g. 0.65)
    into the signed delta the Kafka schema expects (-0.35). A modifier of 1.0
    (HOLD) maps to a delta of 0.0 - no change.
    """
    return round(price_modifier - 1.0, 4)


def _build_rationale(headline: str, reasoning: str) -> str:
    """Joins the short headline and the longer reasoning into one readable string for the UI."""
    headline = headline.strip()
    if headline and headline[-1] not in ".!?":
        headline += "."
    return f"{headline} {reasoning.strip()}"


def build_kafka_payload(entry: dict) -> dict:
    """
    Builds and validates the strict external payload published to Kafka:
        {agent_id, sku, recommendation: {suggested_modifier, confidence}, rationale}

    Handles both COMPLETED entries (full proposal) and ERROR entries (no
    competitor price found, proposal=None). For ERROR entries we publish
    suggested_modifier=0.0 / confidence=0.0 - "confidence based on data
    availability" means zero data in, zero confidence and no recommended
    change out, rather than skipping the SKU on the topic entirely.
    """
    sku = entry["metrics_evaluated"]["sku"]
    justification = entry["justification"]

    if entry["proposal"] is None:
        suggested_modifier = 0.0
        confidence = 0.0
    else:
        suggested_modifier = _modifier_to_delta(entry["proposal"]["price_modifier"])
        confidence = round(entry["proposal"]["confidence_score"], 2)

    payload = KafkaProposal(
        agent_id=entry["agent_id"],
        sku=sku,
        recommendation=KafkaRecommendation(
            suggested_modifier=suggested_modifier,
            confidence=confidence,
        ),
        rationale=_build_rationale(
            justification["headline"], justification["detailed_reasoning"]
        ),
    )
    return payload.model_dump()


def analyze(state: AgentState) -> AgentState:
    """Compare our prices vs Kroger and generate proposals for each SKU."""
    logger.info("[NODE 4/5]   Analysing prices and generating proposals ...")

    results    = []
    errors     = list(state.get("errors", []))
    timestamp  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for product in state["products"]:
        sku       = product["sku_id"]
        our_price = float(product["our_price"])
        price_rec   = state["raw_prices"].get(sku, {"price": None, "error": "No price record found"})
        comp_price  = price_rec["price"]
        fetch_error = price_rec["error"]

        history = _load_recent_history(sku)

        if comp_price is None:
            base_reasoning = fetch_error or f"Could not retrieve Kroger price for SKU {sku}."
            reasoning = f"{base_reasoning} {_describe_unavailable_trend(history)}"
            entry = _failed_entry(
                sku       = sku,
                our_price = our_price,
                headline  = "Competitor Price Unavailable",
                reasoning = reasoning,
            )
        else:
            action, modifier, confidence, base_reasoning = _build_proposal(our_price, comp_price)
            reasoning = f"{base_reasoning} {_describe_trend(history, action, comp_price)}"
            entry = {
                "agent_id":  "competitor_pricing",
                "status":    "COMPLETED",
                "timestamp": timestamp,
                "metrics_evaluated": {
                    "sku":               sku,
                    "our_current_price": round(our_price, 2),
                    "competitor_price":  round(comp_price, 2),
                },
                "proposal": {
                    "suggested_action": action,
                    "price_modifier":   modifier,
                    "confidence_score": confidence,
                },
                "justification": {
                    "headline":          HEADLINE_MAP[action],
                    "detailed_reasoning": reasoning,
                },
            }
            logger.info(
                f"{sku} -> {action} "
                f"(modifier={modifier}, confidence={confidence})"
            )

        results.append(entry)
        _write_log(entry, COMPETITOR_PROPOSAL_LOG)

        # -- Publish to Kafka (competitor-agent topic) --------------------------
        # Only the strict external contract goes on the wire - the full entry
        # above (with metrics_evaluated, raw competitor price, etc.) stays in
        # pricing_report.json. Non-blocking; flush() in main() guarantees
        # delivery before exit.
        try:
            kafka_payload = build_kafka_payload(entry)
            publish_proposal(kafka_payload, key=sku)
        except ValidationError as e:
            logger.warning(f"{sku}: Kafka payload failed schema validation - not published")
            for error in e.errors():
                logger.warning(f"  field={error['loc']}  msg={error['msg']}")

    return {**state, "results": results, "errors": errors}


# ---------------------------------------------
#  NODE 5 - COMPILE REPORT
# ---------------------------------------------
def compile_report(state: AgentState) -> AgentState:
    """Write JSON report to disk and print a summary table."""
    logger.info("[NODE 5/5]   Compiling final report ...")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(state["results"], f, indent=2)

    # -- Pretty summary table --------------------------------------------
    print()
    print("+" + "-"*76 + "+")
    print(f"|{'COMPETITOR PRICING REPORT':^76}|")
    print("+" + "-"*20 + "+" + "-"*10 + "+" + "-"*10 + "+" + "-"*12 + "+" + "-"*20 + "+")
    print(f"|{'SKU':<20}|{'Our $':>10}|{'Kroger $':>10}|{'Action':>12}|{'Modifier':>20}|")
    print("+" + "-"*20 + "+" + "-"*10 + "+" + "-"*10 + "+" + "-"*12 + "+" + "-"*20 + "+")

    for r in state["results"]:
        m    = r["metrics_evaluated"]
        prop = r.get("proposal") or {}
        sku  = m["sku"]
        our  = f"${m['our_current_price']:.2f}" if m["our_current_price"] else "N/A"
        comp = f"${m['competitor_price']:.2f}"  if m["competitor_price"]  else "N/A"
        act  = prop.get("suggested_action", "N/A")
        mod  = str(prop.get("price_modifier", "N/A"))
        print(f"|{sku:<20}|{our:>10}|{comp:>10}|{act:>12}|{mod:>20}|")

    print("+" + "-"*20 + "+" + "-"*10 + "+" + "-"*10 + "+" + "-"*12 + "+" + "-"*20 + "+")

    if state.get("errors"):
        logger.warning("Non-fatal errors encountered:")
        for e in state["errors"]:
            logger.warning(e)

    logger.info(f"Report saved to {OUTPUT_PATH}")
    return {**state, "report_path": OUTPUT_PATH}


# ---------------------------------------------
#  BUILD THE LANGGRAPH
# ---------------------------------------------
graph = StateGraph(AgentState)

graph.add_node("load_csv",       load_csv)
graph.add_node("auth_kroger",    auth_kroger)
graph.add_node("fetch_prices",   fetch_prices)
graph.add_node("analyze",        analyze)
graph.add_node("compile_report", compile_report)

graph.set_entry_point("load_csv")
graph.add_edge("load_csv",       "auth_kroger")
graph.add_edge("auth_kroger",    "fetch_prices")
graph.add_edge("fetch_prices",   "analyze")
graph.add_edge("analyze",        "compile_report")
graph.set_finish_point("compile_report")

app = graph.compile()


# ---------------------------------------------
#  ENTRY POINT
# ---------------------------------------------
if __name__ == "__main__":

    logger.info("COMPETITOR PRICING AGENT - Starting run")

    initial_state: AgentState = {
        "csv_path":     CSV_PATH,
        "location_id":  KROGER_LOCATION_ID,
        "products":     [],
        "access_token": None,
        "auth_error":   None,
        "raw_prices":   {},
        "results":      [],
        "report_path":  None,
        "errors":       [],
    }

    final_state = app.invoke(initial_state)

    # -- Ensure every buffered Kafka message is actually sent before exiting --
    kafka_flush()

    logger.info("FINAL JSON OUTPUT")
    logger.info(json.dumps(final_state["results"], indent=2))
