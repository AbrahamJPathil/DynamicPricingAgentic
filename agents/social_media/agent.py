"""
Graph flow:
  LOAD_CSV ──► FETCH_TRENDS ──► ANALYZE_TREND ──► COMPILE_REPORT

Decision logic:
  Trend rising sharply   → positive suggested_modifier (capture demand)
  Trend falling sharply  → negative suggested_modifier (stimulate demand)
  Trend flat / no signal → suggested_modifier = 0.0 (no action)

Output schema (exact, no extra fields):
{
  "agent_id": "string",
  "sku": "string",
  "confidence": float,
  "recommendation": { "suggested_modifier": float },
  "rationale": "string"
}
"""

import json
import logging
import pandas as pd
from pathlib import Path
from typing import TypedDict, Optional

from pytrends.request import TrendReq
from langgraph.graph import StateGraph
import requests


# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("social_trends_pricing")


# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
AGENT_ID = "social_trends_pricing"

ROOT        = Path(__file__).resolve().parents[2]
CSV_PATH    = ROOT / "data" / "db" / "products_common.csv"
OUTPUT_PATH = ROOT / "data" / "outputs" / "trends_report.json"

TRENDS_TIMEFRAME = "today 3-m"   # rolling 3-month window
TRENDS_GEO        = ""           # "" = worldwide; set e.g. "US" if needed

# Thresholds for trend classification (percentage change, momentum-based)
RISE_THRESHOLD = 15.0    # % increase over window → positive modifier
FALL_THRESHOLD = -15.0   # % decrease over window → negative modifier


# ─────────────────────────────────────────────
#  AGENT STATE
# ─────────────────────────────────────────────
class AgentState(TypedDict):
    csv_path:    str
    products:    list                 # rows from CSV as dicts
    trend_data:  dict                 # sku_id → {"series": list|None, "error": str|None}
    results:     list                 # final list of report dicts
    report_path: Optional[str]
    errors:      list


# ─────────────────────────────────────────────
#  HELPER — builds an entry when no usable trend data exists
# ─────────────────────────────────────────────
def _error_entry(sku: str, rationale: str) -> dict:
    """
    Centralised fallback entry — matches the EXACT output schema.
    No 'status' field — confidence=0.0 and modifier=0.0 communicate
    that no actionable signal was found; rationale explains why.
    """
    logger.warning("  %-20s → confidence=0.0 (%s)", sku, rationale)
    return {
        "agent_id":       AGENT_ID,
        "sku":            sku,
        "confidence":     0.0,
        "recommendation": {"suggested_modifier": 0.0},
        "rationale":      rationale,
    }


# ─────────────────────────────────────────────
#  NODE 1 — LOAD CSV
# ─────────────────────────────────────────────
def load_csv(state: AgentState) -> AgentState:
    """Read products_common.csv and validate required columns."""
    logger.info("[NODE 1/4]  Loading product catalogue from CSV")
    try:
        df = pd.read_csv(state["csv_path"])
        required = {"sku_id", "product_name", "category", "unit", "our_price"}
        missing  = required - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        products = df.to_dict(orient="records")
        logger.info("Loaded %d SKU(s): %s",
                    len(products), [p["sku_id"] for p in products])
        return {**state, "products": products, "errors": []}

    except Exception as exc:
        logger.error("CSV load failed: %s", exc)
        return {**state, "products": [], "errors": [f"CSV_LOAD_ERROR: {exc}"]}


# ─────────────────────────────────────────────
#  NODE 2 — FETCH GOOGLE TRENDS DATA
# ─────────────────────────────────────────────
def _fetch_trend_series(product_name: str) -> tuple[Optional[list], Optional[str]]:
    """
    Query Google Trends (via PyTrends) for interest-over-time data.
    Returns (series, error) — exactly one will be None.
    series is a list of integer interest scores (0-100) over the timeframe.
    """
    try:
        pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25))
        keyword = product_name
        pytrends.build_payload(
            kw_list=[keyword],
            timeframe=TRENDS_TIMEFRAME,
            geo=TRENDS_GEO,
        )
        df = pytrends.interest_over_time()

        if df is None or df.empty:
            return None, f"FETCH_ERROR: No Google Trends data for '{keyword}'"

        series = df[keyword].tolist()

        if len(series) < 2:
            return None, f"FETCH_ERROR: Insufficient data points for '{keyword}'"

        return series, None

    except Exception as exc:
        return None, f"FETCH_ERROR: {exc}"


def fetch_trends(state: AgentState) -> AgentState:
    """For each product, query Google Trends and store the interest series."""
    logger.info("[NODE 2/4]  Fetching Google Trends data via PyTrends")

    trend_data = {}
    errors     = list(state.get("errors", []))

    for product in state["products"]:
        sku  = product["sku_id"]
        name = product["product_name"]

        series, error = _fetch_trend_series(name)
        trend_data[sku] = {"series": series, "error": error}

        if series is not None:
            logger.info("  %-20s → %d data point(s)  [LIVE]", sku, len(series))
        else:
            logger.warning("  %-20s → N/A  [%s]", sku, error)
            errors.append(f"{sku}: {error}")

    return {**state, "trend_data": trend_data, "errors": errors}


# ─────────────────────────────────────────────
#  NODE 3 — ANALYSE TREND & BUILD RECOMMENDATION
# ─────────────────────────────────────────────
def _pct_change(series: list) -> float:
    n = len(series)
    third = max(1, n // 3)
    early_avg  = sum(series[:third]) / third
    recent_avg = sum(series[-third:]) / third

    if early_avg == 0:
        if recent_avg == 0:
            return 0.0  # no interest at all, no signal
        # Can't compute a true ratio — return None to signal "unreliable"
        return None

    return ((recent_avg - early_avg) / early_avg) * 100.0


def _build_recommendation(series: list) -> tuple[float, float, str, str]:
    """
    Returns (suggested_modifier, confidence, action_label, rationale).

    confidence blends:
      - data completeness (more data points → higher confidence)
      - signal strength (larger % change → higher confidence, capped)
    """
    pct_change = _pct_change(series)
    n_points   = len(series)

    if pct_change is None:
        # Baseline interest was zero — can't compute a reliable ratio
        modifier, confidence, action = 0.0, 0.15, "HOLD"
        rationale = (
            "Search interest is too sparse in the early window to compute a reliable "
            "trend — defaulting to no price change until more data is available."
        )
        logger.info("  Trend analysis: pct_change=N/A  action=%s  modifier=%+.4f  confidence=%.2f",
                    action, modifier, confidence)
        return modifier, confidence, action, rationale

    completeness_score = min(1.0, n_points / 90.0)
    signal_score        = min(1.0, abs(pct_change) / 50.0)
    confidence = round(min(0.97, 0.5 * completeness_score + 0.5 * signal_score + 0.1), 2)

    if pct_change >= RISE_THRESHOLD:
        action    = "INCREASE"
        modifier  = round(min(0.15, pct_change / 300), 4)
        rationale = (
            f"Search interest has risen {pct_change:.1f}% over the trailing window, "
            "signalling growing demand — a modest price increase can capture this momentum."
        )
    elif pct_change <= FALL_THRESHOLD:
        action    = "DECREASE"
        modifier  = round(max(-0.15, pct_change / 300), 4)
        rationale = (
            f"Search interest has fallen {abs(pct_change):.1f}% over the trailing window, "
            "signalling cooling demand — a price decrease can help stimulate sales."
        )
    else:
        action    = "HOLD"
        modifier  = 0.0
        rationale = (
            f"Search interest changed only {pct_change:.1f}% over the trailing window — "
            "not a strong enough signal to justify a price change at this time."
        )

    logger.info("  Trend analysis: pct_change=%.1f%%  action=%s  modifier=%+.4f  confidence=%.2f",
                pct_change, action, modifier, confidence)

    return modifier, confidence, action, rationale
def analyze_trend(state: AgentState) -> AgentState:
    """Compare trend signal per SKU and generate pricing recommendations."""
    logger.info("[NODE 3/4]  Analysing trends and generating recommendations")

    results = []
    errors  = list(state.get("errors", []))

    # ── Edge case: CSV failed to load — emit one fallback entry ────────────
    if not state["products"] and state["errors"]:
        results.append(_error_entry(
            sku       = "ALL_SKUS",
            rationale = f"Agent halted at CSV load stage. Error: {state['errors'][0]}",
        ))
        return {**state, "results": results, "errors": errors}

    for product in state["products"]:
        sku         = product["sku_id"]
        trend_rec   = state["trend_data"].get(sku, {"series": None, "error": "No trend record found"})
        series      = trend_rec["series"]
        fetch_error = trend_rec["error"]

        if series is None:
            entry = _error_entry(
                sku       = sku,
                rationale = fetch_error or f"No trend data retrieved for SKU {sku}.",
            )
        else:
            modifier, confidence, action, rationale = _build_recommendation(series)
            entry = {
                "agent_id":       AGENT_ID,
                "sku":            sku,
                "confidence":     confidence,
                "recommendation": {"suggested_modifier": modifier},
                "rationale":      rationale,
            }
            logger.info("  %-20s → %-10s modifier=%+.4f confidence=%.2f",
                        sku, action, modifier, confidence)

        results.append(entry)

    return {**state, "results": results, "errors": errors}


# ─────────────────────────────────────────────
#  NODE 4 — COMPILE REPORT
# ─────────────────────────────────────────────
def compile_report(state: AgentState) -> AgentState:
    """Persist JSON report and print a summary table."""
    logger.info("[NODE 4/4]  Compiling final report")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(state["results"], f, indent=2)

    print()
    print("┌" + "─" * 92 + "┐")
    print(f"│{'SOCIAL TRENDS PRICING REPORT':^92}│")
    print("├" + "─"*16 + "┬" + "─"*12 + "┬" + "─"*10 + "┬" + "─"*50 + "┤")
    print(f"│{'SKU':<16}│{'Modifier':>12}│{'Conf.':>10}│{'Rationale':<50}│")
    print("├" + "─"*16 + "┼" + "─"*12 + "┼" + "─"*10 + "┼" + "─"*50 + "┤")

    for r in state["results"]:
        sku  = r["sku"]
        mod  = f"{r['recommendation']['suggested_modifier']:+.4f}"
        conf = f"{r['confidence']:.2f}"
        rat  = r["rationale"][:48] + ("…" if len(r["rationale"]) > 48 else "")
        print(f"│{sku:<16}│{mod:>12}│{conf:>10}│{rat:<50}│")

    print("└" + "─"*16 + "┴" + "─"*12 + "┴" + "─"*10 + "┴" + "─"*50 + "┘")

    if state.get("errors"):
        logger.warning("Non-fatal errors during run:")
        for e in state["errors"]:
            logger.warning("  • %s", e)

    logger.info("Report saved → %s", OUTPUT_PATH)
    return {**state, "report_path": str(OUTPUT_PATH)}


# ─────────────────────────────────────────────
#  BUILD THE LANGGRAPH
# ─────────────────────────────────────────────
graph = StateGraph(AgentState)

graph.add_node("load_csv",       load_csv)
graph.add_node("fetch_trends",   fetch_trends)
graph.add_node("analyze_trend",  analyze_trend)
graph.add_node("compile_report", compile_report)

graph.set_entry_point("load_csv")
graph.add_edge("load_csv",      "fetch_trends")
graph.add_edge("fetch_trends",  "analyze_trend")
graph.add_edge("analyze_trend", "compile_report")
graph.set_finish_point("compile_report")

app = graph.compile()


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    try:
        if _IPYTHON:
            display(Image(app.get_graph().draw_mermaid_png()))
        else:
            logger.info("Graph structure:\n%s", app.get_graph().draw_mermaid())
    except Exception as exc:
        logger.debug("Graph render skipped: %s", exc)

    logger.info("SOCIAL TRENDS PRICING AGENT — Starting run")

    initial_state: AgentState = {
        "csv_path":    str(CSV_PATH),
        "products":    [],
        "trend_data":  {},
        "results":     [],
        "report_path": None,
        "errors":      [],
    }

    final_state = app.invoke(initial_state)

    logger.info("FINAL JSON OUTPUT\n%s", json.dumps(final_state["results"], indent=2))