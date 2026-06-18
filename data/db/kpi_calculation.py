"""
+------------------------------------------------------------+
|          KPI CALCULATION AGENT  -  LangGraph Edition        |
|     Supabase fetch  *  KPI math  *  Supabase push           |
+------------------------------------------------------------+

Graph flow:
  FETCH_PRODUCTS --> CALCULATE_KPIS --> PUSH_TO_SUPABASE

Reads from:   products_sku   (Supabase table)
Writes to:    kpi_values     (Supabase table)
"""

import os
import logging
from datetime import datetime, timezone
from typing import TypedDict, Optional

from dotenv import load_dotenv
from supabase import create_client, Client
from langgraph.graph import StateGraph

load_dotenv()


# ---------------------------------------------
#  LOGGING
# ---------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("kpi_calculation")


# ---------------------------------------------
#  CONFIGURATION
# ---------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

SOURCE_TABLE = "products_sku"
TARGET_TABLE = "kpi_values"


# ---------------------------------------------
#  AGENT STATE
# ---------------------------------------------
class AgentState(TypedDict):
    products:    list           # rows fetched from products_sku
    kpi_rows:    list           # calculated KPI rows ready for push
    errors:      list
    pushed_count: Optional[int]


# ---------------------------------------------
#  SUPABASE CLIENT (built once, reused)
# ---------------------------------------------
def _get_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise EnvironmentError(
            "Missing SUPABASE_URL or SUPABASE_KEY environment variables. "
            "Set them in your .env file before running this agent."
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------
#  NODE 1 - FETCH PRODUCTS FROM SUPABASE
# ---------------------------------------------
def fetch_products(state: AgentState) -> AgentState:
    """Fetch all rows from products_sku table."""
    logger.info("[NODE 1/3]  Fetching product data from Supabase table '%s'", SOURCE_TABLE)
    try:
        client = _get_client()
        response = client.table(SOURCE_TABLE).select("*").execute()
        products = response.data or []

        logger.info("Fetched %d row(s) from '%s'", len(products), SOURCE_TABLE)
        return {**state, "products": products, "errors": []}

    except Exception as exc:
        msg = f"FETCH_ERROR: Could not fetch from '{SOURCE_TABLE}' - {exc}"
        logger.error(msg)
        return {**state, "products": [], "errors": [msg]}


# ---------------------------------------------
#  NODE 2 - CALCULATE KPIs
# ---------------------------------------------
def _parse_datetime(value) -> Optional[datetime]:
    """Parse an ISO datetime string (or passthrough if already a datetime)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # Handles both 'Z' suffix and '+00:00' style ISO strings
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _calculate_kpis_for_row(product: dict) -> Optional[dict]:
    """
    Calculate all KPIs for a single product row.
    Returns None if required fields are missing - that SKU is skipped,
    not silently zero-filled, so gaps are visible rather than hidden.
    """
    sku_id = product.get("sku_id")

    our_price             = product.get("our_price")
    cost_price            = product.get("cost_price")
    units_sold_last_24h   = product.get("units_sold_last_24h")
    stock_on_hand         = product.get("stock_on_hand")
    avg_daily_units_sold  = product.get("avg_daily_units_sold")
    expiry_datetime_raw   = product.get("expiry_datetime")

    missing = [
        name for name, val in [
            ("our_price", our_price),
            ("cost_price", cost_price),
            ("units_sold_last_24h", units_sold_last_24h),
            ("stock_on_hand", stock_on_hand),
            ("avg_daily_units_sold", avg_daily_units_sold),
            ("expiry_datetime", expiry_datetime_raw),
        ] if val is None
    ]
    if missing:
        logger.warning("  %-20s -> SKIPPED, missing fields: %s", sku_id, missing)
        return None

    expiry_dt = _parse_datetime(expiry_datetime_raw)
    if expiry_dt is None:
        logger.warning("  %-20s -> SKIPPED, unparseable expiry_datetime: %s",
                        sku_id, expiry_datetime_raw)
        return None

    # -- Gross Margin (%) -------------------------------------------------
    gross_margin = round((our_price - cost_price) / our_price * 100, 2) if our_price else None

    # -- Daily Revenue ----------------------------------------------------
    daily_revenue = round(units_sold_last_24h * our_price, 2)

    # -- Weeks of Supply --------------------------------------------------
    weeks_of_supply = (
        round(stock_on_hand / avg_daily_units_sold / 7, 2)
        if avg_daily_units_sold else None
    )

    # -- Days to Expiry ---------------------------------------------------
    now = datetime.now(timezone.utc)
    days_to_expiry = (expiry_dt - now).days

    # -- High Risk Expiry Flag --------------------------------------------
    inventory_coverage_days = (weeks_of_supply * 7) if weeks_of_supply is not None else None
    is_high_risk = (
        days_to_expiry < inventory_coverage_days
        if inventory_coverage_days is not None else None
    )

    # -- Estimated Waste Units --------------------------------------------
    projected_units_sold_before_expiry = days_to_expiry * avg_daily_units_sold
    estimated_waste_units = round(max(0, stock_on_hand - projected_units_sold_before_expiry), 2)

    # -- Avg Daily Sales Revenue ------------------------------------------
    avg_daily_sales_revenue = round(avg_daily_units_sold * our_price, 2)

    return {
        "sku_id":                   sku_id,
        "gross_margin_pct":         gross_margin,
        "daily_revenue":            daily_revenue,
        "weeks_of_supply":          weeks_of_supply,
        "days_to_expiry":           days_to_expiry,
        "is_high_risk":             is_high_risk,
        "estimated_waste_units":    estimated_waste_units,
        "avg_daily_sales_revenue":  avg_daily_sales_revenue,
        "calculated_at":            now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def calculate_kpis(state: AgentState) -> AgentState:
    """Calculate KPIs for every fetched product row."""
    logger.info("[NODE 2/3]  Calculating KPIs")

    kpi_rows = []
    errors   = list(state.get("errors", []))

    if not state["products"] and state["errors"]:
        logger.warning("No products available - skipping KPI calculation.")
        return {**state, "kpi_rows": [], "errors": errors}

    for product in state["products"]:
        sku_id = product.get("sku_id", "UNKNOWN")
        try:
            kpi_row = _calculate_kpis_for_row(product)
            if kpi_row is not None:
                kpi_rows.append(kpi_row)
                logger.info(
                    "  %-20s -> margin=%.1f%%  revenue=$%.2f  WOS=%.2f  high_risk=%s  waste=%.1f",
                    sku_id,
                    kpi_row["gross_margin_pct"] or 0,
                    kpi_row["daily_revenue"],
                    kpi_row["weeks_of_supply"] or 0,
                    kpi_row["is_high_risk"],
                    kpi_row["estimated_waste_units"],
                )
        except Exception as exc:
            msg = f"{sku_id}: CALC_ERROR: {exc}"
            logger.error("  %s", msg)
            errors.append(msg)

    logger.info("Calculated KPIs for %d / %d SKU(s)", len(kpi_rows), len(state["products"]))
    return {**state, "kpi_rows": kpi_rows, "errors": errors}


# ---------------------------------------------
#  NODE 3 - PUSH TO SUPABASE
# ---------------------------------------------
def push_to_supabase(state: AgentState) -> AgentState:
    """Upsert calculated KPI rows into the kpi_values table."""
    logger.info("[NODE 3/3]  Pushing KPI rows to Supabase table '%s'", TARGET_TABLE)

    if not state["kpi_rows"]:
        logger.warning("No KPI rows to push - skipping.")
        return {**state, "pushed_count": 0}

    try:
        client = _get_client()
        response = client.table(TARGET_TABLE).upsert(
            state["kpi_rows"], on_conflict="sku_id"
        ).execute()

        pushed = len(response.data) if response.data else len(state["kpi_rows"])
        logger.info("Pushed %d row(s) to '%s'", pushed, TARGET_TABLE)
        return {**state, "pushed_count": pushed}

    except Exception as exc:
        msg = f"PUSH_ERROR: Could not push to '{TARGET_TABLE}' - {exc}"
        logger.error(msg)
        errors = list(state.get("errors", []))
        errors.append(msg)
        return {**state, "errors": errors, "pushed_count": 0}


# ---------------------------------------------
#  BUILD THE LANGGRAPH
# ---------------------------------------------
graph = StateGraph(AgentState)

graph.add_node("fetch_products",   fetch_products)
graph.add_node("calculate_kpis",   calculate_kpis)
graph.add_node("push_to_supabase", push_to_supabase)

graph.set_entry_point("fetch_products")
graph.add_edge("fetch_products",   "calculate_kpis")
graph.add_edge("calculate_kpis",   "push_to_supabase")
graph.set_finish_point("push_to_supabase")

app = graph.compile()


# ---------------------------------------------
#  ENTRY POINT
# ---------------------------------------------
if __name__ == "__main__":
    logger.info("KPI CALCULATION AGENT - Starting run")

    initial_state: AgentState = {
        "products":     [],
        "kpi_rows":     [],
        "errors":       [],
        "pushed_count": None,
    }

    final_state = app.invoke(initial_state)

    logger.info("Run complete. KPI rows pushed: %s", final_state.get("pushed_count"))
    if final_state.get("errors"):
        logger.warning("Errors encountered during run:")
        for e in final_state["errors"]:
            logger.warning("  * %s", e)