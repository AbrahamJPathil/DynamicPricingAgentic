from google import genai
from google.genai.errors import ServerError
import os
import json
import re
import time

from dotenv import load_dotenv

print("Before load_dotenv")

load_dotenv()

print("After load_dotenv")

print("KEY =", os.getenv("GEMINI_API_KEY"))

from datetime import datetime

from typing import TypedDict

from langgraph.graph import StateGraph

from langchain_google_genai import ChatGoogleGenerativeAI
print("Agent started")

#state
class SeasonalState(TypedDict):

    current_row: dict

    sku_id: str

    current_temp: float

    forecast_temp: float

    weather_event: str

    sales_velocity_ratio: float

    demand_multiplier: float

    urgency: str

    llm_output: dict

    final_output: dict

#Weather Analysis Node
def analyze_weather(state):

    row = state["current_row"]

    state["sku_id"] = row["sku_id"]

    state["current_temp"] = float(
        row["current_temp_c"]
    )

    state["forecast_temp"] = float(
        row["forecast_temp_c"]
    )

    state["weather_event"] = row[
        "weather_event_flag"
    ]

    return state

#Demand Node
def compute_demand(state):

    row = state["current_row"]

    sensitivity = float(
        row["weather_sensitivity_score"]
    )

    multiplier = 1.0

    if state["weather_event"] == "Heatwave":

        multiplier += sensitivity * 0.3

    elif state["weather_event"] == "ColdSnap":

        multiplier += sensitivity * 0.1

    elif state["weather_event"] == "Storm":

        multiplier -= sensitivity * 0.2

    state["demand_multiplier"] = round(
        multiplier,
        2
    )

    return state

#Sales Velocity Node
def compute_velocity(state):

    row = state["current_row"]

    current_sales = int(
        row["units_sold_last_24h"]
    )

    baseline = int(
        row[
            "avg_units_sold_same_week_last_year"
        ]
    )

    ratio = current_sales / max(
        baseline,
        1
    )

    state["sales_velocity_ratio"] = round(
        ratio,
        2
    )

    return state

#Urgency Node
def assign_urgency(state):

    score = state["demand_multiplier"]

    if score >= 1.3:

        state["urgency"] = "HIGH"

    elif score >= 1.1:

        state["urgency"] = "MEDIUM"

    else:

        state["urgency"] = "LOW"

    return state

#Gemini Node
print("Creating Gemini client")

client = genai.Client(
    api_key=os.getenv(
        "GEMINI_API_KEY"
    )
)

print("Gemini client created")

def _fallback_llm_output(state, reason=None):
    urgency = state.get("urgency", "LOW")
    if urgency == "HIGH":
        suggested_action = "DISCOUNT"
        price_modifier = 0.85
    elif urgency == "MEDIUM":
        suggested_action = "DISCOUNT"
        price_modifier = 0.95
    else:
        suggested_action = "HOLD"
        price_modifier = 1.0

    return {
        "suggested_action": suggested_action,
        "price_modifier": price_modifier,
        "confidence_score": 0.0,
        "headline": "Fallback recommendation - LLM unavailable",
        "detailed_reasoning": (
            f"LLM was unavailable or failed. Using deterministic fallback for urgency={urgency}, "
            f"demand_multiplier={state.get('demand_multiplier')}. "
            + (f"Reason: {reason}" if reason else "")
        ).strip(),
    }


def _parse_llm_response(raw):
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
    except json.JSONDecodeError as exc:
        print(f"[call_llm] JSON parse failed: {exc}")
        print(f"[call_llm] Raw output: {raw[:300]}")
        return None

    required_keys = {
        "suggested_action",
        "price_modifier",
        "confidence_score",
        "headline",
        "detailed_reasoning",
    }
    missing_keys = required_keys - set(data.keys())
    if missing_keys:
        print(f"[call_llm] Missing required fields: {missing_keys}")
        print(f"[call_llm] Raw output: {raw[:300]}")
        return None

    return data


def _call_gemini_with_retry(prompt, max_attempts=3, base_delay=2.0):
    for attempt in range(1, max_attempts + 1):
        try:
            return client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt
            )
        except ServerError as exc:
            print(
                f"[call_llm] Gemini ServerError (attempt {attempt}/{max_attempts}): {exc}"
            )
            if attempt == max_attempts:
                raise
            wait_seconds = base_delay * (2 ** (attempt - 1))
            print(f"[call_llm] Retrying in {wait_seconds:.1f}s...")
            time.sleep(wait_seconds)
        except Exception as exc:
            print(f"[call_llm] Unexpected Gemini error: {exc}")
            raise


def call_llm(state):

    print("Calling Gemini...")

    prompt = f"""
You are a Seasonal Intelligence Agent.

Weather Event:
{state["weather_event"]}

Current Temp:
{state["current_temp"]}

Forecast Temp:
{state["forecast_temp"]}

Sales Velocity:
{state["sales_velocity_ratio"]}

Demand Multiplier:
{state["demand_multiplier"]}

Urgency:
{state["urgency"]}

Return ONLY valid JSON.

Rules:
- headline must be less than 8 words.
- detailed_reasoning must contain only 2-3 short sentences.
- Maximum 40 words total.
- No bullet points.
- No numbered lists.
- Be concise and business focused.

Schema:

{{
  "suggested_action":"",
  "price_modifier":0,
  "confidence_score":0,
  "headline":"",
  "detailed_reasoning":""
}}
"""

    print("Sending Gemini request")

    try:
        response = _call_gemini_with_retry(prompt)

    except Exception as exc:

        print(f"[call_llm] Gemini request failed: {exc}")

        state["llm_output"] = _fallback_llm_output(
            state,
            reason=str(exc)
        )

        return state

    print("Gemini response received")

    raw = getattr(response, "text", None)

    if raw is None:
        raw = getattr(response, "content", "")

    raw = raw or ""

    llm_output = _parse_llm_response(raw)

    if llm_output is None:

        state["llm_output"] = _fallback_llm_output(
            state,
            reason="Invalid LLM JSON response"
        )

        return state

    state["llm_output"] = llm_output

    return state


#Output Node
def build_output(state):

    llm = state["llm_output"]

    state["final_output"] = {

        "agent_id": "seasonal_intelligence",

        "sku": state["sku_id"],

        "confidence": float(
            llm["confidence_score"]
        ),

        "recommendation": {

            "suggested_modifier": float(
                llm["price_modifier"]
            )
        },

        "rationale": (
            f"{llm['headline']}. "
            f"{llm['detailed_reasoning']}"
        )
    }

    return state
 #Graph
graph = StateGraph(
    SeasonalState
)

graph.add_node(
    "analyze_weather",
    analyze_weather
)

graph.add_node(
    "compute_demand",
    compute_demand
)

graph.add_node(
    "compute_velocity",
    compute_velocity
)

graph.add_node(
    "assign_urgency",
    assign_urgency
)

graph.add_node(
    "call_llm",
    call_llm
)

graph.add_node(
    "build_output",
    build_output
)

graph.set_entry_point(
    "analyze_weather"
)

graph.add_edge(
    "analyze_weather",
    "compute_demand"
)

graph.add_edge(
    "compute_demand",
    "compute_velocity"
)

graph.add_edge(
    "compute_velocity",
    "assign_urgency"
)

graph.add_edge(
    "assign_urgency",
    "call_llm"
)

graph.add_edge(
    "call_llm",
    "build_output"
)

graph.set_finish_point(
    "build_output"
)

seasonal_graph = graph.compile()

#Entry Function
#orchestrator can call
def seasonal_agent(row):
    print ("inside seasonal_agent function")

    result = seasonal_graph.invoke({

        "current_row": row
    })

    return result[
        "final_output"
    ]
    

if __name__ == "__main__":

    test_row = {

        "sku_id": "MEA001",

        "current_temp_c": 32.8,

        "forecast_temp_c": 35.0,

        "weather_event_flag": "Heatwave",

        "weather_sensitivity_score": 0.9,

        "units_sold_last_24h": 150,

        "avg_units_sold_same_week_last_year": 100
    }

    result = seasonal_agent(
        test_row
    )

    import json

    print(
        json.dumps(
            result,
            indent=4
        )
    ) 
