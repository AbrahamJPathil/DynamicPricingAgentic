"""
Inventory & Perishability Agent
Dynamic Pricing POC - LangGraph + Gemini

Graph nodes:
    load_csv          -> check_perishable -> compute_expiry -> compute_loss
                      -> assign_urgency   -> call_llm       -> build_output
                      -> advance_row      -> (loop or END)

Run:
    python inventory_agent.py --api-key YOUR_KEY
"""

import json
import os
import uuid

from dotenv import load_dotenv

from core.config import PROPOSAL_LOG, VALIDATION_LOG
from core.models import AgentState
from graph.graph import build_graph
from kafka_publisher import flush as kafka_flush, publish_detailed


def main():
    load_dotenv()
    gcp_project = os.getenv("GCP_PROJECT_ID")
    if not gcp_project:
        raise EnvironmentError(
            "GCP_PROJECT_ID not found in environment. "
            "Ensure a .env file exists at the project root with:\n"
            "  GCP_PROJECT_ID=your-gcp-project-id\n"
            "Also set GOOGLE_APPLICATION_CREDENTIALS to point to your service account key JSON file."
        )

    app = build_graph()
    run_id = str(uuid.uuid4())[:8]
    print(f"\n[main] Starting run  run_id={run_id}")

    initial_state: AgentState = {
        "gcp_project": gcp_project,
        "run_id": run_id,
        "rows": [],
        "current_row": None,
        "is_perishable": None,
        "days_to_expiry": None,
        "units_at_risk": None,
        "expiry_loss_rate": None,
        "loss_if_no_action": None,
        "urgency": None,
        "llm_response": None,
        "results": [],
        "all_token_usage": [],
        "row_index": 0,
        "urgency_queue": [],
    }

    final_state = app.invoke(initial_state)

    usage = final_state["all_token_usage"]
    sep = "-" * 62
    print(f"\n[main] {sep}")
    print(f"[main]  TOKEN SUMMARY  run_id={run_id}")
    print(f"[main] {sep}")
    print(
        f"[main]  {'SKU':<10} {'PROMPT':>8} {'COMPLETION':>12} {'TOTAL':>8} {'COST (USD)':>12}"
    )
    print(f"[main] {sep}")
    grand_prompt = grand_completion = grand_total = grand_cost = 0
    for t in usage:
        print(
            f"[main]  {t['sku_id']:<10} {t['prompt_tokens']:>8} "
            f"{t['completion_tokens']:>12} {t['total_tokens']:>8} "
            f"{t['estimated_cost_usd']:>12.6f}"
        )
        grand_prompt += t["prompt_tokens"]
        grand_completion += t["completion_tokens"]
        grand_total += t["total_tokens"]
        grand_cost += t["estimated_cost_usd"]
    print(f"[main] {sep}")
    print(
        f"[main]  {'TOTAL':<10} {grand_prompt:>8} {grand_completion:>12} "
        f"{grand_total:>8} {grand_cost:>12.6f}"
    )
    print(f"[main] {sep}")
    print(f"[main]  Proposal log   -> {PROPOSAL_LOG}")
    print(f"[main]  Validation log -> {VALIDATION_LOG}")
    print(f"[main] {sep}\n")

    kafka_flush()

    print(f"[OK] Done - {len(final_state['results'])} proposal(s) generated\n")
    for output in final_state["results"]:
        print(json.dumps(output, indent=2))
        print()
        publish_detailed(output, key=output["sku_id"])


if __name__ == "__main__":
    main()
