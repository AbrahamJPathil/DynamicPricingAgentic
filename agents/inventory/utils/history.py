"""
Audit log and pricing history utilities for the Inventory & Perishability Agent.
"""

import json
import os
from typing import List

from core.config import PROPOSAL_LOG


def _write_log(record: dict, path: str) -> None:
    """Appends a single JSON record to a JSONL audit file."""
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def _load_recent_history(sku: str, limit: int = 3) -> List[dict]:
    """
    Reads proposals.jsonl and returns up to the last `limit` prior proposals
    for this SKU, oldest first. This is purely a local-file read - it has no
    dependency on Kafka, so it keeps working unchanged regardless of which
    broker/topic build_output_node is publishing to.

    Grounds the LLM prompt in this SKU's own track record so it can't
    hand-wave with the same generic justification every run - if a SKU has
    been discounted three times running and stock still hasn't moved, the
    model should say that explicitly instead of repeating boilerplate.
    """
    if not os.path.exists(PROPOSAL_LOG):
        return []
    history = []
    with open(PROPOSAL_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("sku_id") == sku:
                history.append(record)
    return history[-limit:]


def _format_history(history: List[dict]) -> str:
    """Renders prior proposals as a prompt block, or states plainly that none exist."""
    if not history:
        return "No prior pricing history for this SKU - this is the first time it has been evaluated."
    lines = [
        f"- {r['timestamp']}: action={r['suggested_action']}, modifier={r['price_modifier']}, "
        f"confidence={r['confidence_score']}, units_at_risk={r['units_at_risk']}, "
        f"days_to_expiry={r['days_to_expiry']}, fallback_used={r['fallback_used']}"
        for r in history
    ]
    return "Prior pricing history for this SKU, most recent last:\n" + "\n".join(lines)
