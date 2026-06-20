"""
Kafka publisher for the full competitor pricing report.

Publishes the complete results list (one message per agent run) to the
"competitor-detailed" topic.  The payload is the raw list of report
entries - every SKU, including metrics_evaluated, proposal, and
justification - making it the verbose counterpart to the slim
"competitor-agent" topic messages.

Usage:
    from competitor_detailed_kafka_publisher import publish_report, flush

    publish_report(results_list)   # call once, after the run is complete
    flush()                        # call once, right before the process exits
"""

import json
from typing import Optional

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "competitor-detailed"

_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def _delivery_report(err, msg) -> None:
    """Confluent-Kafka callback fired once per message, async, on the next poll()."""
    if err is not None:
        print(f"[kafka-detailed] delivery failed (key={msg.key()}): {err}")
    else:
        print(
            f"[kafka-detailed] delivered -> topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def publish_report(results: list, key: Optional[str] = None) -> None:
    """
    Serializes the full results list to JSON and produces it onto the
    competitor-detailed topic as a single message.

    This call is non-blocking - the message is buffered and sent asynchronously.

    Args:
        results:  The complete list of per-SKU report dicts (final_state["results"]).
        key:      Optional Kafka message key (e.g. a run-id or timestamp string).
                  When None the message is routed by the default partitioner.
    """
    _producer.produce(
        topic=TOPIC,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(results).encode("utf-8"),
        callback=_delivery_report,
    )
    # Triggers any pending delivery-report callbacks without blocking.
    _producer.poll(0)


def flush(timeout: float = 10.0) -> None:
    """
    Blocks until all buffered messages are sent (or timeout is reached).
    Always call this once before the program exits.
    """
    _producer.flush(timeout)
