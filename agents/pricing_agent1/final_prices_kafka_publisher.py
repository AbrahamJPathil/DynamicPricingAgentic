"""
Lightweight Kafka publisher for the pricing orchestrator agent.
Same pattern as kafka_publisher.py / competitor_kafka_publisher.py,
just pointed at the final_prices topic.

Usage:
    from final_prices_kafka_publisher import publish_proposal, flush

    publish_proposal(payload_dict, key=sku)
    ...
    flush()   # call once, right before the process exits
"""

import json
from typing import Optional

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "final_prices"

_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def _delivery_report(err, msg) -> None:
    """Confluent-Kafka callback fired once per message, async, on the next poll()."""
    if err is not None:
        print(f"[kafka] delivery failed (key={msg.key()}): {err}")
    else:
        print(
            f"[kafka] delivered -> topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def publish_proposal(payload: dict, key: Optional[str] = None) -> None:
    """
    Serializes payload to JSON and produces it onto the final_prices topic.
    This call is non-blocking - the message is buffered and sent asynchronously.
    """
    _producer.produce(
        topic=TOPIC,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(payload).encode("utf-8"),
        callback=_delivery_report,
    )
    _producer.poll(0)


def flush(timeout: float = 10.0) -> None:
    """Blocks until all buffered messages are sent (or timeout is reached)."""
    _producer.flush(timeout)
