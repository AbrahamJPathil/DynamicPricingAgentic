"""
Lightweight Kafka publisher for PoC agents running on this WSL instance.

Every agent gets its own topic; this module currently targets "inventory-agent".
If you add more agents later, either parametrize TOPIC per-call or create one
small publisher module per agent following this same pattern.

Usage:
    from kafka_publisher import publish_proposal, flush

    publish_proposal(output_dict, key=sku_id)
    ...
    flush()   # call once, right before the process exits
"""

import json
from typing import Optional

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "inventory-agent"
TOPIC_DETAILED = "inventory-detailed"

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
    Serializes payload to JSON and produces it onto the inventory-agent topic.
    This call is non-blocking - the message is buffered and sent asynchronously.
    """
    _producer.produce(
        topic=TOPIC,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(payload).encode("utf-8"),
        callback=_delivery_report,
    )
    # Triggers any pending delivery-report callbacks without blocking.
    _producer.poll(0)


def publish_detailed(payload: dict, key: Optional[str] = None) -> None:
    """
    Serializes payload to JSON and produces it onto the inventory-detailed topic.
    Non-blocking - message is buffered and sent asynchronously.
    """
    _producer.produce(
        topic=TOPIC_DETAILED,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(payload).encode("utf-8"),
        callback=_delivery_report,
    )
    _producer.poll(0)


def flush(timeout: float = 10.0) -> None:
    """
    Blocks until all buffered messages are sent (or timeout is reached).
    Always call this once before the program exits, otherwise buffered
    messages that haven't been flushed yet can be silently lost.
    """
    _producer.flush(timeout)
