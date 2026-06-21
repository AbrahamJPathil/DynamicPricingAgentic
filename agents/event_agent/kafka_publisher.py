"""
REAL Kafka publisher - connects to an actual broker via confluent-kafka.
No local JSONL stand-in anymore; this writes to real Kafka topics.

publish_proposal()/flush() keep the EXACT same names and signatures as the
local-mock version this replaces, so agent.py needs ZERO changes to use
this - only the transport underneath changed, from local files back to a
real broker.

Required env var (add to your .env at the project root):
    KAFKA_BOOTSTRAP_SERVERS   - comma-separated host:port list, e.g.
                                 "broker1.example.com:9092,broker2.example.com:9092"

Optional env vars - only set these if your broker requires auth/TLS
(skip entirely for a local/plaintext broker):
    KAFKA_SECURITY_PROTOCOL  - e.g. "SASL_SSL"
    KAFKA_SASL_MECHANISM     - e.g. "PLAIN" or "SCRAM-SHA-256"
    KAFKA_SASL_USERNAME
    KAFKA_SASL_PASSWORD

Add confluent-kafka to requirements_agent.txt:
    confluent-kafka>=2.0.0
"""

import json
import os
from typing import Iterable, Union

from confluent_kafka import Producer
from dotenv import load_dotenv

# Self-sufficient regardless of import order elsewhere (same reasoning as
# supabase_client.py - this module's own os.getenv() calls below run at
# import time, possibly before agent.py's main() calls load_dotenv() itself).
load_dotenv()

_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
if not _BOOTSTRAP_SERVERS:
    raise EnvironmentError(
        "KAFKA_BOOTSTRAP_SERVERS not found in environment. "
        "Add it to your .env file at the project root, e.g.:\n"
        "  KAFKA_BOOTSTRAP_SERVERS=broker1.example.com:9092,broker2.example.com:9092"
    )

_producer_config = {"bootstrap.servers": _BOOTSTRAP_SERVERS}

# Auth/TLS is optional - only wired up if explicitly configured, so this
# still works unmodified against a plaintext local/dev broker.
_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
if _security_protocol:
    _producer_config["security.protocol"] = _security_protocol
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
    if sasl_mechanism:
        _producer_config["sasl.mechanism"] = sasl_mechanism
    if sasl_username:
        _producer_config["sasl.username"] = sasl_username
    if sasl_password:
        _producer_config["sasl.password"] = sasl_password

_producer = Producer(_producer_config)

# Default fan-out: any call that doesn't pass `topic` explicitly publishes
# to both of these real topics with the same payload.
_DEFAULT_TOPICS = ("events-detailed", "calendar-agent")


def _delivery_report(err, msg) -> None:
    """confluent-kafka invokes this once per message, asynchronously, when
    the broker has acked it (or definitively failed to)."""
    if err is not None:
        print(f"[kafka] [ERROR]  delivery failed  topic={msg.topic()}  "
              f"key={msg.key()}:  {err}")
    else:
        print(f"[kafka] delivered -> topic={msg.topic()}  "
              f"partition={msg.partition()}  offset={msg.offset()}")


def publish_proposal(
    payload: dict,
    key: str = None,
    topic: Union[str, Iterable[str]] = _DEFAULT_TOPICS,
) -> None:
    """Publishes payload (as JSON) to each given Kafka topic on the real
    broker. `topic` can be a single topic name (str) or a list/tuple of
    topic names - defaults to both "events-detailed" and "calendar-agent".

    This is non-blocking: produce() queues the message locally and returns
    immediately; delivery is confirmed asynchronously via the callback.
    Call flush() (e.g. once at the end of a run) to block until every
    queued message has actually been delivered or failed."""
    topics = [topic] if isinstance(topic, str) else list(topic)
    value = json.dumps(payload).encode("utf-8")
    key_bytes = key.encode("utf-8") if key is not None else None
    for t in topics:
        _producer.produce(t, key=key_bytes, value=value, callback=_delivery_report)
    # poll(0) is non-blocking - it just serves any delivery callbacks that
    # are already ready, so the producer's internal queue doesn't silently
    # fill up over a long run of many produce() calls without ever flushing.
    _producer.poll(0)


def flush(timeout: float = 10.0) -> None:
    """Blocks until all queued messages are delivered (or the timeout is
    hit), firing _delivery_report for each along the way."""
    remaining = _producer.flush(timeout)
    if remaining > 0:
        print(f"[kafka] [WARNING]  flush timed out after {timeout}s - "
              f"{remaining} message(s) still undelivered")