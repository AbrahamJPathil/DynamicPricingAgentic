"""
LOCAL-TESTING STAND-IN for the real Kafka publisher - no broker, no
confluent-kafka package required.

publish_proposal()/flush() below have the EXACT same names and signatures
as the real confluent-kafka version, so agent.py needs ZERO changes to use
this - only the transport underneath changed, from a real broker back to a
local JSONL file standing in for the "event-agent" topic.

To switch back to real Kafka later (once you have a broker): replace this
file's contents with the real-Kafka version (confluent_kafka.Producer,
topic="event-agent"), and add confluent-kafka back to requirements_agent.txt.
"""

import json
import os

_MOCK_KAFKA_DIR = os.getenv("MOCK_KAFKA_DIR", "mock_kafka")
os.makedirs(_MOCK_KAFKA_DIR, exist_ok=True)
_MOCK_TOPIC_PATH = os.path.join(_MOCK_KAFKA_DIR, "event-agent.jsonl")


def publish_proposal(payload: dict, key: str = None) -> None:
    """Appends to a local JSONL file standing in for the event-agent topic."""
    with open(_MOCK_TOPIC_PATH, "a") as f:
        f.write(json.dumps(payload) + "\n")
    print(f"[mock-kafka] wrote -> {_MOCK_TOPIC_PATH}  (key={key})")


def flush(timeout: float = 10.0) -> None:
    """No-op - the write above is synchronous, so there's nothing buffered to flush."""
    pass
