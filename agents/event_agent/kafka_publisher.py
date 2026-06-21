"""
LOCAL-TESTING STAND-IN for the real Kafka publisher - no broker, no
confluent-kafka package required.

publish_proposal()/flush() below have the EXACT same names and signatures
as the real confluent-kafka version, so agent.py needs ZERO changes to use
this - only the transport underneath changed, from a real broker back to a
set of local JSONL files (one per topic) under MOCK_KAFKA_DIR, each
standing in for the real Kafka topic of the same name - e.g. "event-agent"
or "calendar-detailed".

`topic` defaults to "event-agent" so every existing call site that doesn't
pass it explicitly keeps writing exactly where it always did - this mirrors
the real confluent-kafka Producer.produce(topic, ...) call, which also
takes the topic per-call rather than baking it into one producer per topic.

To switch back to real Kafka later (once you have a broker): replace this
file's contents with the real-Kafka version (confluent_kafka.Producer,
producer.produce(topic, key=key, value=...)), and add confluent-kafka back
to requirements_agent.txt.
"""

import json
import os

_MOCK_KAFKA_DIR = os.getenv("MOCK_KAFKA_DIR", "mock_kafka")
os.makedirs(_MOCK_KAFKA_DIR, exist_ok=True)

_DEFAULT_TOPIC = "event-agent"


def _topic_path(topic: str) -> str:
    """Maps a topic name to its local JSONL stand-in file under MOCK_KAFKA_DIR."""
    return os.path.join(_MOCK_KAFKA_DIR, f"{topic}.jsonl")


def publish_proposal(payload: dict, key: str = None, topic: str = _DEFAULT_TOPIC) -> None:
    """Appends to a local JSONL file standing in for the given Kafka topic."""
    path = _topic_path(topic)
    with open(path, "a") as f:
        f.write(json.dumps(payload) + "\n")
    print(f"[mock-kafka] wrote -> {path}  (topic={topic}, key={key})")


def flush(timeout: float = 10.0) -> None:
    """No-op - the write above is synchronous, so there's nothing buffered to flush."""
    pass
