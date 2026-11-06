"""Shared bits for folder 12."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.kafka_helpers import BOOTSTRAP, SCHEMA_REGISTRY_URL, banner, ensure_topic, log  # noqa: E402,F401

HERE = Path(__file__).resolve().parent
TOPIC = "sr_orders"


def load_schema(name: str) -> str:
    return (HERE / "schemas" / name).read_text()


def pretty(schema_str: str) -> str:
    return json.dumps(json.loads(schema_str))
