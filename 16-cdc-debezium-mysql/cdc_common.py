"""Shared bits for the CDC scripts: connections + Debezium envelope helpers."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "15-kafka-connect"))

import pymysql  # noqa: E402
from connect_api import Connect, print_status  # noqa: E402,F401

MYSQL = dict(host="127.0.0.1", port=3306, user="root", password="rootpw", autocommit=True)
TOPIC_PREFIX = "cdc"                                   # Debezium topic.prefix => cdc.<db>.<table>
TABLE_TOPICS = [f"{TOPIC_PREFIX}.inventory.customers", f"{TOPIC_PREFIX}.inventory.products",
                f"{TOPIC_PREFIX}.inventory.orders"]
OPS = {"r": "READ (snapshot)", "c": "CREATE", "u": "UPDATE", "d": "DELETE", "t": "TRUNCATE"}


def mysql(db: str | None = "inventory"):
    return pymysql.connect(database=db, cursorclass=pymysql.cursors.DictCursor, **MYSQL)


def unwrap(raw: bytes | None):
    """JsonConverter with schemas.enable=true wraps everything as {"schema":..., "payload":...}.
    Returns the payload (the Debezium envelope), or None for a tombstone."""
    if raw is None:
        return None
    doc = json.loads(raw)
    return doc["payload"] if isinstance(doc, dict) and "payload" in doc and "schema" in doc else doc
