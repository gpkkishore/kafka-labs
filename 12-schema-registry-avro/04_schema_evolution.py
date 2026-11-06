#!/usr/bin/env python3
"""KiA 11.4 / AKiA 13.1 — compatibility rules, tested against the registry.

For each compatibility mode we register v1 under a fresh subject, then ask the registry
"would this change be accepted?" (POST /compatibility/subjects/<s>/versions/latest).

  BACKWARD (default): NEW schema can read data written with the OLD one   -> upgrade CONSUMERS first
  FORWARD           : OLD schema can read data written with the NEW one   -> upgrade PRODUCERS first
  FULL              : both
  NONE              : anything goes (dangerous)
  *_TRANSITIVE      : checked against ALL previous versions, not just the latest

    python 12-schema-registry-avro/04_schema_evolution.py
"""
import json

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from sr_common import SCHEMA_REGISTRY_URL, banner

sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

BASE = [{"name": "order_id", "type": "string"}, {"name": "customer", "type": "string"},
        {"name": "amount", "type": "double"}, {"name": "note", "type": "string", "default": ""}]


def schema(fields):
    return Schema(json.dumps({"type": "record", "name": "Order", "namespace": "sr.evo", "fields": fields}), "AVRO")


CHANGES = {
    "add field WITH default":      BASE + [{"name": "coupon", "type": ["null", "string"], "default": None}],
    "add field WITHOUT default":   BASE + [{"name": "region", "type": "string"}],
    "remove field WITH default":   [f for f in BASE if f["name"] != "note"],
    "remove field WITHOUT default": [f for f in BASE if f["name"] != "customer"],
    "change type double->string":  [f if f["name"] != "amount" else {"name": "amount", "type": "string"} for f in BASE],
    "narrow type double->float":   [f if f["name"] != "amount" else {"name": "amount", "type": "float"} for f in BASE],
}
MODES = ["BACKWARD", "FORWARD", "FULL", "NONE"]

results = {}
for mode in MODES:
    subject = f"sr_evolution_{mode.lower()}-value"
    try:
        sr.delete_subject(subject, permanent=False)
        sr.delete_subject(subject, permanent=True)
    except Exception:  # noqa: BLE001
        pass
    sr.register_schema(subject, schema(BASE))
    sr.set_compatibility(subject, mode)
    for change, fields in CHANGES.items():
        results[(change, mode)] = sr.test_compatibility(subject, schema(fields))

banner("Is the change accepted?  (✔ = registry allows registering it)")
print(f"{'change':32s}" + "".join(f"{m:>10s}" for m in MODES))
for change in CHANGES:
    print(f"{change:32s}" + "".join(f"{'✔' if results[(change, m)] else '✘':>10s}" for m in MODES))

banner("Registering an incompatible schema fails with HTTP 409")
subject = "sr_evolution_backward-value"
try:
    sr.register_schema(subject, schema(CHANGES["add field WITHOUT default"]))
except Exception as e:  # noqa: BLE001
    print(f"  {type(e).__name__}: {e}")
print("""
Rules of thumb for Avro:
  * ALWAYS give new fields a default -> safe in every mode.
  * Only remove fields that HAVE a default.
  * Never change a field's type (except Avro promotions int->long->float->double, string<->bytes).
  * Renames: add an "aliases" entry instead of renaming.
Then try 02_avro_consumer.py --reader-v1 after producing v2 records: the old reader silently drops coupon/status.
""")
