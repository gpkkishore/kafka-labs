#!/usr/bin/env python3
"""KiA 11.3 / DG ch.3 "Serializing using Apache Avro" / AKiA 13.1 — produce Avro records.

The AvroSerializer:
  1. registers (or looks up) the schema under subject "<topic>-value" (TopicNameStrategy)
  2. writes  [0x00 magic byte][4-byte schema id][Avro binary body]  — NO field names in the payload!

    python 12-schema-registry-avro/01_avro_producer.py              # v1 records
    python 12-schema-registry-avro/01_avro_producer.py --version 2  # v2 records (coupon + status)
"""
import argparse
import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from sr_common import BOOTSTRAP, SCHEMA_REGISTRY_URL, TOPIC, banner, ensure_topic, load_schema, log

ap = argparse.ArgumentParser()
ap.add_argument("--version", type=int, choices=[1, 2], default=1)
ap.add_argument("-n", type=int, default=5)
args = ap.parse_args()

ensure_topic(TOPIC, 3, 3)
sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
schema_str = load_schema(f"order_v{args.version}.avsc")
value_ser = AvroSerializer(sr, schema_str)          # auto.register.schemas=True by default
key_ser = StringSerializer("utf_8")
p = Producer({"bootstrap.servers": BOOTSTRAP})

banner(f"Producing {args.n} Order v{args.version} records to {TOPIC}")
for i in range(args.n):
    order = {"order_id": f"o-{int(time.time())}-{i}", "customer": random.choice(["alice", "bob", "carol"]),
             "amount": round(random.uniform(5, 300), 2), "created_at": int(time.time() * 1000)}
    if args.version == 2:
        order |= {"coupon": random.choice([None, "BLACKFRIDAY"]), "status": "PAID"}
    p.produce(TOPIC, key=key_ser(order["order_id"]),
              value=value_ser(order, SerializationContext(TOPIC, MessageField.VALUE)),
              on_delivery=lambda e, m: log(f"p{m.partition()}@{m.offset()} {len(m.value())} bytes"))
p.flush()

latest = sr.get_latest_version(f"{TOPIC}-value")
log(f"subject {TOPIC}-value latest version={latest.version} schema id={latest.schema_id}")
try:
    import json as _json
    as_json = len(_json.dumps(order).encode())
    log(f"same record as JSON would be {as_json} bytes (field names repeated in every message)")
except Exception:  # noqa: BLE001
    pass
