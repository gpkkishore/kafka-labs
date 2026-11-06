#!/usr/bin/env python3
"""KiA 11.3 — look at the raw bytes: Confluent wire format, decoded by hand.

    byte 0      : magic byte (always 0)
    bytes 1..4  : schema id, big-endian int  -> GET /schemas/ids/<id>
    bytes 5..   : Avro binary (no field names, varint/zigzag ints, length-prefixed strings)

    python 12-schema-registry-avro/03_wire_format.py
"""
import io
import json

import fastavro
import requests
from confluent_kafka import Consumer, TopicPartition

from sr_common import BOOTSTRAP, SCHEMA_REGISTRY_URL, TOPIC, banner

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "wire", "enable.auto.commit": False})
for p in range(3):
    lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, p))
    if hi > lo:
        c.assign([TopicPartition(TOPIC, p, lo)])
        break
m = c.poll(10)
c.close()
raw = m.value()
banner(f"raw value of {TOPIC}[{m.partition()}]@{m.offset()}: {len(raw)} bytes")
print("  hex :", raw[:48].hex(" "), "..." if len(raw) > 48 else "")
magic, schema_id = raw[0], int.from_bytes(raw[1:5], "big")
print(f"  magic byte = {magic}   schema id = {schema_id}")

schema = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}").json()["schema"]
print(f"  GET /schemas/ids/{schema_id} -> {schema[:100]}...")
record = fastavro.schemaless_reader(io.BytesIO(raw[5:]), fastavro.parse_schema(json.loads(schema)))
print(f"  decoded Avro body ({len(raw) - 5} bytes): {record}")
print("""
That's why EVERY consumer of an Avro topic needs the registry: without the id->schema lookup the bytes
are meaningless. (And why you must never write plain JSON into the same topic.)
""")
