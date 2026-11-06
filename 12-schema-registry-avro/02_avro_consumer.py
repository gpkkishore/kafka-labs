#!/usr/bin/env python3
"""KiA 11.3 — consume Avro records. The deserializer reads the schema id from each message and fetches
(and caches) the WRITER schema from the registry.

Optional --reader-v1: deserialize with the v1 schema as READER schema -> Avro schema resolution drops
fields the old reader doesn't know (this is what an un-upgraded consumer experiences).

    python 12-schema-registry-avro/02_avro_consumer.py
    python 12-schema-registry-avro/02_avro_consumer.py --reader-v1
"""
import argparse
import uuid

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from sr_common import BOOTSTRAP, SCHEMA_REGISTRY_URL, TOPIC, banner, load_schema

ap = argparse.ArgumentParser()
ap.add_argument("--reader-v1", action="store_true")
args = ap.parse_args()

sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
deser = AvroDeserializer(sr, load_schema("order_v1.avsc") if args.reader_v1 else None)
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"sr-{uuid.uuid4().hex[:6]}", "auto.offset.reset": "earliest"})
c.subscribe([TOPIC])
banner(f"Reading {TOPIC} with reader schema = {'v1 (old app)' if args.reader_v1 else 'writer schema (whatever was written)'}")
idle = 0
while idle < 4:
    m = c.poll(1.0)
    if m is None:
        idle += 1 if c.assignment() else 0
        continue
    if m.error():
        continue
    idle = 0
    schema_id = int.from_bytes(m.value()[1:5], "big")
    order = deser(m.value(), SerializationContext(m.topic(), MessageField.VALUE))
    print(f"  schema id {schema_id}  p{m.partition()}@{m.offset():<3d} {order}")
c.close()
