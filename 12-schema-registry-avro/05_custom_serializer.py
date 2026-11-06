#!/usr/bin/env python3
"""DG ch.3 "Custom Serializers" — why you should NOT hand-roll binary formats.

A Customer is encoded as  [4-byte id][4-byte name length][name bytes].
Works... until someone adds a field: old consumers then mis-read every new message.

    python 12-schema-registry-avro/05_custom_serializer.py
"""
import struct
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "sr_custom_serializer"


@dataclass
class Customer:
    customer_id: int
    name: str


class CustomerSerializer:
    def __call__(self, c: Customer) -> bytes:
        name = c.name.encode()
        return struct.pack(">ii", c.customer_id, len(name)) + name


class CustomerDeserializer:
    def __call__(self, data: bytes) -> Customer:
        cid, n = struct.unpack(">ii", data[:8])
        return Customer(cid, data[8:8 + n].decode())


class CustomerV2Serializer:
    """New team member adds 'country' BEFORE the name ... and nobody tells the consumers."""
    def __call__(self, c: Customer, country: str = "IN") -> bytes:
        name, cc = c.name.encode(), country.encode()
        return struct.pack(">ii", c.customer_id, len(cc)) + cc + struct.pack(">i", len(name)) + name


recreate_topic(TOPIC, 1, 3)
p = Producer({"bootstrap.servers": BOOTSTRAP})
p.produce(TOPIC, value=CustomerSerializer()(Customer(1, "Alice")))
p.produce(TOPIC, value=CustomerV2Serializer()(Customer(2, "Bob")))
p.flush()

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "cs", "enable.auto.commit": False})
c.assign([TopicPartition(TOPIC, 0, 0)])
banner("old consumer, CustomerDeserializer")
for _ in range(2):
    m = c.poll(5)
    print(f"  offset {m.offset()} bytes={m.value().hex(' ')}  ->  {CustomerDeserializer()(m.value())}")
c.close()
print("""
Offset 1 decodes as name='IN' — silently WRONG data, no error. A schema registry prevents exactly this:
the change would have been rejected (or carried a new schema id that the reader can resolve).
""")
