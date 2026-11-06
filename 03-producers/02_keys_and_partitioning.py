#!/usr/bin/env python3
"""Ch.4.2 — how a key picks a partition, and a classic cross-language GOTCHA.

Java clients:   partition = toPositive(murmur2(keyBytes)) % numPartitions
librdkafka (Python/Go/C#/...) DEFAULT partitioner is 'consistent_random' (CRC32) -> DIFFERENT partition
for the same key! Set partitioner=murmur2_random to match Java producers (important when mixing languages
and relying on co-partitioning, e.g. Kafka Streams joins).

    python 03-producers/02_keys_and_partitioning.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_partitioning"
PARTITIONS = 6
recreate_topic(TOPIC, PARTITIONS, 3)


def murmur2(data: bytes) -> int:
    """Kafka's Java murmur2 (org.apache.kafka.common.utils.Utils.murmur2), 32-bit."""
    length, seed, m, r = len(data), 0x9747B28C, 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[i * 4:i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail, rem = length & ~3, length % 4
    if rem == 3:
        h ^= data[tail + 2] << 16
    if rem >= 2:
        h ^= data[tail + 1] << 8
    if rem >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def java_partition(key: bytes, n: int) -> int:
    return (murmur2(key) & 0x7FFFFFFF) % n


def produce_and_record(partitioner: str, keys):
    p = Producer({"bootstrap.servers": BOOTSTRAP, "partitioner": partitioner})
    got = {}
    for k in keys:
        p.produce(TOPIC, key=k, value=b"x", on_delivery=lambda e, m: got.__setitem__(m.key().decode(), m.partition()))
    p.flush()
    return got


keys = [f"customer-{i}" for i in range(12)]
default = produce_and_record("consistent_random", keys)
javalike = produce_and_record("murmur2_random", keys)

banner(f"Same key -> which partition?  ({PARTITIONS} partitions)")
print(f"{'key':14s} {'librdkafka default':>20s} {'murmur2_random':>16s} {'computed java murmur2':>22s}")
for k in keys:
    j = java_partition(k.encode(), PARTITIONS)
    mark = "" if javalike[k] == j else "  MISMATCH"
    print(f"{k:14s} {default[k]:>20d} {javalike[k]:>16d} {j:>22d}{mark}")

print("""
* murmur2_random column == our pure-python Java murmur2 => it matches the Java client.
* The default column differs for many keys => a Java and a Python producer writing the same key
  would put it in DIFFERENT partitions (ordering per key is lost across them).
* NULL key: Java >= 2.4 & librdkafka use a "sticky" partitioner: fill a batch for one partition, then switch.
* Run this again after `bin/topics --alter --topic kinaction_partitioning --partitions 8`
  and see keys MOVE (hash % 8 != hash % 6).
""")
