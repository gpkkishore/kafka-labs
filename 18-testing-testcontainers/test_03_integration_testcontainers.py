"""Level 3 — INTEGRATION tests against a REAL broker started by Testcontainers (KiA ch.7.3.1).

Slower (one ~10s broker start per session) but tests real semantics: partitioning, offsets,
group commits, idempotence.
"""
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

pytestmark = pytest.mark.integration


def consume_all(bootstrap, topic, group, expected, timeout_polls=60):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic])
    out = []
    for _ in range(timeout_polls):
        m = c.poll(0.5)
        if m and not m.error():
            out.append(m)
        if len(out) >= expected:
            break
    return c, out


def test_produce_consume_roundtrip(kafka_bootstrap, topic):
    p = Producer({"bootstrap.servers": kafka_bootstrap})
    for i in range(20):
        p.produce(topic, key=str(i), value=f"msg-{i}")
    assert p.flush(10) == 0
    c, msgs = consume_all(kafka_bootstrap, topic, f"g-{uuid.uuid4()}", 20)
    c.close()
    assert sorted(m.value().decode() for m in msgs) == sorted(f"msg-{i}" for i in range(20))


def test_same_key_keeps_order_in_one_partition(kafka_bootstrap, topic):
    p = Producer({"bootstrap.servers": kafka_bootstrap})
    for i in range(30):
        p.produce(topic, key="customer-1", value=str(i))
    p.flush(10)
    c, msgs = consume_all(kafka_bootstrap, topic, f"g-{uuid.uuid4()}", 30)
    c.close()
    assert len({m.partition() for m in msgs}) == 1           # one key -> one partition
    assert [int(m.value()) for m in msgs] == list(range(30))  # strictly ordered


def test_group_resumes_from_committed_offset(kafka_bootstrap, topic):
    p = Producer({"bootstrap.servers": kafka_bootstrap})
    for i in range(10):
        p.produce(topic, value=str(i), partition=0)
    p.flush(10)
    group = f"g-{uuid.uuid4()}"

    c, msgs = consume_all(kafka_bootstrap, topic, group, 10)
    # process only the first 4, commit "next offset to read" = 4
    c.commit(offsets=[TopicPartition(topic, 0, 4)], asynchronous=False)
    c.close()

    c2, rest = consume_all(kafka_bootstrap, topic, group, 6)
    c2.close()
    assert [int(m.value()) for m in rest] == [4, 5, 6, 7, 8, 9]


def test_idempotent_producer_writes_producer_id(kafka_bootstrap, topic):
    p = Producer({"bootstrap.servers": kafka_bootstrap, "enable.idempotence": True})
    for i in range(5):
        p.produce(topic, value=str(i), partition=1)
    assert p.flush(10) == 0
    c = Consumer({"bootstrap.servers": kafka_bootstrap, "group.id": "x", "enable.auto.commit": False})
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, 1), timeout=10)
    c.close()
    assert (lo, hi) == (0, 5)   # exactly 5 records, no duplicates even if retries happened


def test_publisher_against_real_kafka(kafka_bootstrap, topic):
    from app import OrderPublisher
    pub = OrderPublisher(Producer({"bootstrap.servers": kafka_bootstrap}), topic)
    pub.publish({"id": 1, "customer_id": "c9", "amount": 10})
    assert pub.close() == 0 and pub.failed == []
    c, msgs = consume_all(kafka_bootstrap, topic, f"g-{uuid.uuid4()}", 1)
    c.close()
    assert msgs[0].key() == b"c9"
