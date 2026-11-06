"""Level 2 — librdkafka MOCK CLUSTER: real client code paths (batching, callbacks, consumer groups)
against an in-process fake broker. No Docker, no JVM — but not a real Kafka (no compaction, no ACLs, ...).
"""
import uuid

from confluent_kafka import Consumer, Producer


def test_roundtrip_on_mock_cluster(mock_bootstrap):
    topic = "mock-orders"
    p = Producer({"bootstrap.servers": mock_bootstrap})
    for i in range(10):
        p.produce(topic, key=f"k{i % 2}", value=f"v{i}")
    assert p.flush(10) == 0

    c = Consumer({"bootstrap.servers": mock_bootstrap, "group.id": f"g-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    got = []
    for _ in range(50):
        m = c.poll(0.5)
        if m and not m.error():
            got.append(m.value().decode())
        if len(got) == 10:
            break
    c.close()
    assert sorted(got) == sorted(f"v{i}" for i in range(10))
