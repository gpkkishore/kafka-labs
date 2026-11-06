"""Shared fixtures.

* `kafka_bootstrap` (session scope): ONE throwaway single-node KRaft Kafka in Docker for the whole test session
  (starting a broker takes ~10s, so never start one per test). Set KAFKA_TEST_BOOTSTRAP=localhost:9092 to run
  the integration tests against the lab cluster instead (faster, but not isolated).
* `topic`: a fresh, uniquely-named topic per test -> tests never see each other's data.
* `mock_bootstrap`: librdkafka's built-in in-process MOCK cluster — no Docker, no JVM, millisecond startup.
"""
import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

KAFKA_IMAGE = "confluentinc/cp-kafka:7.7.1"


@pytest.fixture(scope="session")
def kafka_bootstrap():
    external = os.getenv("KAFKA_TEST_BOOTSTRAP")
    if external:
        yield external
        return
    try:
        from testcontainers.community.kafka import KafkaContainer   # testcontainers >= 4.9
    except ImportError:
        try:
            from testcontainers.kafka import KafkaContainer
        except ImportError:  # pragma: no cover
            pytest.skip("pip install 'testcontainers[kafka]'")
    container = KafkaContainer(KAFKA_IMAGE).with_kraft()
    container.with_env("KAFKA_HEAP_OPTS", "-Xmx256m")
    with container as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture
def admin(kafka_bootstrap):
    return AdminClient({"bootstrap.servers": kafka_bootstrap})


@pytest.fixture
def topic(admin):
    """Create a 3-partition topic with a random name; delete it afterwards."""
    name = f"test-{uuid.uuid4().hex[:8]}"
    for f in admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)]).values():
        f.result()
    yield name
    admin.delete_topics([name])


@pytest.fixture
def mock_bootstrap():
    """librdkafka mock cluster: a Producer with test.mock.num.brokers starts an in-process fake cluster
    and exposes its address via the 'mock' bootstrap list. We keep that producer alive for the test."""
    from confluent_kafka import Producer
    holder = Producer({"test.mock.num.brokers": 3, "bootstrap.servers": "", "log_level": 0})
    # the mock cluster's bootstrap address is available through list_topics metadata
    md = holder.list_topics(timeout=5)
    servers = ",".join(f"{b.host}:{b.port}" for b in md.brokers.values())
    yield servers
    holder.flush()
