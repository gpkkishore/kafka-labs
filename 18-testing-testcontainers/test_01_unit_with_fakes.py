"""Level 1 — UNIT tests: no Kafka at all. Fast (ms), run on every save.

Test your serialization/business logic as pure functions, and replace the Producer with a fake
(or unittest.mock) to check WHAT gets produced.
"""
import json
from unittest import mock

import pytest

from app import OrderPublisher, to_order_event


def test_event_key_is_customer_id():
    key, value = to_order_event({"id": 1, "customer_id": 42, "amount": 9.5})
    assert key == "42"                                  # key decides the partition => ordering per customer
    assert json.loads(value) == {"id": 1, "amount": 9.5}


def test_invalid_order_is_rejected_before_kafka():
    with pytest.raises(ValueError):
        to_order_event({"id": 1, "customer_id": 42, "amount": 0})


class FakeProducer:
    """Hand-written fake: records calls and fires delivery callbacks on flush()."""
    def __init__(self, fail=False):
        self.sent, self.pending, self.fail = [], [], fail

    def produce(self, topic, key=None, value=None, on_delivery=None, **kw):
        self.sent.append((topic, key, value))
        self.pending.append(on_delivery)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        for cb in self.pending:
            cb("BROKER_DOWN" if self.fail else None, mock.Mock())
        self.pending.clear()
        return 0


def test_publisher_produces_to_topic_with_key():
    fake = FakeProducer()
    pub = OrderPublisher(fake, "orders")
    pub.publish({"id": 7, "customer_id": "c1", "amount": 3})
    pub.close()
    assert fake.sent == [("orders", "c1", b'{"id": 7, "amount": 3}')]
    assert pub.failed == []


def test_publisher_records_delivery_failures():
    pub = OrderPublisher(FakeProducer(fail=True), "orders")
    pub.publish({"id": 7, "customer_id": "c1", "amount": 3})
    pub.close()
    assert pub.failed == ["BROKER_DOWN"]


def test_with_unittest_mock():
    producer = mock.MagicMock()
    OrderPublisher(producer, "orders").publish({"id": 1, "customer_id": 5, "amount": 1})
    producer.produce.assert_called_once()
    assert producer.produce.call_args.kwargs["key"] == "5"
