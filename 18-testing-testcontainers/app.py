"""The "application code" under test. Kept free of global state so it can be tested:
dependencies (producer / consumer) are passed IN, which makes unit tests with fakes trivial."""
import json


def to_order_event(order: dict) -> tuple[str, bytes]:
    """Pure function: business object -> (key, value). Easiest thing to unit test."""
    if order.get("amount", 0) <= 0:
        raise ValueError("amount must be positive")
    return str(order["customer_id"]), json.dumps({"id": order["id"], "amount": order["amount"]}).encode()


class OrderPublisher:
    def __init__(self, producer, topic: str):
        self.producer, self.topic = producer, topic
        self.failed = []

    def _on_delivery(self, err, msg):
        if err is not None:
            self.failed.append(err)

    def publish(self, order: dict) -> None:
        key, value = to_order_event(order)
        self.producer.produce(self.topic, key=key, value=value, on_delivery=self._on_delivery)
        self.producer.poll(0)

    def close(self) -> int:
        return self.producer.flush(10)
