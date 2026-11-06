#!/usr/bin/env python3
"""KiA 9.6 "Tracing option" — the book uses producer/consumer *interceptors* to stamp trace info.
Python has no interceptors, so we wrap produce()/poll() and carry a W3C `traceparent` in record HEADERS,
then measure end-to-end latency per hop (produce -> broker append -> consume).

    python 09-admin-tools/05_tracing_headers.py
"""
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

T = "kinaction_traced"
recreate_topic(T, 3, 3, {"message.timestamp.type": "LogAppendTime"})   # broker stamps append time


class TracingProducer:
    """Like a ProducerInterceptor.onSend(): add trace context + send time to every record."""

    def __init__(self, conf):
        self._p = Producer(conf)

    def produce(self, topic, value, key=None):
        trace_id, span_id = uuid.uuid4().hex, os.urandom(8).hex()
        headers = [("traceparent", f"00-{trace_id}-{span_id}-01"), ("sent_at_ms", str(time.time_ns() // 1_000_000))]
        self._p.produce(topic, value=value, key=key, headers=headers)

    def flush(self):
        self._p.flush()


class TracingConsumer:
    """Like a ConsumerInterceptor.onConsume(): read the context, compute latencies."""

    def __init__(self, conf):
        self._c = Consumer(conf)

    def subscribe(self, topics):
        self._c.subscribe(topics)

    def poll(self, timeout):
        m = self._c.poll(timeout)
        if m is None or m.error():
            return None, None
        h = {k: v.decode() for k, v in m.headers() or []}
        now = time.time_ns() // 1_000_000
        sent = int(h["sent_at_ms"])
        appended = m.timestamp()[1]
        return m, {"trace": h["traceparent"].split("-")[1][:12], "to_broker_ms": appended - sent,
                   "broker_to_consumer_ms": now - appended, "total_ms": now - sent}

    def close(self):
        self._c.close()


p = TracingProducer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
for i in range(10):
    p.produce(T, value=f"payment {i}", key=f"acct-{i % 3}")
    time.sleep(0.1)
p.flush()

c = TracingConsumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"trace-{uuid.uuid4().hex[:4]}", "auto.offset.reset": "earliest"})
c.subscribe([T])
banner("per-record trace (latencies use the broker's LogAppendTime as the middle hop)")
got = 0
while got < 10:
    m, span = c.poll(2)
    if m:
        got += 1
        print(f"  trace={span['trace']}  {m.value().decode():10s}  produce->append {span['to_broker_ms']:4d}ms   "
              f"append->consume {span['broker_to_consumer_ms']:5d}ms   total {span['total_ms']:5d}ms")
c.close()
print("""
* produce->append includes linger.ms batching (20ms here) + network + replication wait (acks=all).
* append->consume is large here only because we consumed AFTER producing everything (that's lag, not latency).
* In real systems use OpenTelemetry's Kafka instrumentation: it injects `traceparent` into headers the same way,
  so a trace spans HTTP request -> Kafka -> consumer -> DB.
""")
