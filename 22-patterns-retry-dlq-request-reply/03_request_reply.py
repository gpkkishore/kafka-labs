#!/usr/bin/env python3
"""Request-reply over Kafka (synchronous call semantics on top of async messaging).

  requester ──(key, headers: correlation-id, reply-to)──▶ patterns.quote-requests ──▶ pricing service
      ▲                                                                                   │
      └──────────── patterns.quote-replies (partition chosen by requester) ◀─────────────┘

* correlation-id matches a reply to the waiting request (a Future per request).
* reply-to tells the service where to answer. Each requester INSTANCE owns its reply partition
  (or its own reply topic) so replies go back to the instance that is waiting.
* Use sparingly: if you need a synchronous answer, REST/gRPC is usually simpler. Kafka request-reply
  shines when the work is queued/slow, must survive restarts, or many services must see the requests.

    python 22-patterns-retry-dlq-request-reply/03_request_reply.py
"""
import json
import random
import sys
import threading
import time
import uuid
from concurrent.futures import Future
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log, recreate_topic  # noqa: E402

REQ, REPLY = "patterns.quote-requests", "patterns.quote-replies"
recreate_topic(REQ, 3, 3)
recreate_topic(REPLY, 3, 3)
stop = threading.Event()


def pricing_service():
    """The 'server': consumes requests, answers to reply-to topic/partition with the same correlation id."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "pricing-svc", "auto.offset.reset": "earliest"})
    p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 0})
    c.subscribe([REQ])
    while not stop.is_set():
        m = c.poll(0.2)
        if m is None or m.error():
            continue
        h = {k: v.decode() for k, v in m.headers()}
        req = json.loads(m.value())
        time.sleep(random.uniform(0.01, 0.2))                     # "work"
        reply = {"sku": req["sku"], "qty": req["qty"], "price": round(req["qty"] * 9.99, 2)}
        p.produce(h["reply-to"], partition=int(h["reply-partition"]), key=m.key(), value=json.dumps(reply),
                  headers=[("correlation-id", h["correlation-id"])])
        p.poll(0)
    p.flush()
    c.close()


class KafkaRequester:
    """The 'client': send() returns a Future completed when the matching reply arrives."""

    def __init__(self, reply_partition: int):
        self.reply_partition = reply_partition
        self.pending: dict[str, Future] = {}
        self.p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 0})
        self.c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"requester-{uuid.uuid4()}",
                           "enable.auto.commit": False})
        # assign() our own reply partition at the END: we only care about replies to requests sent from now on
        lo, hi = self.c.get_watermark_offsets(TopicPartition(REPLY, reply_partition), timeout=10)
        self.c.assign([TopicPartition(REPLY, reply_partition, hi)])
        threading.Thread(target=self._listen, daemon=True).start()

    def _listen(self):
        while not stop.is_set():
            m = self.c.poll(0.1)
            if m is None or m.error():
                continue
            cid = dict(m.headers())["correlation-id"].decode()
            fut = self.pending.pop(cid, None)
            if fut:                      # unknown ids = late replies of timed-out requests -> ignore
                fut.set_result(json.loads(m.value()))

    def send(self, sku: str, qty: int) -> Future:
        cid = str(uuid.uuid4())
        fut = Future()
        self.pending[cid] = fut
        self.p.produce(REQ, key=sku, value=json.dumps({"sku": sku, "qty": qty}),
                       headers=[("correlation-id", cid), ("reply-to", REPLY), ("reply-partition", str(self.reply_partition))])
        self.p.poll(0)
        self.p.flush()
        return fut


threading.Thread(target=pricing_service, daemon=True).start()
client = KafkaRequester(reply_partition=random.randrange(3))
time.sleep(3)   # let the service join its group
banner(f"10 requests; this requester instance listens on {REPLY}[{client.reply_partition}]")
t0 = time.time()
futures = [(sku, client.send(sku, q)) for sku, q in [(f"SKU-{i}", i + 1) for i in range(10)]]
for sku, f in futures:
    try:
        r = f.result(timeout=10)
        log(f"{sku}: {r}")
    except TimeoutError:
        log(f"{sku}: TIMEOUT (always set one! the service may be down)")
print(f"\n10 round-trips in {time.time() - t0:.2f}s (sent concurrently, answers matched by correlation-id)")
stop.set()
time.sleep(0.5)
