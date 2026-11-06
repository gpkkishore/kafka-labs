#!/usr/bin/env python3
"""Non-blocking retries with delayed retry topics + a Dead Letter Queue (DLQ).

   patterns.orders ──main consumer──▶ OK
         │ transient error                     │ poison pill / non-retriable
         ▼                                     ▼
   patterns.orders.retry-5s ──▶ retry-15s ──▶ patterns.orders.dlq   (headers: error, attempts, original topic/partition/offset)

Why not just retry in place? A blocking retry stalls the WHOLE partition (head-of-line blocking) and risks
exceeding max.poll.interval.ms. Retry topics keep the main flow moving. Trade-off: a retried record is
processed AFTER newer records with the same key -> per-key ordering is lost for retried records.

    python 22-patterns-retry-dlq-request-reply/01_retry_topics_and_dlq.py
"""
import json
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, RESET, banner, color_for, log, recreate_topic  # noqa: E402

MAIN = "patterns.orders"
RETRIES = [("patterns.orders.retry-5s", 5), ("patterns.orders.retry-15s", 15)]
DLQ = "patterns.orders.dlq"
for t in [MAIN, DLQ] + [r[0] for r in RETRIES]:
    recreate_topic(t, 1 if t != MAIN else 3, 3)

producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
flaky_attempts: dict[str, int] = {}
stop = threading.Event()


class Transient(Exception):
    """e.g. downstream HTTP 503, DB deadlock — worth retrying later."""


class Poison(Exception):
    """e.g. invalid JSON, schema violation, business rule — retrying never helps."""


def process(raw: bytes, who: str):
    try:
        order = json.loads(raw)
    except json.JSONDecodeError as e:
        raise Poison(f"not JSON: {e}") from e
    if order.get("amount", 0) <= 0:
        raise Poison("amount must be > 0")
    if order.get("flaky_until_attempt"):               # simulated downstream outage
        n = flaky_attempts.get(order["id"], 0) + 1
        flaky_attempts[order["id"]] = n
        if n < order["flaky_until_attempt"]:
            raise Transient(f"payment service unavailable (attempt {n})")
    log(f"{color_for(3)}[{who}] ✔ processed order {order['id']}{RESET}")


def hdrs(msg) -> dict:
    return {k: v.decode() for k, v in (msg.headers() or [])}


def forward(msg, topic, err, attempts, not_before=None):
    h = hdrs(msg)
    headers = {"error": str(err), "error.class": type(err).__name__, "attempts": str(attempts),
               "original.topic": h.get("original.topic", msg.topic()),
               "original.partition": h.get("original.partition", str(msg.partition())),
               "original.offset": h.get("original.offset", str(msg.offset()))}
    if not_before:
        headers["not-before"] = str(not_before)
    producer.produce(topic, key=msg.key(), value=msg.value(), headers=list(headers.items()))
    producer.flush()


def route_failure(msg, err, attempts):
    """Decide: next retry topic, or DLQ."""
    if isinstance(err, Poison) or attempts > len(RETRIES):
        forward(msg, DLQ, err, attempts)
        log(f"{color_for(5)}  ✘ {msg.key().decode()} -> DLQ ({err}){RESET}")
    else:
        topic, delay = RETRIES[attempts - 1]
        forward(msg, topic, err, attempts, not_before=int(time.time() * 1000) + delay * 1000)
        log(f"{color_for(1)}  ↻ {msg.key().decode()} -> {topic} ({err}){RESET}")


def consumer_loop(topic, group, who, delayed=False):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic])
    while not stop.is_set():
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        if delayed:   # wait until the record is due. Retry topics are ordered by due time, so blocking here is fine.
            due = int(hdrs(m).get("not-before", 0))
            while not stop.is_set() and time.time() * 1000 < due:
                time.sleep(0.2)   # (production: c.pause(), keep polling, c.resume() — avoids max.poll.interval.ms)
        attempts = int(hdrs(m).get("attempts", 0))
        try:
            process(m.value(), who)
        except (Transient, Poison) as e:
            route_failure(m, e, attempts + 1)
        c.commit(message=m, asynchronous=False)       # commit AFTER handing off -> nothing lost
    c.close()


threads = [threading.Thread(target=consumer_loop, args=(MAIN, "orders-svc", "main"), daemon=True)]
threads += [threading.Thread(target=consumer_loop, args=(t, f"orders-svc-{t}", t.split(".")[-1], True), daemon=True)
            for t, _ in RETRIES]
[t.start() for t in threads]

banner("producing: 3 good, 1 poison (bad JSON), 1 invalid amount, 1 recovers on 2nd retry, 1 never recovers")
orders = [
    ("o1", json.dumps({"id": "o1", "amount": 10})),
    ("o2", "{this is not json"),
    ("o3", json.dumps({"id": "o3", "amount": -5})),
    ("o4", json.dumps({"id": "o4", "amount": 20, "flaky_until_attempt": 3})),
    ("o5", json.dumps({"id": "o5", "amount": 30, "flaky_until_attempt": 99})),
    ("o6", json.dumps({"id": "o6", "amount": 40})),
    ("o7", json.dumps({"id": "o7", "amount": 50})),
]
for k, v in orders:
    producer.produce(MAIN, key=k, value=v)
producer.flush()
time.sleep(28)          # 5s + 15s retry delays + processing
stop.set()
[t.join(5) for t in threads]

banner(f"DLQ contents ({DLQ})")
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "dlq-inspector", "auto.offset.reset": "earliest",
              "enable.auto.commit": False})
c.subscribe([DLQ])
idle = 0
while idle < 3:
    m = c.poll(1)
    if m is None:
        idle += 1 if c.assignment() else 0
        continue
    h = hdrs(m)
    print(f"  key={m.key().decode()} attempts={h['attempts']} {h['error.class']}: {h['error']}  "
          f"(from {h['original.topic']}[{h['original.partition']}]@{h['original.offset']})")
c.close()
print("\nNext: inspect/fix/replay with 02_dlq_replay.py")
