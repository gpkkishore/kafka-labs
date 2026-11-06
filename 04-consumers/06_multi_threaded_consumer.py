#!/usr/bin/env python3
"""Ch.5 — a KafkaConsumer is NOT thread-safe. Two common scaling patterns:

  A) one consumer per thread/process  (preferred; Kafka splits partitions, see folder 05)
  B) one poll thread + worker pool, commit only what is DONE (keeps per-key order by hashing key -> worker)

This file shows B with per-partition ordering preserved.

    python 04-consumers/06_multi_threaded_consumer.py
"""
import queue
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log  # noqa: E402

TOPIC = "kinaction_orders"
WORKERS = 3
queues = [queue.Queue() for _ in range(WORKERS)]
done_offsets: dict[int, int] = {}
lock = threading.Lock()


def worker(i):
    while True:
        m = queues[i].get()
        if m is None:
            return
        time.sleep(0.05)                             # "slow" processing
        with lock:
            done_offsets[m.partition()] = m.offset() + 1
        log(f"worker-{i} handled p{m.partition()}@{m.offset()}")


threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(WORKERS)]
[t.start() for t in threads]

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"mt-{uuid.uuid4().hex[:4]}",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe([TOPIC])
banner(f"1 poller thread -> {WORKERS} workers (partition p always goes to worker p % {WORKERS})")
idle = 0
while idle < 3:
    m = c.poll(0.5)
    if m is None:
        idle += 1 if c.assignment() else 0
    elif not m.error():
        idle = 0
        queues[m.partition() % WORKERS].put(m)      # same partition -> same worker -> order kept
    with lock:
        if done_offsets:
            c.commit(offsets=[TopicPartition(TOPIC, p, o) for p, o in done_offsets.items()], asynchronous=True)
for q in queues:
    q.put(None)
[t.join() for t in threads]
with lock:
    c.commit(offsets=[TopicPartition(TOPIC, p, o) for p, o in done_offsets.items()], asynchronous=False)
c.close()
print("\nOnly offsets of FINISHED work are committed -> still at-least-once even with a thread pool.")
