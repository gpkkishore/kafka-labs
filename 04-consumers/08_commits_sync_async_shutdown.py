#!/usr/bin/env python3
"""Definitive Guide ch.4 "Commits and Offsets" + "But how do we exit?"

Pattern from the book:
  * commitAsync() inside the loop (fast, no blocking; a failed async commit is NOT retried because a
    later commit may already have succeeded — retrying could move the offset BACKWARDS)
  * commitSync() once in the shutdown path / on partition revocation (blocking + retried => no gap)
  * a signal handler sets a flag so the loop exits cleanly, then close() leaves the group immediately
    (instead of waiting session.timeout.ms for the coordinator to notice the death)

    python 04-consumers/08_commits_sync_async_shutdown.py      # press Ctrl+C after a few seconds
"""
import signal
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, ensure_topic, log  # noqa: E402

TOPIC = "kinaction_orders"
ensure_topic(TOPIC, 3, 3)
running = True


def stop(sig, frame):
    global running
    log(f"got signal {sig} -> finishing the current record, then shutting down")
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def on_commit(err, partitions):
    if err:
        log(f"async commit FAILED ({err}) — not retried on purpose")
    else:
        log(f"async commit ok {[(p.partition, p.offset) for p in partitions if p.offset >= 0]}")


def on_revoke(consumer, partitions):
    # last chance to save progress before another member gets these partitions
    log(f"revoking {[p.partition for p in partitions]} -> commitSync()")
    try:
        consumer.commit(asynchronous=False)
    except Exception as e:  # noqa: BLE001  (nothing to commit yet is fine)
        log(f"  (sync commit: {e})")


c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"exit-demo-{uuid.uuid4().hex[:4]}",
              "auto.offset.reset": "earliest", "enable.auto.commit": False, "on_commit": on_commit})
c.subscribe([TOPIC], on_revoke=on_revoke)
p = Producer({"bootstrap.servers": BOOTSTRAP})
banner("consuming; a background producer adds 1 record/s. Ctrl+C to exit cleanly")
last_produce, n = 0.0, 0
try:
    while running:
        if time.time() - last_produce > 1:
            p.produce(TOPIC, key="tick", value=f"tick {time.time():.0f}")
            p.poll(0)
            last_produce = time.time()
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        n += 1
        if n % 10 == 0:
            c.commit(asynchronous=True)          # cheap, non-blocking
finally:
    log("shutdown: commitSync() + close()")
    try:
        c.commit(asynchronous=False)             # blocking, retried: nothing processed is lost
    except Exception as e:  # noqa: BLE001
        log(f"  (final commit: {e})")
    c.close()                                    # sends LeaveGroup -> immediate rebalance for the others
    p.flush()
    log(f"processed {n} records, bye")
