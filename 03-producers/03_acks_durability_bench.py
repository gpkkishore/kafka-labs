#!/usr/bin/env python3
"""Ch.4.2.2 — "how to go fast (or go safer)": acks=0 vs 1 vs all.

    python 03-producers/03_acks_durability_bench.py

acks=0   : fire and forget. The broker never answers -> no offset is returned. Lost if anything hiccups.
acks=1   : leader wrote it (to its page cache). Lost if the leader dies before followers copied it.
acks=all : leader waited until ALL in-sync replicas have it (and ISR >= min.insync.replicas). Safest.
"""
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_acks"
recreate_topic(TOPIC, 3, 3, {"min.insync.replicas": 2})
N, SIZE, SYNC_N = 20000, 512, 200
payload = b"x" * SIZE

banner(f"throughput: {N} x {SIZE}B (async)   latency: {SYNC_N} one-at-a-time sends")
print(f"{'acks':6s} {'msg/s':>10s} {'median ms':>10s} {'p99 ms':>8s}  offset returned")
for acks in ["0", "1", "all"]:
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": acks, "linger.ms": 5, "log_level": 3,
                  "enable.idempotence": False})   # idempotence requires acks=all; off to compare fairly
    p.produce(TOPIC, value=b"warmup")
    p.flush()                                      # connect + fetch metadata before timing

    # 1) throughput: flood
    t = time.time()
    for i in range(N):
        while True:
            try:
                p.produce(TOPIC, value=payload)
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
    p.flush()
    rate = N / (time.time() - t)

    # 2) latency: one record at a time, wait for its ack
    lat, offsets = [], []
    for i in range(SYNC_N):
        t0 = time.perf_counter()
        p.produce(TOPIC, value=payload, on_delivery=lambda e, m: offsets.append(m.offset()))
        p.flush()
        lat.append((time.perf_counter() - t0) * 1000)
    lat.sort()
    print(f"{acks:6s} {rate:>10,.0f} {statistics.median(lat):>10.2f} {lat[int(len(lat) * .99)]:>8.2f}  {offsets[-1]}")

print("""
Notice:
 * acks=0 returns no real offset (None / -1001) — the broker never told us where it landed.
 * acks=all latency adds the follower replication round trip (followers fetch from the leader).
 * Durability is the COMBINATION: acks=all + replication.factor=3 + min.insync.replicas=2
   -> survives 1 broker loss without losing acknowledged data (see 06-brokers-replication-failover).
""")
