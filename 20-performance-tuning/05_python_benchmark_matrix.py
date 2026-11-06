#!/usr/bin/env python3
"""A tuning matrix with the Python (librdkafka) client. AKiA ch.6 "Performance", DG ch.3/4 configs.

Producer dimensions : partitions, acks, linger.ms/batch.size, compression
Consumer dimensions : fetch.min.bytes / fetch.wait.max.ms, poll() one-by-one vs consume(num_messages=N)

    python 20-performance-tuning/05_python_benchmark_matrix.py            # ~1-2 minutes
    python 20-performance-tuning/05_python_benchmark_matrix.py --quick
    python 20-performance-tuning/05_python_benchmark_matrix.py --csv results.csv
"""
import argparse
import csv
import json
import random
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--quick", action="store_true")
ap.add_argument("--csv")
args = ap.parse_args()
N = 20_000 if args.quick else 60_000
# semi-realistic, compressible payloads (~300B JSON)
PAYLOADS = [json.dumps({"id": i, "user": f"user-{random.randint(1, 5000)}", "event": random.choice(["view", "click", "buy"]),
                        "page": f"/products/{random.randint(1, 300)}", "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)",
                        "ts": int(time.time() * 1000)}).encode() for i in range(2000)]
rows = []


def produce(topic, cfg):
    p = Producer({"bootstrap.servers": BOOTSTRAP, **cfg})
    p.produce(topic, value=b"warmup")
    p.flush()
    t = time.time()
    for i in range(N):
        while True:
            try:
                p.produce(topic, key=str(i % 1000), value=PAYLOADS[i % len(PAYLOADS)])
                break
            except BufferError:
                p.poll(0.01)
        p.poll(0)
    p.flush()
    rate = N / (time.time() - t)
    del p
    return rate


banner(f"PRODUCER matrix ({N} records each, ~{len(PAYLOADS[0])}B JSON)")
print(f"{'partitions':>10s} {'acks':>4s} {'linger':>6s} {'batch':>7s} {'compr':>6s} {'msg/s':>10s} {'MB/s':>6s}")
matrix = [
    (1, "all", 0, 16384, "none"),
    (6, "all", 0, 16384, "none"),
    (6, "1", 5, 131072, "none"),
    (6, "all", 5, 131072, "none"),
    (6, "all", 20, 1048576, "none"),
    (6, "all", 20, 1048576, "lz4"),
    (6, "all", 20, 1048576, "zstd"),
]
if args.quick:
    matrix = matrix[:2] + matrix[-2:]
# create the topics ONCE up front. (Re-creating a topic while an old producer instance is still alive
# can race with auto-topic-creation: its metadata refresh re-creates the topic with the default 3 partitions.)
for parts in sorted({m[0] for m in matrix}):
    recreate_topic(f"perf-matrix-{parts}", parts, 3)
for parts, acks, linger, batch, comp in matrix:
    topic = f"perf-matrix-{parts}"
    rate = produce(topic, {"acks": acks, "linger.ms": linger, "batch.size": batch, "compression.type": comp})
    mbs = rate * len(PAYLOADS[0]) / 1e6
    print(f"{parts:>10d} {acks:>4s} {linger:>6d} {batch:>7d} {comp:>6s} {rate:>10,.0f} {mbs:>6.1f}")
    rows.append({"kind": "producer", "partitions": parts, "acks": acks, "linger_ms": linger, "batch_size": batch,
                 "compression": comp, "msg_per_s": round(rate)})

# consumer side: read the last topic (6 partitions, N records) with different settings
banner("CONSUMER matrix (same data, fresh group each time)")
print(f"{'fetch.min.bytes':>15s} {'fetch.wait.max.ms':>17s} {'api':>22s} {'msg/s':>10s}")
for fmin, fwait, batch_api in [(1, 500, False), (1, 500, True), (1_000_000, 100, True), (1_000_000, 500, True)]:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"perf-{uuid.uuid4()}", "auto.offset.reset": "earliest",
                  "fetch.min.bytes": fmin, "fetch.wait.max.ms": fwait, "enable.auto.commit": True})
    c.subscribe(["perf-matrix-6"])
    got, t0 = 0, None
    deadline = time.time() + 60
    while got < N and time.time() < deadline:  # N of the records written above (topic holds several runs)
        if batch_api:
            msgs = c.consume(num_messages=1000, timeout=1.0)     # like max.poll.records in Java
        else:
            m = c.poll(1.0)
            msgs = [m] if m else []
        for m in msgs:
            if not m.error():
                if t0 is None:
                    t0 = time.time()
                got += 1
    rate = got / (time.time() - t0) if t0 else 0
    c.close()
    api = "consume(1000)" if batch_api else "poll() one-by-one"
    print(f"{fmin:>15d} {fwait:>17d} {api:>22s} {rate:>10,.0f}")
    rows.append({"kind": "consumer", "fetch_min_bytes": fmin, "fetch_wait_max_ms": fwait, "api": api, "msg_per_s": round(rate)})

if args.csv:
    keys = sorted({k for r in rows for k in r})
    with open(args.csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {args.csv}")
print("""
Read the results:
 * 1 -> 6 partitions: more leaders/brokers work in parallel and more consumers can share the work. With
   linger.ms=0 and tiny batches, 6 partitions can even be SLOWER (6x more, smaller requests) — batching first!
 * linger.ms + big batch.size: fewer, larger requests -> the single biggest producer win.
 * compression: CPU on the client, but less network/disk/replication; usually a net win with JSON.
 * fetch.min.bytes high: fewer fetch round-trips, more latency on quiet topics (bounded by fetch.wait.max.ms).
 * consume(num_messages) amortizes Python call overhead (the Python layer is often the bottleneck, not Kafka).
""")
