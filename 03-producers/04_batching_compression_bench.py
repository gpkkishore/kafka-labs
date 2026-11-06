#!/usr/bin/env python3
"""Ch.4.2.2 — batching (linger.ms / batch.size) and compression trade-offs.

Uses librdkafka's statistics callback to read how many BYTES actually went over the wire.

    python 03-producers/04_batching_compression_bench.py
"""
import json
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_batching"
recreate_topic(TOPIC, 3, 3)
N = 30000
# realistic-ish JSON: compressible because field names repeat
records = [json.dumps({"order_id": i, "customer": f"customer-{random.randint(1, 500)}", "status": "CREATED",
                       "amount": round(random.uniform(1, 500), 2), "currency": "USD",
                       "items": [{"sku": f"SKU-{random.randint(1, 50)}", "qty": random.randint(1, 3)}]}).encode()
           for i in range(N)]
raw_bytes = sum(len(r) for r in records)

SETTINGS = [
    ("linger=0   batch=16K none", {"linger.ms": 0, "batch.size": 16384, "compression.type": "none"}),
    ("linger=20  batch=16K none", {"linger.ms": 20, "batch.size": 16384, "compression.type": "none"}),
    ("linger=20  batch=256K none", {"linger.ms": 20, "batch.size": 262144, "compression.type": "none"}),
    ("linger=20  batch=256K gzip", {"linger.ms": 20, "batch.size": 262144, "compression.type": "gzip"}),
    ("linger=20  batch=256K snappy", {"linger.ms": 20, "batch.size": 262144, "compression.type": "snappy"}),
    ("linger=20  batch=256K lz4", {"linger.ms": 20, "batch.size": 262144, "compression.type": "lz4"}),
    ("linger=20  batch=256K zstd", {"linger.ms": 20, "batch.size": 262144, "compression.type": "zstd"}),
]

banner(f"{N} JSON records, {raw_bytes / 1e6:.1f} MB raw")
print(f"{'setting':30s} {'msg/s':>9s} {'wire MB':>8s} {'ratio':>6s} {'requests':>9s} {'avg batch':>10s}")
for name, cfg in SETTINGS:
    stats = {}
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "statistics.interval.ms": 100,
                  "stats_cb": lambda s: stats.update(json.loads(s)), **cfg})
    t = time.time()
    for r in records:
        while True:
            try:
                p.produce(TOPIC, value=r)
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
    p.flush()
    dt = time.time() - t
    time.sleep(0.25)
    p.poll(0.2)  # get a final stats snapshot
    brokers = stats.get("brokers", {}).values()
    tx_bytes = sum(b["txbytes"] for b in brokers)
    tx_reqs = sum(b["req"].get("Produce", 0) for b in brokers)
    print(f"{name:30s} {N / dt:>9,.0f} {tx_bytes / 1e6:>8.2f} {raw_bytes / max(tx_bytes, 1):>6.1f} {tx_reqs:>9d} "
          f"{tx_bytes / max(tx_reqs, 1) / 1024:>8.1f}KB")

print("""
* linger.ms=0 sends tiny batches -> many requests. A few ms of linger => bigger batches, fewer requests.
* Compression works on the whole BATCH, so bigger batches compress better.
* The broker stores the batch compressed as-is (compression.type=producer) -> less disk + less replication traffic.
* zstd/gzip: best ratio, more CPU. lz4/snappy: fast, good ratio. Measure with YOUR data.
""")
