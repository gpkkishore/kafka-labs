#!/usr/bin/env python3
"""Ch.4.1 — producer life-cycle: async send + delivery callback vs. "synchronous" send.

    python 03-producers/01_producer_basics.py

What to notice:
  * async: produce() returns immediately; acks arrive later in batches  -> thousands msg/s
  * sync : flush() after every record waits a full network round trip  -> much slower
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, ensure_topic, log  # noqa: E402

TOPIC = "kinaction_producer_basics"
ensure_topic(TOPIC, 3, 3)
N = 2000

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": "basics",
    "acks": "all",
})

acked = {"ok": 0, "err": 0}
producer.produce(TOPIC, value=b"warmup")
producer.flush()      # first send pays for connecting + metadata; keep it out of the timing


def cb(err, msg):
    acked["err" if err else "ok"] += 1


banner(f"ASYNC: {N} records, delivery callbacks")
t = time.time()
for i in range(N):
    while True:
        try:
            producer.produce(TOPIC, key=str(i % 10), value=f"async-{i}", on_delivery=cb)
            break
        except BufferError:          # local queue full (queue.buffering.max.messages) -> serve callbacks & retry
            producer.poll(0.1)
    producer.poll(0)
producer.flush()
async_s = time.time() - t
log(f"async: {N / async_s:,.0f} msg/s   acked={acked}")

banner(f"SYNC: {N // 10} records, flush() after each")
t = time.time()
for i in range(N // 10):
    producer.produce(TOPIC, key=str(i % 10), value=f"sync-{i}", on_delivery=cb)
    producer.flush()                 # wait for THIS record's ack before sending the next one
sync_s = time.time() - t
log(f"sync : {(N // 10) / sync_s:,.0f} msg/s")

print(f"""
Async is ~{(N / async_s) / ((N // 10) / sync_s):.0f}x faster. Why:
  produce() -> record appended to a per-partition batch in memory (RecordAccumulator in Java)
  -> background thread sends a batch when it's full (batch.size) or old enough (linger.ms)
  -> broker leader writes it, waits for ISR followers (acks=all), replies
  -> librdkafka calls your on_delivery callback during poll()/flush().
Rule of thumb: never flush() per message in production; poll(0) in your loop and flush() on shutdown.
""")
