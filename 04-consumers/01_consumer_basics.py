#!/usr/bin/env python3
"""Ch.5.1 — the poll loop and the "coordinates" of every record: topic, partition, offset (+ key, timestamp).

    python 04-consumers/00_seed_data.py      # once: 3 partitions x 10 records
    python 04-consumers/01_consumer_basics.py
"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, KafkaError  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, RESET, banner, color_for, log  # noqa: E402

TOPIC = "kinaction_orders"
group = f"basics-{uuid.uuid4().hex[:6]}"
c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": group,
    "auto.offset.reset": "earliest",
    "enable.partition.eof": True,         # get an event when we reach the end of a partition
    # fetch tuning (ch. 5.1.1)
    "fetch.min.bytes": 1,                 # broker answers as soon as it has >= 1 byte...
    "fetch.wait.max.ms": 500,             # ...or after 500ms (long polling, no busy loop)
    "max.partition.fetch.bytes": 1048576,
})
c.subscribe([TOPIC], on_assign=lambda cons, parts: log(f"assigned: {[p.partition for p in parts]}"))
banner(f"group={group}")
eof = set()
while True:
    m = c.poll(1.0)
    if m is None:
        continue
    if m.error():
        if m.error().code() == KafkaError._PARTITION_EOF:
            eof.add(m.partition())
            log(f"reached end of partition {m.partition()} (next offset {m.offset()})")
            if len(eof) == len(c.assignment()):
                break
            continue
        raise SystemExit(m.error())
    col = color_for(m.partition())
    log(f"{col}topic={m.topic()} partition={m.partition()} offset={m.offset()}{RESET} key={m.key()} "
        f"value={m.value().decode()} ts={m.timestamp()[1]}")
c.close()
print("""
Things to notice:
 * Offsets increase by 1 inside each partition and restart at 0 per partition.
 * Records of different partitions are interleaved — there is NO order across partitions.
 * poll() returns ONE record in the Python API (consume(num_messages=N) returns a list);
   internally librdkafka prefetches whole batches in the background (queued.min.messages).
""")
