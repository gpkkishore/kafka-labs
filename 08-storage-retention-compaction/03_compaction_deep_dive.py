#!/usr/bin/env python3
"""DG ch.5 "Compaction / How compaction works / Deleted events", KiA 7.4, AKiA 10.4.

  clean part  |  dirty part          | active segment
  (compacted)    (written since last    (never compacted)
                  cleaning)
The cleaner builds an offset map {key -> latest offset} of the dirty part and rewrites old segments keeping only
the latest record per key. Tombstones (value=null) are kept for delete.retention.ms so consumers can see the delete,
then removed.

    python 08-storage-retention-compaction/03_compaction_deep_dive.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log, recreate_topic  # noqa: E402

TOPIC = "kinaction_compaction"
recreate_topic(TOPIC, 1, 3, {
    "cleanup.policy": "compact",
    "segment.ms": 3000,                 # roll often so segments become eligible
    "min.cleanable.dirty.ratio": 0.01,  # clean as soon as 1% of the log is dirty (default 0.5)
    "min.compaction.lag.ms": 0,         # a record may be compacted immediately (default 0)
    "delete.retention.ms": 30000,       # keep tombstones 30s after cleaning (default 24h)
})
p = Producer({"bootstrap.servers": BOOTSTRAP})


def read_all():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "dump", "enable.auto.commit": False})
    tp = TopicPartition(TOPIC, 0)
    lo, hi = c.get_watermark_offsets(tp, timeout=5)
    c.assign([TopicPartition(TOPIC, 0, lo)])
    rows = []
    while True:
        m = c.poll(2)
        if m is None:
            break
        rows.append(f"{m.offset()}:{m.key().decode()}={m.value().decode() if m.value() is not None else '∅'}")
        if m.offset() + 1 >= hi:
            break
    c.close()
    return rows, lo, hi


def dump(title):
    rows, lo, hi = read_all()
    banner(f"{title}  ({len(rows)} records, offsets {lo}..{hi - 1})")
    print("  " + "  ".join(rows))


def tick_until(cond, label, max_s=90):
    """keep producing so the active segment rolls and the cleaner has work, until cond(rows) is true"""
    start, i = time.time(), 0
    while time.time() - start < max_s:
        time.sleep(2)
        p.produce(TOPIC, key="tick", value=str(i))
        p.flush()
        i += 1
        rows, _, _ = read_all()
        if cond(rows):
            log(f"{label} after {time.time() - start:.0f}s")
            return
    log(f"(gave up waiting for: {label})")


# 3 keys updated many times, one deleted
for round_ in range(5):
    for k in ["acct-A", "acct-B", "acct-C"]:
        p.produce(TOPIC, key=k, value=f"balance-{round_}")
p.produce(TOPIC, key="acct-B", value=None)           # tombstone: delete acct-B
p.flush()
dump("BEFORE compaction")

tick_until(lambda rows: not any(r.startswith("0:") for r in rows), "first compaction pass done")
dump("AFTER compaction (latest per key; acct-B's tombstone ∅ still visible)")
log("waiting for delete.retention.ms (30s) to pass, then for the next cleaning to drop the tombstone...")
tick_until(lambda rows: not any("acct-B" in r for r in rows), "tombstone removed", max_s=120)
dump("AFTER delete.retention.ms (acct-B is completely gone)")
print("""
Rules worth remembering
 * Compaction never changes offsets and never reorders -> consumers see gaps.
 * A consumer that reads from the start always ends with the correct latest state per key.
 * A slow consumer (lagging > delete.retention.ms) can MISS a tombstone and keep a deleted key.
 * Records with a null KEY are rejected by compacted topics.
 * cleanup.policy=compact,delete = compaction + time retention (e.g. 'latest state of the last 30 days').
""")
