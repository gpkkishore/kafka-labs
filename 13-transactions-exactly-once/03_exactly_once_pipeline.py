#!/usr/bin/env python3
"""KiA 2.6.2 / AKiA 5.2 / DG ch.6 — consume-transform-produce with EXACTLY-ONCE semantics.

The trick: the consumer's offsets are committed INSIDE the producer's transaction
(send_offsets_to_transaction). Output records + input offsets commit atomically:
crash before commit -> both are rolled back -> input is re-processed -> output is NOT duplicated.

    python 13-transactions-exactly-once/03_exactly_once_pipeline.py --seed 100      # input: 100 orders
    python 13-transactions-exactly-once/03_exactly_once_pipeline.py --crash-after 37 # crash mid-transaction
    python 13-transactions-exactly-once/03_exactly_once_pipeline.py                  # restart, finish
    python 13-transactions-exactly-once/03_exactly_once_pipeline.py --verify         # 100 in, 100 out, 0 dupes
"""
import argparse
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner, ensure_topic, log, recreate_topic  # noqa: E402

IN, OUT, GROUP = "tx_orders_in", "tx_orders_enriched", "eos-enricher"
ap = argparse.ArgumentParser()
ap.add_argument("--seed", type=int)
ap.add_argument("--crash-after", type=int, default=0)
ap.add_argument("--verify", action="store_true")
ap.add_argument("--batch", type=int, default=10, help="records per transaction")
args = ap.parse_args()

if args.seed:
    a = admin()
    try:
        a.delete_consumer_groups([GROUP], request_timeout=10)[GROUP].result(timeout=15)
    except Exception:  # noqa: BLE001
        pass                                  # group didn't exist yet
    for t in (IN, OUT):
        recreate_topic(t, 3, 3, client=a)
        ensure_topic(t, 3, 3, client=a)       # belt and braces: metadata can lag right after a delete
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(args.seed):
        p.produce(IN, key=f"order-{i}", value=f"{i},{i * 10}")
    p.flush()
    log(f"seeded {args.seed} orders into {IN}")
    sys.exit(0)

if args.verify:
    def count(topic, iso):
        c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "verify", "enable.auto.commit": False,
                      "isolation.level": iso})
        parts = admin().list_topics(topic).topics[topic].partitions
        c.assign([TopicPartition(topic, p, 0) for p in parts])
        keys = Counter()
        while (m := c.poll(3)) is not None:
            if not m.error():
                keys[m.key()] += 1
        c.close()
        return keys
    i_keys = count(IN, "read_committed")
    for iso in ["read_committed", "read_uncommitted"]:
        o = count(OUT, iso)
        dupes = sum(v - 1 for v in o.values() if v > 1)
        banner(f"{OUT} as {iso}")
        print(f"  input records: {sum(i_keys.values())}   output records: {sum(o.values())}   "
              f"distinct keys: {len(o)}   duplicates: {dupes}")
    print("\nread_uncommitted shows the duplicates written by the crashed (aborted) transaction; read_committed doesn't.")
    sys.exit(0)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "auto.offset.reset": "earliest",
    "enable.auto.commit": False,          # offsets go through the transaction instead
    "isolation.level": "read_committed",  # never read someone else's aborted data
})
producer = Producer({"bootstrap.servers": BOOTSTRAP, "transactional.id": "eos-enricher-1"})
producer.init_transactions()   # also aborts any transaction left open by a crashed previous instance
consumer.subscribe([IN])

banner(f"EOS loop: {args.batch} records per transaction")
processed, idle = 0, 0
while idle < 5:
    msgs = consumer.consume(num_messages=args.batch, timeout=1.0)
    msgs = [m for m in msgs if not m.error()]
    if not msgs:
        idle += 1 if consumer.assignment() else 0
        continue
    idle = 0
    producer.begin_transaction()
    for m in msgs:
        order_id, amount = m.value().decode().split(",")
        producer.produce(OUT, key=m.key(), value=f"order {order_id} amount={amount} tax={int(amount) * 0.18:.1f}")
        processed += 1
        if args.crash_after and processed >= args.crash_after:
            producer.flush()   # output IS in the log (uncommitted)...
            log(f"💥 crash after {processed} records, mid-transaction (no commit)")
            os._exit(1)        # ...and the input offsets are NOT committed
    # commit the INPUT offsets as part of the SAME transaction
    producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                         consumer.consumer_group_metadata())
    producer.commit_transaction()
    log(f"committed tx with {len(msgs)} records (total {processed})")
consumer.close()
log(f"done, processed {processed} records in this run")
