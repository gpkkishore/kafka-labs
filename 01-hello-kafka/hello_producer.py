#!/usr/bin/env python3
"""Ch.2.1 — produce your first messages and SEE where they land (partition + offset).

    python 01-hello-kafka/hello_producer.py            # 10 messages
    python 01-hello-kafka/hello_producer.py -n 100 --key user-42
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, color_for, ensure_topic, log, RESET  # noqa: E402

TOPIC = "kinaction_helloworld"   # same topic name the book uses

p = argparse.ArgumentParser()
p.add_argument("-n", type=int, default=10)
p.add_argument("--key", default=None, help="fixed key => every message goes to the SAME partition")
args = p.parse_args()

ensure_topic(TOPIC, partitions=3, rf=3)

producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "hello-producer"})


def on_delivery(err, msg):
    """Called from producer.poll()/flush() once the broker acknowledged (or rejected) the record."""
    if err:
        log(f"FAILED: {err}")
        return
    c = color_for(msg.partition())
    log(f"{c}key={msg.key()} -> partition {msg.partition()} @ offset {msg.offset()}{RESET}  value={msg.value().decode()}")


banner(f"Producing {args.n} messages to '{TOPIC}'")
for i in range(args.n):
    key = args.key.encode() if args.key else None   # no key => sticky partitioner spreads batches
    producer.produce(TOPIC, key=key, value=f"hello kafka #{i}".encode(), on_delivery=on_delivery)
    producer.poll(0)          # serve delivery callbacks
producer.flush(10)            # block until everything is acknowledged

print("""
What happened?
 * produce() only APPENDS to an in-memory batch; a background thread ships batches to the
   *leader* broker of each partition. flush() waits for the acks.
 * Each record gets an OFFSET = its position in that partition's append-only commit log.
 * Offsets are per partition: partition 0 and partition 1 both have an offset 0.
 * With --key, all records hash (murmur2) to one partition => ordering for that key.
""")
