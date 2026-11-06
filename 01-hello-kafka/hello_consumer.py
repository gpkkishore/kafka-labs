#!/usr/bin/env python3
"""Ch.2.1 — consume the messages back. Shows the coordinates (topic, partition, offset).

    python 01-hello-kafka/hello_consumer.py                  # new random group => reads from beginning
    python 01-hello-kafka/hello_consumer.py --group g1        # run twice: 2nd run prints nothing new (offsets committed!)
"""
import argparse
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, color_for, log, RESET  # noqa: E402

TOPIC = "kinaction_helloworld"
p = argparse.ArgumentParser()
p.add_argument("--group", default=f"hello-{uuid.uuid4().hex[:6]}")
p.add_argument("--idle-exit", type=float, default=5.0, help="exit after N seconds without messages")
args = p.parse_args()

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": args.group,
    "auto.offset.reset": "earliest",   # where to start when the GROUP has no committed offset yet
    "enable.auto.commit": True,        # commit positions every auto.commit.interval.ms (5s)
})
consumer.subscribe([TOPIC])
banner(f"Consuming '{TOPIC}' as group '{args.group}' (Ctrl+C to stop)")

idle = 0.0
try:
    while idle < args.idle_exit:
        msg = consumer.poll(1.0)
        if msg is None:
            if consumer.assignment():      # only start the idle clock once we own partitions
                idle += 1
            continue
        if msg.error():
            log(f"error: {msg.error()}")
            continue
        idle = 0
        c = color_for(msg.partition())
        log(f"{c}{msg.topic()}[{msg.partition()}]@{msg.offset()}{RESET} key={msg.key()} value={msg.value().decode()}")
finally:
    consumer.close()   # commits final offsets + leaves the group cleanly
    print(f"\nRe-run with --group {args.group} : nothing is re-read, because the group's offsets were committed.")
