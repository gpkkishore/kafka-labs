#!/usr/bin/env python3
"""A producer that NEVER loses an acknowledged record: acks=all + idempotence + retries.
It writes the sequence number of every ACKED record to acked.log so 03_verify_no_data_loss.py can check.

    python 06-brokers-replication-failover/02_durable_producer.py --rate 50
"""
import argparse
import signal
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, ensure_topic, log  # noqa: E402

HERE = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="kinaction_replication")
ap.add_argument("--rate", type=float, default=50)
ap.add_argument("--acks", default="all")
ap.add_argument("--seconds", type=float, default=0, help="stop after N seconds (0 = run until Ctrl+C)")
args = ap.parse_args()
ensure_topic(args.topic, 6, 3, {"min.insync.replicas": 2})

RUN_ID = uuid.uuid4().hex[:8]          # values are "<run>:<seq>" so the verifier can ignore older runs
acked_file = open(HERE / "acked.log", "w")
acked_file.write(f"run {RUN_ID}\n")
stats = {"sent": 0, "acked": 0, "failed": 0}
errors_seen: dict[str, int] = {}


def on_delivery(err, msg):
    if err:
        stats["failed"] += 1
        errors_seen[err.name()] = errors_seen.get(err.name(), 0) + 1
    else:
        stats["acked"] += 1
        acked_file.write(msg.value().decode() + "\n")


p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": args.acks,
    "enable.idempotence": args.acks == "all",
    # keep retrying a record for up to 2 min (the default). Long enough to ride out a broker restart; after that the
    # record FAILS LOUDLY (_MSG_TIMED_OUT) instead of being silently lost.
    "delivery.timeout.ms": 120000,
    "linger.ms": 5,
    # surface the retriable errors librdkafka handles internally, so you can SEE them:
    "error_cb": lambda e: errors_seen.__setitem__(f"(client) {e.name()}", errors_seen.get(f"(client) {e.name()}", 0) + 1),
})
running = True
signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("running", False))
log(f"producing to {args.topic} acks={args.acks} at {args.rate}/s — Ctrl+C to stop")
seq, last, start = 0, time.time(), time.time()
try:
    while running and (not args.seconds or time.time() - start < args.seconds):
        try:
            p.produce(args.topic, key=str(seq % 100), value=f"{RUN_ID}:{seq}", on_delivery=on_delivery)
            seq += 1
            stats["sent"] += 1
        except BufferError:
            pass
        p.poll(0)
        if time.time() - last >= 2:
            last = time.time()
            inflight = len(p)
            log(f"sent={stats['sent']} acked={stats['acked']} failed={stats['failed']} in-flight={inflight} "
                f"errors={errors_seen or '-'}")
        time.sleep(1 / args.rate)
except KeyboardInterrupt:
    pass
log("flushing...")
p.flush(40)
acked_file.close()
log(f"FINAL sent={stats['sent']} acked={stats['acked']} failed={stats['failed']} errors={errors_seen}")
