#!/usr/bin/env python3
"""Make consumer LAG happen on purpose and watch it in Grafana / Prometheus alerts.

  --produce-rate  messages/s produced
  --consumers     number of consumer instances (threads, each its own Consumer in one group)
  --process-ms    processing time per record per consumer -> max throughput = consumers * 1000/process-ms

    python 10-monitoring-prometheus-grafana/01_lag_generator.py --produce-rate 200 --consumers 1 --process-ms 20   # 50/s < 200/s => lag grows
    python 10-monitoring-prometheus-grafana/01_lag_generator.py --produce-rate 200 --consumers 6 --process-ms 20   # 300/s => catches up
"""
import argparse
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, ensure_topic, log  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="kinaction_monitoring")
ap.add_argument("--group", default="kinaction_monitoring_group")
ap.add_argument("--produce-rate", type=float, default=200)
ap.add_argument("--consumers", type=int, default=1)
ap.add_argument("--process-ms", type=float, default=20)
ap.add_argument("--seconds", type=float, default=0, help="0 = forever")
args = ap.parse_args()
ensure_topic(args.topic, 6, 3)
stop = threading.Event()
consumed = [0]


def producer():
    p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 10})
    i = 0
    while not stop.is_set():
        p.produce(args.topic, key=str(i % 100), value=f"event {i}")
        p.poll(0)
        i += 1
        time.sleep(1 / args.produce_rate)
    p.flush()


def consumer(n):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": args.group, "client.id": f"lag-consumer-{n}",
                  "auto.offset.reset": "latest"})
    c.subscribe([args.topic])
    while not stop.is_set():
        m = c.poll(0.5)
        if m and not m.error():
            time.sleep(args.process_ms / 1000)
            consumed[0] += 1
    c.close()


threads = [threading.Thread(target=producer, daemon=True)] + \
          [threading.Thread(target=consumer, args=(i,), daemon=True) for i in range(args.consumers)]
[t.start() for t in threads]
cap = args.consumers * 1000 / args.process_ms
log(f"produce {args.produce_rate:.0f}/s vs consume capacity ≈{cap:.0f}/s -> lag will "
    f"{'GROW' if cap < args.produce_rate else 'stay ~0'}.  Grafana: http://localhost:3000  (Ctrl+C to stop)")
start, last = time.time(), 0
try:
    while not args.seconds or time.time() - start < args.seconds:
        time.sleep(5)
        log(f"consumed {(consumed[0] - last) / 5:.0f}/s")
        last = consumed[0]
except KeyboardInterrupt:
    pass
stop.set()
[t.join(timeout=5) for t in threads]
