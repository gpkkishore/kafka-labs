#!/usr/bin/env python3
"""AKiA 15.3 "Client metrics" / DG ch.10 — expose librdkafka's internal statistics as Prometheus metrics.

The Java client exposes metrics via JMX; librdkafka emits a JSON blob every statistics.interval.ms.
We turn the interesting parts into Prometheus gauges on http://localhost:8000/metrics.
Prometheus (infra) scrapes host.docker.internal:8000 as job "python-clients";
Grafana dashboard: "Kafka Labs — Python clients".

    python 10-monitoring-prometheus-grafana/02_client_metrics_exporter.py --rate 500
"""
import argparse
import json
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402
from prometheus_client import Gauge, start_http_server  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, ensure_topic, log  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--rate", type=float, default=500)
ap.add_argument("--port", type=int, default=8000)
ap.add_argument("--process-ms", type=float, default=1.0)
args = ap.parse_args()
TOPIC = "kinaction_client_metrics"
ensure_topic(TOPIC, 6, 3)

L = ["client_id", "type"]
g_txmsgs = Gauge("rdkafka_tx_msgs_total", "messages sent (cumulative)", L)
g_rxmsgs = Gauge("rdkafka_rx_msgs_total", "messages received (cumulative)", L)
g_queue = Gauge("rdkafka_msg_queue", "messages waiting in the producer queue", L)
g_rtt = Gauge("rdkafka_broker_rtt_avg_ms", "average broker round trip time", L + ["broker"])
g_outbuf = Gauge("rdkafka_broker_outbuf_msgs", "requests waiting to be sent to the broker", L + ["broker"])
g_batch = Gauge("rdkafka_topic_batchsize_avg_bytes", "average producer batch size", L + ["topic"])
g_batchcnt = Gauge("rdkafka_topic_batchcnt_avg", "average messages per batch", L + ["topic"])
g_lag = Gauge("rdkafka_consumer_lag", "consumer lag as seen by the client", L + ["topic", "partition"])
g_rebal = Gauge("rdkafka_cgrp_rebalance_cnt", "rebalances seen by this consumer", L)


def stats_cb(raw):
    s = json.loads(raw)
    cid, typ = s["client_id"], s["type"]
    g_txmsgs.labels(cid, typ).set(s.get("txmsgs", 0))
    g_rxmsgs.labels(cid, typ).set(s.get("rxmsgs", 0))
    g_queue.labels(cid, typ).set(s.get("msg_cnt", 0))
    for b in s.get("brokers", {}).values():
        if b.get("nodeid", -1) < 0:
            continue                                         # skip bootstrap pseudo-brokers
        g_rtt.labels(cid, typ, str(b["nodeid"])).set(b["rtt"]["avg"] / 1000)
        g_outbuf.labels(cid, typ, str(b["nodeid"])).set(b.get("outbuf_msg_cnt", 0))
    for tname, t in s.get("topics", {}).items():
        if typ == "producer":
            g_batch.labels(cid, typ, tname).set(t["batchsize"]["avg"])
            g_batchcnt.labels(cid, typ, tname).set(t["batchcnt"]["avg"])
        for pid, part in t.get("partitions", {}).items():
            if pid != "-1" and part.get("consumer_lag", -1) >= 0:
                g_lag.labels(cid, typ, tname, pid).set(part["consumer_lag"])
    if "cgrp" in s:
        g_rebal.labels(cid, typ).set(s["cgrp"].get("rebalance_cnt", 0))


start_http_server(args.port)
log(f"metrics on http://localhost:{args.port}/metrics   (Ctrl+C to stop)")
stop = threading.Event()


def consume():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "client-metrics-demo", "client.id": "py-consumer",
                  "statistics.interval.ms": 5000, "stats_cb": stats_cb, "auto.offset.reset": "latest"})
    c.subscribe([TOPIC])
    while not stop.is_set():
        m = c.poll(0.2)
        if m and not m.error() and args.process_ms:
            time.sleep(args.process_ms / 1000)
    c.close()


threading.Thread(target=consume, daemon=True).start()
p = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "py-producer", "linger.ms": 20,
              "statistics.interval.ms": 5000, "stats_cb": stats_cb})
try:
    i = 0
    while True:
        p.produce(TOPIC, key=str(i % 50), value=f"m{i}")
        p.poll(0)
        i += 1
        time.sleep(1 / args.rate)
except KeyboardInterrupt:
    stop.set()
    p.flush()
