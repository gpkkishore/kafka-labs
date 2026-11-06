#!/usr/bin/env python3
"""Ch.3.2 / 3.3 — the book's factory example: sensors on assembly-line *stages* emit events.

Design decisions encoded here:
  * key = stage id          -> all events of one stage stay ORDERED in one partition
  * value = JSON with an explicit "schema_version" field (see 12-schema-registry-avro for the Avro way)
  * critical alerts         -> kinaction_alert (acks=all)
  * every reading           -> kinaction_alerttrend (acks=1, lz4, loss acceptable)
  * latest status per sensor-> kinaction_sensor_status (compacted)
  * every user action       -> kinaction_audit (acks=all + idempotence)

    python 02-designing-events/sensor_producer.py --seconds 20
"""
import argparse
import json
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=int, default=20)
ap.add_argument("--rate", type=int, default=20, help="readings per second")
args = ap.parse_args()

safe = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True})
fast = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "1", "linger.ms": 50, "compression.type": "lz4"})

STAGES = ["stage-1", "stage-2", "stage-3", "stage-4"]
SENSORS = {s: [f"{s}-sensor-{i}" for i in range(3)] for s in STAGES}

banner("Factory sensors producing (Ctrl+C to stop)")
end = time.time() + args.seconds
n = alerts = 0
while time.time() < end:
    stage = random.choice(STAGES)
    sensor = random.choice(SENSORS[stage])
    temp = round(random.gauss(70, 12), 1)
    event = {"schema_version": 1, "stage_id": stage, "sensor_id": sensor,
             "temperature_c": temp, "ts": int(time.time() * 1000)}
    payload = json.dumps(event).encode()

    fast.produce("kinaction_alerttrend", key=stage, value=payload)
    safe.produce("kinaction_sensor_status", key=sensor, value=payload)
    if temp > 90:
        alerts += 1
        alert = {**event, "level": "CRITICAL", "reason": "over temperature"}
        safe.produce("kinaction_alert", key=stage, value=json.dumps(alert).encode(),
                     on_delivery=lambda e, m: log(f"ALERT {m.key().decode()} -> p{m.partition()}@{m.offset()}"))
    if random.random() < 0.02:   # someone acknowledges / mutes a sensor
        safe.produce("kinaction_audit", key="operator-7",
                     value=json.dumps({"user": "operator-7", "action": "mute", "sensor_id": sensor}).encode())
    n += 1
    fast.poll(0)
    safe.poll(0)
    time.sleep(1 / args.rate)

fast.flush()
safe.flush()
log(f"done: {n} readings, {alerts} alerts")
