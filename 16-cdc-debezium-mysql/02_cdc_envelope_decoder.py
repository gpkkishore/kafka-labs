#!/usr/bin/env python3
"""Decode Debezium change events: op, before/after, what changed, and the `source` block.

    python 16-cdc-debezium-mysql/02_cdc_envelope_decoder.py              # snapshot + everything so far, then exit
    python 16-cdc-debezium-mysql/02_cdc_envelope_decoder.py --follow     # keep tailing (run 03 in another terminal)

Envelope (value):
  { "before": {...} | null,        # row before the change (null for c / r)
    "after":  {...} | null,        # row after the change  (null for d)
    "op": "r|c|u|d|t",
    "ts_ms": 1700000000123,        # when Debezium processed it
    "source": { "file": "mysql-bin.000003", "pos": 1234, "gtid": "...", "snapshot": "true|last|false",
                "db": "inventory", "table": "customers", "ts_ms": <commit time in MySQL>, ... } }
Key = the primary key, e.g. {"id": 1001}  -> same row always goes to the same partition (ordered per row).
A DELETE produces 2 records: op=d, then a TOMBSTONE (same key, value=null) for log compaction.
"""
import argparse
import json
import uuid

from cdc_common import ROOT, TABLE_TOPICS, OPS, unwrap  # noqa: F401
from confluent_kafka import Consumer

from common.kafka_helpers import BOOTSTRAP, RESET, banner, color_for

ap = argparse.ArgumentParser()
ap.add_argument("--follow", action="store_true")
ap.add_argument("--group", default=None, help="use a fixed group to only see NEW changes on the next run")
args = ap.parse_args()

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": args.group or f"cdc-decoder-{uuid.uuid4().hex[:6]}",
              "auto.offset.reset": "earliest"})
c.subscribe(TABLE_TOPICS + ["cdc"])
banner("Debezium change events (Ctrl+C to stop)")
COL = {"r": 3, "c": 0, "u": 1, "d": 5, "t": 2}
idle, counts = 0, {}
try:
    while args.follow or idle < 5:
        m = c.poll(1.0)
        if m is None:
            idle += 1 if c.assignment() else 0
            continue
        if m.error():
            continue
        idle = 0
        key = unwrap(m.key())
        if m.topic() == "cdc":                                      # schema change topic
            ev = unwrap(m.value())
            print(f"{color_for(2)}DDL{RESET} {ev.get('databaseName')}: {" ".join(ev.get('ddl', '').split())[:110]}")
            continue
        ev = unwrap(m.value())
        table = m.topic().split(".")[-1]
        if ev is None:
            print(f"   {table:9s} TOMBSTONE key={key}  (lets compaction remove this key entirely)")
            counts["tombstone"] = counts.get("tombstone", 0) + 1
            continue
        op, src = ev["op"], ev["source"]
        counts[op] = counts.get(op, 0) + 1
        # source.snapshot: true | first | first_in_data_collection | last_in_data_collection | last | false
        s = str(src.get("snapshot", "false"))
        snap = "binlog" if s in ("false", "None") else ("snapshot" if s == "true" else f"snapshot:{s}")
        lag = ev["ts_ms"] - src["ts_ms"] if src.get("ts_ms") else 0
        head = f"{color_for(COL.get(op, 4))}{OPS.get(op, op):16s}{RESET} {table:9s} key={json.dumps(key):12s} p{m.partition()}@{m.offset():<4d}"
        where = f"[{snap} {src.get('file')}:{src.get('pos')} lag={lag}ms]"
        if op == "u":
            diff = {k: f"{ev['before'].get(k)} -> {v}" for k, v in ev["after"].items() if ev["before"].get(k) != v}
            print(f"{head} changed {diff} {where}")
        elif op == "d":
            print(f"{head} before={ev['before']} {where}")
        else:
            print(f"{head} after={ev['after']} {where}")
except KeyboardInterrupt:
    pass
finally:
    c.close()
    print("\ncounts:", counts)
