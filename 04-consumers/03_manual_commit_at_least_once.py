#!/usr/bin/env python3
"""Ch.5.4 — marking our place: auto commit vs manual commit, and what a crash does.

    python 04-consumers/03_manual_commit_at_least_once.py --mode at-least-once --crash-after 12
    python 04-consumers/03_manual_commit_at_least_once.py --mode at-least-once      # resumes -> see duplicates
    python 04-consumers/03_manual_commit_at_least_once.py --mode at-most-once --crash-after 12
    python 04-consumers/03_manual_commit_at_least_once.py --mode at-most-once       # resumes -> see LOST records

--reset deletes the group's offsets first so you can repeat the experiment.
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner, log  # noqa: E402

TOPIC = "kinaction_orders"
ap = argparse.ArgumentParser()
ap.add_argument("--mode", choices=["at-least-once", "at-most-once"], default="at-least-once")
ap.add_argument("--crash-after", type=int, default=0, help="hard-crash (os._exit) after N records")
ap.add_argument("--reset", action="store_true")
args = ap.parse_args()
GROUP = f"commit-demo-{args.mode}"
COMMIT_EVERY = 5

if args.reset:
    a = admin()   # keep a reference: if the AdminClient is garbage-collected its futures never complete
    try:
        a.delete_consumer_groups([GROUP], request_timeout=10)[GROUP].result()
        log(f"deleted group {GROUP}")
    except Exception as e:  # noqa: BLE001
        log(f"could not delete: {e}")
        log("NON_EMPTY_GROUP right after a crash = the dead member is still registered until "
            "session.timeout.ms (45s) expires. Wait and retry.")
    sys.exit(0)

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "auto.offset.reset": "earliest",
              "enable.auto.commit": False})        # WE decide when the position is saved
c.subscribe([TOPIC])
banner(f"{args.mode}: commit every {COMMIT_EVERY} records, crash after {args.crash_after or 'never'}")
n, idle, batch = 0, 0, []
while idle < 5:
    m = c.poll(1.0)
    if m is None:
        idle += 1 if c.assignment() else 0
        continue
    if m.error():
        continue
    idle = 0
    batch.append(m)
    if args.mode == "at-most-once":
        c.commit(message=m, asynchronous=False)   # 1) commit FIRST ...
        log(f"committed p{m.partition()}@{m.offset() + 1} then processing {m.value().decode()}")  # 2) ... then process
    else:
        log(f"processed p{m.partition()}@{m.offset()} {m.value().decode()}")                   # 1) process FIRST ...
        if len(batch) >= COMMIT_EVERY:
            c.commit(asynchronous=False)                                                        # 2) ... then commit
            log(f"   committed positions {[(p.partition, p.offset) for p in c.position(c.assignment())]}")
            batch.clear()
    n += 1
    if args.crash_after and n >= args.crash_after:
        log("💥 simulated crash (no close(), no final commit)")
        os._exit(1)
c.commit(asynchronous=False) if batch else None
c.close()
log(f"done, {n} records this run")
print("""
Note: the committed value is the offset of the NEXT record to read (last processed + 1).
at-least-once: after the crash, records processed since the last commit are processed AGAIN (duplicates).
at-most-once : the record committed-but-not-processed when we crashed is never processed (loss).
Make processing idempotent (e.g. upsert by key) and at-least-once becomes "effectively once".
""")
