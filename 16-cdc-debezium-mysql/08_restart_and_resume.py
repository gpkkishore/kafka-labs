#!/usr/bin/env python3
"""Failure scenario: the CDC connector is down while the database keeps changing.

Debezium stores the binlog position (file/pos/GTID) as its source offset. After a restart it resumes from there,
so no change is lost — as long as MySQL still has those binlog files (binlog_expire_logs_seconds!).

    python 16-cdc-debezium-mysql/08_restart_and_resume.py
"""
import time

from cdc_common import Connect, mysql, print_status

c = Connect()
NAME = "inventory-cdc"
print("offset before:", c.offsets(NAME)["offsets"][0]["offset"])

c.stop(NAME)                                      # like a crash/outage of the connector
print_status(c.wait_state(NAME, "STOPPED", min_tasks=0))
with mysql().cursor() as cur:
    for i in range(5):
        cur.execute("UPDATE products SET stock = stock + 1 WHERE id = 1")
print("made 5 updates to product 1 while the connector was STOPPED")
time.sleep(2)
c.resume(NAME)
print_status(c.wait_state(NAME, timeout=60))
time.sleep(6)
print("offset after :", c.offsets(NAME)["offsets"][0]["offset"])
print("""
Now run:  python 16-cdc-debezium-mysql/02_cdc_envelope_decoder.py | grep products | tail -6
-> the 5 UPDATEs are there, with source timestamps from when MySQL committed them (lag shows the outage).
If the binlog had been purged meanwhile, the connector would fail and need a new snapshot (snapshot.mode=when_needed).
""")
