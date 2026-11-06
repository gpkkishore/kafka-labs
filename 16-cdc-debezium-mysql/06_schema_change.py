#!/usr/bin/env python3
"""Schema evolution with CDC: ALTER TABLE in MySQL while Debezium is streaming.

What happens:
  * Debezium reads the DDL from the binlog, updates its in-memory schema and appends it to the schema history
    topic (_schema-history.inventory) so it can parse older binlog events after a restart.
  * A schema-change event is published to the topic "cdc" (include.schema.changes=true).
  * New row events carry the new column (and the new Connect schema).
  * Downstream: the JDBC sink with schema.evolution=basic adds the column in Postgres; the Redis sync just stores
    another hash field; Flink/consumers with fixed schemas must be updated (-> Schema Registry, folder 12).

    python 16-cdc-debezium-mysql/06_schema_change.py
"""
import json
import subprocess
import time
import uuid

from cdc_common import mysql, unwrap
from confluent_kafka import Consumer, TopicPartition

from common.kafka_helpers import BOOTSTRAP, banner

with mysql().cursor() as cur:
    cur.execute("SELECT COUNT(*) n FROM information_schema.columns WHERE table_schema='inventory' "
                "AND table_name='customers' AND column_name='loyalty_points'")
    if cur.fetchone()["n"] == 0:
        cur.execute("ALTER TABLE customers ADD COLUMN loyalty_points INT NOT NULL DEFAULT 0")
        print("ALTER TABLE customers ADD COLUMN loyalty_points INT NOT NULL DEFAULT 0")
    cur.execute("UPDATE customers SET loyalty_points = loyalty_points + 100 WHERE id = 1")
time.sleep(6)

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"ddl-{uuid.uuid4()}", "enable.auto.commit": False})
banner("Schema change events in topic 'cdc'")
c.assign([TopicPartition("cdc", 0, 0)])
for _ in range(30):
    m = c.poll(1)
    if m is None:
        break
    if not m.error():
        ev = unwrap(m.value())
        origin = "binlog  " if str(ev["source"].get("snapshot", "false")) == "false" else "snapshot"
        print(f"  {origin} {' '.join(ev['ddl'].split())[:120]}")

banner("Latest event for customer 1 now has loyalty_points (and its schema lists the new field)")
c.unassign()
c2 = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"ddl2-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
c2.subscribe(["cdc.inventory.customers"])
last = None
idle = 0
while idle < 4:
    m = c2.poll(1)
    if m is None:
        idle += 1 if c2.assignment() else 0
        continue
    if not m.error() and m.value() and unwrap(m.key()) == {"id": 1}:
        last = m
        idle = 0
c2.close()
doc = json.loads(last.value())
after_fields = [f for f in doc["schema"]["fields"] if f["field"] == "after"][0]["fields"]
print("  after =", unwrap(last.value())["after"])
print("  schema fields:", [f["field"] for f in after_fields])

pg = subprocess.run(["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "warehouse", "-At", "-c",
                     "SELECT column_name FROM information_schema.columns WHERE table_schema='replica' AND table_name='customers'"],
                    capture_output=True, text=True).stdout.split()
print("\n  Postgres replica.customers columns:", pg, "(loyalty_points added by schema.evolution=basic if 05 is running)")
