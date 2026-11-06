#!/usr/bin/env python3
"""AKiA 11.8 "JDBC Source Connector" / DG ch.7 "MySQL to Elasticsearch" — poll a MySQL table into Kafka.

Modes of the Confluent JDBC source:
  bulk                   re-read the whole table every poll
  incrementing           WHERE id > last_id            -> sees INSERTs only
  timestamp              WHERE updated_at > last_ts    -> sees INSERTs + UPDATEs
  timestamp+incrementing both (robust against equal timestamps)   <- used for customers
NONE of them sees DELETEs, and changes between two polls are merged -> that's why CDC exists (folder 16).

    python 15-kafka-connect/03_jdbc_source_mysql.py
"""
import json
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import pymysql  # noqa: E402
from confluent_kafka import Consumer  # noqa: E402
from connect_api import Connect, print_status  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner  # noqa: E402

c = Connect()
COMMON = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://mysql:3306/shop",
    "connection.user": "app", "connection.password": "apppw",
    "topic.prefix": "kinaction_jdbc_",
    "poll.interval.ms": 2000,
    "db.timezone": "UTC",
}
c.delete("jdbc-customers")
c.delete("jdbc-orders")
time.sleep(1)
c.put("jdbc-customers", {**COMMON, "table.whitelist": "customers", "mode": "timestamp+incrementing",
                          "incrementing.column.name": "id", "timestamp.column.name": "updated_at",
                          "tasks.max": 1})
c.put("jdbc-orders", {**COMMON, "table.whitelist": "orders", "mode": "incrementing",
                       "incrementing.column.name": "id", "tasks.max": 1,
                       # GOTCHA: DECIMAL columns become the Connect "Decimal" logical type = base64 bytes in JSON
                       # (e.g. 'CfY='). numeric.mapping=best_fit only handles NUMERIC, not MySQL DECIMAL, so we
                       # cast with an SMT. Remove the 3 lines below to see the base64 values.
                       "transforms": "amountToDouble",
                       "transforms.amountToDouble.type": "org.apache.kafka.connect.transforms.Cast$Value",
                       "transforms.amountToDouble.spec": "amount:float64"})
print_status(c.wait_state("jdbc-customers"))
print_status(c.wait_state("jdbc-orders"))

db = pymysql.connect(host="127.0.0.1", port=3306, user="app", password="apppw", database="shop", autocommit=True)
with db.cursor() as cur:
    tag = uuid.uuid4().hex[:4]
    cur.execute("INSERT INTO customers (name, email, tier) VALUES (%s, %s, 'bronze')", (f"New {tag}", f"{tag}@example.com"))
    cur.execute("UPDATE customers SET tier='platinum' WHERE id=1")
    cur.execute("INSERT INTO customers (name, email) VALUES ('To Delete', 'gone@example.com')")
    cur.execute("DELETE FROM customers WHERE email='gone@example.com'")   # inserted+deleted between polls
    cur.execute("INSERT INTO orders (customer_id, amount) VALUES (1, 42.00)")
print("\nchanged MySQL: 1 insert, 1 update (alice -> platinum), 1 insert+delete, 1 order\n")
time.sleep(6)

banner("kinaction_jdbc_customers  (JsonConverter with schema: {schema:..., payload:...})")
cons = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"peek-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
cons.subscribe(["kinaction_jdbc_customers", "kinaction_jdbc_orders"])
first = True
idle = 0
while idle < 4:
    m = cons.poll(1)
    if m is None:
        idle += 1 if cons.assignment() else 0
        continue
    if m.error():
        continue
    idle = 0
    doc = json.loads(m.value())
    if first:
        print("  schema of a record:", json.dumps(doc["schema"])[:300], "...")
        first = False
    print(f"  {m.topic():26s} key={m.key()} payload={doc['payload']}")
cons.close()
print("""
Observe:
 * alice appears twice (snapshot + UPDATE picked up by updated_at). The 'To Delete' row may never appear at all
   (inserted and deleted between two polls) and the DELETE itself is invisible.
 * key=None: add the SMTs ValueToKey+ExtractField to key by id -> see 04_single_message_transforms.py.
 * The connector stores {"incrementing": N, "timestamp": ...} as its source offset:
     curl -s localhost:8083/connectors/jdbc-customers/offsets | jq
""")
