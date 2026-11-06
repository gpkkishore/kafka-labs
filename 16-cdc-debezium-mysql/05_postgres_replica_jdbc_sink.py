#!/usr/bin/env python3
"""Replicate MySQL -> Postgres with the Debezium JDBC SINK connector (inserts, updates AND deletes).

The Debezium JDBC sink understands the Debezium envelope natively:
  op c/r/u -> UPSERT by primary key (record_key), op d -> DELETE, schema.evolution=basic -> adds new columns.
(With the Confluent JDBC sink you'd add the ExtractNewRecordState SMT to flatten the envelope first.)

    python 16-cdc-debezium-mysql/05_postgres_replica_jdbc_sink.py
"""
import subprocess
import time

from cdc_common import Connect, mysql, print_status

NAME = "pg-replica-sink"
c = Connect()
c.delete(NAME)
time.sleep(1)
c.put(NAME, {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": 1,
    "topics.regex": r"cdc\.inventory\.(customers|products|orders)",
    "connection.url": "jdbc:postgresql://postgres:5432/warehouse",
    "connection.username": "postgres", "connection.password": "postgres",
    "insert.mode": "upsert",
    "delete.enabled": "true",                 # op=d -> DELETE (needs primary.key.mode=record_key)
    "primary.key.mode": "record_key",
    "schema.evolution": "basic",              # CREATE TABLE / ADD COLUMN automatically
    # topic cdc.inventory.customers -> table replica.customers
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": r"cdc\.inventory\.(.*)",
    "transforms.route.replacement": "$1",
    "table.name.format": "replica.${topic}",
})
print_status(c.wait_state(NAME))

with mysql().cursor() as cur:
    cur.execute("INSERT INTO customers (first_name,last_name,email,city) VALUES ('Replica','Test','replica@test.io','Lisbon')")
    new_id = cur.lastrowid
    cur.execute("UPDATE customers SET city='Porto' WHERE id=%s", (new_id,))
print(f"inserted+updated customer {new_id} in MySQL; waiting for the sink...")
time.sleep(10)


def psql(sql):
    return subprocess.run(["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "warehouse", "-At", "-c", sql],
                          capture_output=True, text=True).stdout.strip()


def compare(title):
    print(f"\n== {title}")
    with mysql().cursor() as cur:
        for t in ("customers", "products", "orders"):
            cur.execute(f"SELECT COUNT(*) n FROM {t}")
            print(f"  {t:9s} mysql={cur.fetchone()['n']:4d}  postgres={psql(f'SELECT COUNT(*) FROM replica.{t}') or '?':>4s}")


compare("row counts after initial load")
print("  postgres row:", psql(f"SELECT id, first_name, city FROM replica.customers WHERE id={new_id}"))

with mysql().cursor() as cur:
    cur.execute("DELETE FROM customers WHERE id=%s", (new_id,))
time.sleep(8)
compare(f"after DELETE of customer {new_id}")
print("  postgres row:", psql(f"SELECT id FROM replica.customers WHERE id={new_id}") or "(gone — delete replicated)")
print("\n  docker exec -it postgres psql -U postgres -d warehouse -c '\\dt replica.*'")
