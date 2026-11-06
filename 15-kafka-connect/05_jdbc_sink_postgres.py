#!/usr/bin/env python3
"""DG ch.7 (MySQL -> Elasticsearch, here MySQL -> Postgres) / AKiA 11.6 — a SINK connector.

   MySQL shop.customers ──jdbc-customers (03)──▶ kinaction_jdbc_customers ──jdbc-sink-pg──▶ Postgres warehouse.customers_copy

insert.mode=upsert + pk.mode=record_value(id): re-delivered records just overwrite -> idempotent sink.
auto.create / auto.evolve: the sink creates and ALTERs the table from the record schema (needs schemas -> JsonConverter
with schemas.enable=true, which is the worker default here, or Avro/Protobuf + Schema Registry).

    python 15-kafka-connect/03_jdbc_source_mysql.py   # first (creates the source topic)
    python 15-kafka-connect/05_jdbc_sink_postgres.py
"""
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import pymysql  # noqa: E402
from connect_api import Connect, print_status  # noqa: E402

from common.kafka_helpers import banner  # noqa: E402

c = Connect()
NAME = "jdbc-sink-pg"
c.delete(NAME)
time.sleep(1)
c.put(NAME, {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "kinaction_jdbc_customers",
    "connection.url": "jdbc:postgresql://postgres:5432/warehouse",
    "connection.user": "postgres", "connection.password": "postgres",
    "table.name.format": "customers_copy",
    "insert.mode": "upsert", "pk.mode": "record_value", "pk.fields": "id",
    "auto.create": "true", "auto.evolve": "true",
    "tasks.max": 1,
})
print_status(c.wait_state(NAME))

db = pymysql.connect(host="127.0.0.1", port=3306, user="app", password="apppw", database="shop", autocommit=True)
with db.cursor() as cur:
    cur.execute("UPDATE customers SET tier='gold', name='Bob Jones (VIP)' WHERE id=2")
print("updated bob in MySQL; waiting for source poll + sink flush...")
time.sleep(10)


def psql(sql):
    return subprocess.run(["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "warehouse", "-c", sql],
                          capture_output=True, text=True).stdout


banner("Postgres: \\d customers_copy  (auto-created from the Connect schema)")
print(psql("\\d customers_copy"))
banner("Postgres: SELECT * FROM customers_copy")
print(psql("SELECT id, name, email, tier, updated_at FROM customers_copy ORDER BY id"))
print("Bob's row was UPSERTed (one row per id, not one per change). A DELETE in MySQL is still not propagated -> CDC (16).")
