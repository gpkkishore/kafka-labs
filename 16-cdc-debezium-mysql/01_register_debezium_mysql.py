#!/usr/bin/env python3
"""AKiA 11.9 / KiA 8.3.2 — register the Debezium MySQL source connector.

What Debezium does on start (snapshot.mode=initial):
  1. Connects as a replica (server.id!) and remembers the current binlog position / GTID set.
  2. SNAPSHOT: reads every existing row -> events with op="r".
  3. STREAMING: tails the binlog from that position -> op="c"/"u"/"d" for every committed change.
  4. Stores its binlog position in _connect_offsets and the table DDL history in _schema-history.inventory,
     so a restart resumes exactly where it stopped (at-least-once).

    python 16-cdc-debezium-mysql/01_register_debezium_mysql.py
"""
import time

from cdc_common import Connect, print_status

NAME = "inventory-cdc"
CONFIG = {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": 1,                                   # the binlog is a single ordered stream -> always 1 task
    "database.hostname": "mysql", "database.port": 3306,
    "database.user": "debezium", "database.password": "dbz",
    "database.server.id": 184054,                     # unique among all MySQL replicas/clients
    "topic.prefix": "cdc",                            # topics: cdc.inventory.customers, ...  + "cdc" for DDL
    "database.include.list": "inventory",
    "table.include.list": "inventory.customers,inventory.products,inventory.orders",
    "snapshot.mode": "initial",
    "include.schema.changes": "true",                 # DDL events -> topic "cdc"
    "schema.history.internal.kafka.bootstrap.servers": "kafka1:29092,kafka2:29092,kafka3:29092",
    "schema.history.internal.kafka.topic": "_schema-history.inventory",
    "decimal.handling.mode": "double",                # DECIMAL -> JSON number instead of base64 bytes
    "tombstones.on.delete": "true",                   # after a delete event, send key+null so compaction can drop the key
    "topic.creation.default.replication.factor": 3,
    "topic.creation.default.partitions": 3,
    "topic.creation.default.cleanup.policy": "compact",   # latest state per primary key is kept forever
    # JSON *with* schema: verbose, but the JDBC sink (05) and Flink's debezium-json format need the types
    "key.converter": "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "true",
}

c = Connect()
c.remove(NAME)          # stop + reset offsets + delete => a fresh snapshot every time you run this
time.sleep(2)
c.put(NAME, CONFIG)
print_status(c.wait_state(NAME, timeout=90))
time.sleep(5)
print("\ntopics created by Debezium:", c.topics(NAME)[NAME]["topics"])
print("stored source offset (binlog file/pos/gtids):", c.offsets(NAME)["offsets"])
print("\nNext: python 16-cdc-debezium-mysql/02_cdc_envelope_decoder.py")
