#!/usr/bin/env python3
"""AKiA 11.7 "Single message transformations" / DG ch.7 "A Deeper Look at Connect".

One JDBC source with a CHAIN of SMTs applied to every record before it is written:

  ValueToKey + ExtractField$Key  -> key = customer id (so compaction/partitioning by id works)
  MaskField$Value                -> email replaced with "" (PII masking)
  ReplaceField$Value             -> rename name -> full_name, drop nothing else
  Cast$Value                     -> id as string
  TimestampConverter$Value       -> updated_at as ISO string instead of epoch millis
  InsertField$Value              -> add static field source_system="mysql-shop" and the Kafka topic name
  RegexRouter                    -> rename topic kinaction_smt_customers -> kinaction_smt_customers_v1
  Filter + predicate             -> drop tier='bronze' records? (Filter works with *predicates* on topic/header/tombstone;
                                    content filtering needs a custom SMT or ksqlDB/Flink -> shown here: HasHeaderKey example in README)

    python 15-kafka-connect/04_single_message_transforms.py
"""
import json
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from confluent_kafka import Consumer  # noqa: E402
from connect_api import Connect, print_status  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner  # noqa: E402

c = Connect()
NAME = "jdbc-customers-smt"
c.delete(NAME)
time.sleep(1)
c.put(NAME, {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://mysql:3306/shop", "connection.user": "app", "connection.password": "apppw",
    "table.whitelist": "customers", "mode": "timestamp+incrementing",
    "incrementing.column.name": "id", "timestamp.column.name": "updated_at", "db.timezone": "UTC",
    "topic.prefix": "kinaction_smt_", "poll.interval.ms": 2000,
    # schemaless JSON so the output is easy to read
    "key.converter": "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",

    "transforms": "createKey,extractId,mask,rename,castId,isoTs,addSource,addTopic,route",
    "transforms.createKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.createKey.fields": "id",
    "transforms.extractId.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractId.field": "id",
    "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
    "transforms.mask.fields": "email",
    "transforms.mask.replacement": "***@***",
    "transforms.rename.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.rename.renames": "name:full_name",
    "transforms.castId.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castId.spec": "id:string",
    "transforms.isoTs.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.isoTs.field": "updated_at",
    "transforms.isoTs.target.type": "string",
    "transforms.isoTs.format": "yyyy-MM-dd'T'HH:mm:ss.SSSX",
    "transforms.addSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.addSource.static.field": "source_system",
    "transforms.addSource.static.value": "mysql-shop",
    "transforms.addTopic.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.addTopic.topic.field": "kafka_topic",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "kinaction_smt_(.*)",
    "transforms.route.replacement": "kinaction_smt_$1_v1",
})
print_status(c.wait_state(NAME))
time.sleep(5)

banner("Records in kinaction_smt_customers_v1 (note the topic name changed by RegexRouter)")
cons = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"peek-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
cons.subscribe(["kinaction_smt_customers_v1"])
idle = 0
while idle < 4:
    m = cons.poll(1)
    if m is None:
        idle += 1 if cons.assignment() else 0
        continue
    if not m.error():
        idle = 0
        print(f"  key={m.key().decode():4s} value={json.dumps(json.loads(m.value()))}")
cons.close()
print("""
SMTs are for light, stateless, per-record changes. Joins, aggregations, lookups -> stream processing
(14-stream-processing-ksqldb, 17-flink-sql). Order in "transforms" matters: InsertField(topic) ran BEFORE
RegexRouter, so kafka_topic still shows the old name.
""")
