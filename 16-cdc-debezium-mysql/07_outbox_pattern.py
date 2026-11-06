#!/usr/bin/env python3
"""Transactional OUTBOX pattern with Debezium's Outbox Event Router SMT.

Problem: a service must update its DB AND publish an event. Two separate writes (DB + Kafka) can't be atomic.
Solution: in ONE local DB transaction write the business row AND an "outbox" row. Debezium captures the outbox
insert from the binlog and the EventRouter SMT turns it into a clean domain event:

    outbox row (aggregatetype=Order, aggregateid=42, type=OrderCreated, payload={...})
        -> topic outbox.event.Order, key="42", value=payload, header eventType=OrderCreated

The outbox row can be deleted right away (the binlog still has the INSERT) so the table never grows.

    python 16-cdc-debezium-mysql/07_outbox_pattern.py
"""
import json
import time
import uuid

from cdc_common import Connect, mysql, print_status
from confluent_kafka import Consumer

from common.kafka_helpers import BOOTSTRAP, banner

NAME = "outbox-cdc"
c = Connect()
c.remove(NAME)
time.sleep(1)
c.put(NAME, {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": 1,
    "database.hostname": "mysql", "database.port": 3306, "database.user": "debezium", "database.password": "dbz",
    "database.server.id": 184055,                      # different from inventory-cdc!
    "topic.prefix": "outboxsrv",
    "table.include.list": "inventory.outbox",
    "snapshot.mode": "no_data",                        # don't replay old outbox rows; only new ones
    "tombstones.on.delete": "false",                   # we delete outbox rows -> don't emit tombstones
    "schema.history.internal.kafka.bootstrap.servers": "kafka1:29092,kafka2:29092,kafka3:29092",
    "schema.history.internal.kafka.topic": "_schema-history.outbox",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field": "aggregatetype",          # -> outbox.event.<aggregatetype>
    "transforms.outbox.table.field.event.key": "aggregateid",
    "transforms.outbox.table.fields.additional.placement": "type:header:eventType",
    "transforms.outbox.table.expand.json.payload": "true",
    "transforms.outbox.route.topic.replacement": "outbox.event.${routedByValue}",
})
print_status(c.wait_state(NAME, timeout=90))
time.sleep(3)

banner("Placing 3 orders: business row + outbox row in ONE transaction, then delete the outbox row")
db = mysql()
db.autocommit(False)
for qty in (1, 2, 3):
    with db.cursor() as cur:
        cur.execute("INSERT INTO orders (customer_id, product_id, quantity) VALUES (1, 2, %s)", (qty,))
        oid = cur.lastrowid
        event = {"orderId": oid, "customerId": 1, "productId": 2, "quantity": qty, "status": "NEW"}
        eid = str(uuid.uuid4())
        cur.execute("INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload) VALUES (%s,'Order',%s,'OrderCreated',%s)",
                    (eid, str(oid), json.dumps(event)))
        cur.execute("DELETE FROM outbox WHERE id=%s", (eid,))      # keep the outbox table empty
    db.commit()
    print(f"  committed order {oid} + outbox event")

banner("Consuming topic outbox.event.Order")
cons = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"outbox-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
cons.subscribe(["outbox.event.Order"])
got, idle = 0, 0
while idle < 10 and got < 3:
    m = cons.poll(1)
    if m is None:
        idle += 1
        continue
    if m.error():
        continue
    got += 1
    hdrs = {k: v.decode() for k, v in m.headers() or []}
    print(f"  key={m.key().decode():4s} headers={hdrs} value={m.value().decode()}")
cons.close()
print("""
Only INSERTs into the outbox become events; the DELETE is ignored by the EventRouter.
Consumers get clean domain events, keyed by aggregate id (ordered per order), with exactly the business payload.
Delivery is at-least-once -> use the 'id' header (event id) for de-duplication in consumers.
""")
