#!/usr/bin/env python3
"""Ch.5.2 — subscribe() (group management, automatic partition assignment + rebalancing)
vs assign() (you pick partitions; no group coordinator involvement, no rebalancing).

    python 04-consumers/05_assign_vs_subscribe.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner  # noqa: E402

TOPIC = "kinaction_orders"

banner("assign(): read ONLY partition 2 from offset 0")
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "assign-demo", "enable.auto.commit": False})
c.assign([TopicPartition(TOPIC, 2, 0)])
for _ in range(5):
    m = c.poll(3)
    if m and not m.error():
        print(f"  p{m.partition()}@{m.offset()} {m.value().decode()}")
c.close()

print("""
Use assign() when:
  * you need a specific partition (tools, replays, a 'standby' that rebuilds state)
  * an external system decides the work split (Flink, Spark, Kafka Connect tasks do this)
Use subscribe() when:
  * you want Kafka to split partitions across N instances and handle failures (05-consumer-groups-rebalancing)
group.id is still needed with assign() if you want to COMMIT offsets, but there are no rebalances.
""")
