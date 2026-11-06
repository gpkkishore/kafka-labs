#!/usr/bin/env python3
"""KiA 9.1.1 / DG ch.9 "Administering Kafka" — everything kafka-topics/kafka-configs/kafka-consumer-groups do, in code.

    python 09-admin-tools/01_admin_client_tour.py
"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import (ConsumerGroupTopicPartitions, Consumer, Producer,  # noqa: E402
                             TopicPartition)
from confluent_kafka.admin import (AlterConfigOpType, ConfigEntry, ConfigResource, ConfigSource,  # noqa: E402
                                   NewPartitions, OffsetSpec)

from common.kafka_helpers import BOOTSTRAP, admin, banner, describe_topic, recreate_topic  # noqa: E402

a = admin()
T = "kinaction_admin"

banner("1. cluster: brokers, controller, cluster id")
cl = a.describe_cluster(include_authorized_operations=False).result()
print(f"  cluster_id={cl.cluster_id} controller={cl.controller.id if cl.controller else '?'}")
for n in cl.nodes:
    print(f"  broker {n.id} {n.host}:{n.port} rack={n.rack}")

banner("2. create topic with configs")
recreate_topic(T, 3, 3, {"retention.ms": 3600000, "min.insync.replicas": 2}, client=a)
describe_topic(T, a)

banner("3. alter topic config INCREMENTALLY (only touches what you name)")
res = ConfigResource(ConfigResource.Type.TOPIC, T, incremental_configs=[
    ConfigEntry("retention.ms", "7200000", incremental_operation=AlterConfigOpType.SET),
    ConfigEntry("cleanup.policy", "compact,delete", incremental_operation=AlterConfigOpType.SET)])
a.incremental_alter_configs([res])[res].result()
cfg = a.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, T)])
for r, f in cfg.items():
    for k, v in sorted(f.result().items()):
        if not v.is_default:
            print(f"  {k} = {v.value}   (source={ConfigSource(v.source).name})")

banner("4. broker config (read-only peek) — dynamic configs can be changed without restart")
b = ConfigResource(ConfigResource.Type.BROKER, "1")
for k, v in sorted(a.describe_configs([b])[b].result().items()):
    if k in ("num.io.threads", "num.network.threads", "log.retention.hours", "default.replication.factor",
             "min.insync.replicas", "auto.create.topics.enable", "broker.rack"):
        print(f"  {k} = {v.value}  read_only={v.is_read_only}")

banner("5. add partitions (3 -> 6). NOTE: existing keys may map to new partitions!")
a.create_partitions([NewPartitions(T, 6)])[T].result()
describe_topic(T, a)

banner("6. produce, consume with a group, then inspect the group + lag + list offsets")
p = Producer({"bootstrap.servers": BOOTSTRAP})
for i in range(60):
    p.produce(T, key=str(i), value=f"v{i}")
p.flush()
g = f"admin-demo-{uuid.uuid4().hex[:4]}"
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": g, "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe([T])
got = 0
while got < 30:
    m = c.poll(2)
    if m and not m.error():
        got += 1
c.commit(asynchronous=False)
c.close()
print(f"  groups: {[x.group_id for x in a.list_consumer_groups().result().valid if x.group_id.startswith('admin-demo')]}")
committed = a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g)])[g].result().topic_partitions
ends = a.list_offsets({TopicPartition(T, tp.partition): OffsetSpec.latest() for tp in committed})
for tp in sorted(committed, key=lambda x: x.partition):
    end = ends[TopicPartition(T, tp.partition)].result().offset
    print(f"  p{tp.partition}: committed={tp.offset} end={end} lag={end - tp.offset if tp.offset >= 0 else 'n/a'}")

banner("7. reset the group's offsets to the beginning (group must be EMPTY)")
a.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(g, [TopicPartition(T, tp.partition, 0) for tp in committed])])[g].result()
print("  done -> next run of that group re-reads everything")

banner("8. delete the group and the topic")
a.delete_consumer_groups([g])[g].result()
a.delete_topics([T])[T].result()
print("  deleted")
