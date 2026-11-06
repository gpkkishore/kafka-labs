#!/usr/bin/env python3
"""Ch.4 — what producer errors look like and which ones are retriable.

    python 03-producers/08_errors_and_retries.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import KafkaException, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_errors"
recreate_topic(TOPIC, 1, 3, {"max.message.bytes": 1024})   # topic only accepts records up to ~1KB

p = Producer({"bootstrap.servers": BOOTSTRAP, "delivery.timeout.ms": 10000})


def cb(err, msg):
    if err is None:
        print(f"  OK  {len(msg.value())}B -> offset {msg.offset()}")
    else:
        print(f"  ERR {len(msg.value())}B -> {err.name()}  retriable={err.retriable()}  fatal={err.fatal()}")


banner("1) record bigger than the topic's max.message.bytes (non-retriable, broker rejects)")
p.produce(TOPIC, value=b"x" * 100, on_delivery=cb)
p.flush()
p.produce(TOPIC, value=b"x" * 5000, on_delivery=cb)
p.flush()

banner("1b) a small and a big record in the SAME batch -> both fail")
print("  max.message.bytes is checked against the whole record BATCH, not each record!")
p.produce(TOPIC, value=b"y" * 100, on_delivery=cb)
p.produce(TOPIC, value=b"y" * 5000, on_delivery=cb)
p.flush()

banner("2) record bigger than the producer's own message.max.bytes (fails locally, before sending)")
try:
    Producer({"bootstrap.servers": BOOTSTRAP, "message.max.bytes": 2000}).produce(TOPIC, value=b"x" * 5000)
except KafkaException as e:
    print(f"  raised immediately: {e.args[0].name()}")

banner("3) unknown partition")
try:
    p.produce(TOPIC, value=b"to partition 7", partition=7, on_delivery=cb)
except KafkaException as e:
    print(f"  raised immediately: {e.args[0].name()} (client knows the partition count from metadata)")

print("""
Retriable errors (NOT_LEADER_OR_FOLLOWER, NOT_ENOUGH_REPLICAS, REQUEST_TIMED_OUT, network) are retried
automatically until delivery.timeout.ms expires — you only see them if they never succeed.
See 06-brokers-replication-failover for NOT_ENOUGH_REPLICAS when 2 of 3 brokers are down.
""")
