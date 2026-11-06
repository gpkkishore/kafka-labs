#!/usr/bin/env python3
"""AKiA 5.2 / KiA 2.6.2 — zombie fencing with transactional.id + epoch.

Scenario: instance A hangs (GC pause, network partition). The orchestrator starts instance B with the SAME
transactional.id. B's init_transactions() bumps the epoch; when A wakes up and tries to commit, the
coordinator rejects it -> A cannot write duplicates.

    python 13-transactions-exactly-once/04_zombie_fencing.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import KafkaException, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, ensure_topic, log  # noqa: E402

TOPIC = "tx_fencing"
ensure_topic(TOPIC, 1, 3)
conf = {"bootstrap.servers": BOOTSTRAP, "transactional.id": "fencing-demo", "log_level": 0}

banner("instance A starts a transaction")
a = Producer(conf)
a.init_transactions()
a.begin_transaction()
a.produce(TOPIC, value=b"from A")
a.flush()
log("A wrote a record, then 'freezes' before committing...")

banner("instance B starts with the SAME transactional.id")
b = Producer(conf)
b.init_transactions()     # epoch+1; A's open transaction is aborted by the coordinator
log("B: init_transactions() OK -> epoch bumped, A is now a zombie")
b.begin_transaction()
b.produce(TOPIC, value=b"from B")
b.commit_transaction()
log("B committed")

banner("A wakes up and tries to commit")
try:
    a.commit_transaction()
    log("A committed?! (should not happen)")
except KafkaException as e:
    err = e.args[0]
    log(f"A rejected: {err.name()}  fatal={err.fatal()}  -> {err.str()[:100]}")
print("""
A fatal error means the producer instance must be closed and recreated; it can never write again with
that epoch. This is how Kafka Streams / Flink / Connect avoid duplicates during failovers.
""")
