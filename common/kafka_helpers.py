"""Small helpers shared by all lab scripts.

Every script does:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from common.kafka_helpers import ...
so you can run it from any directory:  python 03-producers/01_basic_producer.py
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

# The three brokers from infra/docker-compose.yml (EXTERNAL listeners).
# Listing all three is only for *bootstrap*: the client fetches full cluster metadata from
# whichever one answers first, then talks directly to each partition leader.
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")

# ---- pretty printing -------------------------------------------------------
COLORS = ["\033[36m", "\033[33m", "\033[35m", "\033[32m", "\033[34m", "\033[31m", "\033[96m", "\033[93m"]
RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"


def color_for(i: int) -> str:
    return COLORS[i % len(COLORS)]


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(msg: str, prefix: str = "") -> None:
    print(f"{DIM}{ts()}{RESET} {prefix}{msg}", flush=True)


def banner(title: str) -> None:
    print(f"\n{BOLD}{'=' * 78}\n {title}\n{'=' * 78}{RESET}", flush=True)


# ---- admin helpers ---------------------------------------------------------
def admin(**extra) -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP, **extra})


def _delete_and_wait(a: AdminClient, name: str) -> None:
    if name not in a.list_topics(timeout=10).topics:
        return
    for _, f in a.delete_topics([name], operation_timeout=30).items():
        try:
            f.result()
        except KafkaException:
            pass
    # deletion is async on the brokers; wait until metadata forgets the topic
    for _ in range(60):
        if name not in a.list_topics(timeout=10).topics:
            return
        time.sleep(0.5)


def recreate_topic(name: str, partitions: int = 3, rf: int = 3, config: dict | None = None,
                   client: AdminClient | None = None) -> None:
    """Delete (if present) and create a topic with EXACTLY these settings, waiting until it is usable.

    Guards against a classic race: after a delete, any client asking for metadata of the topic (a lingering
    producer, Kafka UI, ...) can make the broker AUTO-CREATE it again with the broker defaults
    (num.partitions=3) before our create_topics() call lands. We verify the partition count and retry.
    """
    a = client or admin()
    cfg = {k: str(v) for k, v in (config or {}).items()}
    for attempt in range(5):
        _delete_and_wait(a, name)
        try:
            a.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=rf, config=cfg)])[name].result()
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise
            time.sleep(1)
            continue                      # someone re-created it (auto-create) -> delete again
        if _wait_ready(a, name, partitions):
            log(f"created topic {BOLD}{name}{RESET} partitions={partitions} rf={rf} config={config or {}}")
            return
    raise RuntimeError(f"could not recreate topic {name}")


def _wait_ready(a: AdminClient, name: str, partitions: int) -> bool:
    for _ in range(60):
        md = a.list_topics(name, timeout=10).topics.get(name)
        if md and md.error is None and len(md.partitions) == partitions and \
                all(p.leader >= 0 for p in md.partitions.values()):
            return True
        time.sleep(0.5)
    return False


def ensure_topic(name: str, partitions: int = 3, rf: int = 3, config: dict | None = None,
                 client: AdminClient | None = None) -> None:
    """Create the topic if it doesn't exist yet (idempotent)."""
    a = client or admin()
    if name in a.list_topics(timeout=10).topics:
        return
    fs = a.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=rf,
                                   config={k: str(v) for k, v in (config or {}).items()})])
    for t, f in fs.items():
        try:
            f.result()
            log(f"created topic {BOLD}{t}{RESET} partitions={partitions} rf={rf} config={config or {}}")
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise
    # wait until every partition has a leader
    for _ in range(60):
        md = a.list_topics(name, timeout=10).topics.get(name)
        if md and md.error is None and len(md.partitions) == partitions and \
                all(p.leader >= 0 for p in md.partitions.values()):
            return
        time.sleep(0.5)


def describe_topic(name: str, client: AdminClient | None = None) -> None:
    """Print leader / replicas / ISR for every partition — like `kafka-topics --describe`."""
    a = client or admin()
    md = a.list_topics(name, timeout=10)
    t = md.topics[name]
    print(f"{BOLD}Topic {name}{RESET}  partitions={len(t.partitions)}")
    for pid in sorted(t.partitions):
        p = t.partitions[pid]
        isr_note = "" if set(p.isrs) == set(p.replicas) else f"  {COLORS[5]}<- under-replicated{RESET}"
        print(f"  partition {pid}: leader={p.leader} replicas={p.replicas} isr={p.isrs}{isr_note}")


def topic_config(name: str, client: AdminClient | None = None) -> dict:
    a = client or admin()
    res = ConfigResource(ConfigResource.Type.TOPIC, name)
    cfg = a.describe_configs([res])[res].result()
    return {k: v.value for k, v in cfg.items()}


def die(msg: str) -> None:
    print(f"{COLORS[5]}{msg}{RESET}", file=sys.stderr)
    sys.exit(1)
