#!/usr/bin/env python3
"""Ch.3.2 — turn the requirement table from the README into topic configs.

Each topic's settings are a *decision* driven by a requirement (see README table).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.kafka_helpers import describe_topic, recreate_topic, topic_config  # noqa: E402

TOPICS = {
    # audit: must never lose data, kept a long time, ordering per user
    "kinaction_audit":      dict(partitions=3, rf=3, config={"min.insync.replicas": 2, "retention.ms": 30 * 24 * 3600 * 1000}),
    # alerts: per-stage ordering (key = stageId), fast; short retention is fine
    "kinaction_alert":      dict(partitions=3, rf=3, config={"retention.ms": 24 * 3600 * 1000}),
    # trend data: high volume, loss tolerated -> lower durability is OK, compress
    "kinaction_alerttrend": dict(partitions=6, rf=2, config={"compression.type": "lz4"}),
    # latest status per sensor: only the LAST value per key matters -> compaction
    "kinaction_sensor_status": dict(partitions=3, rf=3, config={"cleanup.policy": "compact"}),
}

for name, spec in TOPICS.items():
    recreate_topic(name, **spec)
    describe_topic(name)
    interesting = {k: v for k, v in topic_config(name).items()
                   if k in ("min.insync.replicas", "retention.ms", "cleanup.policy", "compression.type")}
    print(f"  config: {interesting}\n")
