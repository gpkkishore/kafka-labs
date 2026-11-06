#!/usr/bin/env python3
"""Ch.4.2 — idempotent producer: no duplicates from retries, ordering kept with up to 5 in-flight requests.

How it works:
  * On start the producer gets a ProducerId (PID) + epoch from the broker.
  * Every batch carries (PID, epoch, partition, baseSequence).
  * If a retry re-sends a batch the leader already wrote, the broker sees the same sequence number
    and drops the duplicate (still acks it). Out-of-order sequence => error instead of reordering.

This script produces with idempotence ON and then shows you the PID/sequence stored in the log
segment itself using kafka-dump-log.

    python 03-producers/05_idempotence_and_ordering.py
"""
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner, recreate_topic  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "kinaction_idempotent"
recreate_topic(TOPIC, 1, 3)

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,      # implies acks=all, retries=MAX, max.in.flight<=5
    "linger.ms": 10,
})
for i in range(20):
    p.produce(TOPIC, key=f"k{i % 3}", value=f"v{i}")
    if i % 7 == 6:
        p.flush()                    # force separate batches => separate baseSequence values
p.flush()

leader = admin().list_topics(TOPIC).topics[TOPIC].partitions[0].leader
banner(f"Batches in {TOPIC}-0 on leader kafka{leader} (look at producerId / baseSequence)")
cmd = [str(ROOT / "bin/kafka-cli"), "kafka-dump-log", "--files",
       f"/var/lib/kafka/data/{TOPIC}-0/00000000000000000000.log"]
env = {"BROKER": f"kafka{leader}", "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"}
out = subprocess.run(cmd, capture_output=True, text=True, env=env).stdout
for line in out.splitlines():
    if line.startswith("baseOffset"):
        f = dict(re.findall(r"(\w+): (\S+)", line))
        print(f"  baseOffset={f['baseOffset']:>3s} lastOffset={f['lastOffset']:>3s} producerId={f['producerId']} "
              f"producerEpoch={f['producerEpoch']} baseSequence={f['baseSequence']} lastSequence={f['lastSequence']} "
              f"isTransactional={f['isTransactional']}")
print("""
Sequences are contiguous per (producerId, partition). A non-idempotent producer writes producerId: -1.
Try: set enable.idempotence False above and re-run — producerId becomes -1, baseSequence -1.
""")
