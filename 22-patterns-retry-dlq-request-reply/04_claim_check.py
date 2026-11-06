#!/usr/bin/env python3
"""Claim-check: large payloads go to an object store; Kafka carries only a small REFERENCE.

Kafka is optimized for many small records (default max ~1 MB: message.max.bytes / max.request.size).
Big blobs (images, PDFs, 50 MB JSON exports) bloat page cache, replication and consumer memory.
Here the "object store" is a local directory (in real life: S3/GCS/Azure Blob).

    python 22-patterns-retry-dlq-request-reply/04_claim_check.py
"""
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, KafkaException, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log, recreate_topic  # noqa: E402

TOPIC = "patterns.documents"
STORE = Path(__file__).resolve().parent / ".object-store"     # git-ignored
STORE.mkdir(exist_ok=True)
recreate_topic(TOPIC, 3, 3)
p = Producer({"bootstrap.servers": BOOTSTRAP})
blob = os.urandom(5 * 1024 * 1024)                              # a 5 MB "PDF"

banner("1) naive: put 5 MB directly into Kafka")
try:
    p.produce(TOPIC, key="doc-naive", value=blob)
    p.flush()
except KafkaException as e:
    print(f"  rejected: {e.args[0].name()} (limit ~1 MB unless you raise message.max.bytes everywhere)")

banner("2) claim check: store blob, send reference")
digest = hashlib.sha256(blob).hexdigest()
obj = STORE / f"{digest}.bin"
obj.write_bytes(blob)                                          # 1. upload first ...
ref = {"doc_id": "doc-1", "uri": obj.as_uri(), "sha256": digest, "size": len(blob), "content_type": "application/pdf"}
p.produce(TOPIC, key="doc-1", value=json.dumps(ref))            # 2. ... then publish the claim check
p.flush()
log(f"produced reference ({len(json.dumps(ref))} bytes instead of {len(blob):,})")

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"docs-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
c.subscribe([TOPIC])
while True:
    m = c.poll(5)
    if m and not m.error():
        break
c.close()
r = json.loads(m.value())
data = Path(r["uri"].removeprefix("file://")).read_bytes()      # 3. consumer redeems the claim check
ok = hashlib.sha256(data).hexdigest() == r["sha256"]
log(f"consumer fetched {len(data):,} bytes from {r['uri']} — checksum {'OK' if ok else 'MISMATCH'}")
print("""
Rules: upload BEFORE producing (never reference a missing object); make blobs immutable (content hash as name);
lifecycle/retention of the store must be >= topic retention; consider encryption + access control on the store.
""")
