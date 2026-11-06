#!/usr/bin/env python3
"""DG ch.7 "Connector Example: File Source and File Sink" / KiA ch.2.4.2 — the classic first pipeline:

     data/input.txt ──FileStreamSource──▶ topic kinaction_connect_file ──FileStreamSink──▶ data/output.txt

    python 15-kafka-connect/02_file_source_sink.py

Then keep appending lines yourself and watch them flow:
    echo "hello again" >> 15-kafka-connect/data/input.txt ; cat 15-kafka-connect/data/output.txt
"""
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from confluent_kafka import Consumer  # noqa: E402
from connect_api import Connect, print_status  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_connect_file"
c = Connect()
for n in ("file-source", "file-sink"):
    c.remove(n)    # stop -> DELETE /offsets -> delete, so a re-run re-reads input.txt from byte 0
recreate_topic(TOPIC, 1, 3)
inp, out = HERE / "data/input.txt", HERE / "data/output.txt"
inp.write_text("".join(f"log line {i}: user-{i % 3} did something\n" for i in range(5)))
out.unlink(missing_ok=True)

banner("Creating file-source and file-sink")
c.put("file-source", {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "tasks.max": 1, "file": "/data/input.txt", "topic": TOPIC,
    # plain strings instead of the worker default JsonConverter-with-schema
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
})
c.put("file-sink", {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": 1, "file": "/data/output.txt", "topics": TOPIC,
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
})
print_status(c.wait_state("file-source"))
print_status(c.wait_state("file-sink"))

with inp.open("a") as f:                      # append while it's running: tailed like `tail -f`
    f.write("appended while running\n")
time.sleep(8)

banner("What is in the topic (key = null, value = the line)")
cons = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"peek-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
cons.subscribe([TOPIC])
seen = 0
for _ in range(10):
    m = cons.poll(1)
    if m and not m.error():
        seen += 1
        print(f"  offset {m.offset()}: key={m.key()} value={m.value().decode()}")
cons.close()

banner("data/output.txt written by the sink")
print(out.read_text() if out.exists() else "  (not yet written — wait a few seconds and cat it)")
print("""The sink is just a CONSUMER GROUP named connect-file-sink:
    bin/groups --describe --group connect-file-sink
The source stores 'how far into the file' in _connect_offsets — delete & recreate the connector
with the same name and it continues where it stopped instead of re-reading the file.""")
