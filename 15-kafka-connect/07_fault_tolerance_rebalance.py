#!/usr/bin/env python3
"""AKiA 11.2 / 11.3 "Distributed mode", "Scalability and fault tolerance".

Two workers (connect, connect-2) share group.id=kafka-labs-connect => ONE Connect cluster.
Connectors/tasks are spread across workers by the same group protocol consumers use
(incremental cooperative rebalancing, KIP-415). We create a sink with 3 tasks, stop one worker,
and watch its tasks move to the survivor after scheduled.rebalance.max.delay.ms (15s here, 5 min by default).

    python 15-kafka-connect/07_fault_tolerance_rebalance.py
"""
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from connect_api import Connect  # noqa: E402

from common.kafka_helpers import banner, ensure_topic, log  # noqa: E402

TOPIC = "kinaction_connect_ft"
ensure_topic(TOPIC, 3, 3)
c = Connect()
NAME = "ft-sink"
c.delete(NAME)
time.sleep(1)
c.put(NAME, {"connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector", "topics": TOPIC,
             "tasks.max": 3, "file": "/data/ft-sink.txt",
             "value.converter": "org.apache.kafka.connect.storage.StringConverter"})
c.wait_state(NAME, min_tasks=3)


def cluster_counts():
    """Tasks per worker across ALL connectors (the rebalancer balances the whole cluster, not one connector)."""
    counts = {}
    for v in c.list().values():
        for t in v["status"]["tasks"]:
            if t["state"] == "RUNNING":
                w = t["worker_id"].split(":")[0]
                counts[w] = counts.get(w, 0) + 1
    return counts


def show():
    st = c.status(NAME)
    tasks = ", ".join(f"t{t['id']}={t['state'][0]}@{t['worker_id'].split(':')[0]}" for t in sorted(st["tasks"], key=lambda t: t["id"]))
    log(f"{NAME}: connector@{st['connector']['worker_id'].split(':')[0]}  {tasks}   | cluster tasks/worker {cluster_counts()}")
    return st


banner("Initial placement (R=RUNNING, U=UNASSIGNED)")
show()

banner("docker stop connect-2  -> its tasks are lost; watch them move after ~15s")
subprocess.run(["docker", "stop", "connect-2"], capture_output=True)
for _ in range(15):
    time.sleep(3)
    try:
        st = show()
    except Exception as e:  # noqa: BLE001 - REST may briefly be busy during rebalance
        log(f"(rest busy: {e})")
        continue
    if all(t["worker_id"].startswith("connect:") and t["state"] == "RUNNING" for t in st["tasks"]):
        break

banner("docker start connect-2  -> after its JVM starts (~30-60s, it scans many plugins) it rejoins and gets work back")
subprocess.run(["docker", "start", "connect-2"], capture_output=True)
for _ in range(40):
    time.sleep(4)
    try:
        show()
    except Exception as e:  # noqa: BLE001
        log(f"(rest busy: {e})")
        continue
    if cluster_counts().get("connect-2"):
        time.sleep(4)
        show()
        break
c.delete(NAME)
print("""
Notes:
 * Connector + task configs live in _connect_configs, so ANY worker can run ANY task.
 * Source offsets live in _connect_offsets; sink offsets are normal consumer-group offsets (group connect-ft-sink).
 * On rejoin, tasks are moved back only as needed to BALANCE the whole cluster (possibly tasks of other
   connectors), and only the moved ones stop — the others keep running (incremental cooperative rebalancing).
 * The REST API of either worker controls the whole cluster (requests are forwarded to the leader worker).
""")
