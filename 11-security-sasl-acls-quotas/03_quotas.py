#!/usr/bin/env python3
"""KiA 10.5 / AKiA 13.3 — quotas: protect the cluster from a noisy client.

The broker doesn't reject over-quota requests; it DELAYS the response (throttle_time_ms) so the
client's average rate drops to the quota. Clients see it as latency, and in metrics as throttle time.

    python 11-security-sasl-acls-quotas/03_quotas.py     (needs 02_acls.py to have granted alice WRITE)
"""
import json
import subprocess
import time

from confluent_kafka import Producer

from sec_common import banner

TOPIC = "sec_orders"
CLI = ["docker", "exec", "kafka-secure", "kafka-configs", "--bootstrap-server", "kafka-secure:29192",
       "--command-config", "/etc/kafka/lab/admin.properties"]
payload = b"x" * 10_000


def run_cli(*args):
    out = subprocess.run(CLI + list(args), capture_output=True, text=True)
    print("  $ kafka-configs", " ".join(args), "\n   ", (out.stdout or out.stderr).strip())


def blast(label, seconds=10):
    """Produce as fast as possible for N seconds; print ACKED MB per second + broker throttle time."""
    stats, acked = {}, [0]
    from sec_common import client_conf
    p = Producer(client_conf("alice", **{"statistics.interval.ms": 1000, "linger.ms": 5,
                                         "queue.buffering.max.kbytes": 512,   # small buffer => backpressure visible
                                         "stats_cb": lambda s: stats.update(json.loads(s))}))

    def cb(err, msg):
        if not err:
            acked[0] += len(msg.value())

    print(f"  {label}")
    start = last = time.time()
    last_bytes = 0
    while time.time() - start < seconds:
        try:
            p.produce(TOPIC, value=payload, on_delivery=cb)
        except BufferError:
            p.poll(0.05)
        p.poll(0)
        if time.time() - last >= 1:
            thr = max((b.get("throttle", {}).get("max", 0) for b in stats.get("brokers", {}).values()), default=0)
            print(f"    t={time.time() - start:4.1f}s  acked {(acked[0] - last_bytes) / 1e6:6.2f} MB/s   broker throttle max={thr} ms")
            last, last_bytes = time.time(), acked[0]
    p.purge()            # drop whatever is still queued locally
    p.flush(5)
    print(f"  => average {acked[0] / 1e6 / seconds:.2f} MB/s")


banner("1) quota producer_byte_rate=512KB/s for User:alice")
run_cli("--alter", "--entity-type", "users", "--entity-name", "alice", "--add-config", "producer_byte_rate=524288")
run_cli("--describe", "--entity-type", "users", "--entity-name", "alice")
time.sleep(2)
blast("alice, 512 KB/s quota", seconds=15)

banner("2) remove the quota -> full speed again")
run_cli("--alter", "--entity-type", "users", "--entity-name", "alice", "--delete-config", "producer_byte_rate")
time.sleep(2)
blast("alice, unlimited", seconds=4)
print("""
NOTE: the broker measures the rate over a sliding window (quota.window.num=11 x 1s). Run phase 2 BEFORE
phase 1 and alice would be throttled for minutes: the window still contains the 80+ MB/s burst.
""")
print("""
Quota types: producer_byte_rate, consumer_byte_rate (network bandwidth), request_percentage (CPU time of
I/O + network threads), controller_mutation_rate (topic creations/deletions).
Entities: user, client-id, user+client-id, and defaults (--entity-default).
Watch kafka_server_*throttle* metrics / client-side 'produce-throttle-time-avg' to detect throttled apps.
""")
