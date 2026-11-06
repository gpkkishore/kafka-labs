#!/usr/bin/env python3
"""One member of a consumer group. Start several (separate terminals, or run_scenario.sh) and watch
partitions being assigned / revoked as members join, leave gracefully, or crash.

    python 05-consumer-groups-rebalancing/consumer_worker.py --name A
    python 05-consumer-groups-rebalancing/consumer_worker.py --name B --strategy cooperative-sticky
    python 05-consumer-groups-rebalancing/consumer_worker.py --name C --static      # static membership
    python 05-consumer-groups-rebalancing/consumer_worker.py --name D --slow-ms 2000 --max-poll-interval-ms 10000

Ctrl+C / SIGTERM  -> graceful: close() sends LeaveGroup -> rebalance starts IMMEDIATELY
kill -9           -> crash: nobody tells the coordinator; it waits session.timeout.ms before rebalancing
"""
import argparse
import os
import signal
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, BOLD, RESET, color_for, log  # noqa: E402

TOPIC = "kinaction_rebalance"
ap = argparse.ArgumentParser()
ap.add_argument("--name", required=True)
ap.add_argument("--group", default="kinaction_rebalance_group")
ap.add_argument("--strategy", default="range", choices=["range", "roundrobin", "cooperative-sticky"])
ap.add_argument("--static", action="store_true", help="static membership: group.instance.id = name")
ap.add_argument("--session-timeout-ms", type=int, default=10000)
ap.add_argument("--max-poll-interval-ms", type=int, default=300000)
ap.add_argument("--slow-ms", type=int, default=0, help="sleep per record (simulate slow processing)")
ap.add_argument("--verbose", action="store_true", help="print every record")
ap.add_argument("--pid-dir", default=None, help="write <name>.pid here (used by run_scenario.sh)")
args = ap.parse_args()

if args.pid_dir:
    Path(args.pid_dir).mkdir(parents=True, exist_ok=True)
    Path(args.pid_dir, f"{args.name}.pid").write_text(str(os.getpid()))

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": args.group,
    "client.id": f"worker-{args.name}",
    "auto.offset.reset": "latest",
    "partition.assignment.strategy": args.strategy,
    "session.timeout.ms": args.session_timeout_ms,     # 10s here (default 45s) so crash demos are quick
    "heartbeat.interval.ms": 2000,
    "max.poll.interval.ms": args.max_poll_interval_ms,
}
if args.static:
    conf["group.instance.id"] = f"instance-{args.name}"   # KIP-345: restart within session timeout => NO rebalance
c = Consumer(conf)

owned: set[int] = set()
fmt = lambda ps: "[" + ",".join(f"{color_for(p)}{p}{RESET}" for p in sorted(ps)) + "]"  # noqa: E731
started = time.time()


def on_assign(consumer, partitions):
    new = {p.partition for p in partitions}
    # EAGER protocols (range, roundrobin): every rebalance revokes EVERYTHING, then hands out a full new set.
    # COOPERATIVE (cooperative-sticky): only the partitions that actually move are revoked/assigned (incremental).
    proto = "COOPERATIVE" if args.strategy == "cooperative-sticky" else "EAGER"
    if proto == "EAGER":
        owned.clear()             # eager: every rebalance starts from zero
    owned.update(new)
    if not new and proto == "COOPERATIVE":
        log(f"{args.name} (cooperative round: nothing new for me, still own {fmt(owned)})")
        return
    log(f"{BOLD}{args.name}{RESET} ASSIGNED   +{fmt(new)} -> now owns {fmt(owned)}  ({proto})")


def on_revoke(consumer, partitions):
    gone = {p.partition for p in partitions}
    owned.difference_update(gone)
    log(f"{BOLD}{args.name}{RESET} REVOKED    -{fmt(gone)} -> now owns {fmt(owned)}   "
        f"(stop processing these, commit offsets NOW)")
    try:
        consumer.commit(asynchronous=False)
    except Exception:  # noqa: BLE001
        pass


def on_lost(consumer, partitions):
    gone = {p.partition for p in partitions}
    owned.difference_update(gone)
    log(f"{BOLD}{args.name}{RESET} LOST       -{fmt(gone)}  (we were kicked out: too slow or session expired; "
        f"another member may already own them — DON'T commit)")


running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
log(f"{BOLD}{args.name}{RESET} joining group '{args.group}' strategy={args.strategy} static={args.static} "
    f"session.timeout.ms={args.session_timeout_ms} pid={os.getpid()}")
counts: Counter = Counter()
last_report = time.time()
while running:
    m = c.poll(0.5)
    if m is not None and not m.error():
        counts[m.partition()] += 1
        if args.verbose:
            log(f"{args.name} {color_for(m.partition())}p{m.partition()}@{m.offset()}{RESET} {m.key().decode()}")
        if args.slow_ms:
            time.sleep(args.slow_ms / 1000)
    if time.time() - last_report >= 5:
        last_report = time.time()
        per = " ".join(f"{color_for(p)}p{p}{RESET}:{n}" for p, n in sorted(counts.items()))
        log(f"{args.name} owns {fmt(owned)} | processed last 5s: {per or '-'}")
        counts.clear()

log(f"{BOLD}{args.name}{RESET} leaving: close() -> commit + LeaveGroup")
c.close()
log(f"{args.name} bye after {time.time() - started:.0f}s")
