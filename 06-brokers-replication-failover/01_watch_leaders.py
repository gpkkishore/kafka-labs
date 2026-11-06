#!/usr/bin/env python3
"""Live leader / replicas / ISR table for a topic. Changes are highlighted.

    python 06-brokers-replication-failover/01_watch_leaders.py            # creates the topic if needed
    python 06-brokers-replication-failover/01_watch_leaders.py --once
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.kafka_helpers import BOLD, RESET, admin, ensure_topic, ts  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="kinaction_replication")
ap.add_argument("--once", action="store_true")
args = ap.parse_args()
a = admin()
ensure_topic(args.topic, 6, 3, {"min.insync.replicas": 2}, a)
prev = {}
RED, GREEN, YEL = "\033[31m", "\033[32m", "\033[33m"
while True:
    try:
        md = a.list_topics(args.topic, timeout=5)
        brokers = sorted(md.brokers)
        t = md.topics[args.topic]
        lines = [f"{BOLD}{ts()} topic={args.topic} live brokers={brokers}{RESET}",
                 f"  {'part':4s} {'leader':>6s}  {'replicas':12s} {'isr':12s}"]
        for pid in sorted(t.partitions):
            p = t.partitions[pid]
            cur = (p.leader, tuple(p.isrs))
            changed = pid in prev and prev[pid] != cur
            isr_col = GREEN if len(p.isrs) == len(p.replicas) else (YEL if len(p.isrs) >= 2 else RED)
            lead = f"{RED}NONE{RESET}" if p.leader < 0 else str(p.leader)
            pref = "" if p.leader == p.replicas[0] else f" {YEL}(not preferred leader {p.replicas[0]}){RESET}"
            lines.append(f"  {pid:<4d} {lead:>6s}  {str(p.replicas):12s} {isr_col}{str(p.isrs):12s}{RESET}"
                         f"{' <- CHANGED' if changed else ''}{pref}")
            prev[pid] = cur
        out = "\n".join(lines)
    except Exception as e:  # noqa: BLE001
        out = f"{ts()} {e}"
    if args.once:
        print(out)
        break
    print("\033[2J\033[H" + out, flush=True)
    time.sleep(2)
