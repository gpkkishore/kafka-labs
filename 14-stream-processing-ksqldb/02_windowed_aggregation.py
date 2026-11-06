#!/usr/bin/env python3
"""Stateful, windowed aggregation on EVENT time, with a grace period for late data.

  tumbling window  10s          : [00-10) [10-20) [20-30) ...      every event in exactly ONE window
  hopping window   20s every 10s: [00-20) [10-30) [20-40) ...      every event in TWO windows

"Stream time" = the highest event timestamp seen so far (it only moves forward).
A window [start, end) is CLOSED and emitted once  stream_time >= end + grace.
Events for a closed window are LATE -> dropped (and counted) — same semantics as Kafka Streams
`TimeWindows.ofSizeAndGrace(...)` + `suppress(untilWindowCloses)`.

    python 14-stream-processing-ksqldb/02_windowed_aggregation.py --seconds 90 --grace 10
"""
import argparse
import json
from collections import defaultdict

from confluent_kafka import Consumer, Producer

from sp_common import BOOTSTRAP, COLORS, PAGEVIEWS, RESET, Runner, banner, dumps, ensure_topic, fmt_ts, log

ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=float, default=90)
ap.add_argument("--size", type=int, default=10, help="tumbling window size (s)")
ap.add_argument("--hop-size", type=int, default=20)
ap.add_argument("--hop-advance", type=int, default=10)
ap.add_argument("--grace", type=int, default=10, help="seconds a window stays open for late events")
args = ap.parse_args()
OUT = "sp_pageviews_per_page_windowed"
ensure_topic(OUT, 3, 3)


def tumbling(ts, size):
    start = ts - ts % size
    return [(start, start + size)]


def hopping(ts, size, adv):
    first = ts - ts % adv - size + adv
    return [(s, s + size) for s in range(first, ts + 1, adv) if s <= ts < s + size]


S, H, A, G = args.size * 1000, args.hop_size * 1000, args.hop_advance * 1000, args.grace * 1000
state = {"tumbling": defaultdict(lambda: defaultdict(int)), "hopping": defaultdict(lambda: defaultdict(int))}
closed = {"tumbling": set(), "hopping": set()}
stream_time, late_dropped, accepted_late = 0, 0, 0

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "sp-windows", "auto.offset.reset": "latest"})
p = Producer({"bootstrap.servers": BOOTSTRAP})
c.subscribe([PAGEVIEWS])
banner(f"tumbling {args.size}s + hopping {args.hop_size}s/{args.hop_advance}s, grace {args.grace}s")
r = Runner(args.seconds)
while r.running():
    m = c.poll(0.5)
    if m is not None and not m.error():
        e = json.loads(m.value())
        ts = e["ts"]
        is_late = ts < stream_time
        stream_time = max(stream_time, ts)
        dropped = False
        for kind, wins in (("tumbling", tumbling(ts, S)), ("hopping", hopping(ts, H, A))):
            for w in wins:
                if w in closed[kind] or w[1] + G <= stream_time:
                    if kind == "tumbling":
                        dropped = True
                        late_dropped += 1
                        log(f"{COLORS[5]}DROPPED late event{RESET} ts={fmt_ts(ts)} for closed window "
                            f"[{fmt_ts(w[0])}-{fmt_ts(w[1])}) stream_time={fmt_ts(stream_time)}")
                    continue
                state[kind][w][e["page"]] += 1
        if is_late and not dropped:
            accepted_late += 1        # out-of-order, but its window was still open (within grace)
    # emit windows whose end + grace has passed
    for kind in ("tumbling", "hopping"):
        for w in sorted(state[kind]):
            if w[1] + G <= stream_time:
                counts = dict(sorted(state[kind].pop(w).items()))
                closed[kind].add(w)
                total = sum(counts.values())
                col = COLORS[0] if kind == "tumbling" else COLORS[2]
                log(f"{col}{kind:8s} window [{fmt_ts(w[0])} - {fmt_ts(w[1])}) FINAL total={total:3d}{RESET} {counts}")
                for page, cnt in counts.items():
                    p.produce(OUT, key=f"{kind}|{page}|{w[0]}", value=dumps(
                        {"window": kind, "start": w[0], "end": w[1], "page": page, "views": cnt}))
    p.poll(0)
p.flush()
c.close()
log(f"late-but-within-grace events counted: {accepted_late}; too-late events dropped: {late_dropped}")
