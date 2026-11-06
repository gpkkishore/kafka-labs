#!/usr/bin/env python3
"""DG ch.5 "File format", AKiA 8.3 — parse a Kafka .log segment BY HAND (record batch format v2, magic=2).

The on-disk format == the wire format: brokers store exactly the bytes the producer sent (that's why zero-copy works).

    python 07-topics-partitions-internals/03_read_segment_in_python.py
"""
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_rawformat"
recreate_topic(TOPIC, 1, 3)
p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "compression.type": "none", "enable.idempotence": True})
p.produce(TOPIC, key=b"user-1", value=b"hello", headers=[("trace", b"abc")])
p.produce(TOPIC, key=b"user-2", value=b"world")
p.produce(TOPIC, key=None, value=b"no key here")
p.flush()
p.produce(TOPIC, key=b"user-1", value=b"second batch")
p.flush()

tmp = Path(tempfile.mkdtemp()) / "seg.log"
subprocess.run(["docker", "cp", f"kafka1:/var/lib/kafka/data/{TOPIC}-0/00000000000000000000.log", str(tmp)], check=True)
data = tmp.read_bytes()


def varint(buf, pos):
    """zig-zag varint (protobuf style) used inside records"""
    shift = result = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            break
        shift += 7
    return (result >> 1) ^ -(result & 1), pos


banner(f"{len(data)} bytes in {TOPIC}-0/00000000000000000000.log")
pos = 0
HEADER = ">qiiBIhiqqqhii"   # baseOffset batchLength partitionLeaderEpoch magic crc attributes lastOffsetDelta
#                            baseTimestamp maxTimestamp producerId producerEpoch baseSequence recordsCount
while pos + 61 <= len(data):
    (base_off, batch_len, leader_epoch, magic, crc, attrs, last_delta, base_ts, max_ts,
     pid, pepoch, base_seq, count) = struct.unpack_from(HEADER, data, pos)
    print(f"\nRECORD BATCH @byte {pos}: baseOffset={base_off} batchLength={batch_len} magic={magic} "
          f"leaderEpoch={leader_epoch} crc={crc:#010x}")
    print(f"  attributes={attrs:#06x} (compression={attrs & 7}, timestampType={'LogAppend' if attrs & 8 else 'Create'}, "
          f"transactional={bool(attrs & 16)}, control={bool(attrs & 32)})")
    print(f"  producerId={pid} producerEpoch={pepoch} baseSequence={base_seq} records={count} baseTimestamp={base_ts}")
    rpos = pos + 61
    for _ in range(count):
        length, rpos = varint(data, rpos)
        end = rpos + length
        rpos += 1                                      # record attributes (unused)
        ts_delta, rpos = varint(data, rpos)
        off_delta, rpos = varint(data, rpos)
        klen, rpos = varint(data, rpos)
        key = None if klen < 0 else data[rpos:rpos + klen]
        rpos += max(klen, 0)
        vlen, rpos = varint(data, rpos)
        val = data[rpos:rpos + vlen]
        rpos += vlen
        nh, rpos = varint(data, rpos)
        hdrs = []
        for _ in range(nh):
            hk, rpos = varint(data, rpos)
            hkey = data[rpos:rpos + hk].decode()
            rpos += hk
            hv, rpos = varint(data, rpos)
            hdrs.append((hkey, data[rpos:rpos + hv]))
            rpos += hv
        print(f"    record: offset={base_off + off_delta} (delta {off_delta}) tsDelta={ts_delta} key={key} value={val} headers={hdrs}")
        rpos = end
    pos += 12 + batch_len                              # baseOffset(8) + batchLength(4) + rest
print("""
Notes
 * Offsets/timestamps inside a batch are DELTAS from the batch header -> tiny varints.
 * With compression, everything after recordsCount is one compressed blob (the broker doesn't recompress).
 * producerId/baseSequence = idempotence; 'transactional'/'control' bits = transactions (folder 13).
""")
