"""Shared constants + MM2 checkpoint decoding (Python port of org.apache.kafka.connect.mirror.Checkpoint)."""
import os
import struct

PRIMARY = os.getenv("BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094")
DR = os.getenv("DR_BOOTSTRAP", "localhost:9292")
TOPIC = "mm2-orders"
REMOTE_TOPIC = f"primary.{TOPIC}"            # DefaultReplicationPolicy: <source alias>.<topic>
GROUP = "mm2-billing"
CHECKPOINTS = "primary.checkpoints.internal"


def _string(buf, pos):
    (n,) = struct.unpack_from(">h", buf, pos)
    pos += 2
    return buf[pos:pos + n].decode(), pos + n


def decode_checkpoint(key: bytes, value: bytes) -> dict:
    """key   = (consumergroup STRING, topic STRING, partition INT32)
       value = header(version INT16) + (upstreamOffset INT64, downstreamOffset INT64, metadata STRING)
       STRING = int16 length + UTF-8 bytes (Kafka protocol 'Struct' encoding)."""
    group, pos = _string(key, 0)
    topic, pos = _string(key, pos)
    (partition,) = struct.unpack_from(">i", key, pos)
    (version,) = struct.unpack_from(">h", value, 0)
    upstream, downstream = struct.unpack_from(">qq", value, 2)
    metadata, _ = _string(value, 18)
    return {"group": group, "topic": topic, "partition": partition, "version": version,
            "upstream_offset": upstream, "downstream_offset": downstream, "metadata": metadata}
