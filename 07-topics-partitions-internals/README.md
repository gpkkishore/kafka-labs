# 07 — Topics, partitions & Kafka internals (KiA ch. 7 · DG ch. 5 "Kafka Internals" · AKiA ch. 4 & 8)

> Goal: open the box. See the **files** a partition is made of, the **record batch bytes**, the sparse **indexes**,
> where **consumer offsets** and **cluster metadata** physically live, and how **replicas are placed**.

## Scripts
| # | Script | Book § | What you see |
|---|--------|--------|--------------|
| 1 | `01_partition_on_disk.sh` | KiA 7.2.1, DG "Physical storage" | `<topic>-<partition>/` dirs, `.log/.index/.timeindex`, checkpoint files |
| 2 | `02_segments_and_indexes.sh` | DG "File management", "Indexes"; AKiA 8.3 | segments roll at `segment.bytes`; `kafka-dump-log` of batches, offset index, time index |
| 3 | `03_read_segment_in_python.py` | DG "File format" | **parse a segment file byte-by-byte** in Python (record batch v2, varints, headers, producerId) |
| 4 | `04_consumer_offsets_internals.py` | KiA 5.3.1, DG ch. 4 | `hash(group) % 50` → coordinator; raw commit records in `__consumer_offsets` |
| 5 | `05_kraft_cluster_metadata.sh` | AKiA 7.1, KiA 6.2 | `__cluster_metadata` Raft log: TOPIC_RECORD, PARTITION_RECORD, REGISTER_BROKER_RECORD... |
| 6 | `06_partition_placement.py` | DG "Partition allocation" | replica spread across brokers & racks, preferred leaders |

Run them in order from the repo root (cluster up).

## The big picture

```
Topic "orders"  (logical)
 ├── partition 0  ── replicas on brokers [1,2,3]  (leader 1)
 │     on disk: /var/lib/kafka/data/orders-0/
 │        00000000000000000000.log        ← closed segment   (records with offsets 0..16)
 │        00000000000000000000.index      ← sparse: relative offset → byte position
 │        00000000000000000000.timeindex  ← sparse: timestamp → offset
 │        00000000000000000017.log        ← ACTIVE segment (only one being appended to)
 │        ...
 ├── partition 1 ...
 └── partition 2 ...
```

### Record batch v2 (what `03_read_segment_in_python.py` decodes)
```
baseOffset int64 | batchLength int32 | partitionLeaderEpoch int32 | magic int8 (=2) | crc uint32 |
attributes int16 (compression, timestampType, isTransactional, isControl) | lastOffsetDelta int32 |
baseTimestamp int64 | maxTimestamp int64 | producerId int64 | producerEpoch int16 | baseSequence int32 |
recordsCount int32 | records...  (each: length, attributes, timestampDelta, offsetDelta, key, value, headers — varints)
```
The broker writes the producer's batch to disk **unchanged** (no recompression, no re-encoding) and serves it to
consumers with `sendfile()` — the core of Kafka's performance.

### Topic configs you should know (KiA 7.1.1)
| Config | Meaning |
|---|---|
| `partitions` | parallelism + ordering unit; can be **increased** but never decreased (and increasing remaps keys) |
| `replication.factor` | copies of each partition (set at creation; change via reassignment) |
| `min.insync.replicas` | min ISR size for `acks=all` writes |
| `segment.bytes` / `segment.ms` | when the active segment rolls (1 GB / 7 days default) |
| `retention.ms` / `retention.bytes` | delete closed segments older/bigger than this (folder 08) |
| `cleanup.policy` | `delete`, `compact` or `compact,delete` |
| `index.interval.bytes` | how sparse the offset index is (4096) |
| `message.timestamp.type` | CreateTime / LogAppendTime |
| `max.message.bytes` | largest record **batch** the topic accepts |

```bash
bin/topics --describe --topic kinaction_segments
bin/kafka-cli kafka-configs --bootstrap-server kafka1:29092 --entity-type topics --entity-name kinaction_segments --describe --all
bin/kafka-cli kafka-log-dirs --bootstrap-server kafka1:29092 --describe --topic-list kinaction_segments   # size per replica per broker
```

### Internal topics
| Topic | Purpose |
|---|---|
| `__consumer_offsets` (50 partitions, compacted) | committed offsets + group metadata |
| `__transaction_state` (50 partitions, compacted) | transaction coordinator state (folder 13) |
| `__cluster_metadata` (KRaft, 1 partition, on controllers) | the whole cluster metadata as an event log |

## Testing with an embedded cluster (KiA 7.3)
The book uses `EmbeddedKafkaCluster` / Testcontainers in Java — the Python equivalent is in `18-testing-testcontainers`.
