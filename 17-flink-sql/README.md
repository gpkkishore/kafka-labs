# 17 — Stream processing with Apache Flink SQL on Kafka

> Goal: run real stream-processing jobs against the lab's Kafka: event-time **windows with watermarks**,
> a **join between a click stream and a CDC table** coming from MySQL via Debezium, continuously updated results
> written back to Kafka with **upsert-kafka**, and **exactly-once** delivery with Kafka transactions.

| Book | Chapter / section |
|------|-------------------|
| Kafka in Action (KiA) | 12 *Stream processing with Kafka Streams and ksqlDB* (concepts: KStream vs KTable, windows); 8.5 Kappa architecture |
| Kafka: The Definitive Guide (DG, 2017) | ch. 11 *Stream Processing*: time, state, stream-table duality, time windows, out-of-sequence events, reprocessing |
| Apache Kafka in Action (AKiA) | ch. 12: 12.2 stream processors, 12.3 stream processing using SQL, 12.4 stream states, 12.5 streaming and time, 12.6 scaling |

## Flink vs Kafka Streams vs ksqlDB

| | Kafka Streams | ksqlDB (folder 14) | **Flink** |
|---|---|---|---|
| What is it | Java **library** inside your app | SQL **server** built on Kafka Streams | separate distributed **cluster** (JobManager + TaskManagers) |
| Sources / sinks | Kafka only | Kafka (+ Connect) | Kafka, files, JDBC, CDC, Iceberg, … |
| State | RocksDB + changelog topics in Kafka | same | RocksDB / heap + **checkpoints** to a file system / S3 |
| Scaling | add app instances (consumer group rebalance) | add servers | change job parallelism, add TaskManagers |
| Exactly-once | `processing.guarantee=exactly_once_v2` | same | checkpoints + transactional Kafka sink (2-phase commit) |
| Best for | microservices that are mostly Kafka-in/Kafka-out | quick SQL over topics | large/complex jobs, many systems, event-time heavy analytics |

## Architecture

```
 01_clickstream_producer.py ──▶ kinaction_flink_clicks (JSON, key=user) ──┐
                                                                          │   Flink session cluster (UI :8085)
 MySQL ─Debezium (16)─▶ cdc.inventory.products (debezium-json) ───────────┤   ┌───────────────────────────────┐
                                                                          └──▶│ 02 TUMBLE 10s per action       │──▶ kinaction_flink_action_counts   (append)
                                                                              │ 03 HOP 30s/10s views/product   │──▶ kinaction_flink_trending        (append)
                                                                              │ 04 clicks ⋈ products → revenue │──▶ kinaction_flink_category_revenue(upsert)
                                                                              │ 05 GROUP BY over CDC changelog │──▶ kinaction_flink_inventory_value (upsert)
                                                                              │ 06 exactly-once copy           │──▶ kinaction_flink_purchases_eos   (transactional)
                                                                              └──────────┬────────────────────┘
                                                                                         └── checkpoints every 10s (offsets + state)
                                                         02_read_results.py / 03_read_committed_vs_uncommitted.py ◀── results
```

## Start

```bash
make up                                                               # Kafka cluster
docker compose -f 16-cdc-debezium-mysql/docker-compose.yml up -d --build   # needed for the CDC jobs (04, 05)
python 16-cdc-debezium-mysql/00_setup_inventory.py
python 16-cdc-debezium-mysql/01_register_debezium_mysql.py            # creates cdc.inventory.products
docker compose -f 17-flink-sql/docker-compose.yml up -d --build       # downloads the Kafka SQL connector jar
open http://localhost:8085                                            # Flink UI
```
Memory: JobManager 640 MB + TaskManager 1 GB (8 slots). On an 8 GB Docker VM you can stop the second Connect worker
(`docker stop connect-2`) while running Flink.

## Run the jobs

```bash
python 17-flink-sql/01_clickstream_producer.py --seconds 0 --late-pct 5 &     # keep events flowing
bash 17-flink-sql/run-sql.sh 02_tumbling_window.sql
bash 17-flink-sql/run-sql.sh 03_hopping_window.sql
bash 17-flink-sql/run-sql.sh 04_cdc_join_revenue.sql
bash 17-flink-sql/run-sql.sh 05_cdc_inventory_value.sql
bash 17-flink-sql/run-sql.sh 06_exactly_once_sink.sql
python 17-flink-sql/02_read_results.py             # window results + upsert tables folded by key
python 16-cdc-debezium-mysql/03_generate_changes.py -n 20   # change prices / stock in MySQL ...
python 17-flink-sql/02_read_results.py             # ... revenue and inventory value change accordingly
python 17-flink-sql/03_read_committed_vs_uncommitted.py      # exactly-once visibility
```
`run-sql.sh` runs `sql-client.sh -i sql/00_init_tables.sql -f sql/<file>`: every `INSERT INTO` becomes a long-running job.
Interactive shell (see `sql/99_interactive_examples.sql` for queries to paste: session windows, Top-N, changelog mode):
```bash
docker compose -f 17-flink-sql/docker-compose.yml run --rm sql-client
```
Cancel jobs in the UI (Running Jobs → Cancel) or `curl -X PATCH 'localhost:8085/jobs/<id>?mode=cancel'`.

## Files

| File | Concept |
|---|---|
| `sql/00_init_tables.sql` | Tables = metadata over topics. Computed `event_time`, **WATERMARK**, Kafka metadata columns, `debezium-json` changelog source, `kafka` vs `upsert-kafka` sinks |
| `sql/02_tumbling_window.sql` | `TUMBLE` window TVF, `COUNT(DISTINCT)` |
| `sql/03_hopping_window.sql` | `HOP` (sliding) windows: one event lands in 3 windows |
| `sql/04_cdc_join_revenue.sql` | regular join stream ⋈ CDC table; retractions when a price changes; temporal join alternative |
| `sql/05_cdc_inventory_value.sql` | aggregation directly over a CDC changelog (+I/-U/+U/-D) |
| `sql/06_exactly_once_sink.sql` | `sink.delivery-guarantee=exactly-once`, transactional id prefix, transaction timeout |
| `sql/99_interactive_examples.sql` | ad-hoc queries: SESSION windows, Top-N, changelog result mode |
| `01_clickstream_producer.py` | event-time timestamps, configurable % of **late** events |
| `02_read_results.py` | read append topics and fold upsert topics into tables (tombstone = delete) |
| `03_read_committed_vs_uncommitted.py` | see transactional output appear per checkpoint |

## Concepts

### Event time, watermarks, late data (AKiA 12.5, DG ch. 11 "Time")
* **Event time** = when it happened (`ts_ms` from the producer). **Processing time** = when Flink sees it.
* `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND` says: *events can be up to 5 s out of order.*
  The watermark is `max(event_time seen) - 5 s`; a window `[t, t+10s)` fires when the watermark passes `t+10s`.
* Events later than that are **dropped** by window aggregations. The producer's `--late-pct` sends events 3–20 s late:
  those 3–5 s late are still counted, older ones are discarded. (Flink UI → job → *Watermarks* tab.)
* Kafka partitions without data hold the watermark back → `table.exec.source.idle-timeout: 10s` (set in `docker-compose.yml`).

### Stream ↔ table duality (KiA 12, DG ch. 11)
* `clicks` is an **append-only stream** (like a KStream).
* `products` from `debezium-json` is a **changelog**: insert `+I`, update `-U/+U`, delete `-D` (like a KTable).
* Aggregating or joining a changelog yields another changelog → write it with **upsert-kafka**: PRIMARY KEY → Kafka key,
  updates → new value for the key, deletes → tombstones. Use a compacted topic and any consumer can rebuild the table.

### Regular vs temporal join (04)
* **Regular join** keeps both inputs in state forever (configure `table.exec.state.ttl` in production). When a product
  price changes, previously joined results are *retracted and recomputed* — revenue reflects the **current** price.
* **Event-time temporal join** `FOR SYSTEM_TIME AS OF c.event_time` uses the product version valid **at purchase time**
  and can drop old versions from state. It needs a watermark on the versioned side too, e.g.
  `ts TIMESTAMP_LTZ(3) METADATA FROM 'value.source.timestamp' VIRTUAL, WATERMARK FOR ts AS ts` on `products`.

### State, checkpoints, exactly-once (AKiA 12.4, 5.2)
* Every 10 s Flink snapshots operator **state + Kafka source offsets** (`execution.checkpointing.interval`) to
  `/tmp/flink-checkpoints` (a Docker volume; S3/HDFS in production). After a failure the job restarts from the last checkpoint.
* Kafka **sink** guarantees: `none` / `at-least-once` (flush on checkpoint, default) / `exactly-once`: write in a Kafka transaction,
  commit when the checkpoint completes. Consumers must use `isolation.level=read_committed`; results appear once per checkpoint.
* Kafka's `transaction.max.timeout.ms` (15 min) must be ≥ the sink's `transaction.timeout.ms` (Flink default 1 h) → set it explicitly.

### Scaling (AKiA 12.6)
`SET 'parallelism.default' = '3';` before an INSERT runs 3 source subtasks — one per Kafka partition. More subtasks than
partitions leaves some idle (same rule as consumer groups). Each job needs task slots: this TaskManager has 8.

## Troubleshooting
* `SQL parse failed. Encountered "views"` — reserved word; rename the column (that's why this lab uses `view_count`).
* Job stuck in `CREATED` — no free task slots; cancel a job or raise `taskmanager.numberOfTaskSlots`.
* Checkpoints stuck *in progress* with `OUT_OF_ORDER_SEQUENCE_NUMBER` warnings in the TaskManager log after a broker
  restart (folder 06 experiments): the idempotent producer inside the sink lost its sequence state. Cancel and resubmit the job.
* `sql-client` errors are logged in the JobManager container: `docker exec flink-jobmanager sh -c 'tail -50 /opt/flink/log/*sql-client*.log'`.

## Exercises
1. Run the producer with `--late-pct 40` and compare `event_count` in `kinaction_flink_action_counts` with what was produced. Change the watermark delay to 30 s: fewer drops, but results arrive 30 s later.
2. Implement the temporal join variant of 04 and compare revenue after a price change.
3. Kill the TaskManager (`docker restart flink-taskmanager`) while jobs run → they restart from the last checkpoint; the exactly-once topic has no duplicates, the at-least-once topics may.
4. Rewrite `02_tumbling_window.sql` in ksqlDB (folder 14) and compare.

## Stop
```bash
docker compose -f 17-flink-sql/docker-compose.yml down -v
```
