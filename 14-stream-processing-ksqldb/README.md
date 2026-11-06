# 14 — Stream processing: Python processors, Kafka Streams concepts, ksqlDB

| Book | Sections |
|---|---|
| Kafka in Action (KiA) | ch. 12 — 12.1 Kafka Streams (KStream, KTable, GlobalKTable, Processor API, setup), 12.2 ksqlDB, 12.3 going further |
| Kafka: The Definitive Guide (DG, 2017) | ch. 11 *Stream Processing* — time, state, stream-table duality, windows, design patterns, Streams architecture & scaling |
| Apache Kafka in Action (AKiA) | ch. 12 — 12.2 stream processors, 12.3 SQL, 12.4 state, 12.5 time, 12.6 scaling Kafka Streams |

> Kafka Streams is a **Java** library. This folder rebuilds its core ideas in small Python programs so you can
> read every line, then shows the same things declaratively with **ksqlDB** (which *is* Kafka Streams under the hood).

## Concepts in one page

| Concept | Meaning | Where in this folder |
|---|---|---|
| **KStream** | unbounded sequence of independent events (inserts) | `sp_pageviews` |
| **KTable** | changelog → latest value per key (upserts; `null` = delete). Partitioned: an instance only holds its partitions | `sp_users` |
| **GlobalKTable** | full copy of a table on *every* instance → join on any key, no co-partitioning | `03_stream_table_join.py` |
| **Stateless op** | filter, map, flatMap, branch — record by record | `01_stateless_filter_map.py` |
| **Stateful op** | aggregate, count, join, windowing — needs a *state store* | `02`, `03`, `04` |
| **State store** | local key-value store (RocksDB) **+ changelog topic** in Kafka for recovery | `04_state_store_changelog.py` |
| **Event time vs processing time** | when it happened (`value.ts`) vs when we saw it | `02_windowed_aggregation.py` |
| **Windows** | tumbling (fixed, non-overlapping), hopping (fixed, overlapping), sliding, session (gap-based) | `02`, `ksqldb/queries.sql` |
| **Grace period / late data** | how long a window accepts out-of-order events before it is final | `02 --grace` |
| **Stream-table duality** | a table is the aggregation of a stream; a stream is the changelog of a table | `04` (changelog = stream of table updates) |
| **Repartitioning** | `selectKey()`/`groupBy` on a new key → data written to an internal `-repartition` topic | `01` re-keys to page |
| **Processor API** | low level: `process(record)`, punctuators, direct store access — what our Python loops look like | all |
| **Scaling** | tasks = input partitions; run more instances with the same `application.id` (= group.id) | run 2 copies of `01` |
| **Exactly-once** | `processing.guarantee=exactly_once_v2` = transactions + offsets in tx (folder 13) | `04` |

```
                        ┌──────────── one stream-processing instance ────────────┐
 sp_pageviews ─────────▶│ source → filter → map ─────────────────────────────────│──▶ sp_purchases          (01)
 (3 partitions          │ source → window(10s, grace) → count → [store] ─────────│──▶ sp_pageviews_per_page_windowed (02)
  = 3 tasks)            │ source → join ◀── table(sp_users, global) ─────────────│──▶ sp_pageviews_enriched (03)
                        │ source → aggregate → [store] ⇄ changelog topic ────────│──▶ sp-user-activity-store-changelog (04)
                        └────────────────────────────────────────────────────────┘
```

## Run the Python processors
Terminal 1 – input data (keeps running):
```bash
python 14-stream-processing-ksqldb/00_clickstream_producer.py --reset --seconds 600 --rate 20
```
Terminal 2..5 – one processor each:
```bash
python 14-stream-processing-ksqldb/01_stateless_filter_map.py      # only purchases, re-keyed by page
python 14-stream-processing-ksqldb/02_windowed_aggregation.py --grace 10
python 14-stream-processing-ksqldb/03_stream_table_join.py         # watch "TABLE UPDATE" change later joins
python 14-stream-processing-ksqldb/04_state_store_changelog.py --seconds 30   # then run it AGAIN
```

What to look for:
* **02**: each tumbling window is printed once as `FINAL` *grace* seconds after it ends. Events that arrive later
  (the producer makes ~10 % of events 5–40 s late) show up as `DROPPED late event`. Try `--grace 45`: nothing is dropped,
  but results are emitted 45 s later — the classic **latency vs completeness** trade-off.
  Hopping windows count each event twice (size 20 s / advance 10 s).
* **03**: the table is bootstrapped from the compacted topic first, then kept up to date; a user moving country changes
  all *later* enrichments only (temporal join).
* **04**: the second run prints `restored 8 keys from N changelog records` and continues counting where it stopped.
  Offsets and changelog writes commit in one transaction, so state and position never diverge. Kafka Streams does the
  same with RocksDB + `<app>-<store>-changelog` topics, plus *standby replicas* (`num.standby.replicas`) to avoid long restores.
* **Scaling**: start a second copy of `01_stateless_filter_map.py` — the 3 partitions are split between the copies
  (consumer group rebalance, folder 05). Stateful apps move their state with the partitions (restore from changelog).

## ksqlDB
```bash
docker compose -f 14-stream-processing-ksqldb/ksqldb/docker-compose.yml up -d     # server :8088 (+ CLI container)
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
ksql> RUN SCRIPT '/ksql/queries.sql';
ksql> SELECT * FROM purchases EMIT CHANGES;                    -- push query (Ctrl+C)
ksql> SELECT * FROM user_activity WHERE user_id = 'u1';        -- pull query against the materialized table
ksql> SELECT * FROM views_per_page_30s WHERE page = '/cart';   -- windowed pull query
ksql> SHOW QUERIES;  EXPLAIN <query_id>;                       -- the Kafka Streams topology behind the SQL
bash 14-stream-processing-ksqldb/ksqldb/rest_examples.sh       # same via the REST API
```
> **Partitioner gotcha (found while building this lab):** ksqlDB pull queries by key (`WHERE user_id='u1'`) compute the
> partition with Java's murmur2. If the topic was written by a librdkafka client with its default partitioner
> (`consistent_random`), the key lives in a *different* partition and the pull query returns **no rows** even though
> `SELECT * FROM user_activity;` shows it. `00_clickstream_producer.py` therefore sets `partitioner=murmur2_random`.
> Same root cause as `03-producers/02_keys_and_partitioning.py`; it also breaks co-partitioned joins in Kafka Streams.

`queries.sql` covers: STREAM and TABLE over existing topics, filter/projection (persistent query → new topic),
stream-table LEFT JOIN, tumbling window with grace period, hopping window, non-windowed aggregate (materialized view)
for pull queries. Every persistent query is a Kafka Streams app: look for `_confluent-ksql-kafka_labs_*` internal
`-changelog` / `-repartition` topics in Kafka UI.

Pick the tool:
| Need | Use |
|---|---|
| simple per-record transforms, any language | plain consumer/producer (this folder's 01) |
| stateful JVM microservice, full control | **Kafka Streams** |
| SQL, quick materialized views, no code | **ksqlDB** |
| large state, complex event time, batch+stream, many sources | **Flink** (folder 17) |

## Clean up
```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088 -f /ksql/drop_all.sql   # drop streams/tables + their topics
docker compose -f 14-stream-processing-ksqldb/ksqldb/docker-compose.yml down
```
