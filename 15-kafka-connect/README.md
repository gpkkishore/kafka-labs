# 15 — Kafka Connect: integrating external systems

> Goal: move data **into** and **out of** Kafka without writing producers or consumers, and understand how Connect
> runs as a fault-tolerant cluster: workers, connectors, tasks, converters, SMTs, offsets and dead letter queues.

| Book | Chapter / section |
|------|-------------------|
| Kafka in Action (KiA) | 2.4.2 Kafka Connect, 3.1.2 "A first change" (file connector), 8.3 tools |
| Kafka: The Definitive Guide (DG, 2017) | ch. 7 *Building Data Pipelines*: when to use Connect vs. producer/consumer, Running Connect, File Source/Sink, MySQL → Elasticsearch, *A Deeper Look at Connect* |
| Apache Kafka in Action (AKiA) | ch. 11: 11.1 What is Connect, 11.2 distributed mode, 11.3 scalability & fault tolerance, 11.4 worker config, 11.5 REST API, 11.6 connector config, 11.7 SMTs, 11.8 JDBC source, 11.9 CDC (→ folder 16) |

## Architecture of this lab

```
                 ┌──────────────── Connect cluster  (group.id = kafka-labs-connect) ───────────────┐
  MySQL shop ───▶│ worker "connect"   :8083  ── JMX :7072 ──▶ Prometheus                            │──▶ Postgres warehouse
  data/*.txt ───▶│ worker "connect-2" :8084                                                          │──▶ data/*.txt
                 │   connectors (logical jobs) ──split into──▶ tasks (threads doing the work)        │
                 └──────┬───────────────────────────────────────────────────────────────────────────┘
                        │ stores ALL its state in Kafka:
                        ├─ _connect_configs  (connector + task configs, compacted, 1 partition)
                        ├─ _connect_offsets  (source offsets: file position, last id/timestamp, binlog pos)
                        └─ _connect_status   (RUNNING / PAUSED / FAILED per connector & task)
```

* **Worker**: a JVM process. Workers with the same `group.id` form a cluster and share the work. Stateless → any worker can run any task.
* **Connector**: the config of a job ("copy table X to topic Y") + logic that splits it into tasks (`tasks.max`).
* **Task**: does the actual copying. Source task = poll the system → produce. Sink task = consume → write the system (a sink is just a consumer group `connect-<name>`).
* **Converter**: bytes ⇄ Connect's internal records (`JsonConverter` with/without schemas, `StringConverter`, Avro/Protobuf with Schema Registry).
* **SMT** (single message transform): small per-record change between connector and converter.

### When Connect, when your own code? (DG ch. 7)
Use Connect when the system already has a connector (databases, S3, Elasticsearch, files, …) — you get offsets, retries, scaling, REST management and monitoring for free.
Write producers/consumers when *you own the application* that creates/uses the data.

## Images & ports

The image is built from `connect-image/Dockerfile`: **Debezium Connect 2.7.3** (Kafka 3.7, all Debezium connectors incl. the JDBC sink)
\+ FileStream connectors + **Confluent JDBC connector 10.7.6** with MySQL & Postgres drivers + Prometheus JMX agent.
The same image is used by `16-cdc-debezium-mysql`.

| Service | Host port | Notes |
|---|---|---|
| `connect` | 8083 | REST API of worker 1; Prometheus scrapes `connect:7072` |
| `connect-2` | 8084 | REST API of worker 2 (same cluster) |
| `mysql` | 3306 | `root/rootpw`, `app/apppw`; db `shop`; binlog ROW + GTID (CDC-ready); `debezium/dbz` user |
| `postgres` | **5433** | `postgres/postgres`; db `warehouse` (5433 because 5432 is often used locally) |

## Start / stop

```bash
make up                                                        # core Kafka cluster first (network kafka-labs)
docker compose -f 15-kafka-connect/docker-compose.yml up -d --build   # first build downloads ~40 MB of jars
curl -s localhost:8083/ | jq                                   # wait until it answers (30-60s: plugin scanning)
...
docker compose -f 15-kafka-connect/docker-compose.yml down -v  # stop + delete MySQL/Postgres data
```
Memory: two workers (512 MB heap each) + MySQL + Postgres ≈ 1.8 GB.

## Scripts (run from the repo root)

| # | Script | Book § | What you see |
|---|---|---|---|
| — | `connect_api.py` | AKiA 11.5 | Python client: one method per REST endpoint (read it as API docs) |
| 1 | `01_rest_api_tour.py` | AKiA 11.5, DG "Running Connect" | plugins, **validate**, create (PUT = idempotent), tasks, pause/resume, restart, topics, **offsets**, stop + reset offsets (KIP-875), delete |
| 2 | `02_file_source_sink.py` | DG 7 "File Source and File Sink", KiA 3.1.2 | `input.txt → topic → output.txt`, append lines live |
| 3 | `03_jdbc_source_mysql.py` | AKiA 11.8, DG "MySQL to Elasticsearch" | `incrementing` vs `timestamp+incrementing` modes; JSON-with-schema envelope; **why DELETEs are invisible**; DECIMAL→base64 gotcha |
| 4 | `04_single_message_transforms.py` | AKiA 11.7 | chain of 9 SMTs: ValueToKey, ExtractField, MaskField, ReplaceField, Cast, TimestampConverter, InsertField ×2, RegexRouter |
| 5 | `05_jdbc_sink_postgres.py` | DG 7, AKiA 11.6 | sink with `upsert` + `auto.create` → table appears in Postgres, updates overwrite rows |
| 6 | `06_error_handling_dlq.py` | DG 7 "Failure handling" | poison message kills a task (`errors.tolerance=none`) vs **DLQ** with error headers |
| 7 | `07_fault_tolerance_rebalance.py` | AKiA 11.2/11.3 | `docker stop connect-2` → its tasks move to `connect` after `scheduled.rebalance.max.delay.ms`; start it → work is rebalanced back |

Run in order 1 → 7 (5 needs the topic created by 3).

## Worker configuration (AKiA 11.4)

Set through env vars in `docker-compose.yml` (the Debezium image maps `CONNECT_FOO_BAR` → `foo.bar`):

| Setting | Value here | Why it matters |
|---|---|---|
| `group.id` | `kafka-labs-connect` | same id = same Connect cluster |
| `config/offset/status.storage.topic` | `_connect_*` | all cluster state lives in Kafka; RF 3 |
| `key/value.converter` | `JsonConverter` (schemas on) | default for connectors that don't override it |
| `offset.flush.interval.ms` | 5000 | how often source offsets / sink commits are written |
| `scheduled.rebalance.max.delay.ms` | 15000 | how long to wait for a lost worker before moving its tasks (**default 5 min**) |
| `connector.client.config.override.policy` | `All` | allows per-connector `producer.override.*` / `consumer.override.*` |

## Connector configuration essentials (AKiA 11.6)

```jsonc
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
  "tasks.max": "1",                         // upper bound; connector decides the real number
  "key.converter": "...", "value.converter": "...",   // override the worker defaults
  "transforms": "a,b", "transforms.a.type": "...",    // SMT chain (order matters)
  "errors.tolerance": "all",                 // none (default) | all
  "errors.deadletterqueue.topic.name": "...",// sinks only
  "producer.override.compression.type": "lz4"         // per-connector client settings
}
```

Useful curl one-liners:
```bash
curl -s localhost:8083/connectors?expand=status | jq                       # everything
curl -s localhost:8083/connectors/jdbc-customers/status | jq               # where do tasks run?
curl -s -X POST 'localhost:8083/connectors/jdbc-customers/restart?includeTasks=true&onlyFailed=true'
curl -s localhost:8083/connectors/jdbc-customers/offsets | jq              # source offset
bin/kafka-cli kafka-console-consumer --bootstrap-server kafka1:29092 --topic _connect_offsets \
  --from-beginning --property print.key=true --timeout-ms 5000            # raw offsets
```

## Monitoring
Prometheus job `kafka-connect` scrapes `connect:7072`. Try in http://localhost:9090:
* `kafka_connect_connector_status` / `kafka_connect_task_status` — 1 per connector/task with a `status` label
* `kafka_connect_source_task_source_record_poll_rate`, `kafka_connect_sink_task_sink_record_send_rate`
* `kafka_connect_task_error_total_record_errors`, `kafka_connect_task_error_deadletterqueue_produce_requests`
* `kafka_connect_worker_rebalance_*` — rebalances in the Connect cluster

## Exercises
1. Delete the `Cast` SMT lines in `03_...` and look at the `amount` field (base64 Decimal). Then try `"value.converter": "io.confluent.connect.avro.AvroConverter"` (needs Schema Registry, folder 12).
2. In `05_...`, change `insert.mode` to `insert` and update a MySQL row twice → duplicate key errors → task FAILED → fix → `restart?onlyFailed=true`.
3. Set `tasks.max=3` on the file sink in `02_...` — how many tasks actually run? (topic has 1 partition)
4. Run `07_...` with `scheduled.rebalance.max.delay.ms` removed (default 300000) — tasks stay UNASSIGNED for 5 minutes. Why is that a sensible default for rolling restarts?

Next: **16-cdc-debezium-mysql** — capture every INSERT/UPDATE/DELETE from the MySQL binlog instead of polling.
