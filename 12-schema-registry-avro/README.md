# 12 — Schema Registry & Avro

| Book | Sections |
|---|---|
| Kafka in Action (KiA) | ch. 11 *Schema registry* — 11.1 maturity model, 11.2 installing, 11.3 REST API & client library, 11.4 compatibility rules, 11.5 alternatives |
| Kafka: The Definitive Guide (DG) | ch. 3 *Serializers* — custom serializers, "Serializing Using Apache Avro", "Using Avro Records with Kafka" |
| Apache Kafka in Action (AKiA) | 13.1 *Schema management* |

> Goal: understand **why** producers and consumers need a contract, how the registry stores it, what goes on the wire,
> and which schema changes are safe.

## KiA's Kafka maturity model (11.1)
| Level | Looks like | Problem |
|---|---|---|
| 0 | Kafka as a pipe, plain strings / ad-hoc JSON | every consumer guesses the format |
| 1 | Producers & consumers agree on formats **in code** (`05_custom_serializer.py`) | changes break readers silently |
| 2 | **Schema registry** — schemas versioned, checked for compatibility | ← this folder |
| 3 | Everything is a stream with schemas: Connect, Streams, ksqlDB all speak Avro/Protobuf | |

## How it works
```
 Producer                         Schema Registry (REST :8081)                       Consumer
   │ AvroSerializer(schema)            │  stores schemas in Kafka topic _schemas         │ AvroDeserializer
   │── POST /subjects/sr_orders-value/versions ──▶ id=2 (cached)                         │
   │                                   │                                                 │
   │ value = 0x00 | 00 00 00 02 | avro-binary ──▶ Kafka topic sr_orders ──▶ read id=2 ──▶ GET /schemas/ids/2 (cached)
```
* **Subject** = name a schema is versioned under. Default *TopicNameStrategy*: `<topic>-key`, `<topic>-value`.
  (RecordNameStrategy / TopicRecordNameStrategy allow several event types per topic.)
* **Schema id** = globally unique; **version** = 1,2,3… per subject.
* Compatibility is checked **when registering** — a bad producer fails fast with HTTP 409 instead of poisoning the topic.

## Run
```bash
make up                                                           # lab cluster
docker compose -f 12-schema-registry-avro/docker-compose.yml up -d   # registry on :8081 (first pull ~1-3 min)

python 12-schema-registry-avro/01_avro_producer.py               # Order v1 (registers schema id 1)
python 12-schema-registry-avro/01_avro_producer.py --version 2   # Order v2: +coupon +status (with defaults)
python 12-schema-registry-avro/02_avro_consumer.py               # each record decoded with ITS writer schema
python 12-schema-registry-avro/02_avro_consumer.py --reader-v1   # "old app": v2 fields are dropped by schema resolution
python 12-schema-registry-avro/03_wire_format.py                 # hex dump: magic byte + schema id + avro body
python 12-schema-registry-avro/04_schema_evolution.py            # compatibility matrix (BACKWARD/FORWARD/FULL/NONE)
python 12-schema-registry-avro/05_custom_serializer.py           # DG ch.3: hand-rolled serializer breaks silently
bash   12-schema-registry-avro/rest_api.sh                       # curl tour of the REST API
```
Also open Kafka UI → *Schema Registry* (http://localhost:8080) and the `sr_orders` topic: the UI decodes Avro for you.

## Compatibility matrix (output of `04_schema_evolution.py`)
| change | BACKWARD | FORWARD | FULL | NONE |
|---|---|---|---|---|
| add field WITH default | ✔ | ✔ | ✔ | ✔ |
| add field WITHOUT default | ✘ | ✔ | ✘ | ✔ |
| remove field WITH default | ✔ | ✔ | ✔ | ✔ |
| remove field WITHOUT default | ✔ | ✘ | ✘ | ✔ |
| change type double→string | ✘ | ✘ | ✘ | ✔ |
| narrow type double→float | ✘ | ✔ | ✘ | ✔ |

| Mode | Guarantee | Deploy order |
|---|---|---|
| BACKWARD (default) | new schema reads old data | upgrade **consumers** first |
| FORWARD | old schema reads new data | upgrade **producers** first |
| FULL | both | any order |
| `*_TRANSITIVE` | against *all* versions, not only the latest | needed when consumers may replay very old data |

## Avro vs JSON Schema vs Protobuf (KiA 11.5)
The registry supports all three (`JSONSerializer`, `ProtobufSerializer` in `confluent_kafka.schema_registry`).
* **Avro** — compact binary, schema needed to read, great evolution rules, Kafka ecosystem default.
* **Protobuf** — compact, code generation, field numbers make evolution easy; popular with gRPC shops.
* **JSON Schema** — human readable payloads, larger messages, weaker evolution semantics.
* **Alternatives to a registry**: shared schema library in git, schema in headers, or self-describing formats — all push the burden to every team.

## Things to try
1. Register an incompatible schema with curl (see `rest_api.sh`) → `{"error_code":409,...}`.
2. `PUT /config/sr_orders-value {"compatibility":"NONE"}`, produce a v3 with `amount` as string, then run `02_avro_consumer.py --reader-v1` → deserialization error. That's what the default protects you from.
3. Look at the `_schemas` topic in Kafka UI: it's compacted, key = subject+version. Delete the registry container and start it again — all schemas come back from Kafka.

## Clean up
```bash
docker compose -f 12-schema-registry-avro/docker-compose.yml down
```
