# 02 — Designing a Kafka project (Book chapter 3)

> Goal: practise the *questions* you ask before writing any Kafka code, and see how each answer turns into a
> concrete **topic / key / producer setting**. Uses the book's factory "sensor events" scenario.

## The design questions (ch. 3.2.3) → decisions

| Question | Factory answer | Kafka decision (see `create_topics.py`) |
|---|---|---|
| Is it OK to lose messages? | Audit: **no**. Trend data: yes, a few. | audit: `acks=all`, `rf=3`, `min.insync.replicas=2`, idempotence. trend: `acks=1`, `rf=2` |
| Do we need ordering? | Per assembly *stage* yes; globally no. | **key = stage_id** → one partition per stage → ordered. Global order would need 1 partition (no scaling). |
| Do we need old values or only the latest? | Dashboard needs only the *current* status per sensor. | `cleanup.policy=compact`, key = sensor_id |
| How long to keep data? | Audit 30 days, alerts 1 day | `retention.ms` per topic |
| How much volume? | Trend = high | more partitions (6), `compression.type=lz4`, `linger.ms` batching |
| How do we evolve the data format? | Fields will be added later | `schema_version` field now → Avro + Schema Registry in `12-schema-registry-avro` |
| Will others consume it? | Yes, multiple teams | Don't delete on read; each team = own **consumer group** |

### Choosing a key — the most important decision
* Records with the same key always go to the same partition (as long as the partition count doesn't change!).
* **Hot keys** = hot partitions. If one stage produced 90% of the traffic, one consumer would do 90% of the work.
* Adding partitions later *re-maps keys* (`hash % N` changes) → breaks per-key ordering for in-flight data. Over-provision partitions up front.

### Event vs command vs state (ch. 3.3)
* **Event**: "temperature was 93.2C at 10:01" — immutable fact. Good default.
* **State**: "sensor-3 status = OK" — use compacted topics.
* **Command**: "shut down line 2" — possible but couples services; prefer events.

## Run it
```bash
python 02-designing-events/create_topics.py          # creates 4 topics with the decided configs, prints them
python 02-designing-events/sensor_producer.py --seconds 20
python 02-designing-events/alert_consumer.py          # proves: each stage lives in exactly ONE partition
python 02-designing-events/status_table.py            # rebuilds latest-status table from the compacted topic
```
Then in Grafana compare *Bytes in / sec* of `kinaction_alerttrend` (lz4) and `kinaction_sensor_status` (uncompressed).

## Exercise
1. Change the trend key from `stage` to `None` in `sensor_producer.py` → re-run `alert_consumer.py`: trend events of a stage now spread over many partitions — no more ordering per stage.
2. Increase `kinaction_alert` to 6 partitions (`bin/topics --alter --topic kinaction_alert --partitions 6`) and produce again: some stages move to a new partition. Why is that dangerous for ordering?
