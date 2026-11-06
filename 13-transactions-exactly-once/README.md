# 13 — Transactions & exactly-once semantics (EOS)

| Book | Sections |
|---|---|
| Kafka in Action (KiA) | 2.6.2 *What exactly-once means*; 4.2 idempotent producer; 12.1 Streams `processing.guarantee` |
| Apache Kafka in Action (AKiA) | 5.2 *Transactions*; 5.1 acknowledgments |
| Kafka: The Definitive Guide (DG, 2017) | ch. 6 *Reliable Data Delivery* (producer retries ⇒ duplicates, consumer commit strategies) |

> Goal: see with your own eyes how Kafka turns at-least-once into exactly-once **inside Kafka**:
> idempotent producer → transactions → `read_committed` consumers → offsets committed in the transaction.

## The building blocks
```
 transactional.id ──▶ Transaction Coordinator (a broker; state in __transaction_state)
                         PID + epoch  (epoch bump = fencing of zombies)
 begin_transaction()
   produce(topicA,p0) ─┐
   produce(topicB,p2) ─┼─▶ records written to the logs immediately, flagged isTransactional
   send_offsets_to_transaction(consumer offsets) ─▶ __consumer_offsets (also part of the tx)
 commit_transaction() ─▶ coordinator writes PREPARE_COMMIT, then COMMIT *control records* (markers)
                         into every partition touched  ─▶ read_committed consumers may now read up to them
```
* **Idempotent producer** (03-producers/05) removes duplicates caused by *retries*.
* **Transactions** make writes to many partitions + the consumer offsets **atomic**.
* **read_committed** consumers only read up to the *Last Stable Offset* (LSO) and skip aborted records.
* Exactly-once is **end-to-end only inside Kafka**. Side effects to external systems (DB, e-mail) need
  idempotent writes or the outbox pattern (see 16-cdc-debezium-mysql).

## Run (lab cluster up: `make up`)
```bash
python 13-transactions-exactly-once/01_transactional_producer.py        # commit, abort, multi-topic commit
python 13-transactions-exactly-once/02_read_committed_vs_uncommitted.py # 7 records vs 4 records, offset gaps
bash   13-transactions-exactly-once/05_dump_control_records.sh          # COMMIT/ABORT markers inside the segment

python 13-transactions-exactly-once/03_exactly_once_pipeline.py --seed 100
python 13-transactions-exactly-once/03_exactly_once_pipeline.py --crash-after 37   # dies mid-transaction
python 13-transactions-exactly-once/03_exactly_once_pipeline.py                    # restart (waits ~45s, see below)
python 13-transactions-exactly-once/03_exactly_once_pipeline.py --verify

python 13-transactions-exactly-once/04_zombie_fencing.py
```

### What you should see
`02_read_committed_vs_uncommitted.py`
```
isolation.level=read_uncommitted: 7 records   offsets 0,1,2,4,5,6,8   (aborted 3..5 visible!)
isolation.level=read_committed:   4 records   offsets 0,1,2,8
```
Offsets 3, 7, 9 are **control records** (`endTxnMarker: COMMIT/ABORT` in the dump) — they consume an offset but are never returned to apps.

`03_exactly_once_pipeline.py --verify` after a crash:
```
tx_orders_enriched as read_committed     input records: 100  output records: 100  duplicates: 0
tx_orders_enriched as read_uncommitted   input records: 100  output records: 107  duplicates: 7
```
The 7 records of the crashed transaction are physically in the log, but aborted: an EOS reader never sees them.
The restart takes ~45 s: the crashed instance is still a group member until `session.timeout.ms` (45 s) expires
(see 05-consumer-groups-rebalancing). `init_transactions()` on restart aborts the dangling transaction immediately.

`04_zombie_fencing.py`: instance B with the same `transactional.id` bumps the epoch; A's commit fails with
`_FENCED` (fatal). In Java this is `ProducerFencedException`.

## Configs that matter
| Where | Config | Value for EOS |
|---|---|---|
| producer | `transactional.id` | stable & unique **per instance** (e.g. `app-<partition>` or pod ordinal) |
| producer | `transaction.timeout.ms` | default 60 s; coordinator aborts transactions older than this |
| consumer | `isolation.level` | `read_committed` (default is `read_uncommitted`!) |
| consumer | `enable.auto.commit` | `false` — offsets are committed through the producer |
| broker | `transaction.state.log.replication.factor` / `min.isr` | 3 / 2 (set in `infra/docker-compose.yml`) |
| Kafka Streams | `processing.guarantee` | `exactly_once_v2` (does all of the above for you) |

## CLI
```bash
bin/kafka-cli kafka-transactions --bootstrap-server kafka1:29092 list
bin/kafka-cli kafka-transactions --bootstrap-server kafka1:29092 describe --transactional-id payments-service-1
bin/kafka-cli kafka-transactions --bootstrap-server kafka1:29092 find-hanging --broker-id 1   # stuck transactions block LSO
```

## Costs & caveats
* Each transaction adds coordinator round trips + markers → batch many records per transaction (100s–1000s).
* A hanging open transaction stops `read_committed` consumers at the LSO → consumer lag grows (watch it in Grafana).
* Transactions don't span clusters (MirrorMaker 2 is at-least-once) and don't cover external systems.
