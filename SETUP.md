# Setup guide

## 1. Prerequisites

| Tool | Version | Check |
|---|---|---|
| Docker Desktop / Docker Engine + Compose v2 | Compose ≥ 2.20 (for `up --wait` and `include:`) | `docker compose version` |
| Docker memory | **≥ 6 GB** (8 GB recommended — see the memory table below) | Docker Desktop → Settings → Resources |
| Python | 3.10+ | `python3 --version` |
| make, curl | any | |
| OS | macOS (Intel/Apple Silicon) or Linux; Windows via WSL2 | |

Free ports used on the host: `9092-9094` (brokers), `8080` (Kafka UI), `9090` (Prometheus), `3000` (Grafana), `9308` (kafka-exporter);
optional stacks use `8081 8082 8083 8084 8085 8088 3306 5433 6379 9192 9292 9392 9393 2181`.

## 2. First run

```bash
git clone <your-repo-url> kafka-labs && cd kafka-labs
make venv                     # .venv with confluent-kafka, requests, redis, PyMySQL, pytest, testcontainers, ...
source .venv/bin/activate
make up                       # pulls images the first time (~2-3 GB), waits until 3 brokers are healthy
make smoke                    # produce 5 + consume them
```
Open:
* **Kafka UI** http://localhost:8080 — topics, messages, partitions, consumer groups, (Schema Registry / Connect when running)
* **Grafana** http://localhost:3000 — no login; dashboards *Kafka Labs — Cluster Overview* and *Kafka Labs — Python clients*
* **Prometheus** http://localhost:9090 — *Status → Targets*, *Alerts*

Stop: `make down` (core) · `make extras-down` (optional stacks) · `make clean` (everything + volumes).

## 3. What `make up` starts (`infra/docker-compose.yml`)

| Container | Image | Purpose |
|---|---|---|
| `kafka1`, `kafka2`, `kafka3` | `confluentinc/cp-kafka:7.7.1` (Apache Kafka 3.7) | KRaft **combined** mode (each node = broker + controller), racks a/b/c, RF=3, `min.insync.replicas=2` |
| `jmx-agent-download` | `curlimages/curl` | one-shot: downloads the Prometheus JMX java agent into a shared volume |
| `kafka-exporter` | `danielqsj/kafka-exporter:v1.8.0` | consumer-group lag, offsets, ISR as Prometheus metrics |
| `prometheus` | `prom/prometheus:v2.55.1` | scrapes brokers (`:7071`), kafka-exporter, Connect, Python clients; alert rules |
| `grafana` | `grafana/grafana:11.3.0` | provisioned datasource + dashboards (anonymous admin) |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | web UI |

Lab-friendly broker settings (don't copy to production): `group.initial.rebalance.delay.ms=0`,
`log.retention.check.interval.ms=10s`, `log.cleaner.backoff.ms=5s`, heap 512 MB, auto topic creation on.

### Listeners — how clients reach the brokers
```
broker kafka1:  INTERNAL://kafka1:29092   ← other containers (Connect, ksqlDB, Flink, exporters, kcat)
                EXTERNAL://localhost:9092 ← your laptop (Python scripts)
                CONTROLLER://:29093       ← KRaft quorum traffic only
```
`common/kafka_helpers.py` uses `BOOTSTRAP=localhost:9092,localhost:9093,localhost:9094` — override with the `BOOTSTRAP` env var
(e.g. a remote cluster). Same for `SCHEMA_REGISTRY_URL` and `CONNECT_URL`.

### Helper commands (`bin/`)
```bash
bin/kafka-cli <any kafka tool> ...   # runs inside kafka1 (BROKER=kafka2 to pick another); KAFKA_OPTS is blanked
bin/topics --describe --topic X      # = kafka-topics --bootstrap-server kafka1:29092 ...
bin/groups --describe --group G      # = kafka-consumer-groups ...
bin/kcat -L                          # kcat in a container on the lab network
```

## 4. Optional stacks

| make target | Folder | Services (host ports) | Needs core? |
|---|---|---|---|
| `rest-up` | 09 | REST Proxy (8082) | yes |
| `security-up` | 11 | standalone SASL/ACL broker (9192) | no (own network) |
| `sr-up` | 12 | Schema Registry (8081) | yes |
| `ksqldb-up` | 14 | ksqlDB server (8088) + CLI | yes |
| `connect-up` | 15 | Connect workers `connect` (8083) & `connect-2` (8084), MySQL (3306), Postgres (5433) | yes |
| `cdc-up` | 16 | everything from 15 (`include:`) + Redis (6379) | yes |
| `flink-up` | 17 | Flink JobManager UI (8085), TaskManager, SQL client | yes (CDC jobs also need 16) |
| `dr-up` | 19 | DR cluster `kafka-dr` (9292) + MirrorMaker 2 | yes |
| `zk-up` | 21 | ZooKeeper (2181) + 2 ZooKeeper-mode brokers (9392/9393) | no (own network) |

### Memory budget (Docker VM)
| Stack | ≈ RAM |
|---|---|
| core (3 brokers + monitoring + UI) | 2.5 GB |
| Connect ×2 + MySQL + Postgres (15/16) | 1.8 GB |
| Flink (17) | 1.6 GB |
| ksqlDB (14) | 0.8 GB |
| Schema Registry / REST Proxy | 0.3 GB each |

On an 8 GB Docker VM run **one** big optional stack at a time (`make extras-down` in between). If you run Flink together
with CDC, `docker stop connect-2` first.

## 5. Running the tests
```bash
make test        # 18-testing-testcontainers: unit tests, librdkafka mock cluster, Testcontainers integration tests
```

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Python client hangs, logs `Connect to ipv4#... failed` | brokers not up (`make status`) or wrong `BOOTSTRAP`. From containers use `kafka1:29092`. |
| `Address already in use ... 7071` when running a Kafka CLI with `docker exec` | the CLI picked up `KAFKA_OPTS` (JMX agent). Use `bin/kafka-cli`, which blanks it. |
| A consumer waits ~45 s before getting partitions | a previous instance of the same group crashed; the coordinator waits `session.timeout.ms`. Expected (see folder 05). |
| `NOT_ENOUGH_REPLICAS` | fewer than 2 brokers in ISR — start the stopped brokers: `docker start kafka1 kafka2 kafka3`. |
| A topic has 3 partitions instead of what the script asked for | something auto-created it right after a delete. Re-run the script (`recreate_topic` verifies and retries). |
| Grafana panels empty | wait 30 s (rate windows are 1 m); check http://localhost:9090/targets. `python-clients` and `kafka-connect` are *down* unless those scripts/stacks run — that's fine. |
| Port already allocated | another local service uses it; change the left side of `ports:` in the compose file. |
| `connect`, `ksqldb` or Flink keep restarting | Docker is out of memory → stop other optional stacks / raise Docker memory. |
| Flink Kafka sink stuck on `OUT_OF_ORDER_SEQUENCE_NUMBER` after a broker restart | cancel and resubmit the job (`17-flink-sql/run-sql.sh`). |
| Apple Silicon warning about `linux/amd64` for kcat | harmless; the image runs under emulation. |

Reset everything: `make clean && make up`.

## 7. Publishing this repo to GitHub

```bash
cd kafka-in-action-labs
git init -b main
git add .
git status            # check: no .venv/, no __pycache__/, no *.pdf, no certs/ (see .gitignore)
git commit -m "Kafka labs: runnable examples for Kafka in Action, Definitive Guide, Apache Kafka in Action"
gh repo create kafka-labs --private --source . --push      # or create it on github.com, then:
# git remote add origin git@github.com:<you>/kafka-labs.git && git push -u origin main
```
Notes:
* `.gitignore` excludes the virtualenv, caches, generated certs, runtime logs, and **all PDFs** — don't commit the books
  (they're copyrighted).
* There are no secrets in the repo; passwords in folder 11 (SASL users) and folders 15/16 (MySQL/Postgres) are demo-only.
