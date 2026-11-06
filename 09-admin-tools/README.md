# 09 — Management: tools & logging (KiA ch. 9 · DG ch. 9 "Administering Kafka" · AKiA 7.4, 14.1)

> Goal: administer the cluster from **code** (AdminClient), the **CLI**, **kcat** and **HTTP** (REST Proxy); know where logs
> are, how listeners/advertised listeners work, and how to trace a record end to end.

## Scripts
| # | File | Book § | What |
|---|------|--------|------|
| 1 | `01_admin_client_tour.py` | KiA 9.1.1, DG ch. 9 | describe cluster, create/alter/describe topics & configs, add partitions, list groups, lag, reset offsets, delete |
| 2 | `02_cli_cheatsheet.sh` | DG ch. 9 | `kafka-topics`, `kafka-configs` (topic/broker/dynamic), `kafka-get-offsets`, `kafka-consumer-groups`, `kafka-log-dirs`, `kafka-metadata-quorum`, `kafka-features` |
| 3 | `03_kcat_examples.sh` | KiA 9.1.2 | kcat metadata, produce, consume with format strings, offset-for-time query |
| 4 | `docker-compose.rest-proxy.yml` + `04_rest_proxy.sh` | KiA 9.1.3 | Confluent REST Proxy v3 (admin + produce) and v2 (consumer instances) |
| 5 | `05_tracing_headers.py` | KiA 9.6 | "interceptor"-style tracing with W3C `traceparent` headers + per-hop latency |
| – | `kafka.service` | KiA 9.2 | systemd unit for a bare-metal broker |

```bash
python 09-admin-tools/01_admin_client_tour.py
bash   09-admin-tools/02_cli_cheatsheet.sh
bash   09-admin-tools/03_kcat_examples.sh
docker compose -f 09-admin-tools/docker-compose.rest-proxy.yml up -d && bash 09-admin-tools/04_rest_proxy.sh
docker compose -f 09-admin-tools/docker-compose.rest-proxy.yml down
python 09-admin-tools/05_tracing_headers.py
```

## Tools at a glance
| Need | Tool |
|---|---|
| automation, apps, operators | **AdminClient** (Java/Python/Go...) |
| ad-hoc ops on a broker host | `kafka-*` CLI (`bin/kafka-cli <tool>` here; `.sh` suffix in Apache tarballs) |
| quick debugging from a laptop | **kcat** |
| non-Kafka-speaking clients | **REST Proxy** (or HTTP sink/source connectors) |
| UI | Kafka UI (http://localhost:8080), AKHQ, Conduktor, Confluent Control Center |
| rebalancing partitions automatically | Cruise Control |

## Logs (KiA 9.3)
| Log | Where (container) | Contents |
|---|---|---|
| server log | `docker logs kafka1` (`server.log` in tarball installs) | startup config dump, leader changes, errors |
| controller log | `controller.log` | KRaft/controller decisions |
| state-change log | `state-change.log` | partition state transitions — gold for incident post-mortems |
| log-cleaner log | `log-cleaner.log` | compaction runs |
| request log | `kafka-request.log` (off by default; DEBUG/TRACE is *very* verbose) | every request |
```bash
docker logs kafka1 2>&1 | grep -iE "leader|isr|shutdown" | tail
bin/kafka-cli kafka-log-dirs --bootstrap-server kafka1:29092 --describe | head -c 600
```
Change log levels at runtime: `kafka-configs --entity-type broker-loggers --entity-name 1 --alter --add-config kafka.server.ReplicaManager=DEBUG`.

## Listeners & advertised listeners (KiA 9.4.1, AKiA 7.4) — the #1 connectivity problem
This lab's broker 1 has:
```
listeners            = INTERNAL://0.0.0.0:29092, CONTROLLER://0.0.0.0:29093, EXTERNAL://0.0.0.0:9092
advertised.listeners = INTERNAL://kafka1:29092,                           EXTERNAL://localhost:9092
```
* `listeners` = what the broker **binds** to. `advertised.listeners` = what the broker **tells clients** in metadata.
* A client connects to the bootstrap address, gets metadata, then connects to the *advertised* address of each leader.
  If that address isn't reachable from the client (e.g. `kafka1:29092` from your Mac) → the famous "connects then hangs".
* So: containers use `kafka1:29092` (INTERNAL), your laptop uses `localhost:9092` (EXTERNAL). Same broker, two names.
* Firewalls: open the client listener ports + inter-broker + controller ports; nothing else.

## Metrics & JMX (KiA 9.5) → see `10-monitoring-prometheus-grafana`

## Other admin topics from the books
* **Partition reassignment / decommissioning** → `06-brokers-replication-failover/reassign_partitions.sh`
* **Preferred leader election** → `06-.../failover_scenario.sh`
* **Dumping log segments** → `07-topics-partitions-internals`
* **Unsafe operations** (DG ch. 9): moving the controller, deleting topics manually from disk, editing `__consumer_offsets` — don't.
