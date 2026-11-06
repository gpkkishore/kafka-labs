# 21 — KRaft vs ZooKeeper (AKiA ch. 7 · DG ch. 5 "Cluster Membership / The Controller" · KiA ch. 2.3.3 & 6.2)

> Goal: see with your own eyes where cluster metadata lives in both worlds — **znodes in ZooKeeper** vs an
> **event log (`__cluster_metadata`) replicated by a Raft quorum** — and compare controller failover.
> The books (DG 2017, KiA 2022) describe ZooKeeper; the core lab cluster runs KRaft; Kafka 4.0 removed ZooKeeper.

## Side by side

| | ZooKeeper mode (`docker-compose.zk.yml`) | KRaft mode (core lab cluster) |
|---|---|---|
| Metadata store | external ZooKeeper ensemble (znodes) | `__cluster_metadata` topic, 1 partition, Raft-replicated among controllers |
| Broker membership | ephemeral znode `/brokers/ids/<id>` (dies with the ZK session) | broker **registers** with the controller + heartbeats (`REGISTER_BROKER_RECORD`) |
| Controller | *one broker* wins the race to create ephemeral `/controller` | Raft leader of the controller quorum (`process.roles=controller` or combined `broker,controller`) |
| Fencing zombies | `/controller_epoch` | Raft leader epoch + broker epochs |
| Controller failover | new controller must **read all metadata from ZK** (O(partitions) — minutes on huge clusters) | standby controllers already have the log in memory → **near-instant** |
| Metadata propagation | controller pushes `UpdateMetadata`/`LeaderAndIsr` RPCs to every broker | brokers **fetch** the metadata log like followers (incremental, ordered, offset-tracked) |
| Scale | ~200k partitions practical limit | millions of partitions designed for |
| Ops | two systems to secure, monitor, upgrade | one system, one security model |

## Files

| File | What |
|---|---|
| `docker-compose.zk.yml` | Legacy cluster: `zookeeper` (port **2181**) + `zk-kafka1` (**9392**) + `zk-kafka2` (**9393**), network `kafka-labs-zk` |
| `01_zk_inspect.sh` | Walk the znodes: `/brokers/ids`, `/controller`, `/controller_epoch`, `/brokers/topics/<t>`, partition `state`, `/config/topics`, `/cluster/id` |
| `02_zk_controller_failover.sh` | Stop the controller broker → ephemeral znode vanishes → other broker takes over, epoch +1 |
| `03_kraft_quorum.sh` | `kafka-metadata-quorum describe --status / --replication` on the core cluster |
| `04_kraft_metadata_log.sh [topic]` | Record-type histogram + last records of `__cluster_metadata` (`kafka-dump-log --cluster-metadata-decoder`) and a tree view with `kafka-metadata-shell` |
| `05_kraft_controller_failover.sh` | Stop the **active controller** of the core cluster, watch the Raft leader/epoch change, start it again |

## Run

```bash
docker compose -f 21-kraft-vs-zookeeper/docker-compose.zk.yml up -d && sleep 20
bash 21-kraft-vs-zookeeper/01_zk_inspect.sh
bash 21-kraft-vs-zookeeper/02_zk_controller_failover.sh
docker compose -f 21-kraft-vs-zookeeper/docker-compose.zk.yml down -v

bash 21-kraft-vs-zookeeper/03_kraft_quorum.sh          # core cluster (make up)
bash 21-kraft-vs-zookeeper/04_kraft_metadata_log.sh
bash 21-kraft-vs-zookeeper/05_kraft_controller_failover.sh
```

### Real output from this lab

ZooKeeper — the partition state is a JSON znode:
```
$ zookeeper-shell get /controller
{"version":2,"brokerid":2,"timestamp":"1793897234259","kraftControllerEpoch":-1}
$ zookeeper-shell get /brokers/topics/zk-demo/partitions/0/state
{"controller_epoch":1,"leader":1,"version":1,"leader_epoch":0,"isr":[1,2]}
```
KRaft — the same information is a *record* in the metadata log:
```
 327 "type":"PARTITION_RECORD"      65 "type":"TOPIC_RECORD"     33 "type":"CONFIG_RECORD"
   3 "type":"REGISTER_BROKER_RECORD" 3 "type":"REGISTER_CONTROLLER_RECORD"   2743 "type":"NO_OP_RECORD" (leader heartbeats)
{"type":"PARTITION_RECORD","data":{"partitionId":0,"topicId":"qdQn...","replicas":[1,2,3],"isr":[1,2,3],"leader":1,"leaderEpoch":0,...}}
```
Controller failover (core cluster):
```
active controller: node 3 (epoch 1)
stopping kafka3 ...
  t+8s leader=1 epoch=2        <- most of the 8s is `docker stop`'s graceful shutdown; the Raft election itself is sub-second
```
Try `docker kill kafkaN` instead of stop to see an *unclean* failure: election happens after `controller.quorum.election.timeout.ms` (1s) once heartbeats stop.

> `kafka-metadata-shell` refuses to open a log directory locked by a running broker, so `04` copies the segment to `/tmp/mdcopy` inside the container first.

## How many controllers? (AKiA 7.1)
* Raft needs a **majority**: 3 controllers tolerate 1 failure, 5 tolerate 2. Never use an even number.
* Production: **dedicated** controller nodes (`process.roles=controller`), small (a few GB RAM), on separate failure domains. **Combined mode** (this lab) is for dev/small clusters only.
* Key settings: `process.roles`, `node.id`, `controller.quorum.voters` (static) or `controller.quorum.bootstrap.servers` (dynamic quorum, Kafka 3.9+), `controller.listener.names`. Storage must be formatted once: `kafka-storage format -t <cluster-id> -c server.properties` (Confluent images do it from `CLUSTER_ID`).

## Migrating ZooKeeper → KRaft (KIP-866, AKiA 7.3)
You **cannot** jump straight to Kafka 4.x from a ZK cluster: migrate on a 3.x "bridge release" (3.5–3.9) first.
1. **Upgrade** the ZK cluster to a bridge release (e.g. 3.9) and set `inter.broker.protocol.version` accordingly.
2. **Deploy a KRaft controller quorum** (3 nodes) with the *same cluster id*, plus
   `zookeeper.metadata.migration.enable=true` and `zookeeper.connect=...` so they can read ZK.
3. **Enable migration on the brokers** (`zookeeper.metadata.migration.enable=true`, `controller.quorum.voters`, `controller.listener.names`) and rolling-restart.
   The KRaft controller copies all metadata from ZK into `__cluster_metadata` and becomes active (`ZK_MIGRATION_STATE_RECORD`). From now on it **dual-writes** to ZK (so you can still roll back).
4. **Restart brokers in KRaft mode**: set `process.roles=broker`, remove `zookeeper.connect` and the migration flag, rolling restart.
5. **Finalize**: remove `zookeeper.connect` / migration flag from the controllers and restart them. ZK is no longer written — rollback is no longer possible. Decommission ZooKeeper.
6. Then upgrade to Kafka 4.x.
Check progress with the JMX metric `kafka.controller:type=KafkaController,name=ZkMigrationState` and `kafka-metadata-quorum describe --status`.

## Connecting to Kafka (AKiA 7.4)
Clients never talk to ZooKeeper or (in KRaft) to controllers directly: they bootstrap from **any broker**, receive metadata (all brokers + partition leaders), then connect to each leader they need. `advertised.listeners` must be resolvable *from the client's network* — that's why the lab brokers advertise `localhost:9092-9094` to your laptop and `kafkaN:29092` to containers (see `infra/docker-compose.yml`).
