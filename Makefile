# Kafka labs — common commands. `make help` lists them.
SHELL := /bin/bash
PY    := .venv/bin/python
CORE  := docker compose -f infra/docker-compose.yml

.DEFAULT_GOAL := help
.PHONY: help venv up down clean status ps logs topics groups test smoke \
        rest-up rest-down security-up security-down sr-up sr-down ksqldb-up ksqldb-down \
        connect-up connect-down cdc-up cdc-down flink-up flink-down dr-up dr-down zk-up zk-down extras-down

help:  ## show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

venv:  ## create .venv and install the Python requirements
	python3 -m venv .venv && .venv/bin/pip install -q --upgrade pip && .venv/bin/pip install -q -r requirements.txt
	@echo "activate with:  source .venv/bin/activate"

up:  ## start core: 3 KRaft brokers + Kafka UI + Prometheus + Grafana + exporters
	$(CORE) up -d --wait
	@$(MAKE) --no-print-directory status

down:  ## stop the core stack (brokers have no volumes: topics/data are reset)
	$(CORE) down

clean: extras-down  ## stop EVERYTHING (core + optional stacks) and delete volumes
	$(CORE) down -v --remove-orphans

status:  ## containers + URLs
	@docker ps --format '  {{.Names}}\t{{.Status}}' | grep -E 'kafka|prometheus|grafana|exporter|schema|connect|ksql|mysql|postgres|redis|flink|mm2|zookeeper|rest-proxy|jobmanager|taskmanager' || true
	@echo ""
	@echo "  Kafka UI    http://localhost:8080"
	@echo "  Grafana     http://localhost:3000   (dashboards: Kafka Labs)"
	@echo "  Prometheus  http://localhost:9090"
	@echo "  bootstrap   localhost:9092,localhost:9093,localhost:9094   (inside docker: kafka1:29092,kafka2:29092,kafka3:29092)"

ps: status

logs:  ## follow broker logs
	$(CORE) logs -f kafka1 kafka2 kafka3

topics:  ## list topics
	@bin/topics --list --exclude-internal

groups:  ## consumer groups with state
	@bin/groups --describe --all-groups --state

smoke:  ## quick end-to-end check: produce + consume
	$(PY) 01-hello-kafka/hello_producer.py -n 5
	$(PY) 01-hello-kafka/hello_consumer.py --idle-exit 3

test:  ## pytest suite (folder 18; Testcontainers needs Docker)
	$(PY) -m pytest 18-testing-testcontainers

# ---- optional stacks ------------------------------------------------------------------------
rest-up:  ## REST Proxy :8082 (folder 09)
	docker compose -f 09-admin-tools/docker-compose.rest-proxy.yml up -d
rest-down:
	docker compose -f 09-admin-tools/docker-compose.rest-proxy.yml down
security-up:  ## standalone SASL/ACL broker :9192 (folder 11, own network)
	docker compose -f 11-security-sasl-acls-quotas/docker-compose.yml up -d
security-down:
	docker compose -f 11-security-sasl-acls-quotas/docker-compose.yml down -v
sr-up:  ## Schema Registry :8081 (folder 12)
	docker compose -f 12-schema-registry-avro/docker-compose.yml up -d
sr-down:
	docker compose -f 12-schema-registry-avro/docker-compose.yml down
ksqldb-up:  ## ksqlDB server :8088 + CLI (folder 14)
	docker compose -f 14-stream-processing-ksqldb/ksqldb/docker-compose.yml up -d
ksqldb-down:
	docker compose -f 14-stream-processing-ksqldb/ksqldb/docker-compose.yml down
connect-up:  ## Kafka Connect x2 :8083/:8084 + MySQL :3306 + Postgres :5433 (folder 15)
	docker compose -f 15-kafka-connect/docker-compose.yml up -d --build
connect-down:
	docker compose -f 15-kafka-connect/docker-compose.yml down -v
cdc-up:  ## Debezium CDC = folder 15 stack + Redis :6379 (folder 16)
	docker compose -f 16-cdc-debezium-mysql/docker-compose.yml up -d --build
cdc-down:
	docker compose -f 16-cdc-debezium-mysql/docker-compose.yml down -v
flink-up:  ## Flink 1.20 :8085 (folder 17)
	docker compose -f 17-flink-sql/docker-compose.yml up -d --build
flink-down:
	docker compose -f 17-flink-sql/docker-compose.yml down -v
dr-up:  ## DR cluster :9292 + MirrorMaker 2 (folder 19)
	docker compose -f 19-mirrormaker2-disaster-recovery/docker-compose.yml up -d
dr-down:
	docker compose -f 19-mirrormaker2-disaster-recovery/docker-compose.yml down -v
zk-up:  ## ZooKeeper-mode cluster :9392/:9393 + ZK :2181 (folder 21, own network)
	docker compose -f 21-kraft-vs-zookeeper/docker-compose.zk.yml up -d
zk-down:
	docker compose -f 21-kraft-vs-zookeeper/docker-compose.zk.yml down -v

EXTRA_COMPOSES := 09-admin-tools/docker-compose.rest-proxy.yml 11-security-sasl-acls-quotas/docker-compose.yml \
  12-schema-registry-avro/docker-compose.yml 14-stream-processing-ksqldb/ksqldb/docker-compose.yml \
  17-flink-sql/docker-compose.yml 16-cdc-debezium-mysql/docker-compose.yml 15-kafka-connect/docker-compose.yml \
  19-mirrormaker2-disaster-recovery/docker-compose.yml 21-kraft-vs-zookeeper/docker-compose.zk.yml

extras-down:  ## stop all optional stacks
	-@for f in $(EXTRA_COMPOSES); do docker compose -f $$f down -v --remove-orphans >/dev/null 2>&1; done; echo "optional stacks stopped"
