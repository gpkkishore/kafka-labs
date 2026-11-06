#!/usr/bin/env python3
"""KiA 10.3.1 / DG ch.? security / AKiA 13.2 — authorization with ACLs.

ACL = (principal, permission ALLOW|DENY, operation, resource type+name+pattern, host)
   e.g. User:alice ALLOW WRITE on TOPIC sec_orders (LITERAL) from host *

    python 11-security-sasl-acls-quotas/02_acls.py           # run the demo
    python 11-security-sasl-acls-quotas/02_acls.py --cleanup # remove topic + ACLs
"""
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import (AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AdminClient,
                                   NewTopic, ResourcePatternType, ResourceType)

from sec_common import BAD, OK, RESET, banner, client_conf

TOPIC, GROUP_PREFIX = "sec_orders", "billing-"
admin = AdminClient(client_conf("admin"))


def acl(user, op, rtype, name, pattern=ResourcePatternType.LITERAL):
    return AclBinding(rtype, name, pattern, f"User:{user}", "*", op, AclPermissionType.ALLOW)


ALL_ACLS = [
    acl("alice", AclOperation.WRITE, ResourceType.TOPIC, TOPIC),
    acl("alice", AclOperation.DESCRIBE, ResourceType.TOPIC, TOPIC),
    acl("bob", AclOperation.READ, ResourceType.TOPIC, TOPIC),
    acl("bob", AclOperation.DESCRIBE, ResourceType.TOPIC, TOPIC),
    acl("bob", AclOperation.READ, ResourceType.GROUP, GROUP_PREFIX, ResourcePatternType.PREFIXED),
]

if "--cleanup" in sys.argv:
    for f in admin.delete_acls([AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY, None, None,
                                                 AclOperation.ANY, AclPermissionType.ANY)]).values():
        print("deleted", len(f.result()), "ACLs")
    admin.delete_topics([TOPIC])
    sys.exit(0)


def produce_as(user):
    p = Producer(client_conf(user, **{"message.timeout.ms": 6000}))
    res = {}
    p.produce(TOPIC, key="o1", value=f"order from {user}", on_delivery=lambda e, m: res.update(err=e))
    p.flush(10)
    e = res.get("err")
    print(f"  {BAD + 'DENIED ' + e.name() if e else OK + 'OK     produced'}{RESET}  ({user} -> WRITE {TOPIC})")


def consume_as(user, group):
    c = Consumer(client_conf(user, **{"group.id": group, "auto.offset.reset": "earliest"}))
    c.subscribe([TOPIC])
    try:
        for _ in range(10):
            m = c.poll(1.0)
            if m is None:
                continue
            if m.error():
                print(f"  {BAD}DENIED {m.error().name()}{RESET}  ({user} -> READ {TOPIC}, group {group})")
                return
            print(f"  {OK}OK     read '{m.value().decode()}'{RESET}  ({user}, group {group})")
            return
        print(f"  (no message within 10s for {user}/{group})")
    except KafkaException as e:
        print(f"  {BAD}DENIED {e.args[0].name()}{RESET}  ({user}, group {group})")
    finally:
        c.close()


banner("0) admin (super user) creates the topic; start with NO ACLs")
admin.delete_acls([AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY, None, None,
                                    AclOperation.ANY, AclPermissionType.ANY)])
try:
    admin.create_topics([NewTopic(TOPIC, 3, 1)])[TOPIC].result()
except KafkaException:
    pass
time.sleep(1)

banner("1) alice produces without any ACL")
produce_as("alice")

banner("2) grant: alice WRITE+DESCRIBE topic; bob READ+DESCRIBE topic; bob READ group prefix 'billing-'")
for b, f in admin.create_acls(ALL_ACLS).items():
    f.result()
time.sleep(1)  # ACL changes propagate through the metadata log
produce_as("alice")

banner("3) bob consumes with an ALLOWED group (prefix billing-) vs a NOT allowed one")
consume_as("bob", f"billing-{uuid.uuid4().hex[:4]}")
consume_as("bob", f"marketing-{uuid.uuid4().hex[:4]}")

banner("4) alice tries to READ (she only has WRITE)")
consume_as("alice", f"billing-{uuid.uuid4().hex[:4]}")

banner("5) describe_acls — the ACL table")
flt = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY, None, None, AclOperation.ANY, AclPermissionType.ANY)
for b in sorted(admin.describe_acls(flt).result(), key=lambda b: (b.principal, b.name)):
    print(f"  {b.principal:11s} {b.permission_type.name:5s} {b.operation.name:9s} {b.restype.name:6s} "
          f"{b.name} ({b.resource_pattern_type.name})")
print("""
Consumers need TWO resources: READ on the TOPIC *and* READ on the consumer GROUP.
Transactional producers also need WRITE/DESCRIBE on TRANSACTIONAL_ID; idempotent ones IDEMPOTENT_WRITE on CLUSTER
(implied by WRITE on a topic since Kafka 3.0).
CLI equivalent:
  docker exec kafka-secure kafka-acls --bootstrap-server kafka-secure:29192 --command-config /etc/kafka/lab/admin.properties --list
""")
