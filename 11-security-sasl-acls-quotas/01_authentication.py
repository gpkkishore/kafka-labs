#!/usr/bin/env python3
"""KiA 10.2 / AKiA 13.2 — authentication: WHO are you? (SASL/PLAIN)

    docker compose -f 11-security-sasl-acls-quotas/docker-compose.yml up -d
    python 11-security-sasl-acls-quotas/01_authentication.py
"""
from confluent_kafka.admin import AdminClient

from sec_common import BAD, OK, RESET, banner, client_conf


def try_connect(label, conf):
    errors = []
    conf["error_cb"] = errors.append          # librdkafka reports auth problems asynchronously here
    a = AdminClient(conf)
    try:
        md = a.list_topics(timeout=6)
        print(f"  {OK}OK{RESET}   {label:30s} -> sees {len(md.brokers)} broker(s), {len(md.topics)} topic(s)")
    except Exception:  # noqa: BLE001
        a.poll(1)                             # serve error_cb
        reason = next((e for e in errors if e.name() == "_AUTHENTICATION"), errors[0] if errors else None)
        text = f"{reason.name()}: {reason.str()[:90]}" if reason else "timeout"
        print(f"  {BAD}FAIL{RESET} {label:30s} -> {text}")


banner("Connecting to the SECURED broker localhost:9192")
try_connect("alice / correct password", client_conf("alice"))
try_connect("alice / WRONG password", client_conf("alice", "nope"))
try_connect("mallory (unknown user)", client_conf("mallory", "x"))
try_connect("no SASL at all (PLAINTEXT)", client_conf(None))
print("""
* Authentication only proves identity -> the broker now knows the principal "User:alice".
* What alice may DO is authorization (ACLs) -> 02_acls.py.
* Alice "sees" 0 topics: metadata is filtered by DESCRIBE permission. Even listing is authorized!
* PLAIN sends the password in clear text: always combine with TLS (SASL_SSL) outside a lab.
""")
