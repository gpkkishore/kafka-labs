"""Shared client config for the secured broker on localhost:9192 (folder 11 only)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.kafka_helpers import BOLD, COLORS, RESET, banner, log  # noqa: E402,F401

SECURE_BOOTSTRAP = "localhost:9192"
USERS = {"admin": "admin-secret", "alice": "alice-secret", "bob": "bob-secret"}
OK, BAD = COLORS[3], COLORS[5]


def client_conf(user: str | None, password: str | None = None, **extra) -> dict:
    """SASL/PLAIN client config. user=None => plain (unauthenticated) connection."""
    conf = {"bootstrap.servers": SECURE_BOOTSTRAP, "log_level": 0, **extra}
    if user:
        conf.update({
            "security.protocol": "SASL_PLAINTEXT",   # SASL_SSL in production (PLAIN sends the password!)
            "sasl.mechanism": "PLAIN",
            "sasl.username": user,
            "sasl.password": password if password is not None else USERS[user],
        })
    return conf
