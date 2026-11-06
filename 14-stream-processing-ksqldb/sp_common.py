"""Shared bits for folder 14 (stream processing)."""
import json
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.kafka_helpers import (BOLD, BOOTSTRAP, COLORS, DIM, RESET, admin, banner, ensure_topic, log,  # noqa: E402,F401
                                  recreate_topic)

PAGEVIEWS = "sp_pageviews"          # stream: key=user_id, value={"user_id","page","action","ts"}
USERS = "sp_users"                  # table (compacted): key=user_id, value={"country","tier"}
PAGES = ["/home", "/search", "/product/42", "/product/7", "/cart", "/checkout"]
USER_IDS = [f"u{i}" for i in range(1, 9)]


class Runner:
    """Ctrl+C / --seconds aware loop helper."""
    def __init__(self, seconds: float):
        self.deadline = time.time() + seconds if seconds else float("inf")
        self.stop = False
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))

    def running(self) -> bool:
        return not self.stop and time.time() < self.deadline


def dumps(d) -> bytes:
    return json.dumps(d).encode()


def fmt_ts(ms: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ms / 1000))
