"""Tiny Kafka Connect REST API client (used by 15-kafka-connect and 16-cdc-debezium-mysql).

Every method maps 1:1 to a REST endpoint so you can read this file as API documentation:

  GET    /                                   worker version + Kafka cluster id
  GET    /connector-plugins                  installed connector classes
  PUT    /connector-plugins/{cls}/config/validate   validate a config without creating anything
  GET    /connectors?expand=status           all connectors + state
  PUT    /connectors/{name}/config           create OR update (idempotent — preferred over POST /connectors)
  GET    /connectors/{name}/status           connector + task state and which WORKER runs each
  GET    /connectors/{name}/tasks            task configs (how the connector split the work)
  GET    /connectors/{name}/topics           topics the connector has used
  GET    /connectors/{name}/offsets          source offsets / sink consumer offsets (Kafka 3.5+)
  PUT    /connectors/{name}/pause|resume|stop
  DELETE /connectors/{name}/offsets          reset offsets of a STOPPED connector (KIP-875, Kafka 3.6+)
  POST   /connectors/{name}/restart?includeTasks=true&onlyFailed=false
  DELETE /connectors/{name}
"""
from __future__ import annotations

import json
import os
import time

import requests

CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")


class Connect:
    def __init__(self, url: str = CONNECT_URL):
        self.url = url.rstrip("/")

    # --- helpers -------------------------------------------------------------
    def _req(self, method: str, path: str, **kw):
        r = requests.request(method, self.url + path, timeout=30, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")
        return r.json() if r.text else None

    # --- cluster ---------------------------------------------------------------
    def info(self):
        return self._req("GET", "/")

    def plugins(self):
        return self._req("GET", "/connector-plugins")

    def validate(self, connector_class: str, config: dict):
        short = connector_class.split(".")[-1]
        return self._req("PUT", f"/connector-plugins/{short}/config/validate",
                         json={"connector.class": connector_class, **config})

    # --- connectors --------------------------------------------------------------
    def list(self):
        return self._req("GET", "/connectors?expand=status")

    def put(self, name: str, config: dict):
        """Create or update a connector (idempotent)."""
        return self._req("PUT", f"/connectors/{name}/config", json={k: str(v) for k, v in config.items()})

    def status(self, name: str):
        return self._req("GET", f"/connectors/{name}/status")

    def tasks(self, name: str):
        return self._req("GET", f"/connectors/{name}/tasks")

    def topics(self, name: str):
        return self._req("GET", f"/connectors/{name}/topics")

    def offsets(self, name: str):
        return self._req("GET", f"/connectors/{name}/offsets")

    def pause(self, name: str):
        return self._req("PUT", f"/connectors/{name}/pause")

    def resume(self, name: str):
        return self._req("PUT", f"/connectors/{name}/resume")

    def stop(self, name: str):
        return self._req("PUT", f"/connectors/{name}/stop")

    def delete_offsets(self, name: str):
        return self._req("DELETE", f"/connectors/{name}/offsets")

    def remove(self, name: str, reset_offsets: bool = True):
        """Delete a connector; optionally wipe its stored offsets first so a re-created one starts from scratch."""
        try:
            self.status(name)
        except RuntimeError:
            return
        if reset_offsets:
            self.stop(name)
            self.wait_state(name, "STOPPED", min_tasks=0)
            self.delete_offsets(name)
        self.delete(name)

    def restart(self, name: str, include_tasks: bool = True, only_failed: bool = False):
        return self._req("POST", f"/connectors/{name}/restart?includeTasks={str(include_tasks).lower()}"
                                 f"&onlyFailed={str(only_failed).lower()}")

    def delete(self, name: str):
        try:
            return self._req("DELETE", f"/connectors/{name}")
        except RuntimeError as e:
            if "404" not in str(e):
                raise

    # --- waiting -----------------------------------------------------------------
    def wait_state(self, name: str, state: str = "RUNNING", timeout: float = 60, min_tasks: int = 1):
        """Wait until the connector AND all its tasks reach `state`; raise with the trace if a task FAILED."""
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = self.status(name)
            except RuntimeError:
                time.sleep(1)
                continue
            tasks = last.get("tasks", [])
            for t in tasks:
                if t["state"] == "FAILED":
                    raise RuntimeError(f"task {t['id']} FAILED:\n{t.get('trace', '')[:2000]}")
            if last["connector"]["state"] == state and len(tasks) >= min_tasks and all(t["state"] == state for t in tasks):
                return last
            time.sleep(1)
        raise TimeoutError(f"{name} not {state} after {timeout}s: {json.dumps(last, indent=1)}")


def print_status(st: dict) -> None:
    c = st["connector"]
    print(f"  connector {st['name']}: {c['state']} on {c['worker_id']}  (type={st.get('type')})")
    for t in sorted(st.get("tasks", []), key=lambda t: t["id"]):
        print(f"    task {t['id']}: {t['state']:8s} on {t['worker_id']}")
