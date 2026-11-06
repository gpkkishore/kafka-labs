#!/usr/bin/env bash
# Show Prometheus scrape targets + alert states from the CLI (same as http://localhost:9090/targets and /alerts)
curl -s localhost:9090/api/v1/targets | python3 -c '
import json, sys
for t in json.load(sys.stdin)["data"]["activeTargets"]:
    lab = t["labels"]
    print("target %-15s %-28s %s" % (lab["job"], lab["instance"], t["health"]))'
echo
curl -s localhost:9090/api/v1/rules | python3 -c '
import json, sys
for g in json.load(sys.stdin)["data"]["groups"]:
    for r in g["rules"]:
        print("alert %-32s state=%-8s active=%d" % (r["name"], r["state"], len(r.get("alerts", []))))'
