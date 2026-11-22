#!/usr/bin/env python3
"""Summarize loadtest JSON or an opt-in Split test log, without hiding drops."""
import argparse
import json
import re
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("log", type=Path)
args = parser.parse_args()
events = []
text = args.log.read_text()
for line in text.splitlines():
    payload = line.split("LOAD ", 1)[-1].strip()
    if payload.startswith("{"):
        try:
            events.append(json.loads(payload))
        except json.JSONDecodeError:
            raise SystemExit("Malformed load JSON: " + line[:160])
stages = [e["stage"] for e in events if e.get("type") == "stage"]
audits = [e for e in events if e.get("type") == "audit"]
if not stages or len(audits) != 1:
    raise SystemExit("Expected completed stages and exactly one final audit")
audit = audits[0]
ack = sum(s["write"]["success"] for s in stages)
valid_counts = all(s["planned"] == s["offered"] ==
                   s["admitted"] + s["dropped_capacity"] + s["dropped_scheduler"]
                   and not s["cancelled"] for s in stages)
summary = {
    "stages": stages,
    "availability_clean": all(not s["cancelled"] and s["dropped_capacity"] ==
                              s["dropped_scheduler"] == 0 and
                              s["logical_scheduled_latency"]["attempts"] ==
                              s["logical_scheduled_latency"]["success"] for s in stages),
    "acknowledged_writes": ack,
    "audit": audit,
    "accounting_valid": valid_counts and ack == audit["acknowledged_writes"],
    "ack_audit_valid": ack > 0 and audit["audit"]["attempts"] ==
                       audit["audit"]["success"] == audit["expected_observations"],
    "split_done_ms": [int(v) for v in re.findall(r"LOAD split_cross_done job=\d+ elapsed_ms=(\d+)", text)],
    "preflight_upper_ms": [float(v) for v in re.findall(r"PERF preflight-duration .*?upper_ms=([\d.]+)", text)],
    "post_done_paths": re.findall(r"PERF recovery path=(\S+) planned=(\d+) sent=(\d+) success=(\d+) errors=(\d+)", text),
    "profile_events": [line.strip() for line in text.splitlines() if "LOAD profile" in line],
    "test_passed": "--- PASS: TestLinuxSplitLoadRamp" in text,
}
print(json.dumps(summary, indent=2))
