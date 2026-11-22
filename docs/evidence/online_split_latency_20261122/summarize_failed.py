import csv
import gzip
import json
import re
from collections import Counter
from pathlib import Path

from compare import percentile, stats

root = Path(__file__).resolve().parent
p = root / "baseline-retry-failed"
assert (p / "exit.txt").read_text().strip() == "1"
markers = [json.loads(line) for line in (p / "markers.jsonl").read_text().splitlines()]
events = [json.loads(s.split("LOAD ", 1)[1]) for _, s in markers if "LOAD {" in s]
assert not any(e["type"] == "audit" for e in events)
stages = [e["stage"] for e in events if e["type"] == "stage"]
assert [s["offered_rps"] for s in stages] == [100, 300, 500, 1000]
with gzip.open(p / "rpc.csv.gz", "rt") as f:
    rows = [(int(i), int(t), int(dt), kind, code) for i, t, dt, kind, code in csv.reader(f)]
assert len(rows) == len({r[0] for r in rows})
offset = 0
for s in stages:
    part = [r for r in rows if offset <= r[0] < offset + s["planned"]]
    for kind in ["read", "write"]:
        ops = [r for r in part if r[3] == kind]
        good = [r for r in ops if r[4] == "OK"]
        assert len(ops) == s[kind]["attempts"] and len(good) == s[kind]["success"]
        assert Counter(r[4] for r in ops if r[4] != "OK") == (s[kind]["errors"] or {})
        assert percentile([r[2] for r in good]) == s[kind]["successful_latency"]["p99_ms"]
    assert len(part) + s["dropped_capacity"] + s["dropped_scheduler"] == s["planned"] == s["offered"]
    offset += s["planned"]
phase = []
for ns, text in markers:
    match = re.search(r"PERF migration elapsed_ms=(\d+) phase=(\d+)", text)
    if match:
        phase.append((ns, int(match[1]), int(match[2])))
assert phase[-1][2] == 3
assert not any("LOAD split_cross_done" in text for _, text in markers)
assert any(e["type"] == "load_complete" for e in events)
start = phase[0][0] - phase[0][1] * 10**6
c = json.loads((root / "comparison.json").read_text())
duration = int(c["common_duration_seconds"] * 10**9)
result = {
    "source_commit": c["source_commits"]["baseline"],
    "fixture_exit": 1,
    "status": "Split did not reach Done within the fixture deadline; catalog split job conflicts were logged",
    "last_phase": 3,
    "split_completed": False,
    "ack_audit_completed": False,
    "post_done_checks_completed": False,
    "reported_write_success_without_final_audit": sum(s["write"]["success"] for s in stages),
    "primary_trace_matches_all_stage_metrics": True,
    "common_rate_window": stats(rows, start, start + duration),
    "phase_clock_offset_spread_ms": (max(n - ms * 10**6 for n, ms, _ in phase) - min(n - ms * 10**6 for n, ms, _ in phase)) / 1e6,
    "dropped_capacity": sum(s["dropped_capacity"] for s in stages),
    "dropped_scheduler": sum(s["dropped_scheduler"] for s in stages),
}
(root / "baseline-retry-failed.json").write_text(json.dumps(result, indent=2) + "\n")
c["baseline_retry_failed"] = result
(root / "comparison.json").write_text(json.dumps(c, indent=2) + "\n")
print(json.dumps(result, indent=2))
