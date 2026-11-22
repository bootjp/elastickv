import argparse
import csv
import gzip
import json
import re
from datetime import datetime, timezone
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("root", type=Path)
args = parser.parse_args()
results = {}
for name in ["baseline", "candidate"]:
    p = args.root / name
    markers = [json.loads(s) for s in (p / "markers.jsonl").read_text().splitlines()]
    events = [json.loads(s.split("LOAD ", 1)[1]) for _, s in markers if "LOAD {" in s]
    cfg = next(e["config"] for e in events if e["type"] == "configuration")
    stage = next(e["stage"] for e in events if e["type"] == "stage" and e["stage"]["index"] == 1)
    sec, frac = stage["started"].rstrip("Z").split(".")
    start = int(datetime.fromisoformat(sec).replace(tzinfo=timezone.utc).timestamp()) * 10**9 + int(frac.ljust(9, "0"))
    end = start + cfg["stage_duration_ns"]
    begin = end - 10 * 10**9
    done = next(ns for ns, text in markers if re.search(r"PERF migration .* phase=7(?: |$)", text))
    assert done < begin, "window must be after Done and at the same offered rate"
    with gzip.open(p / "rpc.csv.gz", "rt") as f:
        rows = [(int(i), int(t), int(d), k, c) for i, t, d, k, c in csv.reader(f)]
    results[name] = {}
    for kind in ["read", "write"]:
        ops = [r for r in rows if r[3] == kind and begin <= r[1] < end]
        good = sorted(r[2] for r in ops if r[4] == "OK")
        assert good
        results[name][kind] = {"p99_ms": good[(len(good) - 1) * 99 // 100] / 1e6, "success_ops_s": len(good) / 10}
print(json.dumps(results, indent=2))
for kind, absolute in [("read", 50), ("write", 100)]:
    a, b = results["baseline"][kind], results["candidate"][kind]
    if b["p99_ms"] > max(absolute, 3 * a["p99_ms"]) or b["success_ops_s"] < 0.9 * a["success_ops_s"]:
        raise SystemExit(f"RED: post-cutover {kind} tail regression")
