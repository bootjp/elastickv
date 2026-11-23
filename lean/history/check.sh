#!/usr/bin/env bash
# Check one hosted kill artifact from the repository root.
set -euo pipefail
workload=${1:?expected workload and store.tar.gz}
archive=${2:?expected store.tar.gz}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
python3 - "$archive" "$scratch/history.edn" "${3:-}" "$workload" <<'PY'
import re, shutil, sys, tarfile
from pathlib import Path
workload = sys.argv[4]
with tarfile.open(sys.argv[1]) as archive:
    candidates = [m for m in archive if m.isfile() and re.fullmatch(r"store/[^/]+/[^/]+/history\.edn", m.name)]
    matches = [m for m in candidates if not sys.argv[3] or m.name == sys.argv[3]]
    if len(matches) != 1:
        raise SystemExit("Pass one history member as the third argument. Candidates:\n" +
                         "\n".join(sorted(m.name for m in candidates)))
    member, = matches
    hosted = "s3" if workload == "s3-offload" else workload
    if member.name.split("/")[1] != f"elastickv-hosted-{hosted}-kill":
        raise SystemExit(f"History {member.name} does not match {workload} kill")
    parameters = Path(sys.argv[1]).with_name("parameters.txt")
    if parameters.is_file():
        lines = parameters.read_text().splitlines()
        for key, expected in (("adapter", workload), ("fault", "kill")):
            if [line for line in lines if line.startswith(key + "=")] != [f"{key}={expected}"]:
                raise SystemExit(f"{parameters} does not identify {workload} kill")
    elif hosted == "s3":
        raise SystemExit("Keep parameters.txt beside the archive to distinguish S3 and S3 offload")
    with archive.extractfile(member) as src, open(sys.argv[2], "wb") as out:
        shutil.copyfileobj(src, out)
PY
classpath=$(cd jepsen && lein classpath)
java -cp "$classpath" clojure.main lean/history/export.clj < "$scratch/history.edn" > "$scratch/history.jsonl"
python3 lean/history/certificate.py "$workload" "$scratch/history.jsonl" "$scratch/certificate.json"
(cd lean && lean -DwarningAsError=true --run OperationHistory.lean "$scratch/certificate.json")
