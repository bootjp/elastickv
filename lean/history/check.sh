#!/usr/bin/env bash
# Check one hosted kill artifact from the repository root.
set -euo pipefail
workload=${1:?expected workload and store.tar.gz}
archive=${2:?expected store.tar.gz}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
python3 - "$archive" "$scratch/history.edn" <<'PY'
import re, shutil, sys, tarfile
with tarfile.open(sys.argv[1]) as archive:
    member, = [m for m in archive if m.isfile() and re.fullmatch(r"store/[^/]+/[^/]+/history\.edn", m.name)]
    with archive.extractfile(member) as src, open(sys.argv[2], "wb") as out:
        shutil.copyfileobj(src, out)
PY
classpath=$(cd jepsen && lein classpath)
java -cp "$classpath" clojure.main lean/history/export.clj < "$scratch/history.edn" > "$scratch/history.jsonl"
python3 lean/history/certificate.py "$workload" "$scratch/history.jsonl" "$scratch/certificate.json"
(cd lean && lean -DwarningAsError=true --run OperationHistory.lean "$scratch/certificate.json")
