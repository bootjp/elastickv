#!/usr/bin/env bash
# Check one hosted kill artifact from the repository root.
set -euo pipefail
workload=${1:?expected workload and store.tar.gz}
archive=${2:?expected store.tar.gz}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
python3 - "$archive" "$scratch/history.edn" "${3:-}" <<'PY'
import re, shutil, sys, tarfile
with tarfile.open(sys.argv[1]) as archive:
    candidates = [m for m in archive if m.isfile() and re.fullmatch(r"store/[^/]+/[^/]+/history\.edn", m.name)]
    matches = [m for m in candidates if not sys.argv[3] or m.name == sys.argv[3]]
    if len(matches) != 1:
        raise SystemExit("Pass one history member as the third argument. Candidates:\n" +
                         "\n".join(sorted(m.name for m in candidates)))
    member, = matches
    with archive.extractfile(member) as src, open(sys.argv[2], "wb") as out:
        shutil.copyfileobj(src, out)
PY
classpath=$(cd jepsen && lein classpath)
java -cp "$classpath" clojure.main lean/history/export.clj < "$scratch/history.edn" > "$scratch/history.jsonl"
python3 lean/history/certificate.py "$workload" "$scratch/history.jsonl" "$scratch/certificate.json"
(cd lean && lean -DwarningAsError=true --run OperationHistory.lean "$scratch/certificate.json")
