#!/usr/bin/env bash
set -euo pipefail

helper="$(cd "$(dirname "$0")" && pwd)/archive-jepsen-store.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
run_dir='2026-10-24T17:40:27.130539178Z'
mkdir -p "$scratch/store/$run_dir" "$scratch/restored"
printf '%s\n' '{:type :ok :f :read :value 3}' > "$scratch/store/$run_dir/history.edn"
ln -s "$run_dir" "$scratch/store/latest"
bash "$helper" "$scratch/store" "$scratch/jepsen-store.tar.gz"
tar -xzf "$scratch/jepsen-store.tar.gz" -C "$scratch/restored"
cmp "$scratch/store/$run_dir/history.edn" "$scratch/restored/store/$run_dir/history.edn"
test "$(readlink "$scratch/restored/store/latest")" = "$run_dir"
bash "$helper" "$scratch/missing" "$scratch/missing.tar.gz"
test ! -e "$scratch/missing.tar.gz"
printf '%s\n' 'Archive round-trip and missing-store checks passed'
