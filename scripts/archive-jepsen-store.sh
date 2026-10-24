#!/usr/bin/env bash
# Preserve Jepsen's timestamp filenames inside one artifact-safe archive.
set -euo pipefail

source_dir=${1:?Usage: archive-jepsen-store.sh SOURCE_DIR OUTPUT.tar.gz}
archive=${2:?Usage: archive-jepsen-store.sh SOURCE_DIR OUTPUT.tar.gz}
if [[ ! -e "$source_dir" ]]; then
  exit 0
fi
tar -czf "$archive" -C "$(dirname "$source_dir")" "$(basename "$source_dir")"
