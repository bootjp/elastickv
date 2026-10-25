#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=jepsen/sqlite_budget.sh
source "$(dirname "$0")/../sqlite_budget.sh"

sqlite_budget 16 64
[[ "$SQLITE_KEY_COUNT" == 64 && "$SQLITE_MAX_WRITES" == 16 ]]

sqlite_budget 3 7
[[ "$SQLITE_KEY_COUNT" == 3 && "$SQLITE_MAX_WRITES" == 7 ]]

sqlite_budget 3 17
[[ "$SQLITE_KEY_COUNT" == 4 && "$SQLITE_MAX_WRITES" == 16 ]]

if sqlite_budget 0 1 2>/dev/null; then
  echo "zero SQLite key count was accepted" >&2
  exit 1
fi

if sqlite_budget 2147483647 2147483647 2>/dev/null; then
  echo "oversized SQLite key count was accepted" >&2
  exit 1
fi
