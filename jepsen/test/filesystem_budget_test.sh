#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=jepsen/filesystem_budget.sh
source "$(dirname "$0")/../filesystem_budget.sh"

filesystem_budget overwrite 16 250
[[ "$FS_KEY_COUNT" == 63 && "$FS_MAX_WRITES" == 64 ]]

# An indeterminate rename can leave many possible register states. Keep each
# Knossos history within the size that completes on the captured CI history.
filesystem_budget rename 16 250
[[ "$FS_KEY_COUNT" == 250 && "$FS_MAX_WRITES" == 16 ]]

filesystem_budget rename 3 7
[[ "$FS_KEY_COUNT" == 3 && "$FS_MAX_WRITES" == 7 ]]

if filesystem_budget rename 2147483647 2147483647 2>/dev/null; then
  echo "oversized key count was accepted" >&2
  exit 1
fi

if filesystem_budget unexpected 1 1 2>/dev/null; then
  echo "unknown write mode was accepted" >&2
  exit 1
fi
