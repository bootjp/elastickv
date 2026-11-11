#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=jepsen/s3_budget.sh
source "$(dirname "$0")/../s3_budget.sh"

s3_budget 16 250
[[ "$S3_KEY_COUNT" == 125 && "$S3_MAX_OPS_PER_KEY" == 32 ]]

s3_budget 3 7
[[ "$S3_KEY_COUNT" == 3 && "$S3_MAX_OPS_PER_KEY" == 7 ]]

if s3_budget 2147483647 2147483647 2>/dev/null; then
  echo "oversized key count was accepted" >&2
  exit 1
fi
