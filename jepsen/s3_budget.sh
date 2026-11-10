#!/usr/bin/env bash

# Keep each Knossos register history small while preserving the requested
# aggregate operation budget across independent S3 object keys.
s3_budget() {
  local requested_keys="$1" requested_ops_per_key="$2" cap=32

  S3_MAX_OPS_PER_KEY=$((requested_ops_per_key < cap ? requested_ops_per_key : cap))
  S3_KEY_COUNT=$(((requested_keys * requested_ops_per_key + S3_MAX_OPS_PER_KEY - 1) / S3_MAX_OPS_PER_KEY))
  if ((S3_KEY_COUNT > 2147483647)); then
    echo "calculated S3 key count exceeds the 32-bit CLI limit" >&2
    return 1
  fi
}
