#!/usr/bin/env bash

# Keep each Knossos register history within 16 operations, including the
# three final reads (one per node), while preserving the requested aggregate
# generated-operation budget across independent register keys.
s3_budget() {
  local requested_keys="$1" requested_ops_per_key="$2" history_cap=16 final_reads=3
  local generated_cap=$((history_cap - final_reads))

  S3_MAX_OPS_PER_KEY=$((requested_ops_per_key < generated_cap ? requested_ops_per_key : generated_cap))
  S3_KEY_COUNT=$(((requested_keys * requested_ops_per_key + S3_MAX_OPS_PER_KEY - 1) / S3_MAX_OPS_PER_KEY))
  if ((S3_KEY_COUNT > 2147483647)); then
    echo "calculated S3 key count exceeds the 32-bit CLI limit" >&2
    return 1
  fi
}
