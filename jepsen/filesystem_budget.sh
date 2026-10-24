#!/usr/bin/env bash

# Sets FS_KEY_COUNT and FS_MAX_WRITES for one filesystem write mode.
filesystem_budget() {
  local mode="$1" requested_keys="$2" requested_writes="$3" cap
  case "$mode" in
    overwrite) cap=64 ;;
    rename) cap=16 ;;
    *) echo "unknown filesystem write mode: $mode" >&2; return 1 ;;
  esac

  FS_MAX_WRITES=$((requested_writes < cap ? requested_writes : cap))
  FS_KEY_COUNT=$(((requested_keys * requested_writes + FS_MAX_WRITES - 1) / FS_MAX_WRITES))
  if ((FS_KEY_COUNT > 2147483647)); then
    echo "calculated filesystem key count exceeds the 32-bit CLI limit" >&2
    return 1
  fi
}
