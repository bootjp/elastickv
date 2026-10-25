#!/usr/bin/env bash

# Sets SQLITE_KEY_COUNT and SQLITE_MAX_WRITES for a requested aggregate budget.
sqlite_budget() {
  local requested_keys="$1" requested_writes="$2"
  for n in "$requested_keys" "$requested_writes"; do
    if [[ ! "$n" =~ ^[1-9][0-9]*$ ]] || (( ${#n} > 10 )) ||
       { (( ${#n} == 10 )) && (( 10#$n > 2147483647 )); }; then
      echo "SQLite key and write counts must be positive 32-bit integers" >&2
      return 1
    fi
  done

  SQLITE_MAX_WRITES=$((requested_writes < 16 ? requested_writes : 16))
  SQLITE_KEY_COUNT=$(((requested_keys * requested_writes + SQLITE_MAX_WRITES - 1) / SQLITE_MAX_WRITES))
  if ((SQLITE_KEY_COUNT > 2147483647)); then
    echo "calculated SQLite key count exceeds the 32-bit CLI limit" >&2
    return 1
  fi
}
