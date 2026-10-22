#!/usr/bin/env bash
set -euo pipefail

# Inspect the complete dependency closure of every executable.
deps=$(go list -deps -f '{{.ImportPath}}' . ./cmd/...)
violations=$(printf '%s\n' "$deps" | awk '
  # These upstream packages are linked by the Go runtime and Pebble itself.
  # Keep the exceptions exact: repository test support is never allowed.
  $0 == "internal/synctest" ||
  $0 == "github.com/cockroachdb/pebble/v2/internal/blobtest" { next }
  $0 == "testing" || /^testing\// ||
  /^github.com\/stretchr\/testify($|\/)/ ||
  /(^|\/)internal\/testutil($|\/)/ || /(^|\/)[^\/]*test($|\/)/ { print }
')
if [[ -n "$violations" ]]; then
  printf 'Production dependency on test support:\n%s\n' "$violations" >&2
  exit 1
fi

# The active dependency graph omits other platforms and build-tagged files.
go run ./internal/testutil/cmd/importguard
