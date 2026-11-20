#!/usr/bin/env bash
set -euo pipefail
while [[ ! -f reviewed-binary.ready ]]; do sleep 1; done
exec ./candidate-reviewed.test "$@"
