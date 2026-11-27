#!/usr/bin/env bash
# No PyPI sentinel is documented by the provider yet: check routing and download.
set -euo pipefail

if [[ "${PIP_INDEX_URL:-}" != 'https://pypi.flatt.tech/simple/' \
  || "${PIP_CONFIG_FILE:-}" != /dev/null \
  || -n "${PIP_EXTRA_INDEX_URL:-}" || -n "${PIP_FIND_LINKS:-}" ]]; then
  echo 'Use the Guard-only pip index, PIP_CONFIG_FILE=/dev/null, and no extra sources.' >&2
  exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
python3 -m pip download --disable-pip-version-check --no-cache-dir --no-deps \
  --only-binary=:all: --dest "$work" six==1.17.0
echo 'Takumi Guard PyPI: Guard-only configuration and fresh wheel download succeeded.'
