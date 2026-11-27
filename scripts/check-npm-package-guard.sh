#!/usr/bin/env bash
# Check the real registry without touching dependencies, locks, or user caches.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root/web/admin"
if [[ "$(npm config get registry)" != 'https://npm.flatt.tech/' ]]; then
  echo 'npm registry must be https://npm.flatt.tech/.' >&2
  exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
touch "$work/user.npmrc" "$work/global.npmrc"
cd "$work"
npm_args=(--registry=https://npm.flatt.tech/ --cache="$work/cache"
  --userconfig="$work/user.npmrc" --globalconfig="$work/global.npmrc"
  --fetch-retries=0 --ignore-scripts --no-audit --no-fund)

npm "${npm_args[@]}" pack is-number@7.0.0 --pack-destination="$work"
if npm "${npm_args[@]}" pack @panda-guard/test-malicious --pack-destination="$work" >blocked.log 2>&1; then
  echo 'Guard unexpectedly accepted the blocked npm test package.' >&2
  exit 1
fi
if ! grep -Fq 'npm.flatt.tech/' blocked.log || ! grep -Fq 'E403' blocked.log; then
  cat blocked.log >&2
  echo 'Expected an explicit HTTP 403 from Takumi Guard.' >&2
  exit 1
fi
echo 'Takumi Guard npm: public package downloaded; blocked test package rejected (403).'
