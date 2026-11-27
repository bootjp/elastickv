#!/usr/bin/env bash
# Exercise the real proxy with isolated state; never touch the caller's cache.
set -euo pipefail

if [[ "$(go env GOPROXY)" != 'https://golang.flatt.tech' ]]; then
  echo 'GOPROXY must be https://golang.flatt.tech without a fallback.' >&2
  exit 1
fi

work=$(mktemp -d)
trap 'chmod -R u+w "$work"; rm -rf "$work"' EXIT
export GOMODCACHE="$work/modules"
export GOPATH="$work/gopath"
export GOWORK=off
export GO111MODULE=on
# A private-module exception must not bypass this public-module canary.
export GOPRIVATE=none GONOPROXY=none GONOSUMDB=none
cd "$work"
go mod init guard-check

# A proxy/network outage must not masquerade as successful blocking.
go mod download github.com/pkg/errors@v0.9.1
if go mod download github.com/flatt-security/hola-takumi-go@v0.1.0 >blocked.log 2>&1; then
  echo 'Guard unexpectedly accepted the blocked test module.' >&2
  exit 1
fi
if ! grep -Fq 'https://golang.flatt.tech/github.com/flatt-security/hola-takumi-go/@v/v0.1.0' blocked.log \
  || ! grep -Fq '403 Forbidden' blocked.log; then
  cat blocked.log >&2
  echo 'Expected an explicit HTTP 403 from Takumi Guard.' >&2
  exit 1
fi
echo 'Takumi Guard: public download succeeded; blocked test module rejected (403).'
