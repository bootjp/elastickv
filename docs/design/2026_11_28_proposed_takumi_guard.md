# Dependency downloads through Takumi Guard

Status: Proposed
Author: bootjp
Date: 2026-11-28

## Scope

Route public Go module downloads in CI, Go image builds, and Jepsen guest
builds through `https://golang.flatt.tech`. Document the same opt-in setup
for contributors. Do not change application code, dependency versions,
production deployments, or machine-wide developer settings.

Extend the same anonymous protection to the admin frontend's npm downloads
and the deployment controller's pip downloads, including portable GitOps
workflow examples. Keep all existing dependency versions and integrity data.

## npm and Python boundaries

- Commit `web/admin/.npmrc` pointing to `https://npm.flatt.tech/`. Copy it
  alongside the manifests before the Docker SPA stage's `npm ci`; runner
  settings do not propagate into the build stage. Set the CI registry before
  dependency installation. Existing npmjs lockfile URLs are supported by
  the provider; verify a cold install rather than updating dependencies.
- Configure pip's index as `https://pypi.flatt.tech/simple/`. Disable pip
  configuration-file loading with `PIP_CONFIG_FILE=/dev/null` and clear
  extra-index and find-links environment settings so an additional source
  cannot silently bypass the Guard. Apply this in CI, controller images,
  and the published workflow examples. Local setup remains opt-in.
- Check npm with an empty cache: fetch a known-good package and require
  HTTP 403 for `@panda-guard/test-malicious`, without executing scripts or
  changing the frontend's lockfile. Check pip's effective boundary settings
  and download a known-good wheel without using the installed/cache state.
  The provider has no documented PyPI sentinel yet, so do not claim that
  Python's malicious-package rejection has been demonstrated.
- Public artifact hosts reached through Guard-issued redirects are expected;
  direct URL, Git, local, cached, and intentionally private dependencies are
  outside this registry integration. Publishing requires an explicit public
  registry and is outside this change.

These boundaries were reviewed before implementation. They add no credentials,
permissions, package updates, runtime behavior, or production mutations.

## Decisions

- Set `GOPROXY` directly, using the provider's documented Go protocol
  integration. No extra Action, account, token, or OIDC permission is needed
  for anonymous blocking.
- Specify only that URL: neither `,direct`, `|direct`, nor another proxy
  may silently bypass a block or outage. Retain Go's checksum verification.
- Set workflow-level environment before toolchain setup and Go commands.
  Set image-stage environment separately; runner environment does not
  propagate into Docker builds. Jepsen SSH sessions also need a persistent
  Go environment setting rather than only a container environment variable.
- Preserve existing caches. Cached modules and local replacements are not
  fetched again and are not retrospectively scanned by this integration.
  Use an isolated empty cache for the blocking check.
- Use `GOPRIVATE` only for intentionally private dependencies. This public
  repository does not need a private-module exception.

## Validation

Check workflow syntax and changed shell syntax. With a fresh temporary
module cache, verify a known-good public module downloads and the provider's
harmless test module `github.com/flatt-security/hola-takumi-go@v0.1.0`
fails with HTTP 403 from the Guard URL. An unrelated network error must not
count as a successful blocking test. Keep the negative check in the test
workflow so future changes cannot silently bypass the configured proxy.

## Risks and recovery

A Guard outage prevents new module downloads. Existing cache entries remain
usable. Reverting the configuration restores the previous download policy;
there is no data or protocol migration. Anonymous mode does not provide
authenticated download history or infection notifications. This is a
known-malicious-module filter, not a replacement for vulnerability scanning
or code review.

## Review

The configuration boundaries were reviewed before implementation: runner,
Docker build stage, and SSH guest settings must be covered independently.
No runtime, storage, replication, or data-format path changes. Acceptance
requires both positive download and precise negative-block evidence.

## Local implementation and verification

Prepared in the working tree; not merged or deployed. Eight Go workflows
set the proxy at workflow scope. Four Go image stages and the Vagrant
controller provisioner set it independently. The test workflow's `rest`
shard runs the empty-cache positive/negative check. Contributor instructions
cover local use, cache limitations, checksum verification, and private modules.

Verified on 2026-11-28:

- The cold-cache canary downloaded `github.com/pkg/errors@v0.9.1` and
  rejected `hola-takumi-go@v0.1.0` with HTTP 403 from the Guard URL.
- `go mod download` with a separate empty cache and the Guard-only proxy
  fetched the repository's dependencies and `go1.27.4` successfully.
- The canary rejected a configuration pointing to `proxy.golang.org`.
- Review regression tests reproduce saved private-module exceptions and
  an existing Go binary outside `/usr/local/go/bin`. Both failed before
  the fixes: use nonempty `none` overrides in the canary and the detected
  Go path in provisioning. The tests run offline in the `rest` shard and
  leave the caller's Go settings unchanged.
- `shellcheck` for the canary, `bash -n` for both changed shell scripts,
  and `git diff --check` passed.
- `actionlint` passed for all eight changed workflows after excluding
  existing `virtualbox` runner-label and SC2086 warnings in `jepsen.yml`.

Review lenses: no data-loss or consistency path changes; no concurrency or
replication changes; cold downloads now depend on Guard availability while
existing caches remain usable; the network canary verifies success and an
explicit block. Full image builds and GitHub-hosted execution have not been
run locally. Production data and backups are outside this change.

## Reference

[Provider release note](https://shisho.dev/docs/ja/r/202605-takumi-guard-golang/).
[npm setup](https://shisho.dev/docs/ja/t/guard/quickstart/npm/),
[PyPI setup](https://shisho.dev/docs/ja/t/guard/quickstart/pypi/), and
[sentinel availability](https://shisho.dev/docs/t/guard/features/admin-deployment/).
