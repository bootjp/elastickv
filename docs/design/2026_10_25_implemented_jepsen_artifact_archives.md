# Jepsen artifact archives

Status: Implemented
Author: bootjp
Date: 2026-10-25

## Goal and evidence

Keep Jepsen 0.3.15 histories downloadable from CI. Run 40116891186 passes
all 294 unit tests but upload-artifact rejects the colons in the new ISO
timestamp directory names. This upload failure also skips later workloads.

## Decisions

Archive result directories as tar.gz files before uploading them. Preserve
original filenames, history bytes, and symlinks inside the archive; do not rename
or delete original evidence. Keep the seal source SHA text alongside its archive.
Use the same helper for per-push and scheduled result uploads. Failure artifacts
may contain the whole store so no history is omitted. Missing result directories
should produce no archive and retain the workflow's missing-file handling.

Only test fixtures and evidence packaging change. No workload, checker,
consistency criterion, storage implementation, or timeout is relaxed.

## Milestones and validation

- [x] Add a regression covering colon-containing directories and preserved bytes (`scripts/test-archive-jepsen-store.sh`).
- [x] Package all directly uploaded Jepsen store paths in both CI workflows (`scripts/archive-jepsen-store.sh` in `jepsen-test.yml` and `jepsen-test-scheduled.yml`).
- [x] Pass archive regression, actionlint, and encrypted/cleartext Jepsen CI (Jepsen Test is green on `main`, e.g. run 40657220651).

The earlier fixture change reproduces the original failure and passes all 294
tests locally and in CI. This follow-up addresses the next failure revealed by
that fix. The existing dependency-update PR carries the focused compatibility
fixes so its full CI can execute.
