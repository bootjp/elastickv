# Bound SQLite Jepsen register histories

Status: Implemented (shipped in #1333; no implementation work follows)
Date: 2026-10-25

## Failure

Scheduled run 40168002264 failed only in SQLite on FUSE. Knossos returned
`:valid? :unknown` and `:cause :out-of-memory` for two independent keys;
their final reads passed. Key 1 had 18 writes, all indeterminate after
transient SQLite/FUSE I/O errors. Replaying that key alone took about a
minute; replaying its first 16 writes finished in seconds. The scheduled
SQLite workload currently allows 64 writes per key.

## Change

Limit each SQLite register to 16 writes and increase the number of keys from
16 to 64, retaining the requested 1024-write aggregate budget. The register
model, linearizability checker, and final-read coverage stay unchanged.
Keep the small pull-request SQLite probe at its existing six keys and
16-write limit.

## Verification

Add a regression check for the scheduled SQLite budget and integer overflow.
Run Jepsen unit tests, then execute the scheduled workflow with the same
inputs as the failed run. Confirm the SQLite checker and final reads pass.

## Risk

The schedule initializes 64 SQLite databases instead of 16 and finally reads
each through every mount. Check that setup and analysis fit within the
existing step timeout.
