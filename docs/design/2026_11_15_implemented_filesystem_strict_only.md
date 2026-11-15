# Filesystem strict-only mounts

## Decision

Retire the supported legacy mount configuration. Every production mount created
by `fsmount.MountService` registers an operation journal session and uses
`WithStrictMode`. `Config` has no mode switch. The server's
`--filesystemStrict` argument is removed, including the positive spelling.
All old spellings fail argument parsing before any service starts. Remove the
argument from existing launch commands. The filesystem remains
optional: an empty mount path still disables it.

Low-level adapter unit fixtures may exercise unjournaled Core methods to pin
error mapping and historical unknown-outcome behavior; they are not a supported
server/mount configuration.

## Recovery and migration

Owner-session recovery stays explicit. The old owner must have stopped before
`--filesystemStrictRecover` may fence and reconcile its session. Healthy peers
must not be recovered by a heal-all operation. Normal CI and managed Jepsen
startup preserve this ordering. There is no timeout-based clearing of unknown
mutation barriers or write replay under a new identity.

Existing file data is retained. Operators stop old mounts cleanly, reconcile
any unresolved legacy outcome, keep data directories and encryption keys, and
start the strict-only binary with a unique stable mount owner. The procedure
is documented in `docs/filesystem.md`.

## Harnesses and evidence

Normal encrypted/cleartext CI starts default-mode mounts. Scheduled Filesystem,
SQLite and single/two-group benchmarks use the same mandatory mode. Remove the
`strict-mode` workflow input and legacy matrix entries; reject explicit retired
workload names and `benchmark-fuse.py --mode legacy`. The node-crash helper
accepts omitted mode flags and enables recovery only after stopping the node.

Regression tests cover the removed CLI argument (bare/true/false), journal registration with
zero-valued mode-free config, stopped-owner argument construction, benchmark
labels and legacy rejection, and default-mode crash restart arguments.

This is a mount-policy change, not a claim that every historical rollout gate
or crash boundary is closed. SQLite WAL/shared-database locking and the limits
in the linked historical strict rollout evidence remain unchanged. Current PR
validation records CI, fault stress and benchmark artifacts on its exact head.
