# Encrypted restart after Raft compaction

## Failure

The sidecar's `raft_applied_index` advances only on sidecar-mutating
encryption entries. Ordinary writes and writer registrations do not advance
it. A healthy encrypted cluster can therefore retain a sidecar at index 6
while the FSM has applied index 877 and its Raft prefix has been compacted.
The startup gap scanner cannot inspect `(6, 877]` and correctly refuses to
guess whether the missing prefix contains another encryption change.

## Snapshot Witness

After a successful bootstrap or rotation apply, the FSM captures the last
sidecar-mutating index and a SHA-256 digest of the replicated sidecar state.
The digest covers wrapped keys, key purposes, active keys, both envelope
cutovers, and the sidecar index. It excludes replica-local epochs, creation
times, and the sidecar serialization version. It is a comparison witness,
not a replacement for authenticated storage values or an epoch authority.

The witness is an `EKVENCP1` extension of the existing length-delimited v2
FSM header. Older v2 readers can skip the extension. A full physical restore
preserves it; the cold-start skip path installs it only after full payload
CRC and Raft-token validation. A newly captured snapshot reuses the witness
restored from its predecessor, rather than trusting a new disk-sidecar read
to rewrite the history represented by that snapshot.

The same witness is synchronously persisted as reserved operational FSM
metadata after each successful encryption apply, before the apply handler
returns. It does not issue a user timestamp or nonce. When the cold-start
skip gate retains a newer durable FSM, it can retain that FSM's newer
witness only through its durable applied index. A checkpoint ahead of that
index must be reconciled by normal WAL replay; new snapshot capture refuses
to pair an unreconciled durable record with an older header. Full restore
requires its body record to match the header. Native physical snapshots
preserve the record and the offline decoder classifies it as operational
metadata rather than an MVCC user version.

The startup guard opens the engine's installed snapshot, not an arbitrary
file in the snapshot directory. Token index, Raft metadata index, token CRC,
payload CRC, checkpoint index, sidecar state digest, and envelope cutover
must agree. Only then does that snapshot cover the compacted prefix. The
snapshot is selected and opened under `snapshotMu`; full checksum verification
and reads then use that same descriptor without holding up snapshot work.
Removing the old path after it has been opened cannot invalidate that reader.
The
retained suffix remains subject to the original gap scan; encrypted entries
are authenticated and decrypted before inspecting their opcode. Missing
keys or invalid authentication tags are startup failures, never "no hit".

A newer snapshot can compact the retained suffix between snapshot selection
and its scan. Only `ErrCompacted` from that scan triggers a retry against the
currently installed, fully reverified snapshot. The verified covered prefix
must advance, and at most three scans are attempted. Absent or inconsistent
proof, CRC/authentication errors, a nonadvancing snapshot, or continuous
compaction past that bound still refuse startup. No compacted history is
treated as harmless merely because a scan was overtaken.

The membership/collision, pre-bump epoch rollback, post-replay epoch rollback,
writer-registration, and physical snapshot recovery paths remain enabled.
The witness deliberately cannot authorize a nonce epoch reset.

## Compatibility and Scope

Older binaries do not recognize the reserved operational key
`_meta_encryption_snapshot_checkpoint`. The ability to skip the v2 header
extension does not establish compatibility with this physical body record.
Downgrading a store written by this version has not been validated as safe.
Mixed-version native physical snapshot transfer to an older follower is not
supported by this change: upgrade the receiving follower before allowing it
to install a snapshot produced by this version. Do not delete the checkpoint
record or bypass snapshot, sidecar, or epoch checks to enable a downgrade.

An old snapshot without a witness does **not** authorize skipping compacted
history. Such a node can still refuse startup and needs an independently
validated operational recovery path. This change does not manufacture
proof for already-compacted legacy data, silently resync a sidecar, remove
snapshot checks, or claim that the separate DynamoDB acknowledged-append
consistency anomaly is fixed. No change from PR #1286 is included.

## Verification

- The compacted-gap regression first failed with the original `(6, 877]`
  scan; the snapshot-witness guard makes the valid case pass.
- Negative controls retain refusal for stale sidecar/index, changed key
  material, future checkpoint, absent/truncated proof, retained rotation,
  snapshot verification error, mismatched token index/CRC, corrupt physical
  payload, invalid encrypted-tail tag, and a rolled-back local epoch.
- Snapshot witness tests cover full restore and the verified header-only
  skip path, including subsequent snapshot production.
- When the sidecar index equals or exceeds the engine's applied index, the
  gap guard does not reopen the snapshot. Engine restore still verifies its
  full CRC; any nonempty gap retains snapshot verification and suffix scans.
- `go test . ./kv ./internal/encryption ./internal/raftengine/etcd -count=1`
  passed. The encryption/snapshot/skip-gate selection also passed with `-race`.
- Scoped `golangci-lint` passed with Go 1.26.6 (0 issues).
- Local three-process encrypted workload, six verified leader transfers,
  snapshot interval 20, then stop/reopen of all three processes preserved
  all 28 items. This guard-specific run used an explicit HTTP leader map;
  it is not evidence for the separate no-map acceptance requirement.

## Caller Audit

`rg` audit covered all production readers of the FSM header (`kvFSM.Restore`,
`ParseSnapshotHeader`, `tsoFSM.Restore`, and startup cutover preflight), the
verified-header skip path, both encryption apply handlers, the startup gap
guard and scanner factory, and the scanner's sole per-entry predicate call.
The old three-result header API remains available to callers that do not
consume the extension. The pure `GuardSidecarBehindRaftLog` API is unchanged.
The no-gap fast path keeps the original sidecar index rather than advancing
it. Its sole production caller still invokes the pure gap guard afterward;
the engine's full-restore and header-only-skip CRC checks are unchanged.
The snapshot-gap retry helper has one production caller, the startup guard.
Each retry recomputes the gap from the unchanged sidecar and a freshly verified
installed snapshot; only the typed suffix-compaction error is retried. The
pure guard and all its other callers retain their original semantics.
