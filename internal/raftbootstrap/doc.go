// Package raftbootstrap provides local durable bootstrap format primitives.
// It does not implement replicated P, policy activation, peer sessions, runtime
// admission, catalog initialization or native WAL/snapshot recovery.
//
// Startup integration contract:
//
//   - Inspect before engine creation. Absent is not proof of freshness; Legacy
//     requires an explicit accepted baseline and coordinated quiesced rollout.
//   - Recover and verify WAL HardState (term/vote), selected snapshot/log,
//     cluster/group identity, rollback lineage and authoritative P independently.
//     Pass only those verified results as Evidence. Policy.BaselineDigest is
//     the original accepted/seed configuration digest, not current ConfState.
//   - ProvisionedSeed requires explicit UUID/roster provisioning and proof of
//     no established identity or data. An intact established identity cannot
//     acquire seed authorization merely because local files are missing.
//   - Publish/re-publish with admission closed, even if Inspect returns Required.
//     Nil acknowledges both durable file publications only. Every error,
//     including after rename, keeps admission closed; retry must reconcile the
//     actual files. There is no ready flag and no unsynced fast-success path.
//   - Mirror authoritative activation/generation via Publish before opening
//     data. RepairRequired can restore the downgrade barrier from retained P
//     even with lost WAL, but NEVER restores lost voting history/registration.
//   - Unknown/corrupt/inconsistent records require explicit recovery outside
//     this primitive. Never clear them or initialize an empty established group.
//   - Keep a single startup publisher per directory, with no concurrent external
//     writers/replacements of that directory. NewStore's mutex is instance-local.
//
// Intent v1 uses big-endian encoding, no padding. Its frame is magic:8
// (EKVBI001), payloadLength:u32, payload, SHA-256(header+payload):32. The payload
// is version:u32, required-state:u32, cluster-UUID:16, group:u64,
// baseline-kind:u32, entry/snapshot/session minimums:3*u32, activation-index:u64,
// generation:u64, baseline-index:u64, AutoLeave:u8, four role sets (each a u32
// count followed by u64 IDs), then a u32 member count and member records
// (ID:u64, address-length:u32, address bytes, public-key-SHA256:32). Roles are
// voters/outgoing-voters/learners/learners-next in that order. IDs/roster are
// numerically sorted and unique; address bytes are preserved, not DNS-normalized.
// Every field is mandatory; unknown versions/kinds, duplicate/invalid records,
// noncanonical encodings, trailing bytes and files over 16 MiB are rejected.
// The checksum detects corruption, not authenticity; there are no secrets or
// log statements in this package. The marker is exactly etcd-bootstrap-v1\n.
package raftbootstrap
