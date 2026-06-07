package kv

import "github.com/bootjp/elastickv/internal/raftengine"

// Round-4 P2 #2 fix: kvFSM must directly implement
// raftengine.AppliedIndexReader so the cold-start skip gate's
// type-assertion (Branch 3) succeeds.
var _ raftengine.AppliedIndexReader = (*kvFSM)(nil)
var _ raftengine.AppliedIndexWriter = (*kvFSM)(nil)

// Branch 3: kvFSM must directly implement
// raftengine.SnapshotHeaderApplier so applyHeaderStateOnSkip in
// wal_store.go can deliver the snapshot header state (HLC ceiling +
// Stage 8a cutover) to the FSM without running the multi-GiB body
// restore. A future rename or signature drift fails the build
// immediately rather than silently degrading the skip optimisation.
var _ raftengine.SnapshotHeaderApplier = (*kvFSM)(nil)

// Codex P1 #934 round 7: kvFSM must directly implement
// raftengine.VolatileEntryClassifier so the engine's cold-start
// duplicate-entry guard can distinguish HLC lease entries (volatile,
// must replay) from KV/MVCC duplicates (idempotency-violating, must
// skip). A future rename of IsVolatileOnlyPayload or accidental
// removal of the raftEncodeHLCLease classification would otherwise
// silently re-introduce the post-snapshot lease replay loss.
var _ raftengine.VolatileEntryClassifier = (*kvFSM)(nil)
