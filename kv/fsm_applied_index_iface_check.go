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
