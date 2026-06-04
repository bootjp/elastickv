package kv

import "github.com/bootjp/elastickv/internal/raftengine"

// Round-4 P2 #2 fix: kvFSM must directly implement
// raftengine.AppliedIndexReader so the cold-start skip gate's
// type-assertion (Branch 3) succeeds.
var _ raftengine.AppliedIndexReader = (*kvFSM)(nil)
var _ raftengine.AppliedIndexWriter = (*kvFSM)(nil)
