package raftengine

import "hash/fnv"

// DeriveNodeID maps a member's string raft ID (--raftId) to the 64-bit
// node ID that identifies it cluster-wide: in Raft configuration, in
// peer sessions, and in the encryption nonce node prefix. It is FNV-64a
// of the ID, with 0 reserved for "no ID" and a zero hash mapped to 1.
//
// The derivation is part of the cluster's persisted identity, so it
// must never change. It lives in the engine-neutral package so callers
// outside the Raft backend do not need to import the concrete engine.
func DeriveNodeID(id string) uint64 {
	if id == "" {
		return 0
	}
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(id))
	nodeID := sum.Sum64()
	if nodeID == 0 {
		return 1
	}
	return nodeID
}
