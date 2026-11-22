package raftengine

import (
	"bytes"
	"context"
	"errors"
)

var ErrVersionCASUnavailable = errors.New("version CAS admission unavailable")

// VersionCASFrame requires routing evidence from the same Raft log as the CAS.
const VersionCASFrame = "\x06CAS\x02"

const versionCASFramePrefix = "\x06CAS"

const MigrationVersionIdentityFrame = "\x06MVI\x01"

func IsVersionCASRaftPayload(payload []byte) bool {
	return bytes.HasPrefix(payload, []byte(versionCASFramePrefix))
}

// RequiresVersionCASProof covers both the activation and subsequent CAS.
func RequiresVersionCASProof(payload []byte) bool {
	return IsVersionCASRaftPayload(payload) || bytes.HasPrefix(payload, []byte(MigrationVersionIdentityFrame))
}

type versionCASProofKey struct{}

type VersionCASProof struct {
	Term               uint64
	ConfigurationIndex uint64
}

func WithVersionCASProof(ctx context.Context, proof VersionCASProof) context.Context {
	return context.WithValue(ctx, versionCASProofKey{}, proof)
}

func VersionCASProposalProof(ctx context.Context) (VersionCASProof, bool) {
	proof, ok := ctx.Value(versionCASProofKey{}).(VersionCASProof)
	return proof, ok
}
