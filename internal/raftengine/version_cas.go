package raftengine

import (
	"bytes"
	"context"
	"errors"
)

var ErrVersionCASUnavailable = errors.New("version CAS admission unavailable")

// VersionCASFrame begins with an opcode for which preceding KV binaries
// halt rather than advance their applied index.
const VersionCASFrame = "\x06CAS\x01"

const MigrationVersionIdentityFrame = "\x06MVI\x01"

func IsVersionCASRaftPayload(payload []byte) bool {
	return bytes.HasPrefix(payload, []byte(VersionCASFrame))
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
