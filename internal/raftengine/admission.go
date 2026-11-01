package raftengine

import (
	"context"
	"errors"
)

var ErrNativeAdmissionDenied = errors.New("native admission denied")

// NativeAdmission serializes native terminal mutations with process-wide
// admission. Implementations must not perform I/O or wait for a quorum in A.
type NativeAdmission interface {
	// BindProposal seals an owned payload into an existing operation H. It must
	// neither mint nor refresh H. Classification and hashing run outside A.
	BindProposal(ctx context.Context, group uint64, payload []byte) (context.Context, error)
	WithNative(ctx context.Context, group uint64, action func(NativeScope) error) error
	ValidateResult(ctx context.Context) error
}

type NativeScope interface {
	// Observe synchronously invalidates affected process-wide admission when
	// actual native authority changes. It must not call the session owner.
	Observe(NativeState)
	Check(ctx context.Context, request NativeRequest) error
	Invalidate()
}

type NativeState struct {
	Term               uint64
	Role               State
	LeaderID           uint64
	ConfigurationIndex uint64
}

type NativeOperation uint8

const (
	NativeProposal NativeOperation = iota + 1
	NativeReadIndex
	NativeMembership
	NativeTransfer
	NativePeerStep
)

// Payload is borrowed synchronously and must not be retained or mutated.
// PeerStep uses an owner-minted receipt's generation, not peer-supplied data.
type NativeRequest struct {
	Operation         NativeOperation
	Payload           []byte
	PayloadDigest     [32]byte
	SessionGeneration uint64
}
