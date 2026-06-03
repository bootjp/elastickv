package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// RaftPayloadWrapper transforms an FSM payload into a §4.2 raft
// envelope just before submission to the engine. The Stage 3 default
// (when no wrapper is installed on a coordinator) is identity —
// payloads pass through unchanged. Stage 6's cluster-flag pipeline
// installs an active wrapper, sourced from the sidecar's currently-
// active raft DEK and a writer-registry-backed nonce factory.
//
// Implementations MUST be safe to call concurrently from many
// goroutines: the coordinator may invoke this on every concurrent
// proposal. Encryption-state transitions (Phase 1 → Phase 2 cutover)
// publish a fresh closure via atomic.Pointer so the wrapper
// observes one consistent (cipher, key_id, nonce_factory) tuple per
// call.
type RaftPayloadWrapper func(payload []byte) ([]byte, error)

// applyRaftPayloadWrap is a coordinator-internal helper that runs
// the configured wrapper, or returns the payload verbatim when no
// wrapper is installed. Centralised so every coordinator call site
// gates payload bytes through the same path — a future audit can
// grep for engine.Propose / proposer.Propose and verify each goes
// through this helper or has an explicit "intentionally cleartext"
// reason.
func applyRaftPayloadWrap(wrap RaftPayloadWrapper, payload []byte) ([]byte, error) {
	if wrap == nil {
		return payload, nil
	}
	wrapped, err := wrap(payload)
	if err != nil {
		return nil, errors.Wrap(err, "kv: raft payload wrap")
	}
	return wrapped, nil
}

// wrappedProposer adapts a raftengine.Proposer so every Propose call
// transparently runs the configured RaftPayloadWrapper. Used by
// transaction.go's applyRequests path and by future code that needs
// to share a single Proposer between callers some of whom wrap and
// some of whom do not — the wrapping decision lives with the
// constructed proposer, not the call site.
//
// When wrap is nil the wrappedProposer is functionally identical to
// the inner proposer; this keeps the Stage 3 default a no-op.
type wrappedProposer struct {
	inner raftengine.Proposer
	wrap  RaftPayloadWrapper
}

// newWrappedProposer returns the inner proposer untouched when the
// wrapper is nil, so the cipher-disabled path stays a single
// pointer assignment.
func newWrappedProposer(inner raftengine.Proposer, wrap RaftPayloadWrapper) raftengine.Proposer {
	if wrap == nil {
		return inner
	}
	return &wrappedProposer{inner: inner, wrap: wrap}
}

func (p *wrappedProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	wrapped, err := applyRaftPayloadWrap(p.wrap, data)
	if err != nil {
		return nil, err
	}
	res, err := p.inner.Propose(ctx, wrapped)
	if err != nil {
		return nil, errors.Wrap(err, "kv: wrapped propose")
	}
	return res, nil
}

// ProposeAdmin applies the configured RaftPayloadWrapper before
// forwarding the payload to the inner ProposeAdmin path. The wrap
// layer is NOT a barrier-exemption concern: it exists so that
// every entry landing at `index > raftEnvelopeCutoverIndex` carries
// an AEAD envelope the §6.3 strict-`>` apply hook can unwrap.
// Admin entries (BootstrapEncryption, RotateDEK,
// RegisterEncryptionWriter, etc.) committed after the cutover are
// no different from user data in that regard — a cleartext admin
// entry above cutover would halt the apply loop on unwrap-failure,
// not be silently passed through.
//
// The lone exception is the EnableRaftEnvelope cutover marker
// itself, which sits exactly at `index == cutover` and must remain
// cleartext for strict-`>` dispatch to leave it alone. That marker
// is proposed via a raw engine reference (adapter/encryption_admin.go
// holds the engine directly as s.proposer), not via the
// wrappedProposer — so this wrap path is never on its way.
//
// ProposeAdmin's only divergence from Propose lives in Stage
// 6E-2d's §7.1 quiescence-barrier check, which is installed on
// Propose only.
func (p *wrappedProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	wrapped, err := applyRaftPayloadWrap(p.wrap, data)
	if err != nil {
		return nil, err
	}
	res, err := p.inner.ProposeAdmin(ctx, wrapped)
	if err != nil {
		return nil, errors.Wrap(err, "kv: wrapped propose-admin")
	}
	return res, nil
}
