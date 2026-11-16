package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const raftEncodeTTLInlineCapability byte = 0x2e

var ttlInlineCapabilityKey = []byte(store.ReplicatedControlPrefix + "absence-capability")

var ErrTTLInlineCapabilityMarker = errors.New("TTL-inline capability marker invalid")
var ErrTTLInlineCapabilityDisabled = errors.New("TTL-inline capability marker unsupported by direct Raft factory")

// WithTTLInlineCapabilityDisabled excludes a legacy direct Raft factory from
// activation and halts it if a capability marker is recovered or applied.
func WithTTLInlineCapabilityDisabled() FSMOption {
	return func(f *kvFSM) { f.ttlInlineCapabilityDisabled = true }
}

// The configuration is the exact native pre-entry image, including joint roles.
// Index is the last applied ConfChange index, not this marker's Raft index.
type ttlInlineCapabilityMarker struct {
	Version        uint8    `json:"version"`
	Index          uint64   `json:"index"`
	Voters         []uint64 `json:"voters"`
	VotersOutgoing []uint64 `json:"voters_outgoing"`
	Learners       []uint64 `json:"learners"`
	LearnersNext   []uint64 `json:"learners_next"`
	AutoLeave      bool     `json:"auto_leave"`
}

func decodeTTLInlineCapabilityMarker(raw []byte) (ttlInlineCapabilityMarker, error) {
	var marker ttlInlineCapabilityMarker
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return marker, errors.Join(ErrTTLInlineCapabilityMarker, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return marker, errors.WithStack(ErrTTLInlineCapabilityMarker)
	}
	if marker.Version != 1 || marker.Index == 0 || len(marker.Voters) == 0 ||
		!validCapabilityIDs(marker.Voters) || !validCapabilityIDs(marker.VotersOutgoing) ||
		!validCapabilityIDs(marker.Learners) || !validCapabilityIDs(marker.LearnersNext) {
		return marker, errors.WithStack(ErrTTLInlineCapabilityMarker)
	}
	return marker, nil
}

func validCapabilityIDs(ids []uint64) bool {
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			return false
		}
		if _, exists := seen[id]; exists {
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}

func sameCapabilityIDs(a, b []uint64) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func (m ttlInlineCapabilityMarker) matches(native raftengine.NativeConfiguration) bool {
	return m.Index == native.Index && m.AutoLeave == native.AutoLeave &&
		sameCapabilityIDs(m.Voters, native.Voters) &&
		sameCapabilityIDs(m.VotersOutgoing, native.VotersOutgoing) &&
		sameCapabilityIDs(m.Learners, native.Learners) &&
		sameCapabilityIDs(m.LearnersNext, native.LearnersNext)
}

// TTLInlineCapabilityActive is safe for coordinator reads on other goroutines.
func (f *kvFSM) TTLInlineCapabilityActive() bool {
	return !f.ttlInlineCapabilityDisabled && f.ttlInlineCapability.Load()
}

// TTLInlineCapabilityRecorded reports whether a valid marker is durable,
// regardless of whether current configuration proof permits activation.
// It is safe to read from another goroutine.
func (f *kvFSM) TTLInlineCapabilityRecorded() bool {
	return !f.ttlInlineCapabilityDisabled && f.ttlInlineCapabilityRecorded.Load()
}

// IsTTLInlineCapabilityMarkerPayload classifies the cleartext FSM command for
// native Raft admission, including a marker not yet applied by the FSM.
func (f *kvFSM) IsTTLInlineCapabilityMarkerPayload(payload []byte) bool {
	return len(payload) != 0 && payload[0] == raftEncodeTTLInlineCapability
}

// InstallTTLInlineRecoveryProof accepts only the startup owner's independently
// verified directory fence and recovered native configuration. It must run
// before serving begins; a later snapshot restore discards this proof.
func (f *kvFSM) InstallTTLInlineRecoveryProof(ctx context.Context, fenced bool, native raftengine.NativeConfiguration) error {
	f.ttlInlineFenceReady.Store(false)
	native.Voters = slices.Clone(native.Voters)
	native.VotersOutgoing = slices.Clone(native.VotersOutgoing)
	native.Learners = slices.Clone(native.Learners)
	native.LearnersNext = slices.Clone(native.LearnersNext)
	native.Members = slices.Clone(native.Members)
	f.ttlInlineRecoveryProof = func() (bool, raftengine.NativeConfiguration) { return fenced, native }
	if err := f.reloadTTLInlineCapability(ctx); err != nil {
		f.InvalidateTTLInlineRecoveryProof()
		return err
	}
	f.ttlInlineFenceReady.Store(fenced && !f.ttlInlineCapabilityDisabled)
	return nil
}

// TTLInlineFenceReady reports whether the startup owner's local proof is still
// valid. It is not a certificate for other members or marker activation.
func (f *kvFSM) TTLInlineFenceReady() bool {
	return !f.ttlInlineCapabilityDisabled && f.ttlInlineFenceReady.Load()
}

// InvalidateTTLInlineRecoveryProof closes capability admission when the
// recovered cut no longer describes the live native state.
func (f *kvFSM) InvalidateTTLInlineRecoveryProof() {
	f.ttlInlineFenceReady.Store(false)
	f.ttlInlineRecoveryProof = nil
	f.ttlInlineCapability.Store(false)
}

func (f *kvFSM) reloadTTLInlineCapability(ctx context.Context) error {
	f.ttlInlineCapability.Store(false)
	if f.store == nil {
		return nil
	}
	raw, err := f.store.GetAt(ctx, ttlInlineCapabilityKey, math.MaxUint64)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "read TTL-inline capability")
	}
	if f.ttlInlineCapabilityDisabled {
		return errors.WithStack(ErrTTLInlineCapabilityDisabled)
	}
	marker, err := decodeTTLInlineCapabilityMarker(raw)
	if err != nil {
		return err
	}
	f.ttlInlineCapabilityRecorded.Store(true)
	// A durable FSM record cannot prove the local directory is downgrade-fenced
	// or that native recovery selected this same ConfState. No production verifier
	// exists yet, so restart and snapshot restore stay closed.
	if f.ttlInlineRecoveryProof != nil {
		fenced, native := f.ttlInlineRecoveryProof()
		if fenced && marker.matches(native) {
			f.ttlInlineCapability.Store(true)
		}
	}
	return nil
}

func (f *kvFSM) applyTTLInlineCapability(ctx context.Context, raw []byte) any {
	if f.ttlInlineCapabilityDisabled {
		return haltErr(errors.WithStack(ErrTTLInlineCapabilityDisabled))
	}
	marker, err := decodeTTLInlineCapabilityMarker(raw)
	if err != nil {
		return haltErr(err)
	}
	if f.pendingNativeConfiguration == nil || f.pendingApplyIdx == 0 {
		return haltErr(errors.WithStack(ErrTTLInlineCapabilityMarker))
	}
	if !marker.matches(*f.pendingNativeConfiguration) {
		return nil // An intervening ConfChange makes this proposal stale.
	}
	writer, ok := f.store.(store.ReplicatedControlStore)
	if !ok {
		return haltErr(errors.Wrap(ErrTTLInlineCapabilityMarker, "store lacks replicated control state"))
	}
	if _, err := writer.ApplyReplicatedControlState(ctx, ttlInlineCapabilityKey, raw, f.pendingApplyIdx); err != nil {
		return haltErr(errors.Wrap(err, "persist TTL-inline capability"))
	}
	if err := f.reloadTTLInlineCapability(ctx); err != nil {
		return haltErr(err)
	}
	return nil
}
