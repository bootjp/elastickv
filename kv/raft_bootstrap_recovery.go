package kv

import (
	"context"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// BootstrapPolicyMetadataKey is the protected, identity-scoped FSM policy key.
// It is not a filesystem policy-cache key or an ordinary MVCC key.
func BootstrapPolicyMetadataKey(identity raftbootstrap.Identity) ([]byte, error) {
	if identity.Cluster == (raftbootstrap.UUID{}) {
		return nil, errors.WithStack(raftbootstrap.ErrInvalid)
	}
	key := append([]byte("P/"), identity.Cluster[:]...)
	return binary.BigEndian.AppendUint64(key, identity.Group), nil
}

type durableBootstrapIndex interface {
	LastAppliedIndex() (uint64, bool, error)
}

// LoadAppliedBootstrapPolicy must run inside the native startup owner's
// post-replay admission callback, before concurrent applies/background writers.
// applied comes from verified native recovery, never a caller RPC or local cache.
// This reads the FSM projection; the caller still owes native WAL identity,
// configuration/manifest and transition-journal reconciliation.
func LoadAppliedBootstrapPolicy(ctx context.Context, st store.MVCCStore, identity raftbootstrap.Identity, applied uint64) (raftbootstrap.PolicyRecord, error) {
	var zero raftbootstrap.PolicyRecord
	metadata, index, err := bootstrapRecoveryStores(st, applied)
	if err != nil {
		return zero, err
	}
	if err := verifyBootstrapAppliedIndex(index, applied); err != nil {
		return zero, err
	}
	key, err := BootstrapPolicyMetadataKey(identity)
	if err != nil {
		return zero, err
	}
	raw, err := metadata.ReadRaftControlMetadata(ctx, key)
	if err != nil {
		return zero, errors.Wrap(err, "read applied bootstrap policy")
	}
	policy, err := raftbootstrap.ParsePolicyRecord(raw)
	if err != nil {
		return zero, errors.Wrap(err, "decode applied bootstrap policy")
	}
	if policy.Identity != identity || !bootstrapPolicyWithinApplied(policy, applied) {
		return zero, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	if err := verifyBootstrapAppliedIndex(index, applied); err != nil {
		return zero, err
	}
	return policy, nil
}

func bootstrapRecoveryStores(st store.MVCCStore, applied uint64) (store.RaftControlMetadataStore, durableBootstrapIndex, error) {
	metadata, ok := st.(store.RaftControlMetadataStore)
	index, indexed := st.(durableBootstrapIndex)
	if !ok || !indexed || applied == 0 {
		return nil, nil, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return metadata, index, nil
}

func verifyBootstrapAppliedIndex(st durableBootstrapIndex, applied uint64) error {
	index, known, err := st.LastAppliedIndex()
	if err != nil {
		return errors.WithStack(err)
	}
	if !known || index != applied {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

func bootstrapPolicyWithinApplied(policy raftbootstrap.PolicyRecord, applied uint64) bool {
	if policy.ActivationIndex > applied || policy.LastConfigurationIndex > applied {
		return false
	}
	if policy.Reservation != nil && policy.Reservation.ID.ReserveIndex > applied {
		return false
	}
	for _, operation := range policy.TerminalOperations {
		if operation.ID.ReserveIndex > applied {
			return false
		}
	}
	return true
}
