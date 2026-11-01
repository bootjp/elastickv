package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (f *kvFSM) ApplyChangesAdmission(payload []byte) (bool, error) {
	return BootstrapApplyChangesAdmission(context.Background(), f.store, f.NativeGroupIdentity(), payload)
}

// BootstrapApplyChangesAdmission distinguishes a NEW activation from a
// deterministic already-active rejection. Repeated activation must not revoke
// healthy sessions when it cannot change P. Call only on the serial apply loop.
func BootstrapApplyChangesAdmission(ctx context.Context, st store.MVCCStore, identity raftbootstrap.Identity, payload []byte) (bool, error) {
	_, matched, err := raftbootstrap.ParseActivationCommand(payload)
	if err != nil || !matched {
		return false, errors.WithStack(err)
	}
	metadata, ok := st.(store.RaftControlMetadataStore)
	if !ok {
		return false, errors.WithStack(ErrGuardedBootstrapApply)
	}
	key, err := BootstrapPolicyMetadataKey(identity)
	if err != nil {
		return false, err
	}
	raw, err := metadata.ReadRaftControlMetadata(ctx, key)
	if errors.Is(err, store.ErrKeyNotFound) {
		return true, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	policy, err := raftbootstrap.ParsePolicyRecord(raw)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if policy.Identity != identity {
		return false, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return false, nil
}
