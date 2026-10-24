package kv

import (
	"context"
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var migrationConfigurationIndexKey = store.MigrationConfigurationIndexKey()

var ErrMigrationConfigurationChanged = errors.New("migration cleanup configuration changed; retry voter barriers")

// MigrationConfigurationIndex is absent on stores that have not observed a
// configuration through the upgraded apply loop. Such stores cannot attest a
// configuration-bound cleanup barrier.
func MigrationConfigurationIndex(ctx context.Context, st store.MVCCStore) (uint64, error) {
	state, err := readNamespaceConfiguration(ctx, st)
	return state.Index, err
}

func (f *kvFSM) AppliedConfigurationIndex() uint64 {
	return f.migrationConfigurationIndex.Load()
}

func (f *kvFSM) RecordAppliedConfiguration(index uint64) error {
	if f.backupFloorLoadErr != nil {
		return errors.Wrap(f.backupFloorLoadErr, "cannot record configuration over unreadable FSM control metadata")
	}
	if index == 0 || index <= f.AppliedConfigurationIndex() {
		return nil
	}
	if f.migrationNamespaceVersion.Load() != 0 {
		return errors.Wrap(ErrMigrationNamespaceProtocol, "active namespace protocol requires the full ordered configuration hook")
	}
	// Reuse the replicated store frontier; configuration bookkeeping must not
	// issue a wall-clock timestamp or change user transaction admission.
	ts := max(f.store.LastCommitTS(), 1)
	value := binary.BigEndian.AppendUint64(nil, index)
	// A crash must not replay an older rejected cleanup against a future
	// durable epoch. Bundle the epoch with the same applied-index boundary.
	if err := f.store.ApplyMutationsRaftAt(context.Background(), []*store.KVPairMutation{{
		Op: store.OpTypePut, Key: migrationConfigurationIndexKey, Value: value,
	}}, nil, ts, ts, index); err != nil {
		return errors.WithStack(err)
	}
	f.migrationConfigurationIndex.Store(index)
	f.migrationNamespaceMembers = nil // Index-only hooks cannot prove apply membership.
	return nil
}

func (f *kvFSM) reloadMigrationConfiguration(ctx context.Context) error {
	state, err := readNamespaceConfiguration(ctx, f.store)
	if err == nil {
		f.migrationConfigurationIndex.Store(state.Index)
		f.migrationNamespaceVersion.Store(state.Version)
		f.migrationNamespaceMembers = state.Members
	}
	return err
}

func (f *kvFSM) verifyMigrationCleanupConfiguration(ctx context.Context, req *pb.CleanupMigrationRequest) error {
	if req.GetRequireConfigurationIndex() {
		if req.GetExpectedConfigurationIndex() == 0 || req.GetExpectedConfigurationIndex() != f.AppliedConfigurationIndex() {
			return errors.WithStack(ErrMigrationConfigurationChanged)
		}
		return nil
	}
	return f.rejectUnboundSourceCleanup(ctx, req.GetJobId())
}

func (f *kvFSM) rejectUnboundSourceCleanup(ctx context.Context, jobID uint64) error {
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, state := range states {
		if state.JobID == jobID && state.Armed && (state.SourceWriteFence || state.SourceReadFence || state.TrackWrites) {
			return errors.WithStack(ErrMigrationConfigurationChanged)
		}
	}
	return nil
}
