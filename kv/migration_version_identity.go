package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var migrationVersionIdentityKey = []byte("migration-version-identity")
var migrationVersionIdentityValue = []byte{1}

// EncodeMigrationVersionIdentityActivation returns the Raft frame that makes
// future migration imports and promotions preserve CAS-visible versions.
func EncodeMigrationVersionIdentityActivation() []byte {
	return []byte(raftengine.MigrationVersionIdentityFrame)
}

// IsMigrationVersionIdentityActivation recognizes the replicated marker frame.
func IsMigrationVersionIdentityActivation(data []byte) bool {
	return bytes.HasPrefix(data, []byte(raftengine.MigrationVersionIdentityFrame))
}

// MigrationVersionIdentityEnabled reports whether the store has applied the
// replicated migration version identity marker.
func MigrationVersionIdentityEnabled(ctx context.Context, st store.MVCCStore) (bool, error) {
	metadata, ok := st.(store.RaftControlMetadataStore)
	if !ok {
		return false, nil
	}
	value, err := metadata.ReadRaftControlMetadata(ctx, migrationVersionIdentityKey)
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !bytes.Equal(value, migrationVersionIdentityValue) {
		return false, errors.New("invalid migration version identity marker")
	}
	return true, nil
}

func (f *kvFSM) applyMigrationVersionIdentityActivation(ctx context.Context, data []byte) any {
	if !bytes.Equal(data, []byte(raftengine.MigrationVersionIdentityFrame)) {
		return haltErr(errors.Mark(errors.New("invalid migration version identity frame"), ErrRaftEntryApply))
	}
	err := f.persistMigrationVersionIdentity(ctx)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "kv/fsm: activate migration version identity"))
	}
	return nil
}

func (f *kvFSM) persistMigrationVersionIdentity(ctx context.Context) error {
	applier, ok := f.store.(store.RaftEntryApplier)
	if !ok || f.pendingApplyIdx == 0 {
		return errors.WithStack(store.ErrRaftControlScopeRequired)
	}
	metadata, ok := f.store.(store.RaftControlMetadataStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	err := applier.ApplyRaftEntry(ctx, f.pendingApplyIdx, func(ctx context.Context) error {
		replayed, err := store.StageRaftEntryProgress(ctx, f.pendingApplyIdx)
		if err != nil || replayed {
			return errors.WithStack(err)
		}
		enabled, err := MigrationVersionIdentityEnabled(ctx, f.store)
		if err != nil || enabled {
			return err
		}
		return errors.WithStack(metadata.StageRaftControlMetadata(ctx, migrationVersionIdentityKey, migrationVersionIdentityValue))
	})
	return errors.WithStack(err)
}
