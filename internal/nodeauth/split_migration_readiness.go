package nodeauth

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/cockroachdb/errors"
)

// SplitMigrationLocalReadinessGate reports whether this node may run split
// migration. Its method value is the readiness callback handed to the
// distribution server, the gRPC coverage gate and the background runners.
func (f Flags) SplitMigrationLocalReadinessGate(ctx context.Context) error {
	if err := f.SplitMigrationPeerAuthReadinessGate(ctx); err != nil {
		return err
	}
	if !adapter.MigrationImportOpcodeEnabledFromEnv() {
		return errors.Errorf("%s is disabled", adapter.MigrationImportOpcodeEnv)
	}
	if !adapter.MigrationPromoteOpcodeEnabledFromEnv() {
		return errors.Errorf("%s is disabled", adapter.MigrationPromoteOpcodeEnv)
	}
	if !adapter.MigrationCleanupOpcodeEnabledFromEnv() {
		return errors.Errorf("%s is disabled", adapter.MigrationCleanupOpcodeEnv)
	}
	return nil
}

func (f Flags) SplitMigrationPeerAuthReadinessGate(context.Context) error {
	if f.AdminInsecureNoAuth || f.SplitMigrationPeerAuthEnforced {
		return nil
	}
	return errors.New("migration is paused during peer auth rollout; enable --splitMigrationPeerAuthEnforced only after every node sends the token")
}
