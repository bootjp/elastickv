package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func MigrationMemberProtocolsConfigured() bool {
	return MigrationImportOpcodeEnabledFromEnv() || MigrationPromoteOpcodeEnabledFromEnv() ||
		MigrationCleanupOpcodeEnabledFromEnv()
}

// Check support before any new member can race a guarded-control proposal.
// Existing durable evidence keeps the requirement after feature flags change.
type MigrationMembershipGate struct {
	store   store.MVCCStore
	enabled bool
	probe   func(context.Context, string) error
	next    raftadmin.MembershipChangeInterceptor
}

func NewMigrationMembershipGate(st store.MVCCStore, enabled bool, probe func(context.Context, string) error, next raftadmin.MembershipChangeInterceptor) *MigrationMembershipGate {
	return &MigrationMembershipGate{store: st, enabled: enabled, probe: probe, next: next}
}

func (g *MigrationMembershipGate) PreAddMember(ctx context.Context, id, address string) error {
	if err := g.check(ctx, address); err != nil {
		return err
	}
	if g.next != nil {
		return errors.WithStack(g.next.PreAddMember(ctx, id, address))
	}
	return nil
}

func (g *MigrationMembershipGate) PrePromoteMember(ctx context.Context, id, address string) error {
	if err := g.check(ctx, address); err != nil {
		return err
	}
	if next, ok := g.next.(raftadmin.MembershipPromotionInterceptor); ok {
		return errors.WithStack(next.PrePromoteMember(ctx, id, address))
	}
	return nil
}

func (g *MigrationMembershipGate) check(ctx context.Context, address string) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	required, err := g.protocolRequired(ctx)
	if err != nil || !required {
		return err
	}
	if g.probe == nil || address == "" {
		return errors.New("migration membership capability probe is not configured")
	}
	return errors.WithStack(g.probe(ctx, address))
}

func (g *MigrationMembershipGate) protocolRequired(ctx context.Context) (bool, error) {
	if g.enabled {
		return true, nil
	}
	if g.store == nil {
		return false, nil
	}
	present, err := kv.MigrationPartitionHistoryPresent(ctx, g.store)
	return present, errors.WithStack(err)
}
