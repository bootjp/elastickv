package splitmigration

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func MemberProtocolsConfigured() bool {
	return adapter.MigrationImportOpcodeEnabledFromEnv() || adapter.MigrationPromoteOpcodeEnabledFromEnv() ||
		adapter.MigrationCleanupOpcodeEnabledFromEnv()
}

// Check support before any new member can race a guarded-control proposal.
// Existing durable evidence keeps the requirement after feature flags change.
type MembershipGate struct {
	store   store.MVCCStore
	enabled bool
	probe   func(context.Context, string) error
	next    raftadmin.MembershipChangeInterceptor
}

func NewMembershipGate(st store.MVCCStore, enabled bool, probe func(context.Context, string) error, next raftadmin.MembershipChangeInterceptor) *MembershipGate {
	return &MembershipGate{store: st, enabled: enabled, probe: probe, next: next}
}

func (g *MembershipGate) PreAddMember(ctx context.Context, id, address string) error {
	if err := g.check(ctx, address); err != nil {
		return err
	}
	if g.next != nil {
		return errors.WithStack(g.next.PreAddMember(ctx, id, address))
	}
	return nil
}

func (g *MembershipGate) PrePromoteMember(ctx context.Context, id, address string) error {
	if err := g.check(ctx, address); err != nil {
		return err
	}
	if next, ok := g.next.(raftadmin.MembershipPromotionInterceptor); ok {
		return errors.WithStack(next.PrePromoteMember(ctx, id, address))
	}
	return nil
}

func (g *MembershipGate) check(ctx context.Context, address string) error {
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

func (g *MembershipGate) protocolRequired(ctx context.Context) (bool, error) {
	if g.enabled {
		return true, nil
	}
	if g.store == nil {
		return false, nil
	}
	present, err := kv.MigrationPartitionHistoryPresent(ctx, g.store)
	return present, errors.WithStack(err)
}
