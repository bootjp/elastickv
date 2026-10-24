package main

import (
	"context"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func splitMigrationApplyMemberSnapshotFactory(groups map[uint64]*kv.ShardGroup, cache *kv.GRPCConnCache, token string) adapter.SplitMigrationApplyMemberSnapshotFactory {
	return func(ctx context.Context, groupID uint64) (adapter.SplitMigrationApplyMemberSnapshot, error) {
		group := groups[groupID]
		if group == nil || group.Engine == nil || cache == nil {
			return adapter.SplitMigrationApplyMemberSnapshot{}, errors.New("migration apply-member group or connection cache is missing")
		}
		engine := group.Engine
		before, err := splitMigrationApplyConfiguration(engine)
		if err != nil {
			return adapter.SplitMigrationApplyMemberSnapshot{}, err
		}
		cfg, err := engine.Configuration(ctx)
		if err != nil {
			return adapter.SplitMigrationApplyMemberSnapshot{}, errors.WithStack(err)
		}
		members, err := splitMigrationApplyMembers(groupID, cfg.Servers, cache, token)
		if err != nil {
			return adapter.SplitMigrationApplyMemberSnapshot{}, err
		}
		after, err := splitMigrationApplyConfiguration(engine)
		if err != nil {
			return adapter.SplitMigrationApplyMemberSnapshot{}, err
		}
		if before != after {
			return adapter.SplitMigrationApplyMemberSnapshot{}, errors.New("migration apply-member configuration changed during resolution")
		}
		return adapter.SplitMigrationApplyMemberSnapshot{ConfigurationIndex: before, Members: members}, nil
	}
}

func splitMigrationApplyConfiguration(engine raftengine.Engine) (uint64, error) {
	reader, ok := engine.(raftengine.AppliedConfigurationReader)
	if !ok {
		return 0, errors.New("migration apply-member engine lacks applied configuration reader")
	}
	before := engine.Status()
	index, supported := reader.AppliedConfigurationIndex()
	after := engine.Status()
	if !supported || index == 0 || before.PendingConfChange || after.PendingConfChange ||
		before.ConfigurationIndex != index || after.ConfigurationIndex != index {
		return 0, errors.New("migration apply-member configuration is not stably applied")
	}
	return index, nil
}

func splitMigrationApplyMembers(groupID uint64, servers []raftengine.Server, cache *kv.GRPCConnCache, token string) ([]adapter.SplitMigrationVoter, error) {
	if len(servers) == 0 {
		return nil, errors.New("migration apply-member configuration is empty")
	}
	members := make([]adapter.SplitMigrationVoter, 0, len(servers))
	ids, addresses := make(map[string]struct{}, len(servers)), make(map[string]struct{}, len(servers))
	for _, server := range servers {
		server.ID, server.Address = strings.TrimSpace(server.ID), strings.TrimSpace(server.Address)
		if _, duplicate := ids[server.ID]; duplicate {
			return nil, errors.New("migration apply-member ID is duplicated")
		}
		if _, duplicate := addresses[server.Address]; duplicate {
			return nil, errors.New("migration apply-member address is duplicated")
		}
		member, err := splitMigrationVoter(groupID, server, cache, token)
		if err != nil {
			return nil, err
		}
		ids[member.ID], addresses[member.Address] = struct{}{}, struct{}{}
		members = append(members, member)
	}
	return members, nil
}
