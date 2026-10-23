package adapter

import "context"

// SplitMigrationApplyMemberSnapshot binds every configured apply member,
// including learners, to an actual applied configuration epoch.
type SplitMigrationApplyMemberSnapshot struct {
	ConfigurationIndex uint64
	Members            []SplitMigrationVoter
}

type SplitMigrationApplyMemberSnapshotFactory func(context.Context, uint64) (SplitMigrationApplyMemberSnapshot, error)

func WithSplitMigrationApplyMemberSnapshotFactory(factory SplitMigrationApplyMemberSnapshotFactory) DistributionServerOption {
	return func(s *DistributionServer) { s.splitApplyMemberFactory = factory }
}
