package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSnapshotOffloadGroupsCarryPerGroupDataDirs pins that each group
// is pointed at its own Raft data dir. Publishing a group's snapshot
// from another group's directory would ship the wrong state under the
// right manifest identity.
func TestSnapshotOffloadGroupsCarryPerGroupDataDirs(t *testing.T) {
	t.Parallel()
	raftDir := t.TempDir()
	runtimes := []*raftGroupRuntime{
		{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 1}}},
		{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 2}}},
		nil, // a nil runtime must be skipped, not panic
	}

	groups := snapshotOffloadGroups(runtimes, raftDir, "n1", true)
	require.Len(t, groups, 2)

	seen := map[uint64]string{}
	for _, g := range groups {
		require.NotNil(t, g.IsLeader, "every group must carry both leadership callbacks")
		require.NotNil(t, g.VerifyLeader)
		seen[g.GroupID] = g.DataDir
	}
	require.Equal(t, filepath.Join(raftDir, "n1", "group-1"), seen[1])
	require.Equal(t, filepath.Join(raftDir, "n1", "group-2"), seen[2])
	require.NotEqual(t, seen[1], seen[2])
}

// TestSnapshotOffloadLeadershipFailsClosedOnAClosedEngine covers
// shutdown: the scheduler outlives startup and races Close(), so a
// runtime whose engine has been cleared must report "not leader"
// rather than panic or, worse, publish.
func TestSnapshotOffloadLeadershipFailsClosedOnAClosedEngine(t *testing.T) {
	t.Parallel()
	rt := &raftGroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 7}}} // engine never set

	require.False(t, snapshotOffloadIsLeader(rt)(),
		"a closed engine must never look like a leader")

	err := snapshotOffloadVerifyLeader(rt)(context.Background())
	require.Error(t, err)
	require.True(t, errors.Is(err, snapshotoffload.ErrInvalidOptions))
}

// TestRunbookRestorePathsMatchGroupDataDir keeps the operations
// runbook's `--data-dir` table honest against the function the server
// actually uses.
//
// A wrong path here is not a cosmetic doc bug: an operator following
// it during disaster recovery restores into a directory the server
// never opens, startup finds the per-group directories empty, and the
// restore is silently ignored.
func TestRunbookRestorePathsMatchGroupDataDir(t *testing.T) {
	t.Parallel()

	const raftDir = "/var/lib/elastickv"
	const raftID = "n1"

	tests := []struct {
		name    string
		groupID uint64
		multi   bool
		want    string
	}{
		{name: "multi-group", groupID: 1, multi: true, want: "/var/lib/elastickv/n1/group-1"},
		{name: "multi-group higher id", groupID: 7, multi: true, want: "/var/lib/elastickv/n1/group-7"},
		{name: "single group", groupID: 1, multi: false, want: "/var/lib/elastickv/n1"},
		{name: "single node group zero", groupID: 0, multi: false, want: "/var/lib/elastickv/n1/group-0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, noderuntime.GroupDataDir(raftDir, raftID, tc.groupID, tc.multi),
				"docs/snapshot_offload_operations.md documents this path for restore")
		})
	}
}

// TestRunbookRestorePathsFollowFromTheGroupTopology pins the runbook table the
// way an operator actually reads it: from --raftGroups to a directory.
//
// TestRunbookRestorePathsMatchGroupDataDir above takes `multi` as an input, so
// it cannot catch the case where a reader derives the wrong `multi` in the first
// place — and that is the case that bites. dataGroupsNeedMultiDirs counts DATA
// groups and excludes group 0, so a node running the dedicated TSO group
// alongside a single data group has two entries in --raftGroups but is NOT
// multi-dir: group 0 lands in group-0 while the data group opens
// <raftDir>/<raftID> directly. Restoring that data group into group-1 puts it
// where startup never looks, and an empty group is not an error.
func TestRunbookRestorePathsFollowFromTheGroupTopology(t *testing.T) {
	t.Parallel()

	const (
		raftDir = "/var/lib/elastickv"
		raftID  = "n1"
	)
	spec := func(ids ...uint64) []noderuntime.GroupSpec {
		out := make([]noderuntime.GroupSpec, 0, len(ids))
		for _, id := range ids {
			out = append(out, noderuntime.GroupSpec{ID: id, Address: "127.0.0.1:50051"})
		}
		return out
	}

	for _, tc := range []struct {
		name    string
		groups  []noderuntime.GroupSpec
		groupID uint64
		want    string
	}{
		{
			name:   "two data groups: each gets its own dir",
			groups: spec(1, 2), groupID: 1,
			want: "/var/lib/elastickv/n1/group-1",
		},
		{
			name:   "two data groups: the second one too",
			groups: spec(1, 2), groupID: 2,
			want: "/var/lib/elastickv/n1/group-2",
		},
		{
			name:   "a single data group opens the node dir",
			groups: spec(1), groupID: 1,
			want: "/var/lib/elastickv/n1",
		},
		{
			name:   "dedicated TSO plus one data group: group 0 is always group-0",
			groups: spec(0, 1), groupID: 0,
			want: "/var/lib/elastickv/n1/group-0",
		},
		{
			// The row that catches people out: two --raftGroups entries but
			// only one DATA group, so this is not a multi-dir deployment.
			name:   "dedicated TSO plus one data group: the data group is NOT group-1",
			groups: spec(0, 1), groupID: 1,
			want: "/var/lib/elastickv/n1",
		},
		{
			name:   "dedicated TSO plus two data groups is multi-dir again",
			groups: spec(0, 1, 2), groupID: 1,
			want: "/var/lib/elastickv/n1/group-1",
		},
		{
			name:   "dedicated TSO plus two data groups: group 0 unchanged",
			groups: spec(0, 1, 2), groupID: 0,
			want: "/var/lib/elastickv/n1/group-0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// requested=true is the operator asking for per-group dirs; the
			// topology decides whether that takes effect.
			multi := noderuntime.EffectiveMultiDataDirs(tc.groups, true)
			require.Equal(t, tc.want, noderuntime.GroupDataDir(raftDir, raftID, tc.groupID, multi),
				"docs/snapshot_offload_operations.md documents this path for restore")
		})
	}
}
