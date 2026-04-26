package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestBuildLeaderForwarder_RejectsMissingDeps(t *testing.T) {
	cache := &kv.GRPCConnCache{}
	cases := []struct {
		name      string
		coord     kv.Coordinator
		cache     *kv.GRPCConnCache
		nodeID    string
		wantSubst string
	}{
		{"nil coordinator", nil, cache, "n1", "coordinator"},
		{"nil conn cache", &kv.Coordinate{}, nil, "n1", "gRPC connection cache"},
		// admin.NewGRPCForwardClient owns the empty-nodeID rejection;
		// we confirm the wrapped error preserves that vocabulary so a
		// misconfigured deployment fails fast at startup with a
		// pinpointed message rather than mysterious 500s at runtime.
		{"empty node id", &kv.Coordinate{}, cache, "", "node id is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd, err := buildLeaderForwarder(tc.coord, tc.cache, tc.nodeID)
			require.Error(t, err)
			require.Nil(t, fwd)
			require.Contains(t, err.Error(), tc.wantSubst)
		})
	}
}

func TestBuildLeaderForwarder_HappyPathReturnsForwarder(t *testing.T) {
	// The production bridge does not dial during construction —
	// resolver / dial calls only happen on the first Forward — so
	// passing real (zero-value) collaborators is enough to confirm
	// the wiring itself is well-formed.
	fwd, err := buildLeaderForwarder(&kv.Coordinate{}, &kv.GRPCConnCache{}, "n1")
	require.NoError(t, err)
	require.NotNil(t, fwd)
}

func TestAdminForwardConnFactory_RejectsEmptyAddr(t *testing.T) {
	// kv.GRPCConnCache.ConnFor returns ErrLeaderNotFound on "". The
	// LeaderForwarder catches the empty address before this layer is
	// reached, but the bridge still surfaces an error rather than a
	// nil client when invoked directly — so a future caller that
	// bypasses the resolver does not get a typed-nil PBAdminForwardClient.
	f := &adminForwardConnFactory{cache: &kv.GRPCConnCache{}}
	cli, err := f.ConnFor("")
	require.Error(t, err)
	require.Nil(t, cli)
}

func TestRoleStoreFromFlags(t *testing.T) {
	cases := []struct {
		name         string
		full         []string
		readOnly     []string
		wantNil      bool
		wantFull     []string
		wantReadOnly []string
	}{
		{name: "both empty produces nil store", wantNil: true},
		{
			name:     "full only",
			full:     []string{"AKIA_F"},
			wantFull: []string{"AKIA_F"},
		},
		{
			name:         "read-only only",
			readOnly:     []string{"AKIA_R"},
			wantReadOnly: []string{"AKIA_R"},
		},
		{
			name:         "mixed roles",
			full:         []string{"AKIA_F1", "AKIA_F2"},
			readOnly:     []string{"AKIA_R1"},
			wantFull:     []string{"AKIA_F1", "AKIA_F2"},
			wantReadOnly: []string{"AKIA_R1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := roleStoreFromFlags(tc.full, tc.readOnly)
			if tc.wantNil {
				require.Nil(t, store)
				return
			}
			require.NotNil(t, store)
			for _, k := range tc.wantFull {
				role, ok := store.LookupRole(k)
				require.True(t, ok, "expected %s present", k)
				require.Equal(t, admin.RoleFull, role)
			}
			for _, k := range tc.wantReadOnly {
				role, ok := store.LookupRole(k)
				require.True(t, ok, "expected %s present", k)
				require.Equal(t, admin.RoleReadOnly, role)
			}
		})
	}
}

func TestAdminForwardServerDeps_ReadyForRegistration(t *testing.T) {
	// The bundle's readyForRegistration gate decides whether
	// startRaftServers wires the gRPC ForwardServer at all. A nil
	// TablesSource (cluster-only build) or nil RoleStore (admin
	// auth disabled) means a registered service would 500 every
	// forwarded call — silently skipping registration is the
	// preferred behaviour.
	require.False(t, adminForwardServerDeps{}.readyForRegistration())
	require.False(t, adminForwardServerDeps{tables: dummyTablesSource{}}.readyForRegistration())
	require.False(t, adminForwardServerDeps{roles: admin.MapRoleStore{}}.readyForRegistration())
	require.True(t, adminForwardServerDeps{
		tables: dummyTablesSource{},
		roles:  admin.MapRoleStore{},
	}.readyForRegistration())
}

func TestBuildAdminLeaderForwarder_NilGateReturnsNoForwarder(t *testing.T) {
	// buildAdminLeaderForwarder is the wrapper in main_admin.go that
	// short-circuits to (nil, nil) when either coordinate or
	// connCache is nil — the explicit "no forwarder" path for
	// single-node / leader-only deployments. A future refactor that
	// drops the guard would silently pass a nil collaborator into
	// buildLeaderForwarder, which would either crash on the nil
	// resolver / cache deref or build a forwarder that panics on
	// the first request. Locking this down keeps the contract intact
	// (Claude review on #648).
	cases := []struct {
		name      string
		coord     kv.Coordinator
		cache     *kv.GRPCConnCache
		nodeID    string
		wantNil   bool
		wantError string
	}{
		{name: "nil coordinator", cache: &kv.GRPCConnCache{}, nodeID: "n1", wantNil: true},
		{name: "nil conn cache", coord: &kv.Coordinate{}, nodeID: "n1", wantNil: true},
		{name: "both nil", nodeID: "n1", wantNil: true},
		{
			name:      "complete deps but empty node id",
			coord:     &kv.Coordinate{},
			cache:     &kv.GRPCConnCache{},
			wantError: "--raftId is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd, err := buildAdminLeaderForwarder(tc.coord, tc.cache, tc.nodeID)
			if tc.wantError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantError)
				require.Nil(t, fwd)
				return
			}
			require.NoError(t, err)
			if tc.wantNil {
				require.Nil(t, fwd)
			} else {
				require.NotNil(t, fwd)
			}
		})
	}
}

func TestBuildAdminLeaderForwarder_HappyPathReturnsForwarder(t *testing.T) {
	fwd, err := buildAdminLeaderForwarder(&kv.Coordinate{}, &kv.GRPCConnCache{}, "n1")
	require.NoError(t, err)
	require.NotNil(t, fwd)
}

// TestAdminHLCPhysicalShiftMatchesKVLogicalBits guards against
// silent drift between admin.FormatBucketCreatedAt's shift constant
// (currently 16) and kv.HLCLogicalBits, the upstream truth the
// timestamp encoding obeys. If a future HLC format change
// re-partitions the wire layout in kv and the admin formatter is
// not updated, this test fails immediately rather than letting
// every CreatedAt render at the wrong hour silently (Claude
// Issue 4 on PR #658).
//
// admin cannot import kv (it is a low-level dependency the admin
// package stays decoupled from), so the assertion lives in main
// where both packages are already in scope.
func TestAdminHLCPhysicalShiftMatchesKVLogicalBits(t *testing.T) {
	// FormatBucketCreatedAt(hlc) shifts hlc right by 16 to recover
	// the wall-clock millis. Shift a known wall-clock value left by
	// kv.HLCLogicalBits and confirm the formatter recovers exactly
	// the right RFC3339 string — if the two constants drift apart,
	// the round-trip produces a wrong year / hour and the test
	// fails.
	const wallMillis = int64(1_777_874_400_000) // 2026-05-04T06:00:00Z
	hlc := uint64(wallMillis) << kv.HLCLogicalBits
	require.Equal(t, "2026-05-04T06:00:00Z", admin.FormatBucketCreatedAt(hlc))
}

// dummyTablesSource is the smallest concrete admin.TablesSource for
// the readyForRegistration gate test — no method body needs to
// execute, so every method just panics. Using a real implementation
// would pull adapter dependencies into a main_admin test that has
// nothing to do with adapter behaviour.
type dummyTablesSource struct{}

func (dummyTablesSource) AdminListTables(_ context.Context) ([]string, error) {
	panic("dummyTablesSource.AdminListTables should not be invoked")
}

func (dummyTablesSource) AdminDescribeTable(_ context.Context, _ string) (*admin.DynamoTableSummary, bool, error) {
	panic("dummyTablesSource.AdminDescribeTable should not be invoked")
}

func (dummyTablesSource) AdminCreateTable(_ context.Context, _ admin.AuthPrincipal, _ admin.CreateTableRequest) (*admin.DynamoTableSummary, error) {
	panic("dummyTablesSource.AdminCreateTable should not be invoked")
}

func (dummyTablesSource) AdminDeleteTable(_ context.Context, _ admin.AuthPrincipal, _ string) error {
	panic("dummyTablesSource.AdminDeleteTable should not be invoked")
}
