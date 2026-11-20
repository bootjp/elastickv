package adapter

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSourceControlProbeAcceptsOnlyDominatingProtection(t *testing.T) {
	base := store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true, RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 3, MinWriteTSExclusive: 20, RetentionPinTS: 1, TrackWrites: true, SourceWriteFence: true, SourceReadFence: true, TransferSizeGuardVersion: 1}
	request := &pb.ProbeMigrationStateRequest{JobId: 7, MigrationJobId: 7, RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCatalogVersion: 3, MinWriteTsExclusive: 10, RetentionPinTs: 5, TrackWrites: true, TransferSizeGuardVersion: 1}
	require.True(t, migrationControlMatches(base, request))
	cases := []struct {
		name   string
		change func(*store.TargetStagedReadinessState, *pb.ProbeMigrationStateRequest)
	}{
		{"unarmed", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) { s.Armed = false }},
		{"released-pin", func(_ *store.TargetStagedReadinessState, r *pb.ProbeMigrationStateRequest) {
			r.RetentionPinTs = ^uint64(0)
		}},
		{"wrong-job", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) { s.MigrationJobID++ }},
		{"wrong-range", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) {
			s.RouteStart = []byte("n")
		}},
		{"wrong-version", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) {
			s.ExpectedCutoverVersion++
		}},
		{"lower-write-floor", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) { s.MinWriteTSExclusive = 9 }},
		{"missing-pin", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) { s.RetentionPinTS = 0 }},
		{"later-pin", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) { s.RetentionPinTS = 6 }},
		{"missing-tracker", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) { s.TrackWrites = false }},
		{"missing-write-fence", func(s *store.TargetStagedReadinessState, r *pb.ProbeMigrationStateRequest) {
			r.SourceWriteFence = true
			s.SourceWriteFence = false
		}},
		{"missing-read-fence", func(s *store.TargetStagedReadinessState, r *pb.ProbeMigrationStateRequest) {
			r.SourceReadFence = true
			s.SourceReadFence = false
		}},
		{"target-disarm", func(_ *store.TargetStagedReadinessState, r *pb.ProbeMigrationStateRequest) { r.TrackWrites = false }},
		{"pin-release", func(_ *store.TargetStagedReadinessState, r *pb.ProbeMigrationStateRequest) { r.RetentionPinTs = 0 }},
		{"missing-size-guard", func(s *store.TargetStagedReadinessState, _ *pb.ProbeMigrationStateRequest) {
			s.TransferSizeGuardVersion = 0
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := base
			req := proto.CloneOf(request)
			tc.change(&state, req)
			require.False(t, migrationControlMatches(state, req))
		})
	}
}
