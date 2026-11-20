package adapter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestNamespaceAdmissionProbesAllMembersWithBoundedConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var active, peak, calls atomic.Int32
		snapshot := NamespaceAdmissionSnapshot{ConfigurationIndex: 41}
		for i := range 5 {
			peer := NamespaceAdmissionPeer{MemberID: uint64(i + 1), Address: fmt.Sprintf("peer-%d", i)}
			peer.Client = namespaceProbeFunc(func(context.Context, *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				count := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); count > old; old = peak.Load() {
					if peak.CompareAndSwap(old, count) {
						break
					}
				}
				calls.Add(1)
				time.Sleep(100 * time.Millisecond)
				return &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 41, NamespaceAdmissionVersion: kv.MigrationNamespaceVersion}, nil
			})
			snapshot.Members = append(snapshot.Members, peer)
		}
		start := time.Now()
		err := EnsureMigrationNamespaceAdmissionWithSnapshot(t.Context(), func(context.Context) (NamespaceAdmissionSnapshot, error) { return snapshot, nil }, func(context.Context, kv.NamespaceAdmissionCommand) error {
			t.Fatal("already active namespace must not be proposed again")
			return nil
		}, nil)
		require.NoError(t, err)
		require.Equal(t, int32(10), calls.Load(), "every member must prove capability and activation")
		require.LessOrEqual(t, peak.Load(), int32(3))
		require.Greater(t, peak.Load(), int32(1))
		require.Less(t, time.Since(start), 500*time.Millisecond)
	})
}
