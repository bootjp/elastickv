package server

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type splitRecoveryPath struct {
	name    string
	prefix  string
	group   uint64
	ingress int
	client  pb.RawKVClient
	owner   pb.RawKVClient
}

type splitRecoveryResult struct {
	name     string
	sent     int
	success  int
	firstErr error
	firstOK  time.Duration
	maxLag   time.Duration
	errors   map[string]int
}

// Check both children of the migrating parent, at a fixed rate, without
// retries. Unique keys and values distinguish each acknowledged write.
func verifySplitWriteRecovery(t *testing.T, nodes []*bootstrapE2ENode, endpoints []bootstrapE2EMultiGroupEndpoint) {
	t.Helper()
	source := waitForGroupLeader(t, nodes, 1, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	target := waitForGroupLeader(t, nodes, 2, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	if source == target {
		other := (source + 1) % len(nodes)
		admin, ok := nodes[target].engineForGroup(2).(raftengine.Admin)
		require.True(t, ok)
		ctx, cancel := context.WithTimeout(t.Context(), bootstrapE2ERPCRequestTimeout)
		err := admin.TransferLeadershipToServer(ctx, endpoints[other].id, endpoints[other].raftAddrs[2])
		cancel()
		require.NoError(t, err)
		waitForGroupLeaderOnNode(t, nodes, 2, other, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
		target = other
	}
	waitForSplitRecoveryRoutes(t, nodes, source)
	paths := []splitRecoveryPath{
		{name: "outside-direct", prefix: "a", group: 1, ingress: source},
		{name: "retained-direct", prefix: "p", group: 1, ingress: source},
		{name: "retained-forwarded", prefix: "p", group: 1, ingress: (source + 1) % len(nodes)},
		{name: "moved-direct", prefix: "z", group: 2, ingress: target},
		{name: "moved-forwarded", prefix: "z", group: 2, ingress: source},
	}
	owners := map[uint64]pb.RawKVClient{
		1: splitRecoveryClient(t, endpoints[source].raftAddrs[1]),
		2: splitRecoveryClient(t, endpoints[target].raftAddrs[2]),
	}
	results := make(chan splitRecoveryResult, len(paths))
	start := time.Now()
	for _, path := range paths {
		path.client = splitRecoveryClient(t, endpoints[path.ingress].raftAddrs[path.group])
		path.owner = owners[path.group]
		leader := source
		if path.group == 2 {
			leader = target
		}
		t.Logf("PERF recovery path=%s group=%d ingress=%d leader=%d term=%d", path.name, path.group,
			path.ingress, leader, nodes[leader].engineForGroup(path.group).Status().Term)
		if path.ingress != leader {
			require.Equal(t, raftengine.StateFollower, nodes[path.ingress].engineForGroup(path.group).State())
		}
		go func() { results <- runSplitRecoveryPath(t.Context(), path, start) }()
	}
	for range paths {
		result := <-results
		t.Logf("PERF recovery path=%s planned=600 sent=%d success=%d errors=%d first_success_ms=%d max_lag_ms=%d error_details=%v first_error=%v",
			result.name, result.sent, result.success, result.sent-result.success,
			result.firstOK.Milliseconds(), result.maxLag.Milliseconds(), result.errors, result.firstErr)
		if result.sent != 600 || result.success != 600 {
			t.Errorf("recovery %s: sent=%d success=%d first_error=%v", result.name, result.sent, result.success, result.firstErr)
		}
	}
	require.Equal(t, source, waitForGroupLeader(t, nodes, 1, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval))
	require.Equal(t, target, waitForGroupLeader(t, nodes, 2, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval))
}

func splitRecoveryClient(t *testing.T, address string) pb.RawKVClient {
	t.Helper()
	conn, err := grpc.NewClient(address, internalutil.GRPCDialOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return pb.NewRawKVClient(conn)
}

func waitForSplitRecoveryRoutes(t *testing.T, nodes []*bootstrapE2ENode, source int) {
	t.Helper()
	snapshot, err := nodes[source].distCatalog.Snapshot(t.Context())
	require.NoError(t, err)
	start := time.Now()
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			for key, group := range map[string]uint64{"a": 1, "p": 1, "z": 2} {
				route, version, ok := node.distEngine.GetRouteWithVersion([]byte(key))
				if !ok || version != snapshot.Version || route.GroupID != group {
					return false
				}
			}
		}
		return true
	}, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	t.Logf("PERF recovery routing_convergence_ms=%d catalog_version=%d", time.Since(start).Milliseconds(), snapshot.Version)
}

func runSplitRecoveryPath(ctx context.Context, path splitRecoveryPath, start time.Time) splitRecoveryResult {
	result := splitRecoveryResult{name: path.name, errors: make(map[string]int)}
	for i := range 600 {
		arrival := start.Add(time.Duration(i) * 100 * time.Millisecond)
		timer := time.NewTimer(time.Until(arrival))
		select {
		case <-ctx.Done():
			timer.Stop()
			return result
		case <-timer.C:
		}
		result.sent++
		key := fmt.Appendf(nil, "%s-recovery-%s-%04d", path.prefix, path.name, i)
		value := fmt.Appendf(nil, "%s/%04d", path.name, i)
		err := splitRecoveryWriteAndRead(ctx, path, key, value)
		result.maxLag = max(result.maxLag, time.Since(arrival))
		if err != nil {
			result.errors[splitRecoveryErrorKind(err)]++
			if result.firstErr == nil {
				result.firstErr = err
			}
			continue
		}
		result.success++
		if result.success == 1 {
			result.firstOK = time.Since(start)
		}
	}
	return result
}

func splitRecoveryErrorKind(err error) string {
	failure := status.Convert(err)
	kind := failure.Code().String()
	for _, detail := range failure.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			kind += "/" + info.GetDomain() + "/" + info.GetReason()
		}
	}
	return kind
}

func splitRecoveryWriteAndRead(ctx context.Context, path splitRecoveryPath, key, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	put, err := path.client.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if !put.GetSuccess() {
		return fmt.Errorf("put unsuccessful")
	}
	get, err := path.owner.RawGet(ctx, &pb.RawGetRequest{Key: key})
	if err != nil {
		return err
	}
	if !get.GetExists() || !bytes.Equal(value, get.GetValue()) {
		return fmt.Errorf("acknowledged value mismatch for %q", key)
	}
	return nil
}
