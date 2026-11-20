package loadtest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type memoryClient struct {
	pb.RawKVClient
	mu        sync.Mutex
	data      map[string][]byte
	delay     time.Duration
	failWrite bool
	puts      int
}

func (c *memoryClient) pause(ctx context.Context) error {
	timer := time.NewTimer(c.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (c *memoryClient) RawPut(ctx context.Context, r *pb.RawPutRequest, _ ...grpc.CallOption) (*pb.RawPutResponse, error) {
	if err := c.pause(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts++
	if c.failWrite {
		return nil, errors.New("response lost")
	}
	c.data[string(r.Key)] = bytes.Clone(r.Value)
	return &pb.RawPutResponse{Success: true}, nil
}
func (c *memoryClient) RawGet(ctx context.Context, r *pb.RawGetRequest, _ ...grpc.CallOption) (*pb.RawGetResponse, error) {
	if err := c.pause(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	value, exists := c.data[string(r.Key)]
	return &pb.RawGetResponse{Exists: exists, Value: bytes.Clone(value)}, nil
}
func configForTest() Config {
	return Config{Rates: []int{200}, StageDuration: 100 * time.Millisecond, Concurrency: 4, ReadPercent: 50,
		Keys: 6, ValueBytes: 128, MaxRequests: 100, Prefix: "isolated", RequestTimeout: time.Second, VerifyWrites: true}
}
func newTestRunner(t *testing.T, c Config) (*Runner, *memoryClient) {
	t.Helper()
	client := &memoryClient{data: make(map[string][]byte)}
	runner, err := New(c, client, client)
	require.NoError(t, err)
	require.NoError(t, runner.Seed(t.Context()))
	return runner, client
}
func TestUniqueAcknowledgedWritesAreAuditedThroughEveryIngress(t *testing.T) {
	runner, client := newTestRunner(t, configForTest())
	stages, err := runner.Run(t.Context(), nil)
	require.NoError(t, err)
	stage := stages[0]
	require.Equal(t, stage.Offered, stage.Admitted+stage.DroppedLate+stage.DroppedCapacity)
	require.Equal(t, stage.Admitted, stage.Read.Attempts+stage.Write.Attempts)
	require.Equal(t, stage.Write.Success, runner.Acknowledged())
	require.Positive(t, runner.Acknowledged())
	unique := 0
	for key := range client.data {
		if strings.Contains(key, "/write/") {
			unique++
		}
	}
	require.Equal(t, runner.Acknowledged(), unique)
	audit := runner.Audit(t.Context())
	require.Equal(t, 2*runner.Acknowledged(), audit.Attempts)
	require.Equal(t, audit.Attempts, audit.Success)
	for key := range client.data {
		if strings.Contains(key, "/write/") {
			delete(client.data, key)
			break
		}
	}
	audit = runner.Audit(t.Context())
	require.Less(t, audit.Success, audit.Attempts)
	require.Equal(t, 2, audit.Errors["value_mismatch"])
}
func TestOverloadDropsSlotsInsteadOfReplayingOrAccumulatingBacklog(t *testing.T) {
	config := configForTest()
	config.Rates = []int{1000}
	config.StageDuration = 40 * time.Millisecond
	config.Concurrency = 1
	config.ReadPercent = 0
	config.VerifyWrites = false
	runner, client := newTestRunner(t, config)
	client.delay = 5 * time.Millisecond
	stages, err := runner.Run(t.Context(), nil)
	require.NoError(t, err)
	stage := stages[0]
	require.Equal(t, 40, stage.Planned)
	require.Equal(t, stage.Planned, stage.Offered)
	require.Equal(t, stage.Offered, stage.Admitted+stage.DroppedCapacity+stage.DroppedLate)
	require.Positive(t, stage.DroppedCapacity)
	require.Equal(t, config.Keys+stage.Write.Attempts, client.puts)
	require.Equal(t, stage.Write.Attempts, runner.Acknowledged())
	audit := runner.Audit(t.Context())
	require.Equal(t, 2*runner.Acknowledged(), audit.Success)
	require.Greater(t, audit.Latency.P50MS, 4.0)
}
func TestUnknownWritesAreNotReplayedOrAddedToAcknowledgedLedger(t *testing.T) {
	config := configForTest()
	config.ReadPercent = 0
	runner, client := newTestRunner(t, config)
	client.failWrite = true
	stages, err := runner.Run(t.Context(), nil)
	require.NoError(t, err)
	require.Zero(t, runner.Acknowledged())
	require.Positive(t, stages[0].Write.Attempts)
	require.Equal(t, config.Keys+stages[0].Write.Attempts, client.puts)
	require.Zero(t, stages[0].Verification.Attempts)
}
func TestTrackingBoundsAndUnusedPrefixAreRequiredBeforeMutation(t *testing.T) {
	config := configForTest()
	config.MaxRequests = 1
	client := &memoryClient{data: make(map[string][]byte)}
	_, err := New(config, client)
	require.Error(t, err)
	require.Zero(t, client.puts)
	config = configForTest()
	runner, client := newTestRunner(t, config)
	before := client.puts
	other, err := New(config, client)
	require.NoError(t, err)
	require.ErrorContains(t, other.Seed(t.Context()), "prefix exists")
	require.Equal(t, before, client.puts)
	_, err = runner.Run(t.Context(), nil)
	require.NoError(t, err)
	_, err = runner.Run(t.Context(), nil)
	require.Error(t, err)
}
func TestCancellationLeavesExplicitIncompleteStage(t *testing.T) {
	runner, _ := newTestRunner(t, configForTest())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	stages, err := runner.Run(ctx, nil)
	require.NoError(t, err)
	require.True(t, stages[0].Cancelled)
	require.Zero(t, runner.Acknowledged())
}

type observedIngress struct {
	pb.RawKVClient
	index    int
	observed map[string]map[int]int
}

func (c observedIngress) RawPut(ctx context.Context, r *pb.RawPutRequest, opts ...grpc.CallOption) (*pb.RawPutResponse, error) {
	if strings.Contains(string(r.Key), "/write/") {
		domain := string(r.Key[:1])
		if c.observed[domain] == nil {
			c.observed[domain] = make(map[int]int)
		}
		c.observed[domain][c.index]++
	}
	return c.RawKVClient.RawPut(ctx, r, opts...)
}
func TestEveryDomainUsesEveryIngressIndependently(t *testing.T) {
	config := configForTest()
	config.ReadPercent = 0
	base := &memoryClient{data: make(map[string][]byte)}
	observed := make(map[string]map[int]int)
	var clients []pb.RawKVClient
	for index := range 3 {
		clients = append(clients, observedIngress{RawKVClient: base, index: index, observed: observed})
	}
	runner, err := New(config, clients...)
	require.NoError(t, err)
	require.NoError(t, runner.Seed(t.Context()))
	var result workerResult
	for id := range uint64(90) {
		runner.execute(t.Context(), request{id: id, scheduled: time.Now()}, &result)
	}
	require.Len(t, observed, 3)
	for _, domain := range []string{"a", "p", "z"} {
		require.Len(t, observed[domain], 3)
		for index := range 3 {
			require.Equal(t, 10, observed[domain][index])
		}
	}
	require.Len(t, result.acknowledged, 90)
}

func TestLedgerRejectsMissingOrDuplicateAcknowledgements(t *testing.T) {
	runner, _ := newTestRunner(t, configForTest())
	runner.acknowledged = []uint64{1, 2, 3}
	require.NoError(t, runner.ValidateLedger(3))
	require.Error(t, runner.ValidateLedger(4))
	runner.acknowledged = []uint64{1, 2, 2}
	require.ErrorContains(t, runner.ValidateLedger(3), "duplicate")
}
