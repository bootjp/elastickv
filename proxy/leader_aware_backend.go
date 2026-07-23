package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// defaultLeaderRefreshInterval is how often the leader-aware backend
	// re-polls the ElasticKV cluster for its current Raft leader. The
	// interval is short enough that a leader election settles quickly but
	// long enough to avoid meaningful INFO traffic overhead.
	defaultLeaderRefreshInterval = 2 * time.Second
	// defaultLeaderRefreshTimeout caps a single INFO replication probe so
	// that one dead node cannot stall the refresh loop for every seed.
	defaultLeaderRefreshTimeout = 1 * time.Second

	raftLeaderRedisField = "raft_leader_redis:"
	infoReplicationArg   = "replication"

	// maxLeaderAwareClients caps the number of cached redis.Client pools so
	// that a long-running process cannot accumulate pools indefinitely as
	// leaders churn (e.g. rolling restarts changing addresses). This is well
	// above a realistic Raft cluster size; when the cap is reached we evict
	// the oldest non-protected entry (FIFO, skipping seeds and the current
	// leader) before inserting a new one.
	maxLeaderAwareClients = 16
)

// ErrNoLeaderBackend is returned when LeaderAwareRedisBackend has no usable
// client (no seed and no discovered leader). It should never happen in
// practice because the backend always falls back to a seed address.
var ErrNoLeaderBackend = errors.New("leader-aware backend has no upstream")

// LeaderAwareRedisBackend routes commands to whichever ElasticKV node is
// currently the Raft leader, discovering the leader via `INFO replication`.
// Seed addresses are used for the initial probe and as fallbacks when the
// current leader becomes unreachable.
type LeaderAwareRedisBackend struct {
	name  string
	opts  BackendOptions
	seeds []string

	refreshInterval time.Duration
	refreshTimeout  time.Duration

	logger *slog.Logger
	// refreshDone coalesces concurrent command failures into one INFO probe.
	// Waiters observe the same refresh instead of serially repeating it.
	refreshMu     sync.Mutex
	refreshDone   chan struct{}
	refreshClosed bool
	refreshCtx    context.Context
	refreshCancel context.CancelFunc

	mu          sync.RWMutex
	clients     map[string]*redis.Client
	clientOrder []string // FIFO insertion order for bounded eviction
	leader      string
	closed      bool
	seedProtect map[string]struct{}

	stopCh    chan struct{}
	done      chan struct{}
	refreshCh chan struct{}
}

// NewLeaderAwareRedisBackend creates a LeaderAwareRedisBackend with the given
// seed addresses. The first command waits for leader discovery instead of
// sending traffic to a seed that may be down. At least one seed is required.
func NewLeaderAwareRedisBackend(seeds []string, name string, opts BackendOptions, logger *slog.Logger) *LeaderAwareRedisBackend {
	return NewLeaderAwareRedisBackendWithInterval(seeds, name, opts, defaultLeaderRefreshInterval, defaultLeaderRefreshTimeout, logger)
}

// NewLeaderAwareRedisBackendWithInterval is the fully-parameterised constructor
// used by tests that need a shorter poll period.
func NewLeaderAwareRedisBackendWithInterval(seeds []string, name string, opts BackendOptions, refreshInterval, refreshTimeout time.Duration, logger *slog.Logger) *LeaderAwareRedisBackend {
	normalized := normalizeSeeds(seeds)
	if len(normalized) == 0 {
		panic("proxy: LeaderAwareRedisBackend requires at least one seed address")
	}
	if logger == nil {
		logger = slog.Default()
	}
	seedProtect := make(map[string]struct{}, len(normalized))
	for _, s := range normalized {
		seedProtect[s] = struct{}{}
	}
	refreshCtx, refreshCancel := context.WithCancel(context.Background())
	b := &LeaderAwareRedisBackend{
		name:            name,
		opts:            opts,
		seeds:           normalized,
		refreshInterval: refreshInterval,
		refreshTimeout:  refreshTimeout,
		logger:          logger,
		clients:         make(map[string]*redis.Client, len(normalized)),
		clientOrder:     make([]string, 0, len(normalized)),
		seedProtect:     seedProtect,
		leader:          "",
		stopCh:          make(chan struct{}),
		done:            make(chan struct{}),
		refreshCh:       make(chan struct{}, 1),
		refreshCtx:      refreshCtx,
		refreshCancel:   refreshCancel,
	}
	for _, addr := range normalized {
		b.ensureClientLocked(addr)
	}
	go b.refreshLoop()
	return b
}

func normalizeSeeds(seeds []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(seeds))
	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func (b *LeaderAwareRedisBackend) refreshLoop() {
	defer close(b.done)
	b.refreshLeader(b.refreshCtx)

	// Use a NewTimer that is reset after each probe completes rather than a
	// Ticker so that when a probe takes longer than refreshInterval (e.g. a
	// dead seed with a 1s probe timeout and a 50ms interval) we don't
	// immediately re-fire a second probe the instant the first returns.
	// This guarantees at least refreshInterval of quiet time between probes.
	timer := time.NewTimer(b.refreshInterval)
	defer timer.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-timer.C:
			b.refreshLeader(b.refreshCtx)
		case <-b.refreshCh:
			if !timer.Stop() {
				// Drain the channel if the timer had already fired so the
				// subsequent Reset doesn't race a pending tick.
				select {
				case <-timer.C:
				default:
				}
			}
			b.refreshLeader(b.refreshCtx)
		}
		timer.Reset(b.refreshInterval)
	}
}

// TriggerRefresh asks the background loop to re-probe the current leader
// immediately. Useful after a command fails in a way that suggests the
// leader has changed. Multiple concurrent calls coalesce into at most one
// extra probe.
func (b *LeaderAwareRedisBackend) TriggerRefresh() {
	select {
	case b.refreshCh <- struct{}{}:
	default:
	}
}

// refreshLeader probes INFO replication on the current leader first, then on
// each seed, and adopts the first advertised leader address. The current
// leader's Redis address is returned by the leader node itself when it's
// healthy, so this converges in one probe during steady state.
func (b *LeaderAwareRedisBackend) refreshLeader(ctx context.Context) {
	b.refreshMu.Lock()
	if b.refreshClosed {
		b.refreshMu.Unlock()
		return
	}
	done := b.refreshDone
	if done == nil {
		done = make(chan struct{})
		b.refreshDone = done
		go b.runLeaderRefresh(done)
	}
	b.refreshMu.Unlock()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (b *LeaderAwareRedisBackend) runLeaderRefresh(done chan struct{}) {
	b.refreshLeaderOnce(b.refreshCtx)

	b.refreshMu.Lock()
	b.refreshDone = nil
	close(done)
	b.refreshMu.Unlock()
}

func (b *LeaderAwareRedisBackend) refreshLeaderOnce(ctx context.Context) {
	candidates := b.candidateAddrs()
	for _, addr := range candidates {
		if ctx.Err() != nil {
			return
		}
		leader, err := b.probeLeader(ctx, addr)
		if err != nil {
			b.logger.Debug("leader probe failed", "addr", addr, "err", err)
			continue
		}
		if leader == "" {
			continue
		}
		b.setLeader(leader)
		return
	}
	// Only warn when the context is still alive; an interrupted probe during
	// shutdown is expected and not a cluster-health signal.
	if ctx.Err() == nil {
		b.logger.Warn("leader discovery could not find an advertised leader",
			"backend", b.name, "candidates", candidates)
	}
}

// RefreshLeaderNow synchronously re-probes the cluster. Callers use this only
// after an explicit not-leader rejection, where retrying is known to be safe.
func (b *LeaderAwareRedisBackend) RefreshLeaderNow(ctx context.Context) {
	b.refreshLeader(ctx)
}

func (b *LeaderAwareRedisBackend) probeLeader(ctx context.Context, addr string) (string, error) {
	cli := b.getOrCreateClient(addr)
	if cli == nil {
		// Backend is closing; treat as a normal probe failure so the loop
		// moves on quickly rather than panicking on a nil dereference.
		return "", ErrNoLeaderBackend
	}
	probeCtx, cancel := context.WithTimeout(ctx, b.refreshTimeout)
	defer cancel()
	raw, err := cli.Info(probeCtx, infoReplicationArg).Result()
	if err != nil {
		return "", fmt.Errorf("info replication %s: %w", addr, err)
	}
	return parseRaftLeaderRedis(raw), nil
}

// parseRaftLeaderRedis extracts the raft_leader_redis field from an
// INFO reply. Empty string means "not present".
func parseRaftLeaderRedis(info string) string {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, raftLeaderRedisField) {
			return strings.TrimSpace(strings.TrimPrefix(line, raftLeaderRedisField))
		}
	}
	return ""
}

func (b *LeaderAwareRedisBackend) candidateAddrs() []string {
	b.mu.RLock()
	leader := b.leader
	b.mu.RUnlock()

	cands := make([]string, 0, len(b.seeds)+1)
	seen := map[string]struct{}{}
	if leader != "" {
		cands = append(cands, leader)
		seen[leader] = struct{}{}
	}
	for _, s := range b.seeds {
		if _, dup := seen[s]; dup {
			continue
		}
		cands = append(cands, s)
		seen[s] = struct{}{}
	}
	return cands
}

func (b *LeaderAwareRedisBackend) setLeader(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.leader == addr {
		return
	}
	prev := b.leader
	b.leader = addr
	b.ensureClientLocked(addr)
	b.logger.Info("elastickv leader updated", "backend", b.name, "from", prev, "to", addr)
}

func (b *LeaderAwareRedisBackend) getOrCreateClient(addr string) *redis.Client {
	b.mu.RLock()
	cli, ok := b.clients[addr]
	b.mu.RUnlock()
	if ok {
		return cli
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ensureClientLocked(addr)
}

func (b *LeaderAwareRedisBackend) ensureClientLocked(addr string) *redis.Client {
	if cli, ok := b.clients[addr]; ok {
		return cli
	}
	// Refuse to create a new pool once Close() has started; otherwise a
	// command that raced shutdown could instantiate a client that no one
	// will ever close.
	if b.closed {
		return nil
	}
	// Evict the oldest non-protected client (FIFO, skipping seeds and the
	// current leader) so long-running leader churn cannot leak pools.
	if len(b.clients) >= maxLeaderAwareClients {
		if !b.evictOneLocked() {
			b.logger.Warn("leader-aware client cache at cap with no evictable entries",
				"backend", b.name, "cap", maxLeaderAwareClients, "rejected", addr)
			return nil
		}
	}
	cli := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           b.opts.DB,
		Password:     b.opts.Password,
		Protocol:     respProtocolV2,
		PoolSize:     b.opts.PoolSize,
		DialTimeout:  b.opts.DialTimeout,
		ReadTimeout:  b.opts.ReadTimeout,
		WriteTimeout: b.opts.WriteTimeout,
	})
	b.clients[addr] = cli
	b.clientOrder = append(b.clientOrder, addr)
	return cli
}

// evictOneLocked removes the oldest client that is neither a seed nor the
// current leader and closes its pool. Returns true when an entry was evicted.
// Must be called with b.mu held for writing.
func (b *LeaderAwareRedisBackend) evictOneLocked() bool {
	for i, addr := range b.clientOrder {
		if _, protected := b.seedProtect[addr]; protected {
			continue
		}
		if addr == b.leader {
			continue
		}
		cli := b.clients[addr]
		delete(b.clients, addr)
		b.clientOrder = append(b.clientOrder[:i], b.clientOrder[i+1:]...)
		if cli != nil {
			if err := cli.Close(); err != nil {
				b.logger.Warn("close evicted leader-aware client failed",
					"backend", b.name, "addr", addr, "err", err)
			}
		}
		return true
	}
	return false
}

// currentClient returns the client for the current leader. The whole
// read happens under a single RLock so the closed flag, leader addr, and
// clients map cannot diverge between reads (i.e. no TOCTOU window with
// Close()). Returns nil once the backend is closing so callers fail fast.
func (b *LeaderAwareRedisBackend) currentClient() *redis.Client {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil
	}
	return b.clients[b.leader]
}

func (b *LeaderAwareRedisBackend) currentClientOrRefresh(ctx context.Context) *redis.Client {
	cli := b.currentClient()
	if cli != nil {
		return cli
	}
	b.RefreshLeaderNow(ctx)
	return b.currentClient()
}

func (b *LeaderAwareRedisBackend) firstSeedClient() *redis.Client {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil
	}
	for _, seed := range b.seeds {
		if cli := b.clients[seed]; cli != nil {
			return cli
		}
	}
	return nil
}

// Do forwards a single command to the current leader. NOTLEADER refreshes the
// cached leader for the next command, but the current command is not replayed:
// leadership-loss errors can be returned after an operation has already applied.
func (b *LeaderAwareRedisBackend) Do(ctx context.Context, args ...any) *redis.Cmd {
	cmd := b.doOnce(ctx, args...)
	switch {
	case isElasticKVNotLeaderError(cmd.Err()):
		b.RefreshLeaderNow(ctx)
	case isLeaderRefreshTransportError(cmd.Err()):
		b.TriggerRefresh()
	}
	return cmd
}

func (b *LeaderAwareRedisBackend) doOnce(ctx context.Context, args ...any) *redis.Cmd {
	cli := b.currentClientOrRefresh(ctx)
	if cli == nil {
		cmd := redis.NewCmd(ctx, args...)
		cmd.SetErr(ErrNoLeaderBackend)
		return cmd
	}
	return cli.Do(ctx, args...)
}

// DoWithTimeout forwards a blocking command with a per-call socket timeout.
func (b *LeaderAwareRedisBackend) DoWithTimeout(ctx context.Context, timeout time.Duration, args ...any) *redis.Cmd {
	cmd := b.doWithTimeoutOnce(ctx, timeout, args...)
	switch {
	case isElasticKVNotLeaderError(cmd.Err()):
		b.RefreshLeaderNow(ctx)
	case isLeaderRefreshTransportError(cmd.Err()):
		b.TriggerRefresh()
	}
	return cmd
}

func (b *LeaderAwareRedisBackend) doWithTimeoutOnce(ctx context.Context, timeout time.Duration, args ...any) *redis.Cmd {
	cli := b.currentClientOrRefresh(ctx)
	if cli == nil {
		cmd := redis.NewCmd(ctx, args...)
		cmd.SetErr(ErrNoLeaderBackend)
		return cmd
	}
	return cli.WithTimeout(effectiveBlockingReadTimeout(timeout)).Do(ctx, args...)
}

// Pipeline forwards a batch to the current leader.
func (b *LeaderAwareRedisBackend) Pipeline(ctx context.Context, cmds [][]any) ([]*redis.Cmd, error) {
	cli := b.currentClientOrRefresh(ctx)
	if cli == nil {
		return nil, ErrNoLeaderBackend
	}
	pipe := cli.Pipeline()
	results := make([]*redis.Cmd, len(cmds))
	for i, args := range cmds {
		results[i] = pipe.Do(ctx, args...)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		var redisErr redis.Error
		if errors.As(err, &redisErr) || errors.Is(err, redis.Nil) {
			for _, result := range results {
				if isElasticKVNotLeaderError(result.Err()) {
					b.TriggerRefresh()
					break
				}
			}
			return results, nil
		}
		if isLeaderRefreshTransportError(err) {
			b.TriggerRefresh()
		}
		return results, fmt.Errorf("pipeline exec: %w", err)
	}
	return results, nil
}

func isRedisScriptCommandName(name string) bool {
	switch strings.ToUpper(name) {
	case "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO", "FUNCTION", "SCRIPT":
		return true
	default:
		return false
	}
}

func isElasticKVNotLeaderError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.TrimSpace(err.Error())
	upper := strings.ToUpper(msg)
	if upper == "NOTLEADER" || strings.HasPrefix(upper, "NOTLEADER ") {
		return true
	}

	// Redis application errors without the NOTLEADER code are not safe to
	// replay. Their text may contain a leader phrase supplied by a script or
	// command, but the command may already have changed state.
	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		return false
	}

	// Keep a closed set for leadership errors that may reach this backend
	// before Redis protocol framing (for example through a gRPC wrapper).
	return msg == "etcd raft engine is not leader" ||
		msg == "raft engine: not leader" ||
		msg == "leader not found" ||
		strings.HasSuffix(msg, "desc = leader not found") ||
		strings.HasSuffix(msg, "desc = raft engine: not leader") ||
		strings.HasSuffix(msg, "desc = etcd raft engine is not leader")
}

func isLeaderRefreshTransportError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr *net.OpError
	return errors.As(err, &netErr) && !netErr.Timeout()
}

// NewPubSub opens a subscribe connection on the current leader.
func (b *LeaderAwareRedisBackend) NewPubSub(ctx context.Context) *redis.PubSub {
	cli := b.currentClientOrRefresh(ctx)
	if cli == nil {
		cli = b.firstSeedClient()
	}
	if cli == nil {
		return nil
	}
	return cli.Subscribe(ctx)
}

// Name returns the backend's logical identifier used in metrics.
func (b *LeaderAwareRedisBackend) Name() string { return b.name }

// PoolStats reports the pool currently serving leader-directed commands.
// Counters reset when leadership moves to a client pool that has not been used
// before, so proxy metrics expose them as gauges rather than Prometheus counters.
func (b *LeaderAwareRedisBackend) PoolStats() BackendPoolStats {
	b.mu.RLock()
	cli := b.clients[b.leader]
	limit := b.opts.PoolSize
	b.mu.RUnlock()
	if cli == nil {
		return BackendPoolStats{Limit: max(limit, 0)}
	}
	return redisPoolStatsSnapshot(cli.PoolStats(), limit)
}

// Close stops the refresh loop and releases every cached client pool.
// It snapshots the client map under the lock before iterating so concurrent
// Do/Pipeline calls that race shutdown cannot mutate the map out from under
// us. After this call returns, currentClient() returns nil for every caller.
func (b *LeaderAwareRedisBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.stopCh)
	b.mu.Unlock()
	b.refreshMu.Lock()
	b.refreshClosed = true
	refreshDone := b.refreshDone
	b.refreshMu.Unlock()
	b.refreshCancel()

	// Wait for the loop and any shared in-flight probe before closing clients.
	<-b.done
	if refreshDone != nil {
		<-refreshDone
	}

	// Snapshot the clients map and swap in an empty replacement under the
	// lock. After this point, currentClient()/getOrCreateClient see no
	// clients (and ensureClientLocked refuses to add any because closed is
	// true), so iterating the snapshot is safe without holding the lock.
	b.mu.Lock()
	snapshot := make(map[string]*redis.Client, len(b.clients))
	for addr, cli := range b.clients {
		snapshot[addr] = cli
	}
	b.clients = map[string]*redis.Client{}
	b.clientOrder = b.clientOrder[:0]
	b.mu.Unlock()

	var firstErr error
	for addr, cli := range snapshot {
		if err := cli.Close(); err != nil {
			b.logger.Warn("close leader-aware client failed", "backend", b.name, "addr", addr, "err", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("close %s client %s: %w", b.name, addr, err)
			}
		}
	}
	return firstErr
}

// CurrentLeader returns the address currently considered the leader. Exposed
// for tests and operational observability.
func (b *LeaderAwareRedisBackend) CurrentLeader() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.leader
}
