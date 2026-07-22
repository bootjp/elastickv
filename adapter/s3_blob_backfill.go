package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	s3BlobBackfillWorkersEnvVar      = "ELASTICKV_S3_CHUNKBLOB_FETCH_WORKERS"
	s3BlobBackfillQueueSizeEnvVar    = "ELASTICKV_S3_CHUNKBLOB_FETCH_QUEUE_SIZE"
	s3BlobBackfillRateEnvVar         = "ELASTICKV_S3_CHUNKBLOB_FETCH_RATE_PER_PEER"
	s3BlobBackfillBurstEnvVar        = "ELASTICKV_S3_CHUNKBLOB_FETCH_BURST_PER_PEER"
	s3BlobBackfillScanIntervalEnvVar = "ELASTICKV_S3_CHUNKBLOB_BACKFILL_SCAN_INTERVAL"

	s3BlobBackfillDefaultWorkers      = 4
	s3BlobBackfillDefaultQueueSize    = 4096
	s3BlobBackfillDefaultRate         = 32
	s3BlobBackfillDefaultBurst        = 8
	s3BlobBackfillDefaultScanInterval = time.Minute
	s3BlobBackfillDefaultScanPageSize = 256
	s3BlobBackfillDefaultMaxAttempts  = 3
	s3BlobBackfillRetryInitial        = 100 * time.Millisecond
	s3BlobBackfillRetryMax            = time.Second
	s3BlobBackfillRetryFactor         = 2

	s3BlobBackfillResultFetched   = "fetched"
	s3BlobBackfillResultLocalHit  = "local_hit"
	s3BlobBackfillResultGone      = "gone"
	s3BlobBackfillResultFailed    = "failed"
	s3BlobBackfillResultQueueDrop = "queue_drop"
)

// S3BlobBackfillConfig bounds follower catch-up work independently from the
// Raft apply loop and from user-path proxy-on-miss reads.
type S3BlobBackfillConfig struct {
	Workers      int
	QueueSize    int
	RatePerPeer  int
	BurstPerPeer int
	ScanInterval time.Duration
	ScanPageSize int
	MaxAttempts  int
	RetryInitial time.Duration
	RetryMax     time.Duration
}

// S3BlobBackfillConfigFromEnv loads the operational bounds for async follower
// blob fetch. Invalid or non-positive values fail startup instead of silently
// removing a resource bound.
func S3BlobBackfillConfigFromEnv() (S3BlobBackfillConfig, error) {
	cfg := defaultS3BlobBackfillConfig()
	var err error
	if cfg.Workers, err = positiveEnvInt(s3BlobBackfillWorkersEnvVar, cfg.Workers); err != nil {
		return S3BlobBackfillConfig{}, err
	}
	if cfg.QueueSize, err = positiveEnvInt(s3BlobBackfillQueueSizeEnvVar, cfg.QueueSize); err != nil {
		return S3BlobBackfillConfig{}, err
	}
	if cfg.RatePerPeer, err = positiveEnvInt(s3BlobBackfillRateEnvVar, cfg.RatePerPeer); err != nil {
		return S3BlobBackfillConfig{}, err
	}
	if cfg.BurstPerPeer, err = positiveEnvInt(s3BlobBackfillBurstEnvVar, cfg.BurstPerPeer); err != nil {
		return S3BlobBackfillConfig{}, err
	}
	rawInterval := strings.TrimSpace(os.Getenv(s3BlobBackfillScanIntervalEnvVar))
	if rawInterval != "" {
		cfg.ScanInterval, err = time.ParseDuration(rawInterval)
		if err != nil || cfg.ScanInterval <= 0 {
			return S3BlobBackfillConfig{}, errors.WithStack(
				errors.Newf("%s must be a positive duration", s3BlobBackfillScanIntervalEnvVar),
			)
		}
	}
	return cfg, nil
}

func positiveEnvInt(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, errors.WithStack(errors.Newf("%s must be a positive integer", name))
	}
	return value, nil
}

func defaultS3BlobBackfillConfig() S3BlobBackfillConfig {
	return S3BlobBackfillConfig{
		Workers:      s3BlobBackfillDefaultWorkers,
		QueueSize:    s3BlobBackfillDefaultQueueSize,
		RatePerPeer:  s3BlobBackfillDefaultRate,
		BurstPerPeer: s3BlobBackfillDefaultBurst,
		ScanInterval: s3BlobBackfillDefaultScanInterval,
		ScanPageSize: s3BlobBackfillDefaultScanPageSize,
		MaxAttempts:  s3BlobBackfillDefaultMaxAttempts,
		RetryInitial: s3BlobBackfillRetryInitial,
		RetryMax:     s3BlobBackfillRetryMax,
	}
}

func normalizeS3BlobBackfillConfig(cfg S3BlobBackfillConfig) S3BlobBackfillConfig {
	defaults := defaultS3BlobBackfillConfig()
	if cfg.Workers <= 0 {
		cfg.Workers = defaults.Workers
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaults.QueueSize
	}
	if cfg.RatePerPeer <= 0 {
		cfg.RatePerPeer = defaults.RatePerPeer
	}
	if cfg.BurstPerPeer <= 0 {
		cfg.BurstPerPeer = defaults.BurstPerPeer
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = defaults.ScanInterval
	}
	if cfg.ScanPageSize <= 0 {
		cfg.ScanPageSize = defaults.ScanPageSize
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaults.MaxAttempts
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = defaults.RetryInitial
	}
	if cfg.RetryMax < cfg.RetryInitial {
		cfg.RetryMax = max(defaults.RetryMax, cfg.RetryInitial)
	}
	return cfg
}

// S3BlobLocalStoreScanner exposes every local shard store so startup and
// periodic scans can recover chunkrefs not observed through the live apply
// hook, including refs installed by snapshot restore.
type S3BlobLocalStoreScanner interface {
	LocalStores() []store.MVCCStore
}

// S3BlobBackfillObserver is an optional extension implemented by the metrics
// observer. Keeping it separate preserves compatibility with small test and
// embedding observers that only implement the M1 metric surface.
type S3BlobBackfillObserver interface {
	ObserveS3ChunkBlobBackfillQueueDepth(depth int)
	ObserveS3ChunkBlobBackfillResult(result string)
}

// S3BlobBackfiller receives non-blocking FSM apply notifications and performs
// all reads, peer RPCs, verification, retries, and local writes on bounded
// background workers.
type S3BlobBackfiller struct {
	config  S3BlobBackfillConfig
	queue   chan []byte
	pending sync.Map

	mu      sync.Mutex
	cancel  context.CancelFunc
	started bool
	wg      sync.WaitGroup
	server  atomic.Pointer[S3Server]

	limitersMu sync.Mutex
	limiters   map[string]*s3BlobTokenBucket
}

var _ kv.ApplyObserver = (*S3BlobBackfiller)(nil)

func NewS3BlobBackfiller(cfg S3BlobBackfillConfig) *S3BlobBackfiller {
	cfg = normalizeS3BlobBackfillConfig(cfg)
	return &S3BlobBackfiller{
		config:   cfg,
		queue:    make(chan []byte, cfg.QueueSize),
		limiters: make(map[string]*s3BlobTokenBucket),
	}
}

// OnApply stays non-blocking because it runs inline on the Raft apply
// goroutine. A full queue drops the notification; the periodic scanner is the
// durable recovery path for that case.
func (b *S3BlobBackfiller) OnApply(op pb.Op, key []byte) {
	if b == nil || op != pb.Op_PUT {
		return
	}
	if _, _, _, _, _, _, _, ok := s3keys.ParseVersionedChunkRefKey(key); !ok {
		return
	}
	b.enqueue(context.Background(), key, false)
}

func (b *S3BlobBackfiller) Start(ctx context.Context, server *S3Server) error {
	if b == nil {
		return nil
	}
	if server == nil || server.blobCluster == nil || server.blobLocalStores == nil {
		return errors.New("s3 blob backfill data path is not configured")
	}
	if _, ok := server.blobLocalStores.(S3BlobLocalStoreScanner); !ok {
		return errors.New("s3 blob backfill local store scanner is not configured")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	b.server.Store(server)
	b.cancel = cancel
	b.started = true
	for range b.config.Workers {
		b.wg.Add(1)
		go b.runWorker(runCtx)
	}
	b.wg.Add(1)
	go b.runScanner(runCtx)
	return nil
}

func (b *S3BlobBackfiller) Stop() {
	if b == nil {
		return
	}
	b.mu.Lock()
	cancel := b.cancel
	b.cancel = nil
	b.started = false
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	b.wg.Wait()
	b.server.Store(nil)
	b.clearPendingQueue()
}

func (b *S3BlobBackfiller) clearPendingQueue() {
	for {
		select {
		case key := <-b.queue:
			b.pending.Delete(string(key))
		default:
			b.pending.Range(func(key, _ any) bool {
				b.pending.Delete(key)
				return true
			})
			return
		}
	}
}

func (b *S3BlobBackfiller) enqueue(ctx context.Context, key []byte, wait bool) bool {
	identity := string(key)
	if _, loaded := b.pending.LoadOrStore(identity, struct{}{}); loaded {
		return true
	}
	queued := bytes.Clone(key)
	if wait {
		select {
		case b.queue <- queued:
			b.observeQueueDepth()
			return true
		case <-ctx.Done():
			b.pending.Delete(identity)
			return false
		}
	}
	select {
	case b.queue <- queued:
		b.observeQueueDepth()
		return true
	default:
		b.pending.Delete(identity)
		b.observeResult(s3BlobBackfillResultQueueDrop)
		return false
	}
}

func (b *S3BlobBackfiller) runWorker(ctx context.Context) {
	defer b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-b.queue:
			b.observeQueueDepth()
			b.backfillWithRetry(ctx, key)
			b.pending.Delete(string(key))
		}
	}
}

func (b *S3BlobBackfiller) backfillWithRetry(ctx context.Context, key []byte) {
	backoff := b.config.RetryInitial
	var lastErr error
	for attempt := 1; attempt <= b.config.MaxAttempts; attempt++ {
		result, err := b.backfillOne(ctx, key)
		if err == nil {
			b.observeResult(result)
			return
		}
		lastErr = err
		if attempt == b.config.MaxAttempts || !waitS3BlobBackfillRetry(ctx, backoff) {
			break
		}
		backoff = min(backoff*s3BlobBackfillRetryFactor, b.config.RetryMax)
	}
	if ctx.Err() == nil {
		b.observeResult(s3BlobBackfillResultFailed)
		slog.Warn("s3 chunkblob backfill failed", "key", string(key), "err", lastErr)
	}
}

func waitS3BlobBackfillRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (b *S3BlobBackfiller) backfillOne(ctx context.Context, refKey []byte) (string, error) {
	server := b.server.Load()
	if server == nil {
		return "", errors.New("s3 blob backfill server is not started")
	}
	metadataStore, ok := server.blobLocalStores.LocalStoreForKey(refKey)
	if !ok || metadataStore == nil {
		return "", errors.New("resolve local s3 chunkref store")
	}
	ref, refCommitTS, gone, err := loadS3BlobBackfillRef(ctx, metadataStore, refKey)
	if gone {
		return s3BlobBackfillResultGone, nil
	}
	if err != nil {
		return "", err
	}
	exists, err := server.localS3ChunkBlobExists(ctx, refKey, ref.ContentSHA256)
	if err != nil {
		return "", err
	}
	if exists {
		return s3BlobBackfillResultLocalHit, nil
	}
	payload, err := b.fetchFromPeers(ctx, refKey, ref)
	if err != nil {
		return "", err
	}
	if err := server.storeFetchedS3ChunkBlob(ctx, refKey, ref.ContentSHA256, payload, refCommitTS); err != nil {
		return "", err
	}
	return s3BlobBackfillResultFetched, nil
}

func loadS3BlobBackfillRef(
	ctx context.Context,
	metadataStore store.MVCCStore,
	refKey []byte,
) (s3keys.ChunkRefValue, uint64, bool, error) {
	value, err := metadataStore.GetAt(ctx, refKey, math.MaxUint64)
	if errors.Is(err, store.ErrKeyNotFound) {
		return s3keys.ChunkRefValue{}, 0, true, nil
	}
	if err != nil {
		return s3keys.ChunkRefValue{}, 0, false, errors.Wrap(err, "read local s3 chunkref")
	}
	ref, ok := s3keys.DecodeChunkRefValue(value)
	if !ok {
		return s3keys.ChunkRefValue{}, 0, false, errors.New("decode local s3 chunkref")
	}
	refCommitTS, exists, err := metadataStore.LatestCommitTS(ctx, refKey)
	if err != nil {
		return s3keys.ChunkRefValue{}, 0, false, errors.Wrap(err, "read local s3 chunkref timestamp")
	}
	if !exists || refCommitTS == 0 {
		return s3keys.ChunkRefValue{}, 0, true, nil
	}
	return ref, refCommitTS, false, nil
}

func (b *S3BlobBackfiller) fetchFromPeers(ctx context.Context, refKey []byte, ref s3keys.ChunkRefValue) ([]byte, error) {
	server := b.server.Load()
	if server == nil {
		return nil, errors.New("s3 blob backfill server is not started")
	}
	current, discoveryErr := server.blobCluster.ReplicasForChunk(ctx, refKey)
	writeTime := writeTimeS3BlobReplicas(ref.ReplicaPeers)
	if discoveryErr != nil && len(writeTime) == 0 {
		return nil, errors.Wrap(discoveryErr, "resolve s3 chunkblob backfill replicas")
	}
	peers := orderedS3BlobPeers(writeTime, current, server.blobCluster.SelfNodeID(), ref.SourcePeer)
	for _, peer := range peers {
		payload, ok, err := b.fetchFromPeer(ctx, server, peer, ref)
		if err != nil {
			return nil, err
		}
		if ok {
			return payload, nil
		}
	}
	return nil, errors.New("s3 chunkblob backfill failed on every replica")
}

func (b *S3BlobBackfiller) fetchFromPeer(
	ctx context.Context,
	server *S3Server,
	peer S3BlobReplica,
	ref s3keys.ChunkRefValue,
) ([]byte, bool, error) {
	if err := b.limiterFor(peer).Wait(ctx); err != nil {
		return nil, false, err
	}
	payload, err := server.blobCluster.FetchChunkBlob(ctx, peer, ref.ContentSHA256)
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			server.observeS3ChunkBlobMismatch()
		}
		if ctx.Err() != nil {
			return nil, false, errors.WithStack(ctx.Err())
		}
		return nil, false, nil
	}
	actual := sha256.Sum256(payload)
	if actual != ref.ContentSHA256 || uint64(len(payload)) != ref.Size { //nolint:gosec // Chunk size is bounded by the S3 data path.
		server.observeS3ChunkBlobMismatch()
		return nil, false, nil
	}
	return payload, true, nil
}

func (b *S3BlobBackfiller) limiterFor(peer S3BlobReplica) *s3BlobTokenBucket {
	identity := peer.NodeID + "\x00" + peer.Address
	b.limitersMu.Lock()
	defer b.limitersMu.Unlock()
	limiter := b.limiters[identity]
	if limiter == nil {
		limiter = newS3BlobTokenBucket(b.config.RatePerPeer, b.config.BurstPerPeer)
		b.limiters[identity] = limiter
	}
	return limiter
}

func (b *S3BlobBackfiller) runScanner(ctx context.Context) {
	defer b.wg.Done()
	b.scanLocalChunkRefs(ctx)
	ticker := time.NewTicker(b.config.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.scanLocalChunkRefs(ctx)
		}
	}
}

func (b *S3BlobBackfiller) scanLocalChunkRefs(ctx context.Context) {
	server := b.server.Load()
	if server == nil {
		return
	}
	scanner, ok := server.blobLocalStores.(S3BlobLocalStoreScanner)
	if !ok {
		return
	}
	for _, localStore := range scanner.LocalStores() {
		if localStore == nil || !b.scanChunkRefsInStore(ctx, localStore) {
			return
		}
	}
}

func (b *S3BlobBackfiller) scanChunkRefsInStore(ctx context.Context, localStore store.MVCCStore) bool {
	start := []byte(s3keys.ChunkRefPrefix)
	end := prefixScanEnd(start)
	for {
		rows, err := localStore.ScanAt(ctx, start, end, b.config.ScanPageSize, math.MaxUint64)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("scan local s3 chunkrefs for backfill", "err", err)
			}
			return ctx.Err() == nil
		}
		for _, row := range rows {
			if row == nil || !b.enqueue(ctx, row.Key, true) {
				return false
			}
		}
		if len(rows) < b.config.ScanPageSize {
			return true
		}
		start = append(bytes.Clone(rows[len(rows)-1].Key), 0)
	}
}

func (b *S3BlobBackfiller) observer() S3BlobBackfillObserver {
	if b == nil {
		return nil
	}
	server := b.server.Load()
	if server == nil || server.blobOffloadObserver == nil {
		return nil
	}
	observer, _ := server.blobOffloadObserver.(S3BlobBackfillObserver)
	return observer
}

func (b *S3BlobBackfiller) observeQueueDepth() {
	if observer := b.observer(); observer != nil {
		observer.ObserveS3ChunkBlobBackfillQueueDepth(len(b.queue))
	}
}

func (b *S3BlobBackfiller) observeResult(result string) {
	if observer := b.observer(); observer != nil {
		observer.ObserveS3ChunkBlobBackfillResult(result)
	}
}

type s3BlobTokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newS3BlobTokenBucket(ratePerSecond, burst int) *s3BlobTokenBucket {
	return &s3BlobTokenBucket{
		rate:   float64(ratePerSecond),
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
	}
}

func (b *s3BlobTokenBucket) Wait(ctx context.Context) error {
	wait := b.reserve(time.Now())
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		b.cancelReservation()
		return errors.WithStack(ctx.Err())
	case <-timer.C:
		return nil
	}
}

func (b *s3BlobTokenBucket) cancelReservation() {
	b.mu.Lock()
	b.tokens = min(b.burst, b.tokens+1)
	b.mu.Unlock()
}

func (b *s3BlobTokenBucket) reserve(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.After(b.last) {
		b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
	b.tokens--
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / b.rate * float64(time.Second))
}
