package monitoring

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry owns the Prometheus registry used by a single Elastickv node.
type Registry struct {
	baseRegistry *prometheus.Registry
	registerer   prometheus.Registerer
	gatherer     prometheus.Gatherer

	dynamo        *DynamoDBMetrics
	redis         *RedisMetrics
	raft          *RaftMetrics
	lua           *LuaMetrics
	hotPath       *HotPathMetrics
	pebble        *PebbleMetrics
	writeConflict *WriteConflictMetrics
	sqs           *SQSMetrics
	sqsObserver   *SQSObserver
	fs            *FileSystemMetrics
	s3            *S3Metrics
	hlc           *HLCMetrics
	hlcObserver   *HLCObserver
	coldStart     *ColdStartMetrics
	coldStartObs  *ColdStartObserver
}

// NewRegistry builds a registry with constant labels that identify the local node.
func NewRegistry(nodeID string, nodeAddress string) *Registry {
	base := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWith(prometheus.Labels{
		"node_id":      nodeID,
		"node_address": nodeAddress,
	}, base)

	r := &Registry{
		baseRegistry: base,
		registerer:   registerer,
		gatherer:     base,
	}
	r.dynamo = newDynamoDBMetrics(registerer)
	r.redis = newRedisMetrics(registerer)
	r.raft = newRaftMetrics(registerer)
	r.lua = newLuaMetrics(registerer)
	r.hotPath = newHotPathMetrics(registerer)
	r.pebble = newPebbleMetrics(registerer)
	r.writeConflict = newWriteConflictMetrics(registerer)
	r.sqs = newSQSMetrics(registerer)
	r.sqsObserver = newSQSObserver(r.sqs)
	r.fs = newFileSystemMetrics(registerer)
	r.s3 = newS3Metrics(registerer)
	r.hlc = newHLCMetrics(registerer)
	r.hlcObserver = newHLCObserver(r.hlc)
	r.coldStart = newColdStartMetrics(registerer)
	r.coldStartObs = newColdStartObserver(r.coldStart)
	return r
}

// ColdStartObserver returns the cold-start snapshot-restore observer
// backed by this registry. The engine receives it through
// raftengine/etcd.OpenConfig.ColdStartObserver and calls it on each
// skip-gate outcome (PR #910 design §9).
func (r *Registry) ColdStartObserver() *ColdStartObserver {
	if r == nil {
		return nil
	}
	return r.coldStartObs
}

// Handler returns an HTTP handler that exposes the Prometheus scrape endpoint.
func (r *Registry) Handler() http.Handler {
	if r == nil || r.gatherer == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(r.gatherer, promhttp.HandlerOpts{})
}

// Gatherer exposes the underlying gatherer for tests and custom exporters.
func (r *Registry) Gatherer() prometheus.Gatherer {
	if r == nil {
		return nil
	}
	return r.gatherer
}

// Registerer exposes the label-wrapped registerer for callers that
// own a metric source whose lifecycle does not fit the
// newXxxMetrics(registerer) pattern in NewRegistry — currently the
// Redis adapter's Lua VM pool, which materializes inside
// NewRedisServer and registers via CounterFunc / GaugeFunc at that
// point. Returns nil if r is nil so callers can dereference safely
// in test fixtures.
func (r *Registry) Registerer() prometheus.Registerer {
	if r == nil {
		return nil
	}
	return r.registerer
}

// DynamoDBObserver returns the DynamoDB request observer backed by this registry.
func (r *Registry) DynamoDBObserver() DynamoDBRequestObserver {
	if r == nil {
		return nil
	}
	return r.dynamo
}

// RedisObserver returns the Redis request observer backed by this registry.
func (r *Registry) RedisObserver() RedisRequestObserver {
	if r == nil {
		return nil
	}
	return r.redis
}

// LuaObserver returns the Lua script execution observer backed by this registry.
func (r *Registry) LuaObserver() LuaScriptObserver {
	if r == nil {
		return nil
	}
	return r.lua
}

// RaftObserver returns the Raft topology observer backed by this registry.
func (r *Registry) RaftObserver() *RaftObserver {
	if r == nil {
		return nil
	}
	return newRaftObserver(r.raft)
}

// RaftProposalObserver returns a group-scoped observer for failed raft proposals.
func (r *Registry) RaftProposalObserver(groupID uint64) *raftProposalObserver {
	if r == nil || r.raft == nil {
		return nil
	}
	return &raftProposalObserver{
		metrics: r.raft,
		group:   strconv.FormatUint(groupID, 10),
	}
}

// LeaseReadObserver returns an observer for the kv coordinator's
// LeaseRead fast-path counter. Returns a zero-value observer when the
// registry is nil so callers can pass the result through without
// checking; the zero value silently drops samples.
func (r *Registry) LeaseReadObserver() LeaseReadObserver {
	if r == nil {
		return LeaseReadObserver{}
	}
	return LeaseReadObserver{metrics: r.hotPath}
}

// LuaFastPathObserver returns an observer for Lua-side redis.call()
// fast-path outcomes (hit / skip / fallback per command). Zero-value
// safe for tests and tools that do not wire a registry.
func (r *Registry) LuaFastPathObserver() LuaFastPathObserver {
	if r == nil {
		return LuaFastPathObserver{}
	}
	return LuaFastPathObserver{metrics: r.hotPath}
}

// DispatchCollector returns a collector that polls the etcd raft
// engine's dispatch counters and exports them to Prometheus. Start it
// with the node's raft sources after engine Open() completes.
func (r *Registry) DispatchCollector() *DispatchCollector {
	if r == nil || r.hotPath == nil {
		return nil
	}
	return newDispatchCollector(r.hotPath)
}

// PebbleCollector returns a collector that polls each Pebble store's
// Metrics() snapshot and mirrors the operationally useful fields
// (L0 sublevels, compaction debt, memtable, block cache) into
// Prometheus. Start it with the node's Pebble sources after the
// stores have been opened.
func (r *Registry) PebbleCollector() *PebbleCollector {
	if r == nil || r.pebble == nil {
		return nil
	}
	return newPebbleCollector(r.pebble)
}

// SetFSMApplySyncMode forwards the resolved ELASTICKV_FSM_SYNC_MODE
// label to the PebbleMetrics gauge so operators can observe the active
// durability posture on this node. Safe to call with a nil registry.
func (r *Registry) SetFSMApplySyncMode(activeLabel string) {
	if r == nil || r.pebble == nil {
		return
	}
	r.pebble.SetFSMApplySyncMode(activeLabel)
}

// SQSPartitionObserver returns the HT-FIFO partition-messages
// observer backed by this registry. Returns nil when the registry
// itself is nil so adapter call sites can pass the result through
// without checking; SQSMetrics.ObservePartitionMessage is also
// nil-receiver safe.
func (r *Registry) SQSPartitionObserver() SQSPartitionObserver {
	if r == nil {
		return nil
	}
	return r.sqs
}

// SQSObserver returns the queue-depth gauge observer backed by
// this registry. Same shape as RaftObserver / RedisObserver: callers
// pull it via the registry, then drive Start(ctx, source, interval)
// from main.go's startMonitoringCollectors.
func (r *Registry) SQSObserver() *SQSObserver {
	if r == nil {
		return nil
	}
	return r.sqsObserver
}

// FileSystemObserver returns the filesystem operational metrics observer backed
// by this registry.
func (r *Registry) FileSystemObserver() FileSystemObserver {
	if r == nil {
		return nil
	}
	return r.fs
}

// S3PutAdmissionObserver returns the S3 PUT admission metrics observer backed
// by this registry. The adapter owns admission decisions and calls this small
// interface directly from the hot path.
func (r *Registry) S3PutAdmissionObserver() S3PutAdmissionObserver {
	if r == nil || r.s3 == nil {
		return nil
	}
	return r.s3
}

// S3BlobOffloadObserver returns the S3 blob-offload metrics observer backed by
// this registry. The offload data path is rollout-gated in the adapter; this
// observer records both fallback decisions and future chunkblob durability
// outcomes.
func (r *Registry) S3BlobOffloadObserver() S3BlobOffloadObserver {
	if r == nil || r.s3 == nil {
		return nil
	}
	return r.s3
}

// WriteConflictCollector returns a collector that polls each MVCC
// store's per-(kind, key_prefix) OCC conflict counters and mirrors
// them into the elastickv_store_write_conflict_total Prometheus
// counter vector. Start it with the node's MVCC sources after the
// stores have been opened.
func (r *Registry) WriteConflictCollector() *WriteConflictCollector {
	if r == nil || r.writeConflict == nil {
		return nil
	}
	return newWriteConflictCollector(r.writeConflict)
}

// HLCObserver returns the HLC physical-ceiling + fence-rejection
// observer backed by this registry. Same shape as SQSObserver: a
// single observer is constructed inside NewRegistry and returned by
// reference here, so callers that pull it more than once observe
// the same lastRejections delta state (returning a fresh observer
// each call would reset lastRejections and risk double-counting
// rejections against the cumulative Prometheus counter — claude
// review on PR #879). Returns nil if r is nil so test fixtures
// can drop the observer without conditional wiring.
func (r *Registry) HLCObserver() *HLCObserver {
	if r == nil || r.hlcObserver == nil {
		return nil
	}
	return r.hlcObserver
}
