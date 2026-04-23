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
	return r
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
