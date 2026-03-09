package monitoring

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry owns the Prometheus registry used by a single Elastickv node.
type Registry struct {
	baseRegistry *prometheus.Registry
	registerer   prometheus.Registerer
	gatherer     prometheus.Gatherer

	dynamo *DynamoDBMetrics
	raft   *RaftMetrics
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
	r.raft = newRaftMetrics(registerer)
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

// RaftObserver returns the Raft topology observer backed by this registry.
func (r *Registry) RaftObserver() *RaftObserver {
	if r == nil {
		return nil
	}
	return newRaftObserver(r.raft)
}
