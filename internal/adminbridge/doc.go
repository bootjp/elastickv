// Package adminbridge adapts the protocol adapters' admin DTOs
// (adapter.AdminQueueSummary, adapter.AdminBucketSummary,
// adapter.AdminTableSummary, ...) to the source interfaces the admin
// HTTP handlers in internal/admin consume, and builds the keyviz
// sources and cluster fan-out aggregators for the admin listener.
//
// Keeping the bridges here lets internal/admin stay free of an adapter
// dependency while the server binary only wires constructors together.
package adminbridge
