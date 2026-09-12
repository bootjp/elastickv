package main

import (
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/stretchr/testify/require"
)

// TestSqsQueuesBridgeExposesTheAdminObserver checks the PRODUCTION wrapper, not
// just the interface.
//
// The admin handler counts its own pre-dispatch rejections by pulling the
// observer off its QueuesSource. In production that source is
// *sqsQueuesBridge, not *adapter.SQSServer — so a handler-side test using a
// stub can pass while production records nothing, because the bridge does not
// satisfy the capability interface.
func TestSqsQueuesBridgeExposesTheAdminObserver(t *testing.T) {
	t.Parallel()

	var source admin.QueuesSource = &sqsQueuesBridge{}
	provider, ok := source.(admin.AdminQueueObserverSource)
	require.True(t, ok,
		"the production bridge must satisfy admin.AdminQueueObserverSource, "+
			"or the handler's pre-dispatch outcomes are never counted in production")

	// With no server: a genuine nil, not a typed-nil interface, which
	// WithAdminQueueObserver would otherwise accept and then call into.
	require.Nil(t, provider.AdminQueueObserver())
}

// And the observer handed over must be the one the adapter records through, or
// the two halves of each counter describe different things.
func TestSqsQueuesBridgeHandsOverTheServersObserver(t *testing.T) {
	t.Parallel()

	// The same derivation main_sqs.go performs: the registry's SQSMetrics
	// serves every SQS observer interface.
	registry := monitoring.NewRegistry("n1", "127.0.0.1:50051")
	partitionObserver := registry.SQSPartitionObserver()
	adminObserver, ok := partitionObserver.(adapter.SQSAdminObserver)
	require.True(t, ok, "the registry's SQSMetrics must serve the adapter's admin observer")

	server := adapter.NewSQSServer(nil, nil, nil, adapter.WithSQSAdminObserver(adminObserver))

	// Through the interface, not the concrete method: if the bridge stops
	// satisfying the capability this fails as a test rather than as a compile
	// error, which is what the handler's lookup actually does at runtime.
	var source admin.QueuesSource = &sqsQueuesBridge{server: server}
	provider, ok := source.(admin.AdminQueueObserverSource)
	require.True(t, ok, "the production bridge must satisfy admin.AdminQueueObserverSource")

	got := provider.AdminQueueObserver()
	require.NotNil(t, got)
	require.Equal(t, adminObserver, got,
		"the handler must count on the same metrics object the adapter uses")
}

// One object serves both sides, so it has to satisfy both interfaces.
func TestSQSMetricsSatisfiesBothObserverInterfaces(t *testing.T) {
	t.Parallel()

	registry := monitoring.NewRegistry("n1", "127.0.0.1:50051")
	partitionObserver := registry.SQSPartitionObserver()

	_, adapterOK := partitionObserver.(adapter.SQSAdminObserver)
	require.True(t, adapterOK, "adapter.SQSAdminObserver")
	_, adminOK := partitionObserver.(admin.AdminQueueObserver)
	require.True(t, adminOK, "admin.AdminQueueObserver")
}
