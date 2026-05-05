package main

import (
	"context"
	"net"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// startSQSServer constructs the SQS adapter and returns the running
// *adapter.SQSServer. The admin listener calls SigV4-bypass admin
// entrypoints against this server (see adapter/sqs_admin.go); those
// admin methods only need the coordinator/store, NOT the public SQS
// HTTP listener. So when sqsAddr is empty the function still
// constructs the server (with a nil net.Listener) — Run() then skips
// httpServer.Serve while the reaper and throttle-sweep goroutines
// still run, keeping retention math behind the admin counters
// correct. The admin bridge in main_admin.go therefore wires
// /admin/api/v1/sqs/* on the wire even on builds that disabled the
// public SigV4 endpoint.
func startSQSServer(
	ctx context.Context,
	lc *net.ListenConfig,
	eg *errgroup.Group,
	sqsAddr string,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	leaderSQS map[string]string,
	region string,
	credentialsFile string,
	partitionResolver *adapter.SQSPartitionResolver,
	partitionObserver adapter.SQSPartitionObserver,
) (*adapter.SQSServer, error) {
	sqsAddr = strings.TrimSpace(sqsAddr)
	var sqsL net.Listener
	if sqsAddr != "" {
		var err error
		sqsL, err = lc.Listen(ctx, "tcp", sqsAddr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to listen on %s", sqsAddr)
		}
	}
	staticCreds, err := loadSigV4StaticCredentialsFile(credentialsFile, "sqs")
	if err != nil {
		if sqsL != nil {
			_ = sqsL.Close()
		}
		return nil, err
	}
	sqsServer := adapter.NewSQSServer(
		sqsL,
		shardStore,
		coordinate,
		adapter.WithSQSLeaderMap(leaderSQS),
		adapter.WithSQSRegion(region),
		adapter.WithSQSStaticCredentials(staticCreds),
		adapter.WithSQSPartitionResolver(partitionResolver),
		adapter.WithSQSPartitionObserver(partitionObserver),
	)
	// Two-goroutine shutdown pattern mirrors startS3Server: one goroutine waits
	// on either ctx.Done() or Run completion to call Stop, the other runs the
	// server and cancels the waiter once it has returned.
	runDoneCtx, runDoneCancel := context.WithCancel(context.Background())
	eg.Go(func() error {
		select {
		case <-ctx.Done():
			sqsServer.Stop()
		case <-runDoneCtx.Done():
		}
		return nil
	})
	eg.Go(func() error {
		err := sqsServer.Run()
		runDoneCancel()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
	return sqsServer, nil
}
