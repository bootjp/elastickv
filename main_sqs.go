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

// startSQSServer stands up the SQS adapter on sqsAddr and returns the
// running *adapter.SQSServer so the admin listener can call SigV4-bypass
// admin entrypoints against it (see adapter/sqs_admin.go). Returns
// (nil, nil) when sqsAddr is empty — that is the "SQS disabled" branch
// and the admin listener leaves /admin/api/v1/sqs/* off the wire.
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
) (*adapter.SQSServer, error) {
	sqsAddr = strings.TrimSpace(sqsAddr)
	if sqsAddr == "" {
		return nil, nil
	}
	sqsL, err := lc.Listen(ctx, "tcp", sqsAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", sqsAddr)
	}
	staticCreds, err := loadSigV4StaticCredentialsFile(credentialsFile, "sqs")
	if err != nil {
		_ = sqsL.Close()
		return nil, err
	}
	sqsServer := adapter.NewSQSServer(
		sqsL,
		shardStore,
		coordinate,
		adapter.WithSQSLeaderMap(leaderSQS),
		adapter.WithSQSRegion(region),
		adapter.WithSQSStaticCredentials(staticCreds),
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
