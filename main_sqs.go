package main

import (
	"context"
	"log/slog"
	"net"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// prepareSQSServer constructs the SQS adapter. The admin listener calls
// SigV4-bypass admin entrypoints against this server (see adapter/sqs_admin.go);
// those admin methods only need the coordinator/store, NOT the public SQS HTTP
// listener. So when sqsAddr is empty the function still constructs the server
// with a nil net.Listener.
func prepareSQSServer(
	ctx context.Context,
	lc *net.ListenConfig,
	sqsAddr string,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	leaderSQS map[string]string,
	region string,
	credentialsFile string,
	partitionResolver *adapter.SQSPartitionResolver,
	partitionObserver adapter.SQSPartitionObserver,
) (*adapter.SQSServer, net.Listener, error) {
	sqsAddr = strings.TrimSpace(sqsAddr)
	sqsL, err := openSQSListener(ctx, lc, sqsAddr)
	if err != nil {
		return nil, nil, err
	}
	staticCreds, err := loadSQSStaticCredentials(credentialsFile, sqsAddr)
	if err != nil {
		closeSQSListenerOnError(sqsL, sqsAddr)
		return nil, nil, err
	}
	var throttleObserver adapter.SQSThrottleObserver
	if o, ok := partitionObserver.(adapter.SQSThrottleObserver); ok {
		throttleObserver = o
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
		adapter.WithSQSThrottleObserver(throttleObserver),
	)
	return sqsServer, sqsL, nil
}

func runSQSServer(ctx context.Context, eg *errgroup.Group, sqsServer *adapter.SQSServer) {
	if sqsServer == nil {
		return
	}
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
}

// openSQSListener returns the bound listener for sqsAddr, or
// (nil, nil) when sqsAddr is empty (listenless / admin-only mode).
// Pulled out of prepareSQSServer so the constructor stays under the
// cyclop budget; the listenless branch is otherwise the same one
// the function-level docstring describes.
func openSQSListener(ctx context.Context, lc *net.ListenConfig, sqsAddr string) (net.Listener, error) {
	if sqsAddr == "" {
		return nil, nil
	}
	l, err := lc.Listen(ctx, "tcp", sqsAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", sqsAddr)
	}
	return l, nil
}

// loadSQSStaticCredentials wraps loadSigV4StaticCredentialsFile with
// the listenless-mode short-circuit: when sqsAddr is empty there is
// no public HTTP listener and the admin /admin/api/v1/sqs/* endpoints
// bypass SigV4, so the credentials file is not needed at all.
// Pre-listenless behavior was "SQS disabled, no creds required" —
// keeping creds optional here preserves that contract instead of
// gating admin-only deployments on a stale or malformed creds path.
func loadSQSStaticCredentials(credentialsFile, sqsAddr string) (map[string]string, error) {
	if sqsAddr == "" {
		return nil, nil
	}
	return loadSigV4StaticCredentialsFile(credentialsFile, "sqs")
}

// closeSQSListenerOnError closes the listener after another startup
// step has failed and logs any Close error so listener cleanup
// failures (port stuck bound, fd leak) are visible to operators
// rather than silently ignored.
func closeSQSListenerOnError(l net.Listener, sqsAddr string) {
	if l == nil {
		return
	}
	if err := l.Close(); err != nil {
		slog.Warn("sqs listener close after startup failure", "addr", sqsAddr, "err", err)
	}
}
