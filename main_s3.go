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

// startS3Server stands up the S3-compatible HTTP listener on s3Addr
// and returns the constructed *adapter.S3Server. Callers that need a
// reference to the running server (e.g. the admin HTTP listener,
// which calls SigV4-bypass admin entrypoints on it) hold the
// returned value; callers that don't can ignore it.
//
// Returns (nil, nil) when s3Addr is empty — that is the well-known
// "S3 disabled" state, not a configuration error. Other failures
// surface as a non-nil error and a nil server.
func startS3Server(
	ctx context.Context,
	lc *net.ListenConfig,
	eg *errgroup.Group,
	s3Addr string,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	leaderS3 map[string]string,
	region string,
	credentialsFile string,
	pathStyleOnly bool,
	readTracker *kv.ActiveTimestampTracker,
) (*adapter.S3Server, error) {
	s3Addr = strings.TrimSpace(s3Addr)
	if s3Addr == "" {
		// (nil, nil) is the explicit "S3 disabled" signal — the empty
		// flag value is a valid configuration, not an error. The
		// nilnil linter is not enabled in .golangci.yaml so no
		// suppression directive is needed.
		return nil, nil
	}
	if !pathStyleOnly {
		return nil, errors.New("virtual-hosted style S3 requests are not implemented")
	}
	s3L, err := lc.Listen(ctx, "tcp", s3Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", s3Addr)
	}
	staticCreds, err := loadS3StaticCredentials(credentialsFile)
	if err != nil {
		_ = s3L.Close()
		return nil, err
	}
	s3Server := adapter.NewS3Server(
		s3L,
		s3Addr,
		shardStore,
		coordinate,
		leaderS3,
		adapter.WithS3Region(region),
		adapter.WithS3StaticCredentials(staticCreds),
		adapter.WithS3ActiveTimestampTracker(readTracker),
	)
	runDoneCtx, runDoneCancel := context.WithCancel(context.Background())
	eg.Go(func() error {
		select {
		case <-ctx.Done():
			s3Server.Stop()
		case <-runDoneCtx.Done():
		}
		return nil
	})
	eg.Go(func() error {
		err := s3Server.Run()
		runDoneCancel()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
	return s3Server, nil
}

func loadS3StaticCredentials(path string) (map[string]string, error) {
	return loadSigV4StaticCredentialsFile(path, "s3")
}
