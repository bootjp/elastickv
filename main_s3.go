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
	putAdmissionObserver adapter.S3PutAdmissionObserver,
	blobOffloadObserver adapter.S3BlobOffloadObserver,
) (*adapter.S3Server, error) {
	s3Server, _, err := prepareS3Server(
		ctx, lc, s3Addr, shardStore, coordinate, leaderS3, region,
		credentialsFile, pathStyleOnly, readTracker, putAdmissionObserver,
		blobOffloadObserver, nil,
	)
	if err != nil {
		return nil, err
	}
	runS3Server(ctx, eg, s3Server)
	return s3Server, nil
}

func prepareS3Server(
	ctx context.Context,
	lc *net.ListenConfig,
	s3Addr string,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	leaderS3 map[string]string,
	region string,
	credentialsFile string,
	pathStyleOnly bool,
	readTracker *kv.ActiveTimestampTracker,
	putAdmissionObserver adapter.S3PutAdmissionObserver,
	blobOffloadObserver adapter.S3BlobOffloadObserver,
	blobBackfiller *adapter.S3BlobBackfiller,
) (*adapter.S3Server, net.Listener, error) {
	s3Server, err := newS3Server(
		s3Addr, shardStore, coordinate, leaderS3, region, credentialsFile,
		pathStyleOnly, readTracker, putAdmissionObserver, blobOffloadObserver, nil, nil, blobBackfiller,
	)
	if err != nil {
		return nil, nil, err
	}
	s3L, err := bindS3Server(ctx, lc, s3Addr, s3Server)
	if err != nil {
		return nil, nil, err
	}
	return s3Server, s3L, nil
}

func newS3Server(
	s3Addr string,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	leaderS3 map[string]string,
	region string,
	credentialsFile string,
	pathStyleOnly bool,
	readTracker *kv.ActiveTimestampTracker,
	putAdmissionObserver adapter.S3PutAdmissionObserver,
	blobOffloadObserver adapter.S3BlobOffloadObserver,
	blobCluster adapter.S3BlobCluster,
	blobPushBlocked func() bool,
	blobBackfiller *adapter.S3BlobBackfiller,
) (*adapter.S3Server, error) {
	s3Addr = strings.TrimSpace(s3Addr)
	peerOnly := s3Addr == "" && blobCluster != nil && blobBackfiller != nil
	if s3Addr == "" && !peerOnly {
		// (nil, nil) is the explicit "S3 disabled" signal — the empty
		// flag value is a valid configuration, not an error. The
		// nilnil linter is not enabled in .golangci.yaml so no
		// suppression directive is needed.
		return nil, nil
	}
	if s3Addr != "" && !pathStyleOnly {
		return nil, errors.New("virtual-hosted style S3 requests are not implemented")
	}
	staticCreds, err := loadS3StaticCredentialsForAddress(s3Addr, credentialsFile)
	if err != nil {
		return nil, err
	}
	minReplicas, err := adapter.S3BlobMinReplicasFromEnv()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	options := []adapter.S3ServerOption{
		adapter.WithS3Region(region),
		adapter.WithS3StaticCredentials(staticCreds),
		adapter.WithS3ActiveTimestampTracker(readTracker),
		adapter.WithS3PutAdmissionObserver(putAdmissionObserver),
		adapter.WithS3BlobOffloadObserver(blobOffloadObserver),
		adapter.WithS3BlobMinReplicas(minReplicas),
		adapter.WithS3BlobPushBlocked(blobPushBlocked),
		adapter.WithS3BlobBackfiller(blobBackfiller),
	}
	if blobCluster != nil {
		options = append(options, adapter.WithS3BlobCluster(blobCluster))
	}
	s3Server := adapter.NewS3Server(
		nil,
		s3Addr,
		shardStore,
		coordinate,
		leaderS3,
		options...,
	)
	return s3Server, nil
}

func bindS3Server(ctx context.Context, lc *net.ListenConfig, s3Addr string, s3Server *adapter.S3Server) (net.Listener, error) {
	s3Addr = strings.TrimSpace(s3Addr)
	if s3Server == nil || s3Addr == "" {
		return nil, nil
	}
	s3L, err := lc.Listen(ctx, "tcp", s3Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", s3Addr)
	}
	s3Server.SetListener(s3L)
	return s3L, nil
}

func runS3Server(ctx context.Context, eg *errgroup.Group, s3Server *adapter.S3Server) {
	if s3Server == nil {
		return
	}
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
		if err := s3Server.StartBlobBackfill(ctx); err != nil {
			s3Server.Stop()
			runDoneCancel()
			return errors.WithStack(err)
		}
		err := s3Server.Run()
		runDoneCancel()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
}

func runS3BlobBackfillOnly(ctx context.Context, eg *errgroup.Group, s3Server *adapter.S3Server) {
	if s3Server == nil {
		return
	}
	eg.Go(func() error {
		if err := s3Server.StartBlobBackfill(ctx); err != nil {
			s3Server.Stop()
			return errors.WithStack(err)
		}
		<-ctx.Done()
		s3Server.Stop()
		return nil
	})
}

func s3BlobNodeEnabled(s3Address, peerTokenFile string) bool {
	return strings.TrimSpace(s3Address) != "" || strings.TrimSpace(peerTokenFile) != ""
}

func loadS3StaticCredentialsForAddress(s3Address, credentialsFile string) (map[string]string, error) {
	if s3Address == "" {
		return nil, nil
	}
	return loadS3StaticCredentials(credentialsFile)
}

func loadS3StaticCredentials(path string) (map[string]string, error) {
	return loadSigV4StaticCredentialsFile(path, "s3")
}
