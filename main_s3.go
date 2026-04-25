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
) error {
	s3Addr = strings.TrimSpace(s3Addr)
	if s3Addr == "" {
		return nil
	}
	if !pathStyleOnly {
		return errors.New("virtual-hosted style S3 requests are not implemented")
	}
	s3L, err := lc.Listen(ctx, "tcp", s3Addr)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on %s", s3Addr)
	}
	staticCreds, err := loadS3StaticCredentials(credentialsFile)
	if err != nil {
		_ = s3L.Close()
		return err
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
	return nil
}

func loadS3StaticCredentials(path string) (map[string]string, error) {
	return loadSigV4StaticCredentialsFile(path, "s3")
}
