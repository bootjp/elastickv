package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
)

type s3CredentialFile struct {
	Credentials []s3CredentialEntry `json:"credentials"`
}

type s3CredentialEntry struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func startS3Server(
	ctx context.Context,
	lc *net.ListenConfig,
	eg *errgroup.Group,
	s3Addr string,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	leaderS3 map[raft.ServerAddress]string,
	region string,
	credentialsFile string,
	pathStyleOnly bool,
	readTracker *kv.ActiveTimestampTracker,
) error {
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
	eg.Go(func() error {
		defer s3Server.Stop()
		stop := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				s3Server.Stop()
			case <-stop:
			}
		}()
		err := s3Server.Run()
		close(stop)
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	})
	return nil
}

func loadS3StaticCredentials(path string) (map[string]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	file := s3CredentialFile{}
	if err := json.Unmarshal(body, &file); err != nil {
		return nil, errors.WithStack(err)
	}
	out := make(map[string]string, len(file.Credentials))
	for _, cred := range file.Credentials {
		accessKeyID := strings.TrimSpace(cred.AccessKeyID)
		secretAccessKey := strings.TrimSpace(cred.SecretAccessKey)
		if accessKeyID == "" || secretAccessKey == "" {
			return nil, errors.New("s3 credentials file contains an empty access key or secret key")
		}
		out[accessKeyID] = secretAccessKey
	}
	return out, nil
}
