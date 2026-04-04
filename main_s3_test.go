package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStartS3ServerRejectsVirtualHostedStyleConfig(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	err := startS3Server(ctx, &net.ListenConfig{}, eg, "localhost:9000", nil, nil, nil, "us-east-1", "", false, nil)
	require.ErrorContains(t, err, "virtual-hosted style S3 requests are not implemented")
}

func TestStartS3ServerAllowsEmptyAddress(t *testing.T) {
	eg, ctx := errgroup.WithContext(context.Background())
	err := startS3Server(ctx, &net.ListenConfig{}, eg, "", nil, nil, nil, "us-east-1", "", false, nil)
	require.NoError(t, err)
}

func TestLoadS3StaticCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s3creds.json")
	err := os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":"secret"}]}`), 0o600)
	require.NoError(t, err)

	creds, err := loadS3StaticCredentials(path)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"akid": "secret"}, creds)
}

func TestLoadS3StaticCredentialsRejectsEmptyValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s3creds.json")
	err := os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":" ","secret_access_key":"secret"}]}`), 0o600)
	require.NoError(t, err)

	_, err = loadS3StaticCredentials(path)
	require.ErrorContains(t, err, "s3 credentials file contains an empty access key or secret key")
}

func TestLoadS3StaticCredentialsRejectsDuplicateAccessKeyIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s3creds.json")
	err := os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":"secret-1"},{"access_key_id":"akid","secret_access_key":"secret-2"}]}`), 0o600)
	require.NoError(t, err)

	_, err = loadS3StaticCredentials(path)
	require.ErrorContains(t, err, `s3 credentials file contains duplicate access key ID: "akid"`)
}
