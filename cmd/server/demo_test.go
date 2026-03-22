package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestEffectiveDemoMetricsToken(t *testing.T) {
	require.Equal(t, "custom-token", effectiveDemoMetricsToken(" custom-token "))
	require.Equal(t, "demo-metrics-token", effectiveDemoMetricsToken(""))
}

func TestSetupMetricsHTTPServerAllowsBlankAddress(t *testing.T) {
	listener, server, err := setupMetricsHTTPServer(context.Background(), net.ListenConfig{}, "", "", http.NewServeMux())
	require.NoError(t, err)
	require.Nil(t, listener)
	require.Nil(t, server)
}

func TestSetupMetricsHTTPServerRejectsInvalidAddressBeforeTokenCheck(t *testing.T) {
	listener, server, err := setupMetricsHTTPServer(context.Background(), net.ListenConfig{}, "localhost", "", http.NewServeMux())
	require.ErrorContains(t, err, `invalid metricsAddress "localhost"`)
	require.Nil(t, listener)
	require.Nil(t, server)
}

func TestSetupPprofHTTPServerAllowsBlankAddress(t *testing.T) {
	listener, server, err := setupPprofHTTPServer(context.Background(), net.ListenConfig{}, "", "")
	require.NoError(t, err)
	require.Nil(t, listener)
	require.Nil(t, server)
}

func TestSetupPprofHTTPServerRejectsInvalidAddressBeforeTokenCheck(t *testing.T) {
	listener, server, err := setupPprofHTTPServer(context.Background(), net.ListenConfig{}, "localhost", "")
	require.ErrorContains(t, err, `invalid pprofAddress "localhost"`)
	require.Nil(t, listener)
	require.Nil(t, server)
}

func TestLoadS3StaticCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s3-creds.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":"secret"}]}`), 0o600))

	creds, err := loadS3StaticCredentials(path)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"akid": "secret"}, creds)
}

func TestLoadS3StaticCredentialsAllowsEmptyPath(t *testing.T) {
	creds, err := loadS3StaticCredentials("")
	require.NoError(t, err)
	require.Nil(t, creds)
}

func TestLoadS3StaticCredentialsRejectsEmptyFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s3-creds.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":""}]}`), 0o600))

	_, err := loadS3StaticCredentials(path)
	require.ErrorContains(t, err, "empty access key or secret key")
}

func TestSetupS3RejectsVirtualHostedStyle(t *testing.T) {
	s3s, err := setupS3(context.Background(), net.ListenConfig{}, store.NewMVCCStore(), nil, "127.0.0.1:50051", "127.0.0.1:9000", "", "us-east-1", "", false, nil)
	require.ErrorContains(t, err, "virtual-hosted style S3 requests are not implemented")
	require.Nil(t, s3s)
}
