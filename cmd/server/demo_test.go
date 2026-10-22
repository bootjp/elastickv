package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestDemoSamplerRouteResolverNormalizesInternalKeys(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	routeID, ok := demoSamplerRouteResolver(engine)([]byte("!redis|meta|z"))

	require.True(t, ok)
	require.Equal(t, uint64(2), routeID)
}

func TestEffectiveDemoMetricsToken(t *testing.T) {
	require.Equal(t, "custom-token", effectiveDemoMetricsToken(" custom-token "))
	require.Equal(t, "demo-metrics-token", effectiveDemoMetricsToken(""))
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
