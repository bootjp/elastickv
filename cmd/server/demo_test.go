package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestDemoFSMExcludesTTLInlineCapability(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	writer, ok := st.(store.ReplicatedControlStore)
	require.True(t, ok)
	_, err := writer.ApplyReplicatedControlState(t.Context(),
		[]byte(store.ReplicatedControlPrefix+"absence-capability"),
		[]byte(`{"version":1,"index":2,"voters":[1]}`), 3)
	require.NoError(t, err)
	fsm := newDemoFSM(st, kv.NewHLC(), nil)
	capability, ok := fsm.(kv.TTLInlineCapabilityReader)
	require.True(t, ok)
	require.False(t, capability.TTLInlineCapabilityActive(), "the direct factory cannot activate from a recovered marker")
	halt, ok := fsm.Apply([]byte{0xff}).(interface{ HaltApply() error })
	require.True(t, ok)
	require.ErrorIs(t, halt.HaltApply(), kv.ErrTTLInlineCapabilityDisabled)

	clean := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, clean.Close()) })
	cleanFSM := newDemoFSM(clean, kv.NewHLC(), nil)
	_, halted := cleanFSM.Apply([]byte{0xff}).(interface{ HaltApply() error })
	require.False(t, halted, "ordinary demo FSM must remain operational")
}

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
	t.Parallel()
	require.Equal(t, "custom-token", effectiveDemoMetricsToken(" custom-token "))
	require.Equal(t, "demo-metrics-token", effectiveDemoMetricsToken(""))
}

func TestLoadS3StaticCredentials(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "s3-creds.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":"secret"}]}`), 0o600))

	creds, err := loadS3StaticCredentials(path)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"akid": "secret"}, creds)
}

func TestLoadS3StaticCredentialsAllowsEmptyPath(t *testing.T) {
	t.Parallel()
	creds, err := loadS3StaticCredentials("")
	require.NoError(t, err)
	require.Nil(t, creds)
}

func TestLoadS3StaticCredentialsRejectsEmptyFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "s3-creds.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"credentials":[{"access_key_id":"akid","secret_access_key":""}]}`), 0o600))

	_, err := loadS3StaticCredentials(path)
	require.ErrorContains(t, err, "empty access key or secret key")
}

func TestSetupS3RejectsVirtualHostedStyle(t *testing.T) {
	t.Parallel()
	s3s, err := setupS3(context.Background(), net.ListenConfig{}, store.NewMVCCStore(), nil, "127.0.0.1:50051", "127.0.0.1:9000", "", "us-east-1", "", false, nil)
	require.ErrorContains(t, err, "virtual-hosted style S3 requests are not implemented")
	require.Nil(t, s3s)
}
