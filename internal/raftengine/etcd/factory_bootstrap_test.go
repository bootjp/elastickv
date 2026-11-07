package etcd

import (
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestFactoryRejectsIncompleteBootstrapBeforeOpeningWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	closed := false
	factory := NewFactory(FactoryConfig{BootstrapStartup: func(raftengine.FactoryConfig, *GRPCTransport) (*BootstrapStartup, error) {
		return &BootstrapStartup{Close: func() { closed = true }}, nil
	}})
	_, err := factory.Create(raftengine.FactoryConfig{GroupID: 3, DataDir: dir})
	require.Error(t, err)
	require.True(t, closed)
	require.NoDirExists(t, filepath.Join(dir, "wal"))
}

func TestFactoryBootstrapGroupMustMatchConfiguredGroup(t *testing.T) {
	t.Parallel()
	closed := false
	factory := NewFactory(FactoryConfig{BootstrapStartup: func(raftengine.FactoryConfig, *GRPCTransport) (*BootstrapStartup, error) {
		return &BootstrapStartup{
			NativeIdentity:     &WALIdentity{Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 4}},
			RecoveredAdmission: func(RecoveredState) error { return nil },
			ServerOptions:      []grpc.ServerOption{grpc.MaxRecvMsgSize(1)},
			Register:           func(grpc.ServiceRegistrar) {}, Close: func() { closed = true },
		}, nil
	}})
	_, err := factory.Create(raftengine.FactoryConfig{GroupID: 3, DataDir: t.TempDir()})
	require.ErrorIs(t, err, ErrWALIdentity)
	require.True(t, closed)
}
