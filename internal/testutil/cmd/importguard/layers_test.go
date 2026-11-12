package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckLayering(t *testing.T) {
	t.Parallel()
	const m = modulePath + "/"
	for _, tc := range []struct {
		name, dir, imp string
		violation      bool
	}{
		{"stdlib is ignored", "store", "fmt", false},
		{"third party is ignored", "store", "github.com/cockroachdb/pebble/v2", false},
		{"store below kv", "store", m + "kv", true},
		{"store subpackage below kv", "store/sub", m + "kv", true},
		{"store may use encryption", "store", m + "internal/encryption", false},
		{"kv may use store", "kv", m + "store", false},
		{"kv may not use adapter", "kv", m + "adapter", true},
		{"kv may not use monitoring", "kv", m + "monitoring", true},
		{"distribution below kv", "distribution", m + "kv", true},
		{"autosplit is not the exact distribution rule", "distribution/autosplit", m + "kv", false},
		{"raft engine below kv", "internal/raftengine/etcd", m + "kv", true},
		{"admin stays off adapter", "internal/admin", m + "adapter", true},
		{"admin may use keyviz", "internal/admin", m + "keyviz", false},
		{"monitoring may use filesystem", "monitoring", m + "internal/filesystem", false},
		{"filesystem may not use monitoring", "internal/filesystem", m + "monitoring", true},
		{"leaf rejects any module package", "internal/fskeys", m + "store", true},
		{"leaf may use its own subpackage", "proto", m + "proto/sub", false},
		{"etcd backend from adapter", "adapter", m + "internal/raftengine/etcd", true},
		{"etcd backend from kv", "kv", m + "internal/raftengine/etcd", true},
		{"etcd backend from root main", ".", m + "internal/raftengine/etcd", false},
		{"etcd backend from a cmd", "cmd/elastickv-snapshot-offload", m + "internal/raftengine/etcd", false},
		{"etcd backend from snapshot offload", "internal/snapshotoffload", m + "internal/raftengine/etcd", false},
		{"etcd subpackage from inside the engine", "internal/raftengine/etcd/transportsoak", m + "internal/raftengine/etcd", false},
		{"engine-neutral raftengine from adapter", "adapter", m + "internal/raftengine", false},
		{"serverwiring only from binaries", "adapter", m + "internal/serverwiring", true},
		{"serverwiring from root main", ".", m + "internal/serverwiring", false},
		{"serverwiring root rule is not a subtree", "kv", m + "internal/serverwiring", true},
		{"encryptionwiring only from binaries", "kv", m + "internal/encryptionwiring", true},
		{"encryptionwiring from root main", ".", m + "internal/encryptionwiring", false},
		{"encryptionwiring may use noderuntime", "internal/encryptionwiring", m + "internal/noderuntime", false},
		{"encryptionwiring may use the etcd backend", "internal/encryptionwiring", m + "internal/raftengine/etcd", false},
		{"adapter may not use encryptionwiring", "adapter", m + "internal/encryptionwiring", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkLayering(tc.dir, tc.imp)
			if tc.violation {
				require.ErrorContains(t, err, "layer violation")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestModuleDir(t *testing.T) {
	t.Parallel()
	root := filepath.FromSlash("/repo")
	require.Equal(t, ".", moduleDir(root, filepath.FromSlash("/repo/main.go")))
	require.Equal(t, "internal/raftengine/etcd", moduleDir(root, filepath.FromSlash("/repo/internal/raftengine/etcd/engine.go")))
	require.Equal(t, "kv", moduleDir(".", filepath.FromSlash("kv/coordinator.go")))
	require.Equal(t, ".", moduleDir(".", "main.go"))
}

// checkTree applies the layer rules with directories relative to the
// walked root and reports every violation, not only the first.
func TestCheckTreeReportsEveryLayerViolation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeSource(t, root, "main.go", "package main\nimport _ \""+modulePath+"/internal/raftengine/etcd\"\n")
	writeSource(t, root, "kv/ok.go", "package kv\nimport _ \""+modulePath+"/store\"\n")
	writeSource(t, root, "adapter/backend.go", "package adapter\nimport _ \""+modulePath+"/internal/raftengine/etcd\"\n")
	writeSource(t, root, "store/upward.go", "package store\nimport _ \""+modulePath+"/kv\"\n")
	err := checkTree(root)
	require.ErrorContains(t, err, filepath.Join("adapter", "backend.go"))
	require.ErrorContains(t, err, filepath.Join("store", "upward.go"))
	require.NotContains(t, err.Error(), "main.go")
	require.NotContains(t, err.Error(), "ok.go")
}
