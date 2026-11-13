package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
		{"etcd backend from the server assembly", "internal/server", m + "internal/raftengine/etcd", false},
		{"etcd backend from a cmd", "cmd/elastickv-snapshot-offload", m + "internal/raftengine/etcd", false},
		{"etcd backend from snapshot offload", "internal/snapshotoffload", m + "internal/raftengine/etcd", false},
		{"etcd subpackage from inside the engine", "internal/raftengine/etcd/transportsoak", m + "internal/raftengine/etcd", false},
		{"engine-neutral raftengine from adapter", "adapter", m + "internal/raftengine", false},
		{"serverwiring only from binaries", "adapter", m + "internal/serverwiring", true},
		{"serverwiring from the server assembly", "internal/server", m + "internal/serverwiring", false},
		{"serverwiring root rule is not a subtree", "kv", m + "internal/serverwiring", true},
		{"noderuntime only from binaries", "adapter", m + "internal/noderuntime", true},
		{"noderuntime from the server assembly", "internal/server", m + "internal/noderuntime", false},
		{"adminbridge only from binaries", "internal/admin", m + "internal/adminbridge", true},
		{"snapshotoffloadwiring only from binaries", "internal/snapshotoffload", m + "internal/snapshotoffloadwiring", true},
		{"leaderbalance only from binaries", "kv", m + "internal/leaderbalance", true},
		{"leaderbalance from the server assembly", "internal/server", m + "internal/leaderbalance", false},
		{"fsmount only from binaries", "internal/filesystem", m + "internal/fsmount", true},
		{"fsmount from a cmd", "cmd/server", m + "internal/fsmount", false},
		{"encryptionwiring only from binaries", "kv", m + "internal/encryptionwiring", true},
		{"encryptionwiring from the server assembly", "internal/server", m + "internal/encryptionwiring", false},
		{"encryptionwiring may use noderuntime", "internal/encryptionwiring", m + "internal/noderuntime", false},
		{"encryptionwiring may use the etcd backend", "internal/encryptionwiring", m + "internal/raftengine/etcd", false},
		{"adapter may not use encryptionwiring", "adapter", m + "internal/encryptionwiring", true},
		{"clusterbootstrap only from binaries", "kv", m + "internal/clusterbootstrap", true},
		{"clusterbootstrap from the server assembly", "internal/server", m + "internal/clusterbootstrap", false},
		{"root main may import the server assembly", ".", m + "internal/server", false},
		{"root main may not import kv", ".", m + "kv", true},
		{"root main may not import wiring below the server", ".", m + "internal/clusterbootstrap", true},
		{"root main may not import the etcd backend", ".", m + "internal/raftengine/etcd", true},
		{"server assembly only from root main", "cmd/server", m + "internal/server", true},
		{"adapter may not use the server assembly", "adapter", m + "internal/server", true},
		{"kv may not use the server assembly", "kv", m + "internal/server", true},
		{"serverwiring is not the server assembly", "internal/serverwiring", m + "internal/adminbridge", true},
		{"clusterbootstrap may use encryptionwiring", "internal/clusterbootstrap", m + "internal/encryptionwiring", false},
		{"clusterbootstrap may use nodeauth", "internal/clusterbootstrap", m + "internal/nodeauth", false},
		{"encryptionwiring may not use clusterbootstrap", "internal/encryptionwiring", m + "internal/clusterbootstrap", true},
		{"internal leaf may use proto", "internal", m + "proto", false},
		{"internal leaf rule is exact", "internal", m + "internal/encryption", true},
		{"allowlist admits the package's own subpackage", "internal/peersession", m + "internal/peersession/wire", false},
		{"allowlist entry internal is the helper package alone", "internal/peersession", m + "internal/encryption", true},
		{"allowlist entry internal admits the helper package", "internal/peersession", m + "internal", false},
		{"backup stays a codec", "internal/backup", m + "store", true},
		{"raftadmin may use the neutral engine", "internal/raftadmin", m + "internal/raftengine", false},
		{"raftadmin may not use the etcd backend", "internal/raftadmin", m + "internal/raftengine/etcd", true},
		{"raftadmission may not use kv", "internal/raftadmission", m + "kv", true},
		{"runtimehealth may not use kv", "internal/runtimehealth", m + "kv", true},
		{"snapshot offload may use the etcd backend", "internal/snapshotoffload", m + "internal/raftengine/etcd", false},
		{"snapshot offload may not use kv", "internal/snapshotoffload", m + "kv", true},
		{"autosplit may use kv", "distribution/autosplit", m + "kv", false},
		{"autosplit may not use adapter", "distribution/autosplit", m + "adapter", true},
		{"leaderbalance may not use store", "internal/leaderbalance", m + "store", true},
		{"tsowiring imports only kv", "internal/tsowiring", m + "store", true},
		{"snapshotoffloadwiring may use its library", "internal/snapshotoffloadwiring", m + "internal/snapshotoffload", false},
		{"adminbridge may not use the server assembly", "internal/adminbridge", m + "internal/server", true},
		{"nodeauth may use adapter", "internal/nodeauth", m + "adapter", false},
		{"nodeauth may not use noderuntime", "internal/nodeauth", m + "internal/noderuntime", true},
		{"noderuntime may not use nodeauth", "internal/noderuntime", m + "internal/nodeauth", true},
		{"noderuntime may not use encryptionwiring", "internal/noderuntime", m + "internal/encryptionwiring", true},
		{"encryptionwiring may use noderuntime", "internal/encryptionwiring", m + "internal/noderuntime", false},
		{"encryptionwiring may not use nodeauth", "internal/encryptionwiring", m + "internal/nodeauth", true},
		{"encryptionwiring may not use adminbridge", "internal/encryptionwiring", m + "internal/adminbridge", true},
		{"clusterbootstrap may use raftadmission", "internal/clusterbootstrap", m + "internal/raftadmission", false},
		{"clusterbootstrap may not use the server assembly", "internal/clusterbootstrap", m + "internal/server", true},
		{"server assembly may not use proxy", "internal/server", m + "proxy", true},
		{"backup only from adapter and binaries", "internal/admin", m + "internal/backup", true},
		{"adapter may use backup", "adapter", m + "internal/backup", false},
		{"peersession from the etcd transport", "internal/raftengine/etcd", m + "internal/peersession", false},
		{"peersession not from kv", "kv", m + "internal/peersession", true},
		{"raftadmission only from the wiring layers", "adapter", m + "internal/raftadmission", true},
		{"runtimehealth only from the server assembly", "internal/admin", m + "internal/runtimehealth", true},
		{"autosplit only from binaries and server wiring", "adapter", m + "distribution/autosplit", true},
		{"autosplit from server wiring", "internal/serverwiring", m + "distribution/autosplit", false},
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

func TestCheckCoverage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, dir string
		covered   bool
	}{
		{"deny rule", "kv", true},
		{"deny rule covers subpackages", "kv/sub", true},
		{"allowlist", "internal/nodeauth", true},
		{"exact rule does not cover subpackages", "distribution/newsub", false},
		{"exact allowlist does not cover subpackages", "internal/newpkg", false},
		{"root is checked by rootImports", ".", true},
		{"binaries are exempt", "cmd/elastickv-admin", true},
		{"nested module is exempt", "third_party/go-fuse/fuse", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkCoverage(tc.dir)
			if tc.covered {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "has no layer rule or import allowlist")
			}
		})
	}
}

// Every package a rule names must exist, so a rename or removal cannot leave
// a rule that silently guards nothing.
func TestRulesNameExistingPackages(t *testing.T) {
	t.Parallel()
	repoRoot := filepath.Join("..", "..", "..", "..")
	var named []string
	for _, rules := range [][]layerRule{layerRules, importAllowlists} {
		for _, rule := range rules {
			named = append(named, rule.Dir)
			named = append(named, rule.Only...)
			for _, denied := range rule.Deny {
				if denied != anyModulePackage {
					named = append(named, denied)
				}
			}
		}
	}
	for _, restricted := range restrictedPackages {
		named = append(named, restricted.Pkg)
		for _, from := range restricted.AllowedFrom {
			if from != "." {
				named = append(named, from)
			}
		}
	}
	named = append(named, upperLayers...)
	named = append(named, rootImports...)
	for _, pkg := range named {
		info, err := os.Stat(filepath.Join(repoRoot, filepath.FromSlash(pkg)))
		if !assert.NoError(t, err, "rule names missing package %s", pkg) {
			continue
		}
		assert.True(t, info.IsDir(), "rule names %s, which is not a directory", pkg)
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
	writeSource(t, root, "main.go", "package main\nimport _ \""+modulePath+"/internal/server\"\n")
	writeSource(t, root, "internal/server/backend.go", "package server\nimport _ \""+modulePath+"/internal/raftengine/etcd\"\n")
	writeSource(t, root, "kv/ok.go", "package kv\nimport _ \""+modulePath+"/store\"\n")
	writeSource(t, root, "adapter/backend.go", "package adapter\nimport _ \""+modulePath+"/internal/raftengine/etcd\"\n")
	writeSource(t, root, "store/upward.go", "package store\nimport _ \""+modulePath+"/kv\"\n")
	err := checkTree(root)
	require.ErrorContains(t, err, filepath.Join("adapter", "backend.go"))
	require.ErrorContains(t, err, filepath.Join("store", "upward.go"))
	require.NotContains(t, err.Error(), "main.go")
	require.NotContains(t, err.Error(), filepath.Join("server", "backend.go"))
	require.NotContains(t, err.Error(), "ok.go")
}

func TestCheckTreeReportsPackagesWithoutARule(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeSource(t, root, "kv/ok.go", "package kv\n")
	writeSource(t, root, "internal/newpkg/a.go", "package newpkg\n")
	writeSource(t, root, "internal/newpkg/b.go", "package newpkg\n")
	err := checkTree(root)
	require.ErrorContains(t, err, "internal/newpkg has no layer rule or import allowlist")
	require.Equal(t, 1, strings.Count(err.Error(), "internal/newpkg has no layer rule"), "one report per package")
	require.NotContains(t, err.Error(), "kv has no")
}
