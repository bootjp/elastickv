package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

// modulePath is the repository module. Layer rules use module-relative
// package paths ("kv", "internal/raftengine/etcd").
const modulePath = "github.com/bootjp/elastickv"

// anyModulePackage in a deny list forbids every package of this module.
const anyModulePackage = "*"

// layerRule forbids production source under Dir from importing the
// module packages in Deny, or, when Only is set, any module package
// outside Only. Dir and every Deny / Only entry cover the package and its
// subpackages; Exact limits Dir to the package itself. A rule that is not
// Exact also lets Dir import its own subpackages.
type layerRule struct {
	Dir   string
	Exact bool
	Deny  []string
	Only  []string
	Why   string
}

// restrictedPackage may be imported only from the listed directories
// and their subdirectories, and from inside its own subtree. "." is the
// repository-root main package alone.
type restrictedPackage struct {
	Pkg         string
	AllowedFrom []string
	Why         string
}

// upperLayers sit above the data plane: protocol ingress, observability
// sinks, operator surfaces and binary wiring. Storage, replication and
// routing must not reach up into them.
var upperLayers = []string{
	"adapter",
	"monitoring",
	"proxy",
	"distribution/autosplit",
	"internal/admin",
	"internal/adminbridge",
	"internal/clusterbootstrap",
	"internal/encryptionwiring",
	"internal/fsmount",
	"internal/leaderbalance",
	"internal/nodeauth",
	"internal/noderuntime",
	"internal/server",
	"internal/serverwiring",
	"internal/snapshotoffload",
	"internal/snapshotoffloadwiring",
	"internal/tsowiring",
}

func withUpper(extra ...string) []string {
	return append(append([]string{}, upperLayers...), extra...)
}

// layerRules fixes the dependency direction between components. Every
// rule held when it was added; a violation is a new upward dependency
// and should be inverted (an interface or callback owned by the lower
// package) rather than allowed here.
var layerRules = []layerRule{
	{Dir: "proto", Deny: []string{anyModulePackage}, Why: "generated protobuf code is a leaf"},
	{Dir: "keyviz", Deny: []string{anyModulePackage}, Why: "the key visualizer sampler is a leaf the data plane calls into"},
	{Dir: "proxy", Deny: []string{anyModulePackage}, Why: "the Redis migration proxy reaches Elastickv over the wire only"},
	{Dir: "internal/fskeys", Deny: []string{anyModulePackage}, Why: "key codecs are leaves"},
	{Dir: "internal/s3keys", Deny: []string{anyModulePackage}, Why: "key codecs are leaves"},
	{Dir: "internal/pathsafety", Deny: []string{anyModulePackage}, Why: "utility leaf"},
	{Dir: "internal/monoclock", Deny: []string{anyModulePackage}, Why: "utility leaf"},
	{Dir: "internal/memwatch", Deny: []string{anyModulePackage}, Why: "utility leaf"},
	{Dir: "internal/encryption/kek", Deny: []string{anyModulePackage}, Why: "KEK providers are leaves"},
	{Dir: "internal/encryption/fsmwire", Deny: []string{anyModulePackage}, Why: "wire codec leaf"},
	{Dir: "internal/raftbootstrap", Deny: []string{anyModulePackage}, Why: "bootstrap protocol types are shared leaves"},
	{Dir: "internal/peersession/wire", Deny: []string{anyModulePackage}, Why: "wire codec leaf"},
	{
		Dir:  "store",
		Deny: withUpper("kv", "distribution", "keyviz", "internal/raftengine", "internal/filesystem", "internal/backup"),
		Why:  "MVCC storage is the bottom of the data plane",
	},
	{
		Dir: "distribution", Exact: true,
		Deny: withUpper("kv", "keyviz", "internal/raftengine", "internal/filesystem"),
		Why:  "the route catalog sits below the coordinator that reads it",
	},
	{
		Dir:  "internal/raftengine",
		Deny: withUpper("kv", "store", "distribution", "keyviz", "internal/filesystem"),
		Why:  "the Raft engine replicates opaque entries and must not know the KV state machine",
	},
	{
		Dir:  "internal/encryption",
		Deny: withUpper("kv", "store", "distribution", "internal/raftengine", "internal/filesystem"),
		Why:  "encryption is consumed by storage and Raft, not the other way round",
	},
	{
		Dir:  "kv",
		Deny: withUpper("internal/filesystem", "internal/backup"),
		Why:  "the coordinator never depends on protocol, observability or wiring packages",
	},
	{
		Dir:  "internal/filesystem",
		Deny: upperLayers,
		Why:  "the filesystem service is a data-plane client; metrics and wiring observe it from above",
	},
	{
		Dir:  "monitoring",
		Deny: []string{"adapter", "proxy", "kv", "store", "distribution", "internal/admin", "internal/adminbridge", "internal/serverwiring"},
		Why:  "observability exposes observer interfaces the data plane calls; it does not call back into it",
	},
	{
		Dir: "internal/admin",
		Deny: []string{
			"adapter", "kv", "store", "distribution", "monitoring", "internal/raftengine",
			"internal/encryption", "internal/filesystem", "internal/adminbridge", "internal/serverwiring",
		},
		Why: "the admin HTTP layer depends on its own source interfaces; adapters are bridged in from above",
	},
	{
		Dir:  "adapter",
		Deny: []string{"proxy", "internal/adminbridge", "internal/serverwiring", "internal/snapshotoffload", "internal/noderuntime", "internal/nodeauth", "internal/encryptionwiring", "internal/clusterbootstrap", "internal/server"},
		Why:  "protocol adapters must not depend on binary wiring",
	},
}

// importAllowlists name every module package a package may import. The
// data-plane entries pin the layers the deny rules above describe, so an
// import that no deny rule anticipated still fails; the rest cover the
// packages between the data plane and the binaries and encode the server
// wiring order noderuntime / nodeauth -> encryptionwiring ->
// clusterbootstrap -> server. Widening an allowlist is a layering
// decision: prefer inverting the dependency.
var importAllowlists = []layerRule{
	// Data plane, bottom up.
	{Dir: "internal/encryption", Only: []string{}, Why: "encryption is a library over its own KEK and wire subpackages"},
	{Dir: "store", Only: []string{"internal/encryption", "internal/raftbootstrap", "internal/s3keys"}, Why: "MVCC storage is the bottom of the data plane"},
	{Dir: "distribution", Exact: true, Only: []string{"internal/fskeys", "internal/s3keys", "proto", "store"}, Why: "the route catalog sits on storage, below the coordinator"},
	{Dir: "internal/raftengine", Exact: true, Only: []string{"internal/monoclock"}, Why: "the engine-neutral interface knows nothing of a backend or the state machine"},
	{
		Dir: "internal/raftengine/etcd",
		Only: []string{
			"internal", "internal/encryption", "internal/monoclock", "internal/pathsafety", "internal/peersession",
			"internal/raftbootstrap", "internal/raftengine", "proto",
		},
		Why: "the etcd backend replicates opaque entries over authenticated peer sessions",
	},
	{
		Dir: "kv",
		Only: []string{
			"distribution", "internal", "internal/encryption", "internal/fskeys", "internal/monoclock",
			"internal/raftbootstrap", "internal/raftengine", "internal/s3keys", "keyviz", "proto", "store",
		},
		Why: "the coordinator drives storage, routing and replication, and nothing above them",
	},
	{Dir: "internal/filesystem", Only: []string{"internal/fskeys", "kv", "store"}, Why: "the filesystem service is a data-plane client"},
	{Dir: "monitoring", Only: []string{"internal/encryption", "internal/filesystem", "internal/raftengine"}, Why: "observability exposes observer interfaces and reads engine state"},
	{Dir: "internal/admin", Only: []string{"keyviz", "proto"}, Why: "the admin HTTP layer depends on its own source interfaces"},
	{
		Dir: "adapter",
		Only: []string{
			"distribution", "internal", "internal/admin", "internal/backup", "internal/encryption", "internal/fskeys",
			"internal/peersession", "internal/raftadmin", "internal/raftbootstrap", "internal/raftengine",
			"internal/s3keys", "keyviz", "kv", "monitoring", "proto", "store",
		},
		Why: "protocol adapters sit on the coordinator and never reach into wiring",
	},

	// Between the data plane and the binaries.
	{Dir: "internal", Exact: true, Only: []string{"proto"}, Why: "shared helpers (token files, gRPC options) are imported by every layer, so they stay a leaf over proto"},
	{Dir: "internal/backup", Only: []string{"internal/pathsafety", "internal/s3keys", "proto"}, Why: "the backup format is a codec that adapters and offline tools share"},
	{Dir: "internal/peersession", Only: []string{"internal", "internal/raftbootstrap", "proto"}, Why: "peer-session authentication sits below the Raft transport that uses it"},
	{Dir: "internal/raftadmin", Only: []string{"internal/raftengine", "proto"}, Why: "the Raft admin service works through the engine-neutral interface"},
	{Dir: "internal/raftadmission", Only: []string{"internal", "internal/peersession", "internal/raftbootstrap", "internal/raftengine", "proto"}, Why: "Raft admission checks work through the engine-neutral interface"},
	{Dir: "internal/runtimehealth", Only: []string{"internal/monoclock", "internal/raftengine"}, Why: "runtime health reads engine state only"},
	{Dir: "internal/snapshotoffload", Only: []string{"internal/raftengine", "internal/pathsafety", "proto"}, Why: "snapshot offload reads the engine's snapshots and nothing above it"},
	{Dir: "distribution/autosplit", Only: []string{"distribution", "keyviz", "kv", "proto"}, Why: "the auto-split scheduler drives the coordinator and the route catalog only"},
	{Dir: "internal/leaderbalance", Only: []string{"internal/raftengine", "kv", "proto"}, Why: "the leader-balance scheduler sees groups through narrow interfaces"},
	{Dir: "internal/fsmount", Only: []string{"internal", "internal/filesystem", "kv", "monitoring"}, Why: "the FUSE mount wires the filesystem service only"},
	{Dir: "internal/tsowiring", Only: []string{"kv"}, Why: "a <lib>wiring package imports only its library"},
	{Dir: "internal/snapshotoffloadwiring", Only: []string{"internal/snapshotoffload"}, Why: "a <lib>wiring package imports only its library"},
	{Dir: "internal/adminbridge", Only: []string{"adapter", "internal/admin", "internal/raftengine", "keyviz", "kv", "proto"}, Why: "admin bridges adapt protocol adapters to the admin sources only"},
	{Dir: "internal/serverwiring", Only: []string{"adapter", "distribution", "keyviz", "kv", "monitoring", "proto"}, Why: "server wiring helpers build adapters and observers, not the assembly"},
	{Dir: "internal/nodeauth", Only: []string{"adapter", "internal"}, Why: "node and admin gRPC auth sits at the bottom of the server wiring"},
	{
		Dir:  "internal/noderuntime",
		Only: []string{"adapter", "distribution", "internal", "internal/raftengine", "kv", "proto", "store"},
		Why:  "node runtime configuration sits at the bottom of the server wiring",
	},
	{
		Dir: "internal/encryptionwiring",
		Only: []string{
			"adapter", "internal", "internal/admin", "internal/encryption", "internal/noderuntime",
			"internal/raftadmin", "internal/raftengine", "kv", "monitoring", "proto", "store",
		},
		Why: "encryption wiring builds on noderuntime, below protected bootstrap",
	},
	{
		Dir: "internal/clusterbootstrap",
		Only: []string{
			"adapter", "distribution", "internal", "internal/encryption", "internal/encryptionwiring", "internal/nodeauth",
			"internal/noderuntime", "internal/peersession", "internal/raftadmission", "internal/raftbootstrap",
			"internal/raftengine", "kv", "proto", "store",
		},
		Why: "protected bootstrap builds on encryption wiring, below the server assembly",
	},
	{Dir: "internal/server", Deny: []string{"proxy"}, Why: "the Redis migration proxy is a binary of its own"},
}

// containerPackage is the shared helper package at internal/. Its
// subdirectories are unrelated packages, so an Only entry naming it admits
// the helper package alone, not the internal/ tree.
const containerPackage = "internal"

// coverageExempt directories hold binaries (the root is checked by
// rootImports) or a nested module; every other production package must
// have a layer rule or an import allowlist.
var coverageExempt = []string{".", "cmd", "third_party"}

// restrictedPackages may only be imported at the binary wiring edge.
var restrictedPackages = []restrictedPackage{
	{
		Pkg:         "internal/raftengine/etcd",
		AllowedFrom: []string{"internal/server", "cmd", "internal/raftengine", "internal/snapshotoffload", "internal/encryptionwiring", "internal/clusterbootstrap"},
		Why:         "the concrete Raft backend is chosen at the wiring edge; everything else uses internal/raftengine",
	},
	{Pkg: "internal/serverwiring", AllowedFrom: []string{"internal/server", "cmd"}, Why: "server wiring is for binaries only"},
	{Pkg: "internal/adminbridge", AllowedFrom: []string{"internal/server", "cmd"}, Why: "admin bridges are wired by binaries only"},
	{Pkg: "internal/noderuntime", AllowedFrom: []string{"internal/server", "cmd", "internal/encryptionwiring", "internal/clusterbootstrap"}, Why: "node runtime configuration is resolved by binaries and the wiring layers above it"},
	{Pkg: "internal/leaderbalance", AllowedFrom: []string{"internal/server", "cmd"}, Why: "the leader-balance scheduler is started by binaries only"},
	{Pkg: "internal/fsmount", AllowedFrom: []string{"internal/server", "cmd"}, Why: "the FUSE mount is started by binaries only"},
	{Pkg: "internal/encryptionwiring", AllowedFrom: []string{"internal/server", "cmd", "internal/clusterbootstrap"}, Why: "encryption wiring is assembled by binaries and protected bootstrap"},
	{Pkg: "internal/clusterbootstrap", AllowedFrom: []string{"internal/server", "cmd"}, Why: "protected bootstrap is assembled by binaries only"},
	{Pkg: "internal/snapshotoffloadwiring", AllowedFrom: []string{"internal/server", "cmd"}, Why: "snapshot offload start-up is wired by binaries only"},
	{Pkg: "internal/tsowiring", AllowedFrom: []string{"internal/server", "cmd"}, Why: "TSO wiring is configured by binaries only"},
	{Pkg: "internal/nodeauth", AllowedFrom: []string{"internal/server", "cmd", "internal/clusterbootstrap"}, Why: "node and admin gRPC authentication is configured by binaries only"},
	{Pkg: "internal/server", AllowedFrom: []string{"."}, Why: "the server assembly is started by the root binary only"},
	{Pkg: "internal/backup", AllowedFrom: []string{"adapter", "cmd"}, Why: "the backup format is produced by the adapters and read by offline tools"},
	{Pkg: "internal/peersession", AllowedFrom: []string{"adapter", "internal/raftengine", "internal/raftadmission", "internal/clusterbootstrap"}, Why: "peer sessions are opened by the Raft transport and checked by admission and bootstrap"},
	{Pkg: "internal/raftadmin", AllowedFrom: []string{"adapter", "cmd", "internal/encryptionwiring", "internal/server"}, Why: "the Raft admin service is registered by the wiring layers"},
	{Pkg: "internal/raftadmission", AllowedFrom: []string{"internal/clusterbootstrap", "internal/server"}, Why: "Raft admission is installed by the wiring layers"},
	{Pkg: "internal/runtimehealth", AllowedFrom: []string{"internal/server"}, Why: "runtime health is served by the server assembly"},
	{Pkg: "internal/snapshotoffload", AllowedFrom: []string{"cmd", "internal/server", "internal/snapshotoffloadwiring"}, Why: "snapshot offload is started by binaries and its wiring"},
	{Pkg: "distribution/autosplit", AllowedFrom: []string{"cmd", "internal/server", "internal/serverwiring"}, Why: "the auto-split scheduler is started by binaries and server wiring"},
}

// rootImports are the only module packages the repository-root main
// package may import: main.go parses the command line and hands it to the
// server assembly, which owns everything else.
var rootImports = []string{"internal/server"}

// checkLayering reports a layer violation when a production file in the
// module-relative directory dir imports importPath.
func checkLayering(dir, importPath string) error {
	pkg, ok := modulePackage(importPath)
	if !ok {
		return nil
	}
	if err := checkRootImports(dir, pkg); err != nil {
		return err
	}
	if err := checkLayerRules(dir, pkg); err != nil {
		return err
	}
	return checkRestrictedPackages(dir, pkg)
}

// modulePackage returns importPath relative to the module ("." for the
// module root), or false for a package outside the module.
func modulePackage(importPath string) (string, bool) {
	if importPath == modulePath {
		return ".", true
	}
	pkg, ok := strings.CutPrefix(importPath, modulePath+"/")
	return pkg, ok
}

func checkRootImports(dir, pkg string) error {
	if dir != "." {
		return nil
	}
	for _, allowed := range rootImports {
		if pkg == allowed {
			return nil
		}
	}
	return fmt.Errorf("layer violation: the root main package may import only %s, not %s (add the wiring to internal/server)",
		strings.Join(rootImports, ", "), pkg)
}

func checkLayerRules(dir, pkg string) error {
	for _, rules := range [][]layerRule{layerRules, importAllowlists} {
		for _, rule := range rules {
			if rule.applies(dir) && rule.denies(pkg) {
				return fmt.Errorf("layer violation: %s must not import %s (%s)", dir, pkg, rule.Why)
			}
		}
	}
	return nil
}

// checkCoverage reports a production package that no layer rule or import
// allowlist applies to, so a new package has to declare its place in the
// layering.
func checkCoverage(dir string) error {
	for _, exempt := range coverageExempt {
		if dir == exempt || (exempt != "." && within(dir, exempt)) {
			return nil
		}
	}
	for _, rules := range [][]layerRule{layerRules, importAllowlists} {
		for _, rule := range rules {
			if rule.applies(dir) {
				return nil
			}
		}
	}
	return fmt.Errorf("layer violation: %s has no layer rule or import allowlist; add one to layers.go", dir)
}

func checkRestrictedPackages(dir, pkg string) error {
	for _, restricted := range restrictedPackages {
		if within(pkg, restricted.Pkg) && !restricted.allows(dir) {
			return fmt.Errorf("layer violation: %s may only be imported from %s, not from %s (%s)",
				restricted.Pkg, strings.Join(restricted.AllowedFrom, ", "), dir, restricted.Why)
		}
	}
	return nil
}

func (r layerRule) applies(dir string) bool {
	if r.Exact {
		return dir == r.Dir
	}
	return within(dir, r.Dir)
}

func (r layerRule) denies(pkg string) bool {
	if r.Only != nil && !r.allowedByOnly(pkg) {
		return true
	}
	for _, denied := range r.Deny {
		if (denied == anyModulePackage && !within(pkg, r.Dir)) || within(pkg, denied) {
			return true
		}
	}
	return false
}

func (r layerRule) allowedByOnly(pkg string) bool {
	if !r.Exact && within(pkg, r.Dir) {
		return true
	}
	for _, allowed := range r.Only {
		if allowed == containerPackage {
			if pkg == containerPackage {
				return true
			}
			continue
		}
		if within(pkg, allowed) {
			return true
		}
	}
	return false
}

func (r restrictedPackage) allows(dir string) bool {
	if within(dir, r.Pkg) {
		return true
	}
	for _, allowed := range r.AllowedFrom {
		if dir == allowed || (allowed != "." && within(dir, allowed)) {
			return true
		}
	}
	return false
}

// within reports whether pkg is base or one of its subpackages.
func within(pkg, base string) bool {
	return pkg == base || strings.HasPrefix(pkg, base+"/")
}

// moduleDir converts a walked file path to its slash-separated package
// directory relative to root ("." for root itself).
func moduleDir(root, file string) string {
	rel, err := filepath.Rel(root, filepath.Dir(file))
	if err != nil || rel == "" {
		return "."
	}
	return filepath.ToSlash(rel)
}
