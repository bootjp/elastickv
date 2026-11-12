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
// module packages in Deny. Dir and every Deny entry cover the package
// and its subpackages; Exact limits Dir to the package itself.
type layerRule struct {
	Dir   string
	Exact bool
	Deny  []string
	Why   string
}

// restrictedPackage may be imported only from the listed directories
// and their subdirectories. "." is the repository-root main package
// alone.
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
	"internal/noderuntime",
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
		Deny: []string{"proxy", "internal/adminbridge", "internal/serverwiring", "internal/snapshotoffload"},
		Why:  "protocol adapters must not depend on binary wiring",
	},
}

// restrictedPackages may only be imported at the binary wiring edge.
var restrictedPackages = []restrictedPackage{
	{
		Pkg:         "internal/raftengine/etcd",
		AllowedFrom: []string{".", "cmd", "internal/raftengine", "internal/snapshotoffload"},
		Why:         "the concrete Raft backend is chosen at the wiring edge; everything else uses internal/raftengine",
	},
	{Pkg: "internal/serverwiring", AllowedFrom: []string{".", "cmd"}, Why: "server wiring is for binaries only"},
	{Pkg: "internal/adminbridge", AllowedFrom: []string{".", "cmd"}, Why: "admin bridges are wired by binaries only"},
	{Pkg: "internal/noderuntime", AllowedFrom: []string{".", "cmd"}, Why: "node runtime configuration is resolved by binaries only"},
	{Pkg: "internal/snapshotoffloadwiring", AllowedFrom: []string{".", "cmd"}, Why: "snapshot offload start-up is wired by binaries only"},
	{Pkg: "internal/tsowiring", AllowedFrom: []string{".", "cmd"}, Why: "TSO wiring is configured by binaries only"},
}

// checkLayering reports a layer violation when a production file in the
// module-relative directory dir imports importPath.
func checkLayering(dir, importPath string) error {
	pkg, ok := modulePackage(importPath)
	if !ok {
		return nil
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

func checkLayerRules(dir, pkg string) error {
	for _, rule := range layerRules {
		if rule.applies(dir) && rule.denies(pkg) {
			return fmt.Errorf("layer violation: %s must not import %s (%s)", dir, pkg, rule.Why)
		}
	}
	return nil
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
	for _, denied := range r.Deny {
		if (denied == anyModulePackage && !within(pkg, r.Dir)) || within(pkg, denied) {
			return true
		}
	}
	return false
}

func (r restrictedPackage) allows(dir string) bool {
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
