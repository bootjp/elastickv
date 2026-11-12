package main

import (
	"flag"
	"time"

	"github.com/bootjp/elastickv/internal/serverwiring"
)

const (
	heartbeatTimeout           = 200 * time.Millisecond
	electionTimeout            = 2000 * time.Millisecond
	raftMetricsObserveInterval = 5 * time.Second
	dirPerm                    = raftDirPerm

	etcdTickInterval      = 10 * time.Millisecond
	etcdHeartbeatMinTicks = 1
	etcdElectionMinTicks  = 2
	etcdMaxSizePerMsg     = 1 << 20
	etcdMaxInflightMsg    = 1024
	defaultTSOBatchSize   = 256
	defaultTSOReload      = 5 * time.Second
	defaultBackupTTL      = 30 * time.Minute
	defaultBackupMaxTTL   = time.Hour
	defaultBackupDeadline = 5 * time.Second
	defaultBackupHeadroom = 1000
	defaultBackupPageSize = 1024
	defaultBackupPinLimit = 4

	defaultFilesystemRootMode              = 0o755
	defaultFilesystemPlacementScanInterval = 30 * time.Second
	defaultFilesystemLeaseReapInterval     = 30 * time.Second
	splitMigrationCapabilityProbeTimeout   = 2 * time.Second
)

// serverFlags holds the command-line flags of the server, grouped by
// area. Each area's struct registers its own flags and lives next to the
// wiring that consumes it. Only main reads cliFlags; run() hands each
// area its own values, so helpers never read flag state.
//
// Areas not listed here are still package-level flag globals and move
// into serverFlags one area at a time (root package decomposition, M2).
type serverFlags struct {
	healthAddress   string
	filesystem      filesystemFlags
	leaderBalance   leaderBalanceFlags
	tso             tsoFlags
	snapshotOffload snapshotOffloadFlags
	adminHTTP       adminHTTPFlags
	backup          backupFlags
	nodeAuth        nodeAuthFlags
	encryption      encryptionFlags
	listeners       listenerFlags
	raft            raftFlags
	topology        topologyFlags
	consistency     consistencyFlags
	// keyViz and autoSplit are shared with the cmd/server demo through
	// internal/serverwiring. The sampler runs entirely in-memory on each
	// node, feeds AdminServer.GetKeyVizMatrix, and is disabled unless
	// --keyvizEnabled or --autoSplit is set.
	keyViz    *serverwiring.KeyVizFlags
	autoSplit *serverwiring.AutoSplitFlags
}

func registerServerFlags(fs *flag.FlagSet) *serverFlags {
	f := &serverFlags{}
	fs.StringVar(&f.healthAddress, "healthAddress", "", "private HTTP health listener (empty disables); exposes /livez and /readyz")
	f.filesystem.register(fs)
	f.leaderBalance.register(fs)
	f.tso.register(fs)
	f.snapshotOffload.register(fs)
	f.adminHTTP.register(fs)
	f.backup.register(fs)
	f.nodeAuth.register(fs)
	f.encryption.register(fs)
	f.listeners.register(fs)
	f.raft.register(fs)
	f.topology.register(fs)
	f.consistency.register(fs)
	f.keyViz = serverwiring.RegisterKeyVizFlags(fs)
	f.autoSplit = serverwiring.RegisterAutoSplitFlags(fs)
	return f
}

// cliFlags is bound to the process command line. Only main() reads it.
var cliFlags = registerServerFlags(flag.CommandLine)

var (
	raftBootstrap        = flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	raftBootstrapMembers = flag.String("raftBootstrapMembers", "", "Comma-separated bootstrap raft members (raftID=host:port,...)")
	raftGroupPeers       = flag.String("raftGroupPeers", "", "Semicolon-separated per-group bootstrap members (groupID=raftID@host:port,...)")
	raftJoinMembers      = flag.String("raftJoinMembers", "", "Comma-separated raft members used only for transport discovery while this fresh node joins an existing single-group cluster (raftID=host:port,...); requires --raftJoinAsLearner")
	raftJoinAsLearner    = flag.Bool("raftJoinAsLearner", false, "Local node expects to join an existing cluster as a learner; if a post-apply ConfState lists this node as a voter instead, an ERROR-level alarm fires (the node keeps running -- the flag is an operator alarm, not a consensus veto). See docs/design/2026_04_26_implemented_raft_learner.md §4.5.")
)
