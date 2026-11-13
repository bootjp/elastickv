package main

import (
	"flag"
	"time"

	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/bootjp/elastickv/internal/snapshotoffloadwiring"
	"github.com/bootjp/elastickv/internal/tsowiring"
)

const (
	heartbeatTimeout           = 200 * time.Millisecond
	electionTimeout            = 2000 * time.Millisecond
	raftMetricsObserveInterval = 5 * time.Second
	dirPerm                    = noderuntime.RaftDirPerm

	etcdTickInterval      = 10 * time.Millisecond
	etcdHeartbeatMinTicks = 1
	etcdElectionMinTicks  = 2
	etcdMaxSizePerMsg     = 1 << 20
	etcdMaxInflightMsg    = 1024
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
	tso             tsowiring.Flags
	snapshotOffload snapshotoffloadwiring.Flags
	adminHTTP       adminHTTPFlags
	backup          backupFlags
	nodeAuth        nodeauth.Flags
	encryption      encryptionwiring.Flags
	listeners       listenerFlags
	raft            raftFlags
	topology        topologyFlags
	consistency     consistencyFlags
	bootstrap       raftBootstrapFlags
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
	f.tso.Register(fs)
	f.snapshotOffload.Register(fs)
	f.adminHTTP.register(fs)
	f.backup.register(fs)
	f.nodeAuth.Register(fs)
	f.encryption.Register(fs)
	f.listeners.register(fs)
	f.raft.register(fs)
	f.topology.register(fs)
	f.consistency.register(fs)
	f.bootstrap.register(fs)
	f.keyViz = serverwiring.RegisterKeyVizFlags(fs)
	f.autoSplit = serverwiring.RegisterAutoSplitFlags(fs)
	return f
}

// cliFlags is bound to the process command line. Only main() reads it.
var cliFlags = registerServerFlags(flag.CommandLine)
