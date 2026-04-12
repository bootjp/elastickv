package hashicorp

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bootjp/elastickv/internal/raftstore"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const (
	migrationTempSuffix = ".migrating"
	peerSpecParts       = 2
	migrationDirPerm    = 0o755
)

// MigrationPeer represents a single node in the hashicorp raft cluster.
type MigrationPeer struct {
	ID      string
	Address string
}

// MigrationStats holds summary info about a completed migration.
type MigrationStats struct {
	SnapshotBytes int64
	Peers         int
}

// ParsePeers parses a comma-separated "id=host:port" list into MigrationPeer
// values. The format matches the etcd migration tool for consistency.
func ParsePeers(raw string) ([]MigrationPeer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	peers := make([]MigrationPeer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idAddr := strings.SplitN(part, "=", peerSpecParts)
		if len(idAddr) != peerSpecParts {
			return nil, errors.WithStack(errors.Newf("invalid peer format %q, expected id=host:port", part))
		}
		id := strings.TrimSpace(idAddr[0])
		addr := strings.TrimSpace(idAddr[1])
		if id == "" || addr == "" {
			return nil, errors.WithStack(errors.Newf("invalid peer format %q, id and address must be non-empty", part))
		}
		peers = append(peers, MigrationPeer{ID: id, Address: addr})
	}
	return peers, nil
}

// MigrateFSMStore performs a reverse migration from etcd/raft to hashicorp
// raft. It reads an FSM PebbleStore snapshot and creates the directory
// structure that hashicorp/raft expects: a raft.db PebbleStore for log/stable
// state and a snapshots/ directory containing the FSM snapshot with peer
// configuration.
//
// The source FSM store (fsm.db) is read-only and shared between both engines;
// this function only creates the hashicorp-specific artifacts.
func MigrateFSMStore(storePath string, destDataDir string, peers []MigrationPeer) (*MigrationStats, error) {
	destDataDir, tempDir, err := prepareMigrationDest(storePath, destDataDir, peers)
	if err != nil {
		return nil, err
	}
	snapshotBytes, err := seedHashicorpDir(storePath, tempDir, peers)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	if err := finalizeMigrationDir(tempDir, destDataDir); err != nil {
		return nil, err
	}
	return &MigrationStats{
		SnapshotBytes: snapshotBytes,
		Peers:         len(peers),
	}, nil
}

func prepareMigrationDest(storePath string, destDataDir string, peers []MigrationPeer) (string, string, error) {
	switch {
	case storePath == "":
		return "", "", errors.WithStack(errors.New("source FSM store path is required"))
	case destDataDir == "":
		return "", "", errors.WithStack(errors.New("destination data dir is required"))
	case len(peers) == 0:
		return "", "", errors.WithStack(errors.New("at least one peer is required"))
	}

	destDataDir = filepath.Clean(destDataDir)
	if err := ensureMigrationPathAbsent(destDataDir, "destination"); err != nil {
		return "", "", err
	}
	tempDir := destDataDir + migrationTempSuffix
	if err := ensureMigrationPathAbsent(tempDir, "temporary destination"); err != nil {
		return "", "", err
	}
	return destDataDir, tempDir, nil
}

func ensureMigrationPathAbsent(path string, kind string) error {
	if _, err := os.Stat(path); err == nil {
		return errors.WithStack(errors.Newf("%s already exists: %s", kind, path))
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return nil
}

// seedHashicorpDir creates the hashicorp raft directory structure inside
// tempDir with a raft.db stable store and a snapshot containing the FSM data.
// The snapshot is streamed directly from the source PebbleStore to the
// raft.SnapshotSink to avoid buffering the entire FSM in memory.
func seedHashicorpDir(storePath string, tempDir string, peers []MigrationPeer) (int64, error) {
	if err := os.MkdirAll(tempDir, migrationDirPerm); err != nil {
		return 0, errors.WithStack(err)
	}

	// Create the raft.db PebbleStore with initial stable state.
	rs, err := raftstore.NewPebbleStore(filepath.Join(tempDir, "raft.db"))
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer func() {
		if err := rs.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close raft store: %v\n", err)
		}
	}()

	// Set initial term to 1. Hashicorp raft reads "CurrentTerm" on startup.
	if err := rs.SetUint64([]byte("CurrentTerm"), 1); err != nil {
		return 0, errors.WithStack(err)
	}

	// Create the FileSnapshotStore to hold the FSM snapshot.
	fss, err := raft.NewFileSnapshotStore(tempDir, 1, os.Stderr)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// Build the raft configuration from peers.
	configuration := raft.Configuration{Servers: peersToRaftMigrationServers(peers)}

	// Create an in-memory transport (required by FileSnapshotStore.Create but
	// not used for actual communication during migration).
	_, transport := raft.NewInmemTransport("")
	defer transport.Close()

	// Create a snapshot sink at index=1, term=1.
	sink, err := fss.Create(raft.SnapshotVersionMax, 1, 1, configuration, 1, transport)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// Stream FSM snapshot directly to the sink (no in-memory buffering).
	snapshotBytes, err := streamFSMSnapshotToSink(storePath, sink)
	if err != nil {
		_ = sink.Cancel()
		return 0, err
	}

	if err := sink.Close(); err != nil {
		return 0, errors.WithStack(err)
	}

	fmt.Fprintf(os.Stderr, "  created snapshot with %d bytes of FSM data\n", snapshotBytes)
	return snapshotBytes, nil
}

// streamFSMSnapshotToSink opens the source PebbleStore, takes a snapshot,
// and streams it directly to the given io.Writer (typically a raft.SnapshotSink).
func streamFSMSnapshotToSink(storePath string, w io.Writer) (int64, error) {
	source, err := store.NewPebbleStore(storePath)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close source store: %v\n", err)
		}
	}()

	snapshot, err := source.Snapshot()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer snapshot.Close()

	n, err := snapshot.WriteTo(w)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func peersToRaftMigrationServers(peers []MigrationPeer) []raft.Server {
	servers := make([]raft.Server, 0, len(peers))
	for _, p := range peers {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(p.ID),
			Address:  raft.ServerAddress(p.Address),
		})
	}
	return servers
}

func finalizeMigrationDir(tempDir string, destDataDir string) error {
	if err := os.Rename(tempDir, destDataDir); err != nil {
		_ = os.RemoveAll(tempDir)
		return errors.WithStack(err)
	}
	if err := syncDir(filepath.Dir(destDataDir)); err != nil {
		// Don't remove destDataDir here — the rename succeeded and the data
		// is already in place. Deleting it would cause total data loss.
		return err
	}
	return nil
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	return errors.WithStack(f.Sync())
}
