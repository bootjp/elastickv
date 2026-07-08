package etcd

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistedPeersV2RoundTripMixedSuffrage(t *testing.T) {
	dir := t.TempDir()
	peers := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageLearner},
		{NodeID: 3, ID: "n3", Address: "127.0.0.1:7003", Suffrage: SuffrageVoter},
	}

	require.NoError(t, savePersistedPeers(dir, 42, peers))

	loaded, ok, err := LoadPersistedPeers(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, peers, loaded)
}

func TestPersistedPeersV1FileReadsAsAllVoter(t *testing.T) {
	dir := t.TempDir()
	path := peersFilePath(dir)

	// Hand-craft a v1 file: magic + version=1 + index + count + per-peer
	// (nodeID, id, address) without the v2 suffrage byte.
	require.NoError(t, replaceFile(path, func(w io.Writer) error {
		return writeV1PeersFile(w, 7, []Peer{
			{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
			{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		})
	}))

	loaded, ok, err := LoadPersistedPeers(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
	}, loaded)
}

func writeV1PeersFile(w io.Writer, index uint64, peers []Peer) error {
	writer := bufio.NewWriter(w)
	if _, err := writer.Write(peersFileMagic[:]); err != nil {
		return err
	}
	if err := writeU32(writer, peersFileVersionV1); err != nil {
		return err
	}
	if err := writeU64(writer, index); err != nil {
		return err
	}
	count, err := uint32Len(len(peers))
	if err != nil {
		return err
	}
	if err := writeU32(writer, count); err != nil {
		return err
	}
	for _, peer := range peers {
		if err := writeV1PeerEntry(writer, peer); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func writeV1PeerEntry(w io.Writer, peer Peer) error {
	if err := writeU64(w, peer.NodeID); err != nil {
		return err
	}
	if err := writeString(w, peer.ID); err != nil {
		return err
	}
	return writeString(w, peer.Address)
}

// TestPersistedPeersWriterAlwaysEmitsV3 pins the writer-side
// invariant that savePersistedPeers writes the v3 header: current
// peers keep the v2 per-peer suffrage byte, followed by the bootstrap
// seed marker fields.
func TestPersistedPeersWriterAlwaysEmitsV3(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, savePersistedPeers(dir, 1, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
	}))

	file, err := os.Open(filepath.Join(dir, peersFileName))
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)
	version, err := readPeersFileHeader(reader)
	require.NoError(t, err)
	require.Equal(t, peersFileVersionV3, version)
}

func TestPersistedPeersBootstrapSeedRoundTripAndDeactivate(t *testing.T) {
	dir := t.TempDir()
	peers := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
	}
	seed := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
	}

	require.NoError(t, savePersistedPeersWithBootstrapSeed(dir, 1, peers, seed))
	state, ok, err := loadPersistedPeersState(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, state.BootstrapSeedActive)
	require.Equal(t, seed, state.BootstrapSeed)

	changed := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 3, ID: "n3", Address: "127.0.0.1:7003", Suffrage: SuffrageVoter},
	}
	require.NoError(t, writeCurrentPersistedPeers(dir, 2, changed))
	state, ok, err = loadPersistedPeersState(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, state.BootstrapSeedActive)
	require.Equal(t, seed, state.BootstrapSeed)
	require.Equal(t, changed, state.Peers)
}

func TestPersistedPeersSnapshotSaveDeactivatesChangedBootstrapSeed(t *testing.T) {
	dir := t.TempDir()
	seed := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
	}

	require.NoError(t, savePersistedPeersWithBootstrapSeed(dir, 1, seed, seed))

	reorderedSeed := []Peer{
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
	}
	require.NoError(t, savePersistedPeers(dir, 2, reorderedSeed))
	state, ok, err := loadPersistedPeersState(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, state.BootstrapSeedActive)
	require.Equal(t, seed, state.BootstrapSeed)

	changed := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 3, ID: "n3", Address: "127.0.0.1:7003", Suffrage: SuffrageVoter},
	}
	require.NoError(t, savePersistedPeers(dir, 3, changed))
	state, ok, err = loadPersistedPeersState(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, state.BootstrapSeedActive)
	require.Equal(t, seed, state.BootstrapSeed)
	require.Equal(t, changed, state.Peers)
}

func TestValidateBootstrapSeedActiveMismatchRejected(t *testing.T) {
	seed := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001", Suffrage: SuffrageVoter},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002", Suffrage: SuffrageVoter},
	}
	persisted := persistedPeers{BootstrapSeed: seed, BootstrapSeedActive: true}

	require.NoError(t, validateBootstrapSeed(persisted, true, []Peer{
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7002"},
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
	}))

	err := validateBootstrapSeed(persisted, true, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
		{NodeID: 3, ID: "n3", Address: "127.0.0.1:7003"},
	})
	require.ErrorIs(t, err, errClusterMismatch)

	inactive := persisted
	inactive.BootstrapSeedActive = false
	require.NoError(t, validateBootstrapSeed(inactive, true, []Peer{
		{NodeID: 1, ID: "n1", Address: "changed:7001"},
	}))
}

// TestPersistedPeersV2UnknownSuffrageRejected pins the validation in
// readPersistedPeer that rejects suffrage bytes outside the known
// (0=voter, 1=learner) set, so a future binary that introduces a new
// suffrage variant cannot silently coerce its peers to voter on a
// build that does not understand the new value.
func TestPersistedPeersV2UnknownSuffrageRejected(t *testing.T) {
	dir := t.TempDir()
	path := peersFilePath(dir)

	require.NoError(t, replaceFile(path, func(w io.Writer) error {
		writer := bufio.NewWriter(w)
		if _, err := writer.Write(peersFileMagic[:]); err != nil {
			return err
		}
		if err := writeU32(writer, peersFileVersionV2); err != nil {
			return err
		}
		if err := writeU64(writer, 1); err != nil {
			return err
		}
		if err := writeU32(writer, 1); err != nil {
			return err
		}
		// peer entry with an unknown suffrage byte.
		if err := writeU64(writer, 1); err != nil {
			return err
		}
		if err := writeU8(writer, 0xFF); err != nil {
			return err
		}
		if err := writeString(writer, "n1"); err != nil {
			return err
		}
		if err := writeString(writer, "127.0.0.1:7001"); err != nil {
			return err
		}
		return writer.Flush()
	}))

	_, _, err := LoadPersistedPeers(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown peer suffrage byte")
}

func TestPersistedPeersUnknownVersionRejected(t *testing.T) {
	dir := t.TempDir()
	path := peersFilePath(dir)
	require.NoError(t, replaceFile(path, func(w io.Writer) error {
		writer := bufio.NewWriter(w)
		if _, err := writer.Write(peersFileMagic[:]); err != nil {
			return err
		}
		if err := writeU32(writer, 99); err != nil {
			return err
		}
		return writer.Flush()
	}))

	_, _, err := LoadPersistedPeers(dir)
	require.Error(t, err)
}
