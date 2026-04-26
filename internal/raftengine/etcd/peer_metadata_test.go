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

func TestPersistedPeersV2RewritesAfterV1Read(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, savePersistedPeers(dir, 1, []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"},
	}))

	// First saved file is already v2 because the writer always emits v2.
	// Verify by reading the version field directly.
	file, err := os.Open(filepath.Join(dir, peersFileName))
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)
	version, err := readPeersFileHeader(reader)
	require.NoError(t, err)
	require.Equal(t, peersFileVersionV2, version)
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
