package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
	etcdsnap "go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

func TestRunWritesRestorePair(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	input := filepath.Join(dir, "payload.fsm")
	payload := []byte("EKVPBBL1payload")
	require.NoError(t, os.WriteFile(input, payload, 0o600))

	var stdout bytes.Buffer
	err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "100",
		"--term", "7",
		"--voters", "n1,node:2",
		"--learners", "n3",
	}, &stdout)
	require.NoError(t, err)
	require.Contains(t, stdout.String(), "crc32c=")

	crc := crc32.Checksum(payload, crc32cTable)
	fsmPath := filepath.Join(dir, "raft", "fsm-snap", "0000000000000064.fsm")
	fsmBytes, err := os.ReadFile(fsmPath)
	require.NoError(t, err)
	require.Equal(t, append(payload, be32(crc)...), fsmBytes)

	snap, err := etcdsnap.New(zap.NewNop(), filepath.Join(dir, "raft", "snap")).Load()
	require.NoError(t, err)
	require.Equal(t, uint64(100), snap.Metadata.Index)
	require.Equal(t, uint64(7), snap.Metadata.Term)
	require.Equal(t, []uint64{etcdraftengine.DeriveNodeID("n1"), 2}, snap.Metadata.ConfState.Voters)
	require.Equal(t, []uint64{etcdraftengine.DeriveNodeID("n3")}, snap.Metadata.ConfState.Learners)
	require.Equal(t, encodeSnapshotToken(100, crc), snap.Data)
}

func TestRunRejectsExistingTargetWithoutForce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	input := filepath.Join(dir, "payload.fsm")
	require.NoError(t, os.WriteFile(input, []byte("payload"), 0o600))

	args := []string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "1",
		"--term", "1",
		"--voters", "n1",
	}
	require.NoError(t, run(args, bytes.NewBuffer(nil)))
	err := run(args, bytes.NewBuffer(nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")

	forced := append(append([]string(nil), args...), "--force")
	require.NoError(t, run(forced, bytes.NewBuffer(nil)))
}

func TestParseNodeIDsRequiresVoters(t *testing.T) {
	t.Parallel()
	_, err := parseNodeIDs("--voters", "", true)
	require.Error(t, err)

	got, err := parseNodeIDs("--voters", "n1,node:2", true)
	require.NoError(t, err)
	require.Equal(t, []uint64{etcdraftengine.DeriveNodeID("n1"), 2}, got)

	_, err = parseNodeIDs("--voters", "node:0", true)
	require.ErrorContains(t, err, "node id must be > 0")
}

func TestParseRequiredUint64TrimsWhitespace(t *testing.T) {
	t.Parallel()
	got, err := parseRequiredUint64("--index", " 0x64 ")
	require.NoError(t, err)
	require.Equal(t, uint64(100), got)
}

func TestParseOneNodeIDTrimsExplicitID(t *testing.T) {
	t.Parallel()
	got, err := parseOneNodeID(" node: 2 ")
	require.NoError(t, err)
	require.Equal(t, uint64(2), got)
}

func TestEnsureCanWriteForceRemovesExistingTarget(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "target.fsm")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
	require.NoError(t, ensureCanWrite(path, true))
	require.NoFileExists(t, path)
}

func be32(v uint32) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, v)
	return out
}
