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
	"go.etcd.io/etcd/server/v3/storage/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
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

	requireWALSnapshot(t, filepath.Join(dir, "raft", "wal"), *snap)
}

func TestRunRejectsExistingTargetWithoutForce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	input := filepath.Join(dir, "payload.fsm")
	require.NoError(t, os.WriteFile(input, []byte("EKVPBBL1payload"), 0o600))

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

func TestRunRejectsVoterLearnerOverlap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	input := filepath.Join(dir, "payload.fsm")
	require.NoError(t, os.WriteFile(input, []byte("EKVPBBL1payload"), 0o600))

	err := run([]string{
		"--input", input,
		"--data-dir", filepath.Join(dir, "raft"),
		"--index", "1",
		"--term", "1",
		"--voters", "node:2",
		"--learners", "node:2",
	}, bytes.NewBuffer(nil))
	require.ErrorContains(t, err, "both voter and learner")
}

func TestRunRejectsInvalidInputWithoutReplacingExistingArtifacts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "raft")
	input := filepath.Join(dir, "payload.fsm")
	oldPayload := []byte("EKVPBBL1old-payload")
	require.NoError(t, os.WriteFile(input, oldPayload, 0o600))

	args := []string{
		"--input", input,
		"--data-dir", dataDir,
		"--index", "10",
		"--term", "3",
		"--voters", "n1",
	}
	require.NoError(t, run(args, bytes.NewBuffer(nil)))

	fsmPath := filepath.Join(dataDir, "fsm-snap", "000000000000000a.fsm")
	oldFSM, err := os.ReadFile(fsmPath)
	require.NoError(t, err)
	oldSnap, err := etcdsnap.New(zap.NewNop(), filepath.Join(dataDir, "snap")).Load()
	require.NoError(t, err)
	requireWALSnapshot(t, filepath.Join(dataDir, "wal"), *oldSnap)

	badInput := filepath.Join(dir, "bad.fsm")
	require.NoError(t, os.WriteFile(badInput, []byte("not-a-pebble-snapshot"), 0o600))
	forced := []string{
		"--input", badInput,
		"--data-dir", dataDir,
		"--index", "10",
		"--term", "3",
		"--voters", "n1",
		"--force",
	}
	err = run(forced, bytes.NewBuffer(nil))
	require.ErrorContains(t, err, "EKVPBBL1")

	gotFSM, err := os.ReadFile(fsmPath)
	require.NoError(t, err)
	require.Equal(t, oldFSM, gotFSM)
	gotSnap, err := etcdsnap.New(zap.NewNop(), filepath.Join(dataDir, "snap")).Load()
	require.NoError(t, err)
	require.Equal(t, oldSnap.Metadata, gotSnap.Metadata)
	require.Equal(t, oldSnap.Data, gotSnap.Data)
	requireWALSnapshot(t, filepath.Join(dataDir, "wal"), *gotSnap)
}

func TestRunRejectsAlreadyFooterSealedInput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	input := filepath.Join(dir, "payload.fsm")
	require.NoError(t, os.WriteFile(input, []byte("EKVPBBL1payload"), 0o600))

	sourceDataDir := filepath.Join(dir, "source")
	require.NoError(t, run([]string{
		"--input", input,
		"--data-dir", sourceDataDir,
		"--index", "1",
		"--term", "1",
		"--voters", "n1",
	}, bytes.NewBuffer(nil)))

	err := run([]string{
		"--input", filepath.Join(sourceDataDir, "fsm-snap", "0000000000000001.fsm"),
		"--data-dir", filepath.Join(dir, "target"),
		"--index", "2",
		"--term", "1",
		"--voters", "n1",
	}, bytes.NewBuffer(nil))
	require.ErrorContains(t, err, "already footer-sealed")
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

func be32(v uint32) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, v)
	return out
}

func requireWALSnapshot(t *testing.T, walDir string, snap raftpb.Snapshot) {
	t.Helper()

	walSnaps, err := wal.ValidSnapshotEntries(zap.NewNop(), walDir)
	require.NoError(t, err)

	var found bool
	for _, walSnap := range walSnaps {
		if walSnap.Index != snap.Metadata.Index || walSnap.Term != snap.Metadata.Term {
			continue
		}
		found = true
		require.NotNil(t, walSnap.ConfState)
		require.Equal(t, snap.Metadata.ConfState, *walSnap.ConfState)

		w, err := wal.Open(zap.NewNop(), walDir, walSnap)
		require.NoError(t, err)
		_, hardState, entries, err := w.ReadAll()
		require.NoError(t, err)
		require.NoError(t, w.Close())
		require.Equal(t, snap.Metadata.Term, hardState.Term)
		require.Equal(t, snap.Metadata.Index, hardState.Commit)
		require.Empty(t, entries)
	}
	require.True(t, found, "missing WAL snapshot entry for %d/%d", snap.Metadata.Term, snap.Metadata.Index)
}
