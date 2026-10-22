package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
)

type checkpointGapEngine struct {
	stubGapEngine
	payload       []byte
	snapshotIndex uint64
	snapshotErr   error
	snapshotCalls int
}

func (e *checkpointGapEngine) EncryptionSnapshot() (io.ReadCloser, uint64, error) {
	e.snapshotCalls++
	return io.NopCloser(bytes.NewReader(e.payload)), e.snapshotIndex, e.snapshotErr
}

func TestEncryptionCheckpointReadsSnapshotOnlyForGap(t *testing.T) {
	for _, applied := range []uint64{5, 6, 7} {
		t.Run(fmt.Sprintf("applied-%d", applied), func(t *testing.T) {
			engine := &checkpointGapEngine{
				stubGapEngine: stubGapEngine{appliedIndex: applied},
				snapshotErr:   io.ErrUnexpectedEOF,
			}
			sc := &encryption.Sidecar{RaftAppliedIndex: 6}
			start, err := sidecarGapStartFromSnapshot(engine, sc)
			if applied > sc.RaftAppliedIndex {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				require.Equal(t, 1, engine.snapshotCalls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, sc.RaftAppliedIndex, start)
			require.Zero(t, engine.snapshotCalls)
		})
	}
}

type checkpointScanner struct {
	t          *testing.T
	start, end uint64
	hit        bool
}

func (s *checkpointScanner) HasEncryptionRelevantEntryInRange(start, end uint64) (bool, error) {
	if start < s.start {
		return false, etcdraft.ErrCompacted
	}
	require.Equal(s.t, s.start, start)
	require.Equal(s.t, s.end, end)
	return s.hit, nil
}

func checkpointHeader(t *testing.T, sc *encryption.Sidecar) []byte {
	t.Helper()
	cp, err := encryption.SidecarCheckpoint(sc)
	require.NoError(t, err)
	var out bytes.Buffer
	out.WriteString("EKVTHLC2")
	require.NoError(t, binary.Write(&out, binary.BigEndian, uint16(64)))
	require.NoError(t, binary.Write(&out, binary.BigEndian, uint64(1234)))
	require.NoError(t, binary.Write(&out, binary.BigEndian, sc.RaftEnvelopeCutoverIndex))
	out.WriteString("EKVENCP1")
	require.NoError(t, binary.Write(&out, binary.BigEndian, cp.Index))
	out.Write(cp.State[:])
	return out.Bytes()
}

func TestEncryptionCompactedRestartCheckpoint(t *testing.T) {
	for _, variant := range []string{"valid", "stale-sidecar", "mismatched-keys", "future-checkpoint", "missing-proof", "retained-rotation", "snapshot-verification-error", "truncated-proof"} {
		t.Run(variant, func(t *testing.T) {
			sc := &encryption.Sidecar{Version: encryption.SidecarVersion, RaftAppliedIndex: 6,
				StorageEnvelopeActive: true, StorageEnvelopeCutoverIndex: 5, RaftEnvelopeCutoverIndex: 6,
				Active: encryption.ActiveKeys{Storage: 1, Raft: 2},
				Keys:   map[string]encryption.SidecarKey{"1": {Purpose: "storage", Wrapped: []byte("wrapped-storage"), LocalEpoch: 8}, "2": {Purpose: "raft", Wrapped: []byte("wrapped-raft")}}}
			payload := checkpointHeader(t, sc)
			engine := &checkpointGapEngine{stubGapEngine: stubGapEngine{appliedIndex: 877,
				scanner: &checkpointScanner{t: t, start: 860, end: 877}}, payload: payload, snapshotIndex: 860}
			switch variant {
			case "stale-sidecar":
				sc.RaftAppliedIndex = 5
			case "mismatched-keys":
				key := sc.Keys["1"]
				key.Wrapped = []byte("different-wrapped-key")
				sc.Keys["1"] = key
			case "future-checkpoint":
				binary.BigEndian.PutUint64(engine.payload[34:42], 861)
			case "missing-proof":
				engine.payload = []byte("EKVTHLC1\x00\x00\x00\x00\x00\x00\x00\x01")
			case "retained-rotation":
				scanner, ok := engine.scanner.(*checkpointScanner)
				require.True(t, ok)
				scanner.hit = true
			case "snapshot-verification-error":
				engine.snapshotErr = io.ErrUnexpectedEOF
			case "truncated-proof":
				engine.payload = payload[:len(payload)-1]
			}
			path := filepath.Join(t.TempDir(), "keys.json")
			require.NoError(t, encryption.WriteSidecar(path, sc))
			err := runSidecarBehindRaftLogGuard(engine, path, 1)
			if variant == "valid" {
				require.NoError(t, err, "verified snapshot witnesses the compacted prefix; retained tail still scanned")
			} else {
				require.Error(t, err, "uncertain or inconsistent metadata must refuse startup")
			}
		})
	}
}

func TestEncryptionCheckpointDoesNotAuthorizeEpochRollback(t *testing.T) {
	sc := &encryption.Sidecar{RaftAppliedIndex: 6, Keys: map[string]encryption.SidecarKey{"1": {LocalEpoch: 8}}}
	payload := checkpointHeader(t, sc)
	_, _, before, err := kv.ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(bytes.NewReader(payload)))
	require.NoError(t, err)
	key := sc.Keys["1"]
	key.LocalEpoch = 1
	sc.Keys["1"] = key
	after, err := encryption.SidecarCheckpoint(sc)
	require.NoError(t, err)
	require.Equal(t, before, after, "snapshot state is replica-independent, not an epoch authorization")
	st := newRegistrationTestStore(t)
	preSeedRegistryRow(t, st, 1, 1, 8)
	reg, err := store.WriterRegistryFor(st)
	require.NoError(t, err)
	err = checkLocalEpochRollbackBeforeBump(reg, 1, 1, key.LocalEpoch, true)
	require.ErrorIs(t, err, encryption.ErrLocalEpochRollback)
}
