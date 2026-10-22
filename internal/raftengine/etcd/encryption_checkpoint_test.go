package etcd

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestEncryptionScannerEncryptedRetainedTail(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "rotation", true: "invalid-tag"}[corrupt], func(t *testing.T) {
			cipher, id := raftCipherFixture(t)
			entry := envelopeEntry(t, cipher, id, 2, []byte{fsmwire.OpRotation})
			if corrupt {
				entry.Data[len(entry.Data)-1] ^= 1
			}
			engine := newTestEngine(nil, cipher, func() uint64 { return 1 })
			engine.storage = scannerWithEntries(t, newEntry(1, fsmwire.OpBootstrap), entry).storage
			hit, err := engine.EncryptionScanner().HasEncryptionRelevantEntryInRange(1, 2)
			if corrupt {
				require.Error(t, err, "ciphertext that cannot be inspected must refuse startup")
			} else {
				require.NoError(t, err)
				require.True(t, hit, "encrypted rotation must not be treated as an ordinary write")
			}
		})
	}
}

func TestEncryptionSnapshotRequiresMatchingPhysicalEnvelope(t *testing.T) {
	for _, variant := range []string{"valid", "stale-token", "mismatched-index", "corrupt-payload"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			payload := []byte("verified FSM snapshot bytes")
			crc, path := writeFSMFileForTest(t, dir, 100, payload)
			index := uint64(100)
			if variant == "stale-token" {
				crc ^= 1
			}
			if variant == "mismatched-index" {
				index = 99
			}
			if variant == "corrupt-payload" {
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				require.NoError(t, err)
				_, err = file.WriteAt([]byte("X"), 0)
				require.NoError(t, err)
				require.NoError(t, file.Close())
			}
			storage := etcdraft.NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Data: encodeSnapshotToken(index, crc), Metadata: testSnapshotMetadata(100, 1, nil)}))
			engine := &Engine{storage: storage, fsmSnapDir: filepath.Clean(dir)}
			reader, gotIndex, err := engine.EncryptionSnapshot()
			if variant != "valid" {
				require.Error(t, err)
				require.Nil(t, reader)
				return
			}
			require.NoError(t, err)
			defer reader.Close()
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, payload, got)
			require.Equal(t, uint64(100), gotIndex)
		})
	}
}
