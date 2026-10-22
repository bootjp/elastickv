package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/backup"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
)

// onDiskFSMFile writes one Redis string through a real Pebble store, takes
// the KV FSM's snapshot (the FSM snapshot header in front of the store
// stream), and persists it with the Raft engine's snapshot file writer,
// which appends the CRC32C footer. The result is the
// `fsm-snap/<index>.fsm` file an operator finds in a node's data
// directory. It returns the file path and the footer's CRC32C.
func onDiskFSMFile(t *testing.T, userKey, body string) (string, uint32) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(filepath.Join(dir, "fsm.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	value := append([]byte{0xFF, 0x01, 0x00}, body...)
	muts := []*store.KVPairMutation{{Op: store.OpTypePut, Key: []byte(backup.RedisStringPrefix + userKey), Value: value}}
	if err := st.ApplyMutationsRaftAt(context.Background(), muts, nil, 1, 1, 64); err != nil {
		t.Fatalf("apply: %v", err)
	}

	snap, err := kv.NewKvFSMWithHLC(st, kv.NewHLC()).Snapshot()
	if err != nil {
		t.Fatalf("fsm snapshot: %v", err)
	}
	defer snap.Close()
	var payload bytes.Buffer
	if _, err := snap.WriteTo(&payload); err != nil {
		t.Fatalf("write fsm snapshot: %v", err)
	}
	payloadPath := filepath.Join(dir, "payload")
	mustWriteFSM(t, payloadPath, payload.Bytes())

	res, err := etcdraftengine.PreparePhysicalSnapshotRestore(etcdraftengine.PhysicalSnapshotRestoreOptions{
		InputFSMPath: payloadPath,
		DataDir:      filepath.Join(dir, "node"),
		Index:        64,
		Term:         1,
		Peers:        []etcdraftengine.Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:50051"}},
	})
	if err != nil {
		t.Fatalf("persist .fsm: %v", err)
	}
	return res.FSMPath, res.CRC32C
}

// TestRun_DecodesOnDiskFSMFile drives the CLI over the file a running node
// actually persists: the FSM snapshot header, the store stream, and the
// engine's CRC32C footer. The decoder used to expect the raw store stream
// at byte 0, so every on-disk `.fsm` failed on its magic.
func TestRun_DecodesOnDiskFSMFile(t *testing.T) {
	t.Parallel()
	in, crc := onDiskFSMFile(t, "greeting", "hello")
	out := filepath.Join(t.TempDir(), "out")

	var stderr bytes.Buffer
	if err := run([]string{"-input", in, "-output", out, "-cluster-id", "ek-disk"}, &stderr); err != nil {
		t.Fatalf("run: %v\nstderr=%s", err, stderr.String())
	}
	assertBlobMatches(t, filepath.Join(out, "redis", "db_0", "strings", "greeting.bin"), "hello")
	assertManifest(t, filepath.Join(out, "MANIFEST.json"), manifestShape{
		Phase:         "phase0-snapshot-decode",
		SnapshotIndex: 64,
		ClusterID:     "ek-disk",
	})
	body, err := os.ReadFile(filepath.Join(out, "MANIFEST.json")) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m struct {
		Source struct {
			FSMCRC32C string `json:"fsm_crc32c"`
		} `json:"source"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if want := fmt.Sprintf("%08x", crc); m.Source.FSMCRC32C != want {
		t.Fatalf("source.fsm_crc32c = %q, want the verified footer %q", m.Source.FSMCRC32C, want)
	}
}
