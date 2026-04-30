package encryption_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

// fixtureSidecar returns a minimally-valid Sidecar value for write-then-read
// round-trip tests. Values mirror the §5.1 illustrative JSON.
func fixtureSidecar() *encryption.Sidecar {
	return &encryption.Sidecar{
		Version:                  encryption.SidecarVersion,
		RaftAppliedIndex:         184273,
		StorageEnvelopeActive:    true,
		RaftEnvelopeCutoverIndex: 184201,
		Active: encryption.ActiveKeys{
			Storage: 305419896,
			Raft:    2596069104,
		},
		Keys: map[string]encryption.SidecarKey{
			"305419896": {
				Purpose:    encryption.SidecarPurposeStorage,
				Wrapped:    []byte{0x01, 0x02, 0x03, 0x04},
				Created:    "2026-04-29T10:00:00Z",
				LocalEpoch: 7,
			},
			"2596069104": {
				Purpose:    encryption.SidecarPurposeRaft,
				Wrapped:    []byte{0x05, 0x06, 0x07, 0x08},
				Created:    "2026-04-29T10:00:00Z",
				LocalEpoch: 7,
			},
		},
	}
}

func sidecarPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), encryption.SidecarFilename)
}

func TestWriteSidecar_RoundTrip(t *testing.T) {
	path := sidecarPath(t)
	sc := fixtureSidecar()

	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}

	got, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if !reflect.DeepEqual(got, sc) {
		t.Fatalf("round-trip mismatch:\n got  = %#v\n want = %#v", got, sc)
	}
}

// TestWriteSidecar_NoTmpFileLeftBehind exercises the §5.1 crash-durable
// write protocol's success path: after a clean Write, the tmp file
// must NOT remain on disk.
func TestWriteSidecar_NoTmpFileLeftBehind(t *testing.T) {
	path := sidecarPath(t)
	if err := encryption.WriteSidecar(path, fixtureSidecar()); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	tmpPath := path + ".tmp"
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp file %q still exists after successful write (stat err=%v)", tmpPath, err)
	}
}

func TestWriteSidecar_OverwriteExisting(t *testing.T) {
	path := sidecarPath(t)
	first := fixtureSidecar()
	first.RaftAppliedIndex = 100
	if err := encryption.WriteSidecar(path, first); err != nil {
		t.Fatalf("WriteSidecar first: %v", err)
	}
	second := fixtureSidecar()
	second.RaftAppliedIndex = 200
	if err := encryption.WriteSidecar(path, second); err != nil {
		t.Fatalf("WriteSidecar second: %v", err)
	}
	got, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if got.RaftAppliedIndex != 200 {
		t.Fatalf("Overwrite did not take effect: RaftAppliedIndex=%d, want 200", got.RaftAppliedIndex)
	}
}

func TestWriteSidecar_TmpFileMode(t *testing.T) {
	// Skip on Windows-style file systems where mode bits don't apply.
	// The current test target is unix; we just verify the tmp path is
	// 0o600 immediately after a write that we deliberately make fail
	// at rename so the tmp file is left behind for inspection. Since
	// we cannot easily make rename fail in a portable way here, we
	// instead verify the final file's mode (which inherits from the
	// tmp file).
	path := sidecarPath(t)
	if err := encryption.WriteSidecar(path, fixtureSidecar()); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	mode := st.Mode().Perm()
	// We do not require an exact 0o600 because some umask settings can
	// further restrict; just verify that no group/other read or write
	// bit is set, which is the security property we care about.
	if mode&0o077 != 0 {
		t.Fatalf("sidecar file is group/other readable or writable (mode=%o)", mode)
	}
}

func TestWriteSidecar_SetsVersion(t *testing.T) {
	path := sidecarPath(t)
	sc := fixtureSidecar()
	sc.Version = 0 // caller forgot to set
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	if sc.Version != encryption.SidecarVersion {
		t.Fatalf("WriteSidecar did not set Version: got %d", sc.Version)
	}
	got, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if got.Version != encryption.SidecarVersion {
		t.Fatalf("on-disk Version = %d, want %d", got.Version, encryption.SidecarVersion)
	}
}

func TestWriteSidecar_RejectsNil(t *testing.T) {
	path := sidecarPath(t)
	err := encryption.WriteSidecar(path, nil)
	if err == nil {
		t.Fatal("WriteSidecar(nil) returned no error")
	}
}

func TestWriteSidecar_RejectsBadPurpose(t *testing.T) {
	path := sidecarPath(t)
	sc := fixtureSidecar()
	sc.Keys["999"] = encryption.SidecarKey{
		Purpose: "bogus",
		Wrapped: []byte{0x01},
	}
	err := encryption.WriteSidecar(path, sc)
	if !errors.Is(err, encryption.ErrSidecarPurpose) {
		t.Fatalf("expected ErrSidecarPurpose, got %v", err)
	}
}

func TestWriteSidecar_RejectsReservedKeyIDInMap(t *testing.T) {
	path := sidecarPath(t)
	sc := fixtureSidecar()
	sc.Keys["0"] = encryption.SidecarKey{
		Purpose: encryption.SidecarPurposeStorage,
	}
	err := encryption.WriteSidecar(path, sc)
	if !errors.Is(err, encryption.ErrSidecarReservedKeyID) {
		t.Fatalf("expected ErrSidecarReservedKeyID, got %v", err)
	}
}

func TestWriteSidecar_RejectsNonNumericKeyID(t *testing.T) {
	path := sidecarPath(t)
	sc := fixtureSidecar()
	sc.Keys["abc"] = encryption.SidecarKey{
		Purpose: encryption.SidecarPurposeStorage,
	}
	err := encryption.WriteSidecar(path, sc)
	if !errors.Is(err, encryption.ErrSidecarKeyIDFormat) {
		t.Fatalf("expected ErrSidecarKeyIDFormat, got %v", err)
	}
}

func TestReadSidecar_Missing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.json")
	_, err := encryption.ReadSidecar(path)
	if err == nil {
		t.Fatal("ReadSidecar(missing) returned no error")
	}
	if !encryption.IsNotExist(err) {
		t.Fatalf("expected IsNotExist=true, got err=%v", err)
	}
}

func TestReadSidecar_RejectsUnknownVersion(t *testing.T) {
	path := sidecarPath(t)
	for _, v := range []int{0, 2, 42, -1} {
		raw, err := json.Marshal(map[string]any{
			"version":                     v,
			"raft_applied_index":          0,
			"storage_envelope_active":     false,
			"raft_envelope_cutover_index": 0,
			"active":                      map[string]any{"storage": 0, "raft": 0},
			"keys":                        map[string]any{},
		})
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		_, err = encryption.ReadSidecar(path)
		if !errors.Is(err, encryption.ErrSidecarVersion) {
			t.Fatalf("version=%d: expected ErrSidecarVersion, got %v", v, err)
		}
	}
}

func TestReadSidecar_RejectsCorruptJSON(t *testing.T) {
	path := sidecarPath(t)
	cases := []struct {
		name string
		raw  []byte
	}{
		{"empty", []byte{}},
		{"truncated", []byte(`{"version":1,"raft_app`)},
		{"not json", []byte(`hello world`)},
		{"wrong type", []byte(`{"version":"one"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.raw, 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			_, err := encryption.ReadSidecar(path)
			if err == nil {
				t.Fatal("ReadSidecar accepted corrupt JSON")
			}
		})
	}
}

func TestReadSidecar_RejectsBadPurpose(t *testing.T) {
	path := sidecarPath(t)
	raw := []byte(`{
        "version": 1,
        "raft_applied_index": 0,
        "storage_envelope_active": false,
        "raft_envelope_cutover_index": 0,
        "active": {"storage": 0, "raft": 0},
        "keys": {
            "1": {"purpose": "rogue", "wrapped": "AQ==", "created": "x", "local_epoch": 0}
        }
    }`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := encryption.ReadSidecar(path)
	if !errors.Is(err, encryption.ErrSidecarPurpose) {
		t.Fatalf("expected ErrSidecarPurpose, got %v", err)
	}
}

func TestReadSidecar_RejectsReservedKeyIDInMap(t *testing.T) {
	path := sidecarPath(t)
	raw := []byte(`{
        "version": 1,
        "raft_applied_index": 0,
        "storage_envelope_active": false,
        "raft_envelope_cutover_index": 0,
        "active": {"storage": 0, "raft": 0},
        "keys": {
            "0": {"purpose": "storage", "wrapped": "AQ==", "created": "x", "local_epoch": 0}
        }
    }`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := encryption.ReadSidecar(path)
	if !errors.Is(err, encryption.ErrSidecarReservedKeyID) {
		t.Fatalf("expected ErrSidecarReservedKeyID, got %v", err)
	}
}

func TestReadSidecar_RejectsNonNumericKeyID(t *testing.T) {
	path := sidecarPath(t)
	raw := []byte(`{
        "version": 1,
        "raft_applied_index": 0,
        "storage_envelope_active": false,
        "raft_envelope_cutover_index": 0,
        "active": {"storage": 0, "raft": 0},
        "keys": {
            "not-a-number": {"purpose": "storage", "wrapped": "AQ==", "created": "x", "local_epoch": 0}
        }
    }`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := encryption.ReadSidecar(path)
	if !errors.Is(err, encryption.ErrSidecarKeyIDFormat) {
		t.Fatalf("expected ErrSidecarKeyIDFormat, got %v", err)
	}
}

func TestReadSidecar_RejectsKeyIDOverflowingUint32(t *testing.T) {
	path := sidecarPath(t)
	// 4294967296 == math.MaxUint32 + 1
	raw := []byte(`{
        "version": 1,
        "raft_applied_index": 0,
        "storage_envelope_active": false,
        "raft_envelope_cutover_index": 0,
        "active": {"storage": 0, "raft": 0},
        "keys": {
            "4294967296": {"purpose": "storage", "wrapped": "AQ==", "created": "x", "local_epoch": 0}
        }
    }`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := encryption.ReadSidecar(path)
	if !errors.Is(err, encryption.ErrSidecarKeyIDFormat) {
		t.Fatalf("expected ErrSidecarKeyIDFormat, got %v", err)
	}
}

// TestWriteSidecar_PartialWriteCleanup confirms that when a write fails
// AFTER the tmp file is created (we simulate this by making the parent
// dir refuse the rename via a removed dir), no orphaned tmp file is
// left behind.
func TestWriteSidecar_PartialWriteCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, encryption.SidecarFilename)
	// Pre-create path as a directory so os.Rename fails with EEXIST/EISDIR.
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("Mkdir target-as-dir: %v", err)
	}

	err := encryption.WriteSidecar(path, fixtureSidecar())
	if err == nil {
		t.Fatal("WriteSidecar succeeded despite path being a directory")
	}
	tmpPath := path + ".tmp"
	if _, statErr := os.Stat(tmpPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("tmp file leaked after write failure: stat err=%v", statErr)
	}
}

func TestSidecarPurpose_Constants(t *testing.T) {
	// Pin the wire format constants so a future refactor can't silently
	// rename the JSON-visible strings.
	if encryption.SidecarPurposeStorage != "storage" {
		t.Fatalf("SidecarPurposeStorage = %q, want %q", encryption.SidecarPurposeStorage, "storage")
	}
	if encryption.SidecarPurposeRaft != "raft" {
		t.Fatalf("SidecarPurposeRaft = %q, want %q", encryption.SidecarPurposeRaft, "raft")
	}
}
