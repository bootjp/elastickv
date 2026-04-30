package encryption

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	pkgerrors "github.com/cockroachdb/errors"
)

// sidecarFileMode is the umask-respecting file mode used for both the
// keys.json file and the keys.json.tmp intermediate. 0o600 means the
// wrapped DEK material is readable only by the elastickv process owner;
// the unwrapped DEK never appears on disk regardless.
const sidecarFileMode = 0o600

// SidecarVersion is the wire version of the on-disk sidecar JSON.
//
// Version 1 carries the §5.1 layout (active, keys, raft_applied_index,
// storage_envelope_active, raft_envelope_cutover_index). Future versions
// extend the layout via additive JSON fields plus a bump here; mismatched
// versions are rejected at read time so an older binary cannot silently
// drop fields it does not understand.
const SidecarVersion = 1

// SidecarFilename is the standard filename inside <dataDir>/encryption/.
const SidecarFilename = "keys.json"

// SidecarTmpFilename is the filename used for the §5.1 crash-durable write
// protocol's intermediate write.
const SidecarTmpFilename = SidecarFilename + ".tmp"

// SidecarPurposeStorage / SidecarPurposeRaft are the only purposes the
// reader recognises. Stage 6 may add more.
const (
	SidecarPurposeStorage = "storage"
	SidecarPurposeRaft    = "raft"
)

// Sidecar is the parsed §5.1 keys.json layout.
//
// All fields persisted under the §5.1 illustrative JSON are represented
// here. Fields not yet present in the design (Stage 9 audit log, etc.)
// are omitted; they will be added as additive fields when the relevant
// stage ships.
type Sidecar struct {
	Version                  int        `json:"version"`
	RaftAppliedIndex         uint64     `json:"raft_applied_index"`
	StorageEnvelopeActive    bool       `json:"storage_envelope_active"`
	RaftEnvelopeCutoverIndex uint64     `json:"raft_envelope_cutover_index"`
	Active                   ActiveKeys `json:"active"`
	// Keys is keyed by the decimal string form of key_id (per §5.1's
	// "JSON object keys must be strings, but the on-disk envelope and
	// the in-memory keystore always work in the binary uint32 form").
	Keys map[string]SidecarKey `json:"keys"`
}

// ActiveKeys holds the active key_id per envelope purpose. A zero
// value (== ReservedKeyID) means "not bootstrapped" per §5.1.
type ActiveKeys struct {
	Storage uint32 `json:"storage"`
	Raft    uint32 `json:"raft"`
}

// SidecarKey holds the metadata for a single wrapped DEK.
//
// Wrapped is the KEK-wrapped DEK bytes (encoding/json base64-encodes
// []byte automatically). Created is an ISO-8601 timestamp string;
// the package keeps it as a plain string rather than time.Time so a
// future timezone-format addition does not break older readers.
// LocalEpoch is consumed by the §4.1 nonce construction.
type SidecarKey struct {
	Purpose    string `json:"purpose"`
	Wrapped    []byte `json:"wrapped"`
	Created    string `json:"created"`
	LocalEpoch uint16 `json:"local_epoch"`
}

// Errors returned by sidecar I/O.
var (
	// ErrSidecarVersion indicates ReadSidecar saw a wire version this
	// build does not know how to parse. Use the message and the offending
	// version to decide whether to upgrade the binary or fall back.
	ErrSidecarVersion = errors.New("encryption: unsupported sidecar version")

	// ErrSidecarPurpose indicates a Sidecar.Keys entry has a "purpose"
	// field outside the recognised set ({"storage","raft"}). The reader
	// fails closed rather than silently treating an unknown purpose as a
	// known one — a typo'd or future-version sidecar must be the
	// operator's explicit upgrade decision.
	ErrSidecarPurpose = errors.New("encryption: unsupported sidecar key purpose")

	// ErrSidecarKeyIDFormat indicates a Sidecar.Keys map key was not a
	// decimal uint32 string per §5.1.
	ErrSidecarKeyIDFormat = errors.New("encryption: sidecar key_id is not a decimal uint32")

	// ErrSidecarReservedKeyID indicates a Sidecar.Keys map carries
	// key_id 0, which §5.1 reserves as the "not bootstrapped" sentinel.
	// On-disk presence of 0 in the keys map is malformed input.
	ErrSidecarReservedKeyID = errors.New("encryption: sidecar key_id 0 is reserved")
)

// ReadSidecar parses the keys.json file at path. It validates the wire
// version, the per-key purpose, and the decimal-uint32 form of every
// keys-map entry, and rejects malformed sidecars with typed errors.
//
// ReadSidecar does NOT KEK-unwrap the DEK bytes — it just hands the
// caller a parsed struct. Wrapping is the kek.Wrapper's job at a
// higher layer.
func ReadSidecar(path string) (*Sidecar, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path comes from operator config, not user input
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "encryption: read sidecar %q", path)
	}
	var sc Sidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		return nil, pkgerrors.Wrapf(err, "encryption: parse sidecar %q", path)
	}
	if sc.Version != SidecarVersion {
		return nil, pkgerrors.Wrapf(ErrSidecarVersion,
			"got version %d, want %d (path=%q)", sc.Version, SidecarVersion, path)
	}
	if err := validateSidecar(&sc); err != nil {
		return nil, pkgerrors.Wrapf(err, "encryption: validate sidecar %q", path)
	}
	return &sc, nil
}

// validateSidecar enforces the constraints ReadSidecar applies after
// successful JSON unmarshal.
func validateSidecar(sc *Sidecar) error {
	for idStr, k := range sc.Keys {
		if err := validateSidecarKey(idStr, k); err != nil {
			return err
		}
	}
	return nil
}

func validateSidecarKey(idStr string, k SidecarKey) error {
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return pkgerrors.Wrapf(ErrSidecarKeyIDFormat, "%q: %v", idStr, err)
	}
	if uint32(id) == ReservedKeyID {
		return pkgerrors.Wrapf(ErrSidecarReservedKeyID, "found key_id %s in keys map", idStr)
	}
	switch k.Purpose {
	case SidecarPurposeStorage, SidecarPurposeRaft:
	default:
		return pkgerrors.Wrapf(ErrSidecarPurpose, "key_id=%s purpose=%q", idStr, k.Purpose)
	}
	return nil
}

// WriteSidecar persists sc to path using the §5.1 crash-durable write
// protocol:
//
//  1. Build the new contents in memory (sc.Version is set to
//     SidecarVersion so the caller never has to remember).
//  2. Write to <path>.tmp, then file.Sync().
//  3. os.Rename(<path>.tmp, <path>).
//  4. dir.Sync() on the parent directory so the rename is durable.
//
// Skipping step 2 or 4 is a data-loss-class bug: a power loss between
// the rename and the directory inode flush can roll back keys.json
// while the rotation's Raft entry is already committed, stranding
// ciphertext under a wrap that has effectively disappeared. Per §5.1
// this is treated as a hard precondition, not an optimisation.
//
// The temp file is created with mode 0o600 so a stale tmp left behind
// after a crash is not world-readable.
func WriteSidecar(path string, sc *Sidecar) (retErr error) {
	if sc == nil {
		return pkgerrors.New("encryption: WriteSidecar: sc is nil")
	}
	sc.Version = SidecarVersion
	if err := validateSidecar(sc); err != nil {
		return pkgerrors.Wrap(err, "encryption: validate before write")
	}

	data, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return pkgerrors.Wrap(err, "encryption: marshal sidecar")
	}

	dir := filepath.Dir(path)
	tmpPath := filepath.Join(dir, filepath.Base(path)+".tmp")

	if err := writeTmpAndFsync(tmpPath, data); err != nil {
		return err
	}
	// Best-effort cleanup of the tmp file if anything below fails. On
	// the success path it has already been renamed and removal is a
	// no-op.
	defer func() {
		if retErr != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := os.Rename(tmpPath, path); err != nil {
		return pkgerrors.Wrapf(err, "encryption: rename %q -> %q", tmpPath, path)
	}
	if err := syncDir(dir); err != nil {
		return pkgerrors.Wrapf(err, "encryption: fsync dir %q", dir)
	}
	return nil
}

// writeTmpAndFsync writes data into tmpPath and fsyncs the file.
// Helper exists so WriteSidecar does not nest its own defer-on-error
// inside the rename block.
func writeTmpAndFsync(tmpPath string, data []byte) error {
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, sidecarFileMode)
	if err != nil {
		return pkgerrors.Wrapf(err, "encryption: open %q", tmpPath)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write(data); err != nil {
		return pkgerrors.Wrapf(err, "encryption: write %q", tmpPath)
	}
	if err := f.Sync(); err != nil {
		return pkgerrors.Wrapf(err, "encryption: fsync %q", tmpPath)
	}
	return nil
}

// syncDir opens dir read-only and calls fsync on its file descriptor.
//
// On most POSIX filesystems this is what makes os.Rename durable. Some
// environments (NFS, certain FUSE mounts) return an error rather than
// silently treating it as a no-op; the caller must surface that error
// — the §5.1 protocol explicitly refuses to start on filesystems that
// cannot guarantee durability of the rename.
func syncDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // dir comes from operator config
	if err != nil {
		return pkgerrors.Wrapf(err, "encryption: open dir %q", dir)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		return pkgerrors.Wrapf(err, "encryption: fsync dir %q", dir)
	}
	return nil
}

// IsNotExist reports whether err is a "sidecar file does not exist"
// error from ReadSidecar. Provided as a convenience so callers can
// branch on first-boot vs. malformed sidecar without unwrapping the
// fs.PathError manually.
func IsNotExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}
