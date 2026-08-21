package backup

import (
	"bytes"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

// FinalizeDump writes CHECKSUMS and then atomically publishes MANIFEST.json as
// the final dump-tree entry. A failure or process crash before publication
// leaves no manifest, so restore tooling cannot mistake a partial dump for a
// complete artifact.
func FinalizeDump(root string, manifest Manifest) error {
	payload, err := marshalManifestPayload(manifest)
	if err != nil {
		return err
	}
	manifestPath := filepath.Join(root, ManifestFilename)
	if err := requireManifestAbsent(manifestPath); err != nil {
		return err
	}
	if err := WriteChecksumsWithVirtualFile(root, ManifestFilename, payload); err != nil {
		return errors.Wrap(err, "write CHECKSUMS")
	}
	return publishManifest(root, manifestPath, payload)
}

func marshalManifestPayload(manifest Manifest) ([]byte, error) {
	var payload bytes.Buffer
	if err := WriteManifest(&payload, manifest); err != nil {
		return nil, err
	}
	return payload.Bytes(), nil
}

func requireManifestAbsent(manifestPath string) error {
	if _, err := os.Lstat(manifestPath); err == nil {
		return errors.New("backup: MANIFEST.json already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.WithStack(err)
	}
	return nil
}

func publishManifest(root, manifestPath string, payload []byte) error {
	tmpPath, err := writeManifestTemp(root, payload)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	if err := os.Rename(tmpPath, manifestPath); err != nil {
		return errors.WithStack(err)
	}
	if err := syncBackupDirectory(root); err != nil {
		_ = os.Remove(manifestPath)
		return err
	}
	return nil
}

func writeManifestTemp(root string, payload []byte) (path string, retErr error) {
	tmp, err := os.CreateTemp(root, ".MANIFEST.json.*.tmp")
	if err != nil {
		return "", errors.WithStack(err)
	}
	path = tmp.Name()
	defer func() {
		if retErr != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
		}
	}()
	if _, err := io.Copy(tmp, bytes.NewReader(payload)); err != nil {
		return path, errors.WithStack(err)
	}
	if err := tmp.Sync(); err != nil {
		return path, errors.WithStack(err)
	}
	if err := tmp.Close(); err != nil {
		return path, errors.WithStack(err)
	}
	return path, nil
}

func syncBackupDirectory(path string) error {
	dir, err := os.Open(path) //nolint:gosec // operator-selected dump root
	if err != nil {
		return errors.WithStack(err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return errors.WithStack(err)
	}
	return errors.WithStack(dir.Close())
}
