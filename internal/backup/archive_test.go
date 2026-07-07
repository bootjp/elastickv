package backup

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPackUnpackDumpTreeZstdRoundTrip(t *testing.T) {
	root := writeArchiveFixture(t)
	var buf bytes.Buffer
	require.NoError(t, PackDumpTree(root, &buf, ArchiveCompressionZstd))

	out := filepath.Join(t.TempDir(), "unpacked")
	require.NoError(t, UnpackDumpTree(bytes.NewReader(buf.Bytes()), out, ArchiveCompressionZstd))
	require.NoError(t, VerifyChecksums(out))
	require.FileExists(t, filepath.Join(out, "MANIFEST.json"))
	require.FileExists(t, filepath.Join(out, CHECKSUMSFilename))
	require.FileExists(t, filepath.Join(out, "redis", "db_0", "strings", "key.bin"))
}

func TestUnpackDumpTreeRejectsTraversal(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "../escape",
		Mode: 0o600,
		Size: 1,
	}))
	_, err := tw.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	out := filepath.Join(t.TempDir(), "out")
	err = UnpackDumpTree(bytes.NewReader(buf.Bytes()), out, ArchiveCompressionNone)
	require.ErrorIs(t, err, ErrArchivePathUnsafe)
	_, statErr := os.Stat(out)
	require.True(t, os.IsNotExist(statErr))
}

func TestPackDumpTreeRejectsSymlink(t *testing.T) {
	root := writeArchiveFixture(t)
	require.NoError(t, os.Symlink("MANIFEST.json", filepath.Join(root, "link")))
	var buf bytes.Buffer
	err := PackDumpTree(root, &buf, ArchiveCompressionNone)
	require.ErrorIs(t, err, ErrArchiveNonRegular)
}

func TestPackDumpTreeRejectsUnchecksummedFile(t *testing.T) {
	root := writeArchiveFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(root, "extra.bin"), []byte("extra"), 0o600))
	var buf bytes.Buffer
	err := PackDumpTree(root, &buf, ArchiveCompressionNone)
	require.ErrorIs(t, err, ErrArchiveUnchecksummedFile)
}

func TestCleanArchiveRelPathAllowsRootDotSlash(t *testing.T) {
	rel, err := cleanArchiveRelPath("./")
	require.NoError(t, err)
	require.Equal(t, ".", rel)
}

func writeArchiveFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "redis", "db_0", "strings"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "redis", "db_0", "strings", "key.bin"), []byte("value"), 0o600))
	m := NewPhase0SnapshotManifest(time.Unix(0, 0))
	m.ElastickvVersion = "test"
	m.ClusterID = "cluster-a"
	m.LastCommitTS = 1
	m.Source = &Source{FSMPath: "source.fsm"}
	m.Adapters = &Adapters{Redis: &Adapter{}}
	f, err := os.Create(filepath.Join(root, "MANIFEST.json"))
	require.NoError(t, err)
	require.NoError(t, WriteManifest(f, m))
	require.NoError(t, f.Close())
	require.NoError(t, WriteChecksums(root))
	return root
}
