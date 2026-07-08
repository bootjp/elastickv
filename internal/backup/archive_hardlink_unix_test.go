//go:build unix

package backup

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackDumpTreeRejectsHardLinkedFile(t *testing.T) {
	root := writeArchiveFixture(t)
	external := filepath.Join(t.TempDir(), "external.bin")
	require.NoError(t, os.WriteFile(external, []byte("external"), 0o600))
	linked := filepath.Join(root, "hardlink.bin")
	require.NoError(t, os.Link(external, linked))
	require.NoError(t, WriteChecksums(root))

	var buf bytes.Buffer
	err := PackDumpTree(root, &buf, ArchiveCompressionNone)
	require.ErrorIs(t, err, ErrArchiveNonRegular)
}
