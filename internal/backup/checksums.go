package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// CHECKSUMSFilename is the on-disk name of the dump-tree-wide
// sha256sum(1)-compatible checksum file. Stored at the dump root so
// `sha256sum -c CHECKSUMS` from the same root verifies the dump
// without elastickv tooling — the documented vendor-independent
// recovery property of Phase 0a.
const CHECKSUMSFilename = "CHECKSUMS"

// ErrChecksumsMalformedLine is returned by VerifyChecksums when a
// CHECKSUMS line does not match the expected `<hex>  <path>` shape.
// Typed so callers can distinguish "CHECKSUMS itself corrupt" from
// "file content tampering".
var ErrChecksumsMalformedLine = errors.New("backup: malformed CHECKSUMS line")

// ErrChecksumMismatch is returned by VerifyChecksums when a
// recomputed digest does not match the stored value.
var ErrChecksumMismatch = errors.New("backup: checksum mismatch")

// WriteChecksums walks root recursively, computes sha256 over every
// regular file encountered, and writes a sha256sum(1)-compatible
// CHECKSUMS file at root/CHECKSUMS.
//
// Each line is `<hex>  <relative-path>\n` (two spaces, "binary"
// mode in sha256sum vocabulary). The relative path is rooted at
// `root` itself and uses forward-slash separators so the file is
// portable across operating systems (sha256sum on Linux and the
// macOS/BSD shasum tool both accept this shape).
//
// CHECKSUMS itself is excluded from the listing — the file is
// written last and lists every other file in the tree.
//
// Determinism: the line ordering is lexicographic on the relative
// path so two invocations on the same dump tree produce
// byte-identical CHECKSUMS files. The design (§"CHECKSUMS") relies
// on this for the "encode → decode round-trip is byte-identical"
// property.
func WriteChecksums(root string) error {
	entries, err := collectChecksumEntries(root)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].relPath < entries[j].relPath
	})
	out, err := os.Create(filepath.Join(root, CHECKSUMSFilename)) //nolint:gosec // operator-supplied output dir
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = out.Close() }()
	for _, e := range entries {
		if _, err := fmt.Fprintf(out, "%s  %s\n", e.hex, e.relPath); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.WithStack(out.Sync())
}

// checksumEntry is one row of the CHECKSUMS file.
type checksumEntry struct {
	hex     string
	relPath string
}

// collectChecksumEntries walks root and returns a (hex, relPath)
// list of every regular file other than root/CHECKSUMS. Symlinks
// are skipped — sha256sum does not follow them by default and the
// dump tree should never contain any (the encoders write only
// regular files).
func collectChecksumEntries(root string) ([]checksumEntry, error) {
	var entries []checksumEntry
	skipPath := filepath.Join(root, CHECKSUMSFilename)
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		if path == skipPath {
			return nil
		}
		sum, err := sha256File(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return errors.WithStack(err)
		}
		entries = append(entries, checksumEntry{
			hex:     sum,
			relPath: filepath.ToSlash(rel),
		})
		return nil
	})
	if walkErr != nil {
		return nil, errors.WithStack(walkErr)
	}
	return entries, nil
}

// sha256File returns the lowercase-hex SHA-256 digest of path's
// contents. The file is read in O(1) memory via io.Copy into the
// hash; even multi-GB dump artifacts stream without buffering.
func sha256File(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // walker-supplied path under operator-chosen root
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", errors.WithStack(err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// VerifyChecksums reads root/CHECKSUMS, recomputes sha256 of every
// listed file, and returns the first mismatch as an error. Files
// referenced by CHECKSUMS but missing on disk surface as the same
// error class so a partial-restore is obvious.
//
// Used by the Phase 0b encoder's self-test path: after encoding a
// directory tree back into a `.fsm`, it re-runs the decoder and
// asserts WriteChecksums + VerifyChecksums round-trip cleanly.
func VerifyChecksums(root string) error {
	body, err := os.ReadFile(filepath.Join(root, CHECKSUMSFilename)) //nolint:gosec // operator-supplied output dir
	if err != nil {
		return errors.WithStack(err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		want, rel, ok := splitChecksumLine(line)
		if !ok {
			return errors.Wrapf(ErrChecksumsMalformedLine, "line %d: %q", i+1, line)
		}
		got, err := sha256File(filepath.Join(root, rel))
		if err != nil {
			return errors.Wrapf(err, "verifying %s", rel)
		}
		if got != want {
			return errors.Wrapf(ErrChecksumMismatch, "%s: have %s want %s", rel, got, want)
		}
	}
	return nil
}

// splitChecksumLine parses one sha256sum(1) line of the form
// `<hex>  <relative-path>`. Returns (hex, path, ok). Lines with the
// wrong shape are reported as not-ok so the caller can surface a
// CHECKSUMS-corruption error rather than ingest garbage.
func splitChecksumLine(line string) (string, string, bool) {
	// sha256sum's "binary" mode separates digest and filename with
	// exactly two spaces. Some tools emit `<hex> *<path>` for
	// binary mode; we accept both.
	const sha256HexLen = 64
	if len(line) < sha256HexLen+3 {
		return "", "", false
	}
	hexPart := line[:sha256HexLen]
	if !isLowerHex(hexPart) {
		return "", "", false
	}
	rest := line[sha256HexLen:]
	if !strings.HasPrefix(rest, "  ") && !strings.HasPrefix(rest, " *") {
		return "", "", false
	}
	return hexPart, rest[2:], true
}

func isLowerHex(s string) bool {
	for _, r := range s {
		isDigit := r >= '0' && r <= '9'
		isLowerAF := r >= 'a' && r <= 'f'
		if !isDigit && !isLowerAF {
			return false
		}
	}
	return true
}
