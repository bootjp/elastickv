package backup

import (
	"bufio"
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

// maxChecksumLineLen bounds the bufio.Scanner buffer for one
// CHECKSUMS line. A well-formed entry is 64 hex chars + 2 spaces +
// the relative path; with NAME_MAX=255 on Linux, even a
// pathologically deep tree (255 chars × ~30 levels) stays well
// under 8 KiB. The cap exists so a corrupt CHECKSUMS with a
// missing newline cannot force the scanner to buffer an
// arbitrarily-long "line" — that is the OOM vector the gemini
// security-high finding on PR #810 flagged when the previous
// implementation slurped the whole file via os.ReadFile.
const maxChecksumLineLen = 8 * 1024

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
// Memory: streaming. Uses bufio.Scanner with a fixed 8 KiB per-line
// budget so a multi-million-file dump tree's CHECKSUMS file does
// not need to fit in memory, and a corrupt CHECKSUMS without
// newlines cannot trick us into buffering arbitrarily much (the
// gemini security-high finding on PR #810 — the previous
// os.ReadFile + strings.Split path materialised the whole file).
//
// Path safety: every CHECKSUMS line's relative-path field is
// validated by validateChecksumRelPath before being joined with
// root, so an adversarial CHECKSUMS shipped alongside a backup
// (e.g. with a `../../etc/shadow` entry) cannot escape root and
// fingerprint files the operator did not intend to expose. The
// coderabbit critical finding on PR #810.
//
// Used by the Phase 0b encoder's self-test path: after encoding a
// directory tree back into a `.fsm`, it re-runs the decoder and
// asserts WriteChecksums + VerifyChecksums round-trip cleanly.
func VerifyChecksums(root string) error {
	f, err := os.Open(filepath.Join(root, CHECKSUMSFilename)) //nolint:gosec // operator-supplied output dir
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, maxChecksumLineLen), maxChecksumLineLen)
	lineNum := 0
	for sc.Scan() {
		lineNum++
		if err := verifyOneChecksumLine(root, sc.Text(), lineNum); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// verifyOneChecksumLine handles a single CHECKSUMS line: parse,
// path-validate, recompute, compare. Extracted from VerifyChecksums
// so the parent stays under the project's cyclop ceiling and the
// per-line failure modes (malformed shape, traversal, symlink
// escape, missing file, mismatch) each surface as a distinct
// typed error.
func verifyOneChecksumLine(root, line string, lineNum int) error {
	if line == "" {
		return nil
	}
	want, rel, ok := splitChecksumLine(line)
	if !ok {
		return errors.Wrapf(ErrChecksumsMalformedLine, "line %d: %q", lineNum, line)
	}
	cleaned, err := validateChecksumRelPath(rel)
	if err != nil {
		return errors.Wrapf(err, "line %d", lineNum)
	}
	joined := filepath.Join(root, cleaned)
	if err := refuseSymlinkComponents(root, cleaned); err != nil {
		return errors.Wrapf(err, "line %d", lineNum)
	}
	got, err := sha256File(joined)
	if err != nil {
		return errors.Wrapf(err, "verifying %s", cleaned)
	}
	if got != want {
		return errors.Wrapf(ErrChecksumMismatch, "%s: have %s want %s", cleaned, got, want)
	}
	return nil
}

// refuseSymlinkComponents walks rel one path segment at a time
// starting at root and rejects the first component that is a
// symlink. This closes the codex r2 P1 symlink-escape attack on
// PR #810: after the textual `..`-traversal guard runs,
// `os.Open(filepath.Join(root, "safe/file"))` would still follow a
// symlink at `safe/file` (or at any of its parent components) to
// a file outside root and hash whatever it found. A Phase 0a dump
// produced by the encoder writes only regular files, so refusing
// every symlink found in the path keeps the verifier honest
// regardless of which component an adversary planted the link in.
//
// The per-component walk catches BOTH terminal symlinks (where
// `cleaned` itself names a symlink) AND intermediate ones (where
// a parent directory along the way is a symlink). syscall
// O_NOFOLLOW would only cover the terminal case, and
// filepath.EvalSymlinks runs the resolution we are trying to
// avoid in the first place.
func refuseSymlinkComponents(root, rel string) error {
	cur := root
	for _, seg := range strings.Split(rel, string(filepath.Separator)) {
		if seg == "" {
			continue
		}
		cur = filepath.Join(cur, seg)
		info, err := os.Lstat(cur)
		if err != nil {
			return errors.WithStack(err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.Wrapf(ErrChecksumsSymlinkEscape,
				"symlink in path: %s", cur)
		}
	}
	return nil
}

// ErrChecksumsSymlinkEscape is returned by VerifyChecksums when a
// CHECKSUMS line's resolved path traverses a symlink. Typed so a
// caller branching on errors.Is can distinguish symlink-tampering
// from textual `..`-traversal (ErrChecksumsPathTraversal).
var ErrChecksumsSymlinkEscape = errors.New("backup: CHECKSUMS path traverses symlink")

// validateChecksumRelPath rejects CHECKSUMS rows whose path field
// could escape the dump root or otherwise resolve outside the
// regular-file subtree the verifier expects. The acceptance rule
// is `filepath.IsLocal` — Go-stdlib's lexical "stays within the
// subtree rooted at the evaluation directory" check, which
// covers:
//
//   - absolute paths (any platform);
//   - traversal via `..` segments (before or after Clean);
//   - on Windows, reserved device names like `CON`, `NUL`,
//     `COM1`-`COM9`, `LPT1`-`LPT9`, `PRN`, `AUX`, `CONIN$`,
//     `CONOUT$`, etc. — opened via `os.Open` these would yield
//     the host's console / device, not a dump file, and produce
//     hash mismatches keyed off host state. Codex r3 P1 on
//     PR #810.
//
// Empty and "." are rejected explicitly because IsLocal returns
// true for "." (it does stay within the subtree, but it names
// the root directory itself, which is not a file we can hash);
// honest CHECKSUMS lines name regular files, not the root dir.
//
// Returns the Cleaned form so callers join it without redoing
// the canonicalisation. Mirrors the same guard the S3 encoder
// applies to bucket/object path segments before scratch-dir
// layout.
func validateChecksumRelPath(rel string) (string, error) {
	if rel == "" || rel == "." {
		return "", errors.Wrapf(ErrChecksumsMalformedLine, "empty path field")
	}
	if !filepath.IsLocal(rel) {
		return "", errors.Wrapf(ErrChecksumsPathTraversal,
			"not a local path (absolute / traversal / reserved name): %q", rel)
	}
	cleaned := filepath.Clean(rel)
	// IsLocal already rejects absolute paths and `..` traversal
	// before/after Clean; the post-Clean recheck below is
	// belt-and-braces against a future stdlib change that would
	// loosen IsLocal but keep the cleaned-shape invariants.
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", errors.Wrapf(ErrChecksumsPathTraversal, "escapes root after Clean: %q", rel)
	}
	if filepath.IsAbs(cleaned) {
		return "", errors.Wrapf(ErrChecksumsPathTraversal, "absolute after Clean: %q", rel)
	}
	return cleaned, nil
}

// ErrChecksumsPathTraversal is returned by VerifyChecksums when a
// CHECKSUMS line's path field would resolve outside the dump root.
// Typed so a future verifier built on top of this package can
// branch on errors.Is and distinguish path-shape attacks from
// honest content-mismatch.
var ErrChecksumsPathTraversal = errors.New("backup: CHECKSUMS path escapes dump root")

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
