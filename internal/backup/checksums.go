package backup

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
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
	if err := writeAllChecksumLines(out, entries); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return errors.WithStack(err)
	}
	// Surface Close errors instead of swallowing them. On NFS /
	// some FUSE backends Close is the authoritative durability
	// signal — writeback failures buffered after the Sync()
	// fsync surface here, not at Write time. A swallowed Close
	// would let the CLI declare success with a truncated or
	// unreadable CHECKSUMS file. coderabbit Major on PR #810
	// round 4 (same shape as the gemini r1 medium on
	// emitManifest, applied here too).
	if err := out.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// writeAllChecksumLines emits the formatted CHECKSUMS rows. Split
// out from WriteChecksums so the parent stays a thin
// open-write-sync-close skeleton — its job is the durability
// contract; the per-line escaping is delegated.
func writeAllChecksumLines(w io.Writer, entries []checksumEntry) error {
	for _, e := range entries {
		line, prefix := formatChecksumLine(e.hex, e.relPath)
		if _, err := fmt.Fprintf(w, "%s%s\n", prefix, line); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// formatChecksumLine builds one CHECKSUMS row using GNU coreutils
// sha256sum(1) escape conventions. When the path contains `\n`,
// `\r`, or `\\`, sha256sum prefixes the entire line with `\\` and
// replaces the offending bytes with `\\n` / `\\r` / `\\\\`. Phase 0a
// emits the same shape so `sha256sum -c CHECKSUMS` from the dump
// root parses a tampered or path-noisy filename without corrupting
// the line-based output (codex r4 P2 on PR #810: the previous
// `%s  %s\n` print would let a filename containing `\n` inject a
// fake CHECKSUMS row).
//
// Returns (line-body, prefix). The prefix is either "" or "\\";
// callers concatenate `prefix + body + "\n"` exactly like the
// reference implementation.
func formatChecksumLine(hex, relPath string) (string, string) {
	if !strings.ContainsAny(relPath, "\n\r\\") {
		return hex + "  " + relPath, ""
	}
	var sb strings.Builder
	sb.Grow(len(relPath) + 4) //nolint:mnd // small heuristic to avoid early grow
	for _, r := range relPath {
		switch r {
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\\':
			sb.WriteString(`\\`)
		default:
			sb.WriteRune(r)
		}
	}
	return hex + "  " + sb.String(), `\`
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
	verified := 0
	for sc.Scan() {
		lineNum++
		got, err := verifyOneChecksumLine(root, sc.Text(), lineNum)
		if err != nil {
			return err
		}
		if got {
			verified++
		}
	}
	if err := sc.Err(); err != nil {
		return errors.WithStack(err)
	}
	// Refuse a CHECKSUMS that yielded zero verified rows. An empty
	// (or all-blank) file would otherwise pass verification and
	// hide a producer-side corruption — the dump tree itself is
	// non-empty (WriteChecksums lists every regular file), so a
	// CHECKSUMS with no rows is always a signal, never a benign
	// shape. Codex r5 P2 on PR #810.
	if verified == 0 {
		return errors.WithStack(ErrChecksumsEmpty)
	}
	return nil
}

// ErrChecksumsEmpty is returned by VerifyChecksums when the
// CHECKSUMS file parsed to zero checksum rows (empty file, all
// blank lines, or comment-only — none of which are valid Phase 0a
// dump shapes). Typed so callers can distinguish "CHECKSUMS file
// did not list anything" from "a listed file mismatched".
var ErrChecksumsEmpty = errors.New("backup: CHECKSUMS contains no checksum rows")

// verifyOneChecksumLine handles a single CHECKSUMS line: parse,
// path-validate, recompute, compare. Extracted from VerifyChecksums
// so the parent stays under the project's cyclop ceiling and the
// per-line failure modes (malformed shape, traversal, symlink
// escape, missing file, mismatch) each surface as a distinct
// typed error.
//
// Returns (verified, err): verified=true when the line was a real
// checksum row that VerifyChecksums should count toward the
// "at least one row" empty-file guard (blank lines do not count).
// A missing file referenced by an honest CHECKSUMS row is
// surfaced as ErrChecksumMismatch — partial restores and
// adversarial deletions land in the same typed failure class
// callers branch on. Codex r5 P2 on PR #810.
func verifyOneChecksumLine(root, line string, lineNum int) (bool, error) {
	if line == "" {
		return false, nil
	}
	want, rel, ok := splitChecksumLine(line)
	if !ok {
		return false, errors.Wrapf(ErrChecksumsMalformedLine, "line %d: %q", lineNum, line)
	}
	cleaned, err := validateChecksumRelPath(rel)
	if err != nil {
		return false, errors.Wrapf(err, "line %d", lineNum)
	}
	joined := filepath.Join(root, cleaned)
	if err := refuseSymlinkComponents(root, cleaned); err != nil {
		// Missing file along the resolved path is a partial-
		// restore / tamper signal — same operational category
		// as a hash mismatch. The Lstat-walk caller bubbles up
		// raw fs.ErrNotExist; promote it to the typed
		// ErrChecksumMismatch class so callers branching on
		// errors.Is keep covering this case. Codex r5 P2 on
		// PR #810.
		if errors.Is(err, fs.ErrNotExist) {
			return false, errors.Wrapf(ErrChecksumMismatch,
				"%s: missing on disk", cleaned)
		}
		return false, errors.Wrapf(err, "line %d", lineNum)
	}
	got, err := sha256File(joined)
	if err != nil {
		// A missing target is a partial-restore or tamper
		// signal; either way it is operationally identical to
		// a checksum mismatch for the caller. Surface as
		// ErrChecksumMismatch so `errors.Is(err,
		// ErrChecksumMismatch)` covers both cases. Codex r5 P2
		// on PR #810.
		if errors.Is(err, fs.ErrNotExist) {
			return false, errors.Wrapf(ErrChecksumMismatch,
				"%s: missing on disk", cleaned)
		}
		return false, errors.Wrapf(err, "verifying %s", cleaned)
	}
	if got != want {
		return false, errors.Wrapf(ErrChecksumMismatch, "%s: have %s want %s", cleaned, got, want)
	}
	return true, nil
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
// `<hex>  <relative-path>` (or its `\\`-prefixed escaped variant
// when the path contains `\n`/`\r`/`\\`). Returns (hex, path, ok).
// Lines with the wrong shape are reported as not-ok so the
// caller can surface a CHECKSUMS-corruption error rather than
// ingest garbage.
//
// Escape convention mirrors GNU coreutils: a leading `\\` signals
// that the rest of the path has `\n`/`\r`/`\\` written as the
// two-byte sequences `\\n`, `\\r`, `\\\\`. Codex r4 P2 on PR #810:
// without this, a filename containing `\n` would split across
// lines and a verifier would see a bogus second row.
func splitChecksumLine(line string) (string, string, bool) {
	escaped := strings.HasPrefix(line, `\`)
	if escaped {
		line = line[1:]
	}
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
	body := rest[2:]
	if !escaped {
		return hexPart, body, true
	}
	unescaped, ok := unescapeChecksumPath(body)
	if !ok {
		return "", "", false
	}
	return hexPart, unescaped, true
}

// unescapeChecksumPath reverses formatChecksumLine's `\n`/`\r`/`\\`
// substitutions. Returns (path, ok). An unrecognised `\<byte>` or
// a dangling trailing `\` is treated as malformed — the line is
// rejected at the caller via the ok=false return.
func unescapeChecksumPath(body string) (string, bool) {
	var sb strings.Builder
	sb.Grow(len(body))
	for i := 0; i < len(body); i++ {
		if body[i] != '\\' {
			sb.WriteByte(body[i])
			continue
		}
		i++
		if i >= len(body) {
			return "", false
		}
		switch body[i] {
		case 'n':
			sb.WriteByte('\n')
		case 'r':
			sb.WriteByte('\r')
		case '\\':
			sb.WriteByte('\\')
		default:
			return "", false
		}
	}
	return sb.String(), true
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
