package backup

import (
	"archive/tar"
	"bufio"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
)

const (
	ArchiveCompressionNone = "none"
	ArchiveCompressionZstd = "zstd"

	archiveDefaultDirPerm  fs.FileMode = 0o755
	archiveDefaultFilePerm fs.FileMode = 0o600
)

var (
	ErrArchiveCompressionUnsupported = errors.New("backup archive: unsupported compression")
	ErrArchivePathUnsafe             = errors.New("backup archive: unsafe path")
	ErrArchiveNonRegular             = errors.New("backup archive: non-regular file")
	ErrArchiveDestinationExists      = errors.New("backup archive: destination exists")
	ErrArchiveUnchecksummedFile      = errors.New("backup archive: file is not listed in CHECKSUMS")
)

// PackDumpTree writes root as a tar stream, optionally wrapped in zstd.
// MANIFEST.json and CHECKSUMS are verified before the archive is emitted so a
// corrupt tree is not shipped as a restore candidate.
func PackDumpTree(root string, out io.Writer, compression string) error {
	if out == nil {
		return errors.New("backup archive: output writer is nil")
	}
	if err := verifyDumpRoot(root); err != nil {
		return err
	}
	w, closeFn, err := archiveWriter(out, compression)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(w)
	if err := writeTarTree(tw, root); err != nil {
		_ = tw.Close()
		_ = closeFn()
		return err
	}
	if err := tw.Close(); err != nil {
		_ = closeFn()
		return errors.WithStack(err)
	}
	return closeFn()
}

// UnpackDumpTree extracts a tar or tar+zstd stream into a new output root,
// then verifies MANIFEST.json and CHECKSUMS. Existing output roots are refused
// so an archive cannot merge into or overwrite unrelated data.
func UnpackDumpTree(in io.Reader, outputRoot string, compression string) error {
	if in == nil {
		return errors.New("backup archive: input reader is nil")
	}
	if outputRoot == "" {
		return errors.New("backup archive: output root is required")
	}
	if _, err := os.Stat(outputRoot); err == nil {
		return errors.Wrapf(ErrArchiveDestinationExists, "%s", outputRoot)
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	r, closeFn, err := archiveReader(in, compression)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outputRoot, archiveDefaultDirPerm); err != nil {
		_ = closeFn()
		return errors.WithStack(err)
	}
	if err := readTarTree(tar.NewReader(r), outputRoot); err != nil {
		_ = closeFn()
		_ = os.RemoveAll(outputRoot)
		return err
	}
	if err := closeFn(); err != nil {
		_ = os.RemoveAll(outputRoot)
		return err
	}
	if err := verifyDumpRoot(outputRoot); err != nil {
		_ = os.RemoveAll(outputRoot)
		return err
	}
	return nil
}

func verifyDumpRoot(root string) error {
	if err := requireArchiveRegularPath(root, "MANIFEST.json"); err != nil {
		return err
	}
	if err := requireArchiveRegularPath(root, CHECKSUMSFilename); err != nil {
		return err
	}

	f, err := os.Open(filepath.Join(root, "MANIFEST.json")) //nolint:gosec // operator-supplied dump root
	if err != nil {
		return errors.WithStack(err)
	}
	_, readErr := ReadManifest(f)
	closeErr := f.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return errors.WithStack(closeErr)
	}
	if err := VerifyChecksums(root); err != nil {
		return err
	}
	return verifyArchiveHasNoUnchecksummedFiles(root)
}

func archiveWriter(out io.Writer, compression string) (io.Writer, func() error, error) {
	switch compression {
	case ArchiveCompressionNone, "":
		return out, func() error { return nil }, nil
	case ArchiveCompressionZstd:
		zw, err := zstd.NewWriter(out)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return zw, zw.Close, nil
	default:
		return nil, nil, errors.Wrapf(ErrArchiveCompressionUnsupported, "%q", compression)
	}
}

func archiveReader(in io.Reader, compression string) (io.Reader, func() error, error) {
	switch compression {
	case ArchiveCompressionNone, "":
		return in, func() error { return nil }, nil
	case ArchiveCompressionZstd:
		zr, err := zstd.NewReader(in)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return zr, func() error { zr.Close(); return nil }, nil
	default:
		return nil, nil, errors.Wrapf(ErrArchiveCompressionUnsupported, "%q", compression)
	}
}

func writeTarTree(tw *tar.Writer, root string) error {
	paths, err := collectArchivePaths(root)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := writeTarPath(tw, root, path); err != nil {
			return err
		}
	}
	return nil
}

func collectArchivePaths(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if path == root {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return errors.WithStack(err)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.Wrapf(ErrArchiveNonRegular, "%s", path)
		}
		if info.Mode().IsRegular() {
			if err := refuseArchiveHardLink(info, path); err != nil {
				return err
			}
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sort.Strings(paths)
	return paths, nil
}

func writeTarPath(tw *tar.Writer, root, path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return errors.WithStack(err)
	}
	hdr, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return errors.WithStack(err)
	}
	hdr.Name = filepath.ToSlash(rel)
	if err := tw.WriteHeader(hdr); err != nil {
		return errors.WithStack(err)
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	f, err := os.Open(path) //nolint:gosec // path came from WalkDir under root
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(tw, f)
	return errors.WithStack(err)
}

func readTarTree(tr *tar.Reader, outputRoot string) error {
	for {
		hdr, err := tr.Next()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return errors.WithStack(err)
		}
		if err := extractTarEntry(tr, outputRoot, hdr); err != nil {
			return err
		}
	}
}

func extractTarEntry(tr *tar.Reader, outputRoot string, hdr *tar.Header) error {
	rel, err := cleanArchiveRelPath(hdr.Name)
	if err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	target := filepath.Join(outputRoot, rel)
	switch hdr.Typeflag {
	case tar.TypeDir:
		return errors.WithStack(os.MkdirAll(target, archiveMode(hdr.FileInfo().Mode().Perm(), archiveDefaultDirPerm)))
	case tar.TypeReg, 0:
		return extractRegularTarEntry(tr, target, hdr)
	default:
		return errors.Wrapf(ErrArchiveNonRegular, "%s type %c", hdr.Name, hdr.Typeflag)
	}
}

func extractRegularTarEntry(tr *tar.Reader, target string, hdr *tar.Header) error {
	if err := os.MkdirAll(filepath.Dir(target), archiveDefaultDirPerm); err != nil {
		return errors.WithStack(err)
	}
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, archiveMode(hdr.FileInfo().Mode().Perm(), archiveDefaultFilePerm)) //nolint:gosec
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
		_ = f.Close()
		return errors.WithStack(err)
	}
	if err := f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func cleanArchiveRelPath(name string) (string, error) {
	name = strings.TrimPrefix(name, "./")
	if name == "" || name == "." {
		return ".", nil
	}
	if !filepath.IsLocal(name) {
		return "", errors.Wrapf(ErrArchivePathUnsafe, "%q", name)
	}
	cleaned := filepath.Clean(filepath.FromSlash(name))
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) || filepath.IsAbs(cleaned) {
		return "", errors.Wrapf(ErrArchivePathUnsafe, "%q", name)
	}
	return cleaned, nil
}

func archiveMode(mode fs.FileMode, fallback fs.FileMode) fs.FileMode {
	if mode == 0 {
		return fallback
	}
	return mode
}

func requireArchiveRegularPath(root string, rel string) error {
	path := filepath.Join(root, rel)
	info, err := os.Lstat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if !info.Mode().IsRegular() {
		return errors.Wrapf(ErrArchiveNonRegular, "%s", path)
	}
	return refuseArchiveHardLink(info, path)
}

func refuseArchiveHardLink(info os.FileInfo, path string) error {
	if err := refuseHardLink(info, path); err != nil {
		return errors.Wrapf(ErrArchiveNonRegular, "hard-linked file %s: %v", path, err)
	}
	return nil
}

func verifyArchiveHasNoUnchecksummedFiles(root string) error {
	expected, err := readChecksumManifestPaths(root)
	if err != nil {
		return err
	}
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if path == root || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return errors.WithStack(err)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return errors.WithStack(err)
		}
		rel = filepath.ToSlash(rel)
		if rel == CHECKSUMSFilename {
			return nil
		}
		if _, ok := expected[rel]; !ok {
			return errors.Wrapf(ErrArchiveUnchecksummedFile, "%s", rel)
		}
		return nil
	})
	return errors.WithStack(err)
}

func readChecksumManifestPaths(root string) (map[string]struct{}, error) {
	f, err := os.Open(filepath.Join(root, CHECKSUMSFilename)) //nolint:gosec // operator-supplied dump root
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()

	paths := make(map[string]struct{})
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, maxChecksumLineLen), maxChecksumLineLen)
	lineNum := 0
	for sc.Scan() {
		lineNum++
		line := sc.Text()
		if line == "" {
			continue
		}
		_, rel, ok := splitChecksumLine(line)
		if !ok {
			return nil, errors.Wrapf(ErrChecksumsMalformedLine, "line %d: %q", lineNum, line)
		}
		cleaned, err := validateChecksumRelPath(rel)
		if err != nil {
			return nil, errors.Wrapf(err, "line %d", lineNum)
		}
		paths[filepath.ToSlash(cleaned)] = struct{}{}
	}
	if err := sc.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return paths, nil
}
