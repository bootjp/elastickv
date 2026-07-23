package snapshotoffload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

type ObjectStore interface {
	PutObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error)
	GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error)
	HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error)
}

type PutOptions struct {
	Size        int64
	SHA256      string
	ContentType string
}

type ObjectInfo struct {
	Key  string
	Size int64
	// SHA256 is optional for metadata-only Head/Get paths; PutObject returns it
	// when the writer verified the committed content.
	SHA256               string
	ServerSideEncryption string
	SSEKMSKeyID          string
}

type LocalStore struct {
	root string
}

const localStoreDirPerm = 0o755

func NewLocalStore(root string) (*LocalStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.Wrap(ErrInvalidOptions, "local store root is required")
	}
	return &LocalStore{root: filepath.Clean(root)}, nil
}

func (s *LocalStore) PutObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
	if err := validatePutOptions(opts); err != nil {
		return ObjectInfo{}, err
	}
	finalPath, err := s.pathForKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), localStoreDirPerm); err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	tmpPath, info, err := writeLocalObjectTemp(ctx, filepath.Dir(finalPath), key, body, opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	return s.commitTempObject(key, tmpPath, finalPath, info)
}

func (s *LocalStore) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectInfo{}, errors.WithStack(err)
	}
	objectPath, err := s.pathForKey(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	info, err := s.objectInfoForPath(key, objectPath)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	file, err := os.Open(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ObjectInfo{}, errors.Wrapf(ErrObjectNotFound, "object %s", key)
		}
		return nil, ObjectInfo{}, errors.WithStack(err)
	}
	return file, info, nil
}

func (s *LocalStore) HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, false, errors.WithStack(err)
	}
	objectPath, err := s.pathForKey(key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	info, err := s.objectInfoForPath(key, objectPath)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return ObjectInfo{}, false, nil
		}
		return ObjectInfo{}, false, err
	}
	return info, true, nil
}

func (s *LocalStore) pathForKey(key string) (string, error) {
	if s == nil {
		return "", errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	normalized := normalizeObjectKey(key)
	if normalized == "" || normalized == "." || normalized == ".." || strings.HasPrefix(normalized, "../") {
		return "", errors.Wrapf(ErrInvalidOptions, "invalid object key %q", key)
	}
	return filepath.Join(s.root, filepath.FromSlash(normalized)), nil
}

func (s *LocalStore) objectInfoForPath(key, objectPath string) (ObjectInfo, error) {
	stat, err := os.Stat(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, errors.Wrapf(ErrObjectNotFound, "object %s", key)
		}
		return ObjectInfo{}, errors.WithStack(err)
	}
	if !stat.Mode().IsRegular() {
		return ObjectInfo{}, errors.Wrapf(ErrInvalidOptions, "object %s is not a regular file", key)
	}
	return ObjectInfo{
		Key:  normalizeObjectKey(key),
		Size: stat.Size(),
	}, nil
}

func (s *LocalStore) hashedObjectInfoForPath(key, objectPath string) (ObjectInfo, error) {
	file, err := os.Open(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, errors.Wrapf(ErrObjectNotFound, "object %s", key)
		}
		return ObjectInfo{}, errors.WithStack(err)
	}
	defer func() { _ = file.Close() }()
	stat, err := file.Stat()
	if err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	if !stat.Mode().IsRegular() {
		return ObjectInfo{}, errors.Wrapf(ErrInvalidOptions, "object %s is not a regular file", key)
	}
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	return ObjectInfo{
		Key:    normalizeObjectKey(key),
		Size:   stat.Size(),
		SHA256: hex.EncodeToString(sum.Sum(nil)),
	}, nil
}

func writeLocalObjectTemp(
	ctx context.Context,
	dir string,
	key string,
	body io.Reader,
	opts PutOptions,
) (string, ObjectInfo, error) {
	tmp, err := os.CreateTemp(dir, ".put-*")
	if err != nil {
		return "", ObjectInfo{}, errors.WithStack(err)
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, sum), contextReader{ctx: ctx, reader: body})
	if err != nil {
		return "", ObjectInfo{}, errors.WithStack(err)
	}
	gotSHA := hex.EncodeToString(sum.Sum(nil))
	if n != opts.Size {
		return "", ObjectInfo{}, errors.Wrapf(ErrIntegrity, "object %s wrote %d bytes, expected %d", key, n, opts.Size)
	}
	if gotSHA != opts.SHA256 {
		return "", ObjectInfo{}, errors.Wrapf(ErrIntegrity, "object %s wrote sha256 %s, expected %s", key, gotSHA, opts.SHA256)
	}
	if err := tmp.Sync(); err != nil {
		return "", ObjectInfo{}, errors.WithStack(err)
	}
	if err := tmp.Close(); err != nil {
		return "", ObjectInfo{}, errors.WithStack(err)
	}
	keep = true
	return tmpPath, ObjectInfo{Key: normalizeObjectKey(key), Size: opts.Size, SHA256: opts.SHA256}, nil
}

func (s *LocalStore) commitTempObject(key, tmpPath, finalPath string, expected ObjectInfo) (ObjectInfo, error) {
	if err := os.Link(tmpPath, finalPath); err != nil {
		if !os.IsExist(err) {
			return ObjectInfo{}, errors.WithStack(err)
		}
		return s.verifyExistingObject(key, finalPath, expected)
	}
	finalDir := filepath.Dir(finalPath)
	if err := syncDir(finalDir); err != nil {
		if removeErr := os.Remove(finalPath); removeErr != nil && !os.IsNotExist(removeErr) {
			err = errors.CombineErrors(err, errors.WithStack(removeErr))
		}
		return ObjectInfo{}, errors.WithStack(err)
	}
	return expected, nil
}

func (s *LocalStore) verifyExistingObject(key, finalPath string, expected ObjectInfo) (ObjectInfo, error) {
	info, err := s.hashedObjectInfoForPath(key, finalPath)
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.Size == expected.Size && info.SHA256 == expected.SHA256 {
		return info, nil
	}
	return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "object %s already exists with different content", key)
}

func validatePutOptions(opts PutOptions) error {
	switch {
	case opts.Size < 0:
		return errors.Wrap(ErrInvalidOptions, "object size must be >= 0")
	case !isSHA256Hex(opts.SHA256):
		return errors.Wrap(ErrInvalidOptions, "object sha256 must be 64 lowercase hex characters")
	default:
		return nil
	}
}

func normalizeObjectKey(key string) string {
	key = strings.TrimSpace(key)
	key = strings.TrimPrefix(key, "/")
	return path.Clean(key)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return 0, errors.WithStack(err)
		}
	}
	n, err := r.reader.Read(p)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, err //nolint:wrapcheck // io.Reader must return io.EOF unwrapped so io.Copy treats it as normal completion.
		}
		return n, errors.WithStack(err)
	}
	if r.ctx != nil {
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			return n, errors.WithStack(ctxErr)
		}
	}
	return n, nil
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if w.ctx != nil {
		if err := w.ctx.Err(); err != nil {
			return 0, errors.WithStack(err)
		}
	}
	n, err := w.writer.Write(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	if w.ctx != nil {
		if ctxErr := w.ctx.Err(); ctxErr != nil {
			return n, errors.WithStack(ctxErr)
		}
	}
	return n, nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	return errors.WithStack(f.Sync())
}

func hexSHA256Bytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func isSHA256Hex(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
