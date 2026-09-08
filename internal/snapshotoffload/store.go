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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

type ObjectStore interface {
	PutObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error)
	GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error)
	HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error)
}

// ObjectRefresher updates an already-verified object's store metadata while
// preserving its content. Publish uses this when it reuses a content-addressed
// payload, so retention's payload grace window applies to the new publish too.
type ObjectRefresher interface {
	RefreshObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error)
}

// ObjectRef is one object seen by ListObjects.
//
// UpdatedAt is the object store's own last-modified time, not a value
// carried inside the object. Retention uses it only for the phase-2
// payload grace period, where the question is "has this object been
// sitting unreferenced long enough", which is a property of the store
// rather than of the snapshot.
type ObjectRef struct {
	Key       string
	Size      int64
	UpdatedAt time.Time
	// ETag is the store's opaque version token for the exact bytes
	// observed. It is the precondition DeleteObjectIfUnmodified uses
	// to make reclamation a compare-and-delete rather than a
	// check-then-delete. Empty when the store cannot supply one, in
	// which case the size + mtime pair is the fallback precondition.
	ETag string
}

// DeletePrecondition is the state a caller observed for an object,
// used to make deletion conditional on the object not having changed
// since.
type DeletePrecondition struct {
	ETag      string
	Size      int64
	UpdatedAt time.Time
}

// PreconditionFor returns the precondition describing ref.
func PreconditionFor(ref ObjectRef) DeletePrecondition {
	return DeletePrecondition{ETag: ref.ETag, Size: ref.Size, UpdatedAt: ref.UpdatedAt}
}

// RetentionStore is an ObjectStore that also supports the listing and
// deletion that retention/GC needs (design §5).
//
// It is a separate interface rather than extra methods on ObjectStore
// so the publish and restore paths keep working against a store that
// can only put/get/head, while GC is a compile-time error to construct
// over such a store. A silently-no-op GC would be far worse: retention
// would appear configured while the bucket grew without bound.
//
// ListObjects is all-or-error by contract: it MUST return every object
// under prefix or a non-nil error. §5 makes no-deletes-on-partial-scan
// a safety property, and a lister that silently truncated a page would
// make live payloads look unreferenced.
type RetentionStore interface {
	ObjectStore
	ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error)
	DeleteObject(ctx context.Context, key string) error

	// DeleteObjectIfUnmodified deletes key only if it still matches
	// cond, and returns ErrObjectModified otherwise.
	//
	// This is what makes payload reclamation safe against a
	// concurrent publish. The grace window alone is not enough: a
	// publisher that reuses a content-addressed payload refreshes the
	// object to restart its grace, and an unconditional delete can
	// still land between GC observing the old state and the publisher
	// committing its manifest — leaving a committed manifest pointing
	// at bytes that no longer exist. Making the delete conditional on
	// the exact state GC validated turns that race into a skip.
	DeleteObjectIfUnmodified(ctx context.Context, key string, cond DeletePrecondition) error
}

var _ RetentionStore = (*LocalStore)(nil)

// ListObjects walks the local root below prefix. Directories and
// irregular files are skipped; the returned keys are slash-separated
// and relative to the store root, matching the keys PutObject accepts.
func (s *LocalStore) ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error) {
	if s == nil {
		return nil, errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	root, err := s.listRootForPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var refs []ObjectRef
	walk := func(walkPath string, entry os.DirEntry, err error) error {
		if err != nil {
			return s.walkEntryError(walkPath, root, err)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.WithStack(ctxErr)
		}
		ref, ok, refErr := s.objectRefForWalkEntry(walkPath, entry)
		if refErr != nil {
			return refErr
		}
		if ok {
			refs = append(refs, ref)
		}
		return nil
	}
	if err := filepath.WalkDir(root, walk); err != nil {
		return nil, errors.Wrapf(err, "list objects under %q", prefix)
	}
	return refs, nil
}

// walkEntryError translates a WalkDir error. A missing root is an
// empty listing, not a failure: a bucket that has never been published
// to has no group tree yet, and GC over it must be a clean no-op.
func (s *LocalStore) walkEntryError(walkPath, root string, err error) error {
	if os.IsNotExist(err) && walkPath == root {
		return filepath.SkipAll
	}
	return errors.WithStack(err)
}

// objectRefForWalkEntry converts one walk entry into an ObjectRef,
// reporting ok=false for entries that are not objects (directories,
// sockets, symlinks, and the in-progress ".put-*" temp files
// PutObject creates).
func (s *LocalStore) objectRefForWalkEntry(walkPath string, entry os.DirEntry) (ObjectRef, bool, error) {
	if entry.IsDir() || !entry.Type().IsRegular() {
		return ObjectRef{}, false, nil
	}
	if strings.HasPrefix(entry.Name(), ".put-") {
		return ObjectRef{}, false, nil
	}
	info, err := entry.Info()
	if err != nil {
		return ObjectRef{}, false, errors.WithStack(err)
	}
	rel, err := filepath.Rel(s.root, walkPath)
	if err != nil {
		return ObjectRef{}, false, errors.WithStack(err)
	}
	return ObjectRef{
		Key:       filepath.ToSlash(rel),
		Size:      info.Size(),
		UpdatedAt: info.ModTime(),
	}, true, nil
}

// listRootForPrefix resolves the directory a listing should walk.
//
// cleanObjectPrefix preserves ".." segments, so joining it blindly
// would let a traversing prefix enumerate an ancestor or sibling tree
// and leak those files' names, sizes and timestamps — while every
// other local-store operation rejects the equivalent key through
// pathForKey.
func (s *LocalStore) listRootForPrefix(prefix string) (string, error) {
	cleaned := cleanObjectPrefix(prefix)
	if cleaned == "." {
		return s.root, nil
	}
	// cleanObjectPrefix uses path (slash) semantics, but filepath.Join
	// below interprets the platform separator — so on Windows a
	// prefix like `..\sibling` would survive a slash-only check and
	// then escape the root. Reject the native form too.
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.ContainsRune(cleaned, '\\') {
		return "", errors.Wrapf(ErrInvalidOptions, "invalid object prefix %q", prefix)
	}
	return filepath.Join(s.root, filepath.FromSlash(cleaned)), nil
}

// DeleteObjectIfUnmodified removes key only when it still matches
// cond. See the RetentionStore contract for why the condition matters.
func (s *LocalStore) DeleteObjectIfUnmodified(ctx context.Context, key string, cond DeletePrecondition) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	objectPath, err := s.pathForKey(key)
	if err != nil {
		return err
	}

	s.deleteMu.Lock()
	defer s.deleteMu.Unlock()

	stat, err := os.Stat(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Already gone: the caller's intent is satisfied.
			return nil
		}
		return errors.Wrapf(err, "stat object %s", key)
	}
	if stat.Size() != cond.Size || !stat.ModTime().Equal(cond.UpdatedAt) {
		return errors.Wrapf(ErrObjectModified,
			"object %s changed since it was validated for deletion", key)
	}
	if err := os.Remove(objectPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "delete object %s", key)
	}
	// Persist the unlink before reporting success. Without the
	// directory sync a crash can resurrect an object GC already
	// counted as reclaimed.
	return syncDir(filepath.Dir(objectPath))
}

// DeleteObject removes one object. A already-absent object is not an
// error: GC must be idempotent across retries and a concurrent
// reclamation of the same key is a benign race.
func (s *LocalStore) DeleteObject(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	objectPath, err := s.pathForKey(key)
	if err != nil {
		return err
	}
	if err := os.Remove(objectPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "delete object %s", key)
	}
	// Persist the unlink before reporting success. Without the
	// directory sync a crash can resurrect an object GC already
	// counted as reclaimed.
	return syncDir(filepath.Dir(objectPath))
}

type PutOptions struct {
	Size        int64
	SHA256      string
	ContentType string
}

type ObjectInfo struct {
	Key       string
	Size      int64
	UpdatedAt time.Time
	// SHA256 is optional for metadata-only Head/Get paths; PutObject returns it
	// when the writer verified the committed content.
	SHA256               string
	ServerSideEncryption string
	SSEKMSKeyID          string
	// ETag is the store's opaque version token, when it supplies one.
	// Retention uses it as the compare-and-delete precondition.
	ETag string
}

type LocalStore struct {
	root string
	// deleteMu serialises conditional deletes against refreshes made
	// through this same store value. Within one process that makes
	// compare-and-delete atomic. It cannot coordinate two processes
	// sharing a directory — POSIX has no compare-and-unlink — so a
	// cross-process local deployment keeps the residual race that the
	// S3 store closes with If-Match. LocalStore is the dev/test and
	// single-writer store; production offload targets S3.
	deleteMu sync.Mutex
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

func (s *LocalStore) RefreshObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
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
	// Held across the replace so an in-process reclamation of this
	// same payload cannot land between a validating stat and the
	// unlink. See DeleteObjectIfUnmodified.
	s.deleteMu.Lock()
	defer s.deleteMu.Unlock()
	return s.replaceObject(key, tmpPath, finalPath, info)
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
		Key:       normalizeObjectKey(key),
		Size:      stat.Size(),
		UpdatedAt: stat.ModTime(),
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
		Key:       normalizeObjectKey(key),
		Size:      stat.Size(),
		UpdatedAt: stat.ModTime(),
		SHA256:    hex.EncodeToString(sum.Sum(nil)),
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
	return s.verifyExistingObject(key, finalPath, expected)
}

func (s *LocalStore) replaceObject(key, tmpPath, finalPath string, expected ObjectInfo) (ObjectInfo, error) {
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	return s.verifyExistingObject(key, finalPath, expected)
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
