package snapshotoffload

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	iofs "io/fs"
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

// ObjectRefresher rewrites an already-verified object while preserving its
// canonical content. Publish uses this to make reuse of a small manifest
// visible to retention; content-addressed payloads are coordinated by claims
// and are not re-uploaded merely to advance metadata.
type ObjectRefresher interface {
	RefreshObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error)
}

// ObjectClaim is an exclusive claim over one object key. Publishers hold a
// payload claim until its manifest is committed; retention holds the same
// claim while it validates and deletes that payload. The shared claim closes
// the final same-content race that an ETag precondition cannot detect.
type ObjectClaim interface {
	Release(ctx context.Context) error
}

// ObjectClaimStore can acquire a cross-process claim for one object key.
// Implementations must use storage-visible conditional operations rather than
// a process-local mutex: publication and GC may run on different nodes.
type ObjectClaimStore interface {
	AcquireObjectClaim(ctx context.Context, key string) (ObjectClaim, error)
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
	ObjectClaimStore
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
var _ ObjectClaimStore = (*LocalStore)(nil)

// ListObjects walks the pinned local root below prefix. Directories and
// irregular files are skipped, while symlinks fail the all-or-error listing
// contract closed. Returned keys are slash-separated and relative to the
// store root, matching the keys PutObject accepts.
func (s *LocalStore) ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error) {
	if s == nil {
		return nil, errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	listRoot, err := s.listRootForPrefix(prefix)
	if err != nil {
		return nil, err
	}
	root, err := s.pinnedRoot()
	if err != nil {
		return nil, err
	}
	if err := rejectSymlinkComponents(root, listRoot); err != nil {
		return nil, err
	}
	return s.listObjectsFromRoot(ctx, root, listRoot, prefix)
}

func (s *LocalStore) listObjectsFromRoot(
	ctx context.Context, root *os.Root, listRoot, prefix string,
) ([]ObjectRef, error) {
	var refs []ObjectRef
	walk := func(walkPath string, entry os.DirEntry, err error) error {
		if err != nil {
			return s.walkEntryError(walkPath, listRoot, err)
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
	if err := iofs.WalkDir(root.FS(), listRoot, walk); err != nil {
		return nil, errors.Wrapf(err, "list objects under %q", prefix)
	}
	return refs, nil
}

// walkEntryError translates a WalkDir error. A missing root is an
// empty listing, not a failure: a bucket that has never been published
// to has no group tree yet, and GC over it must be a clean no-op.
func (s *LocalStore) walkEntryError(walkPath, root string, err error) error {
	if os.IsNotExist(err) && walkPath == root {
		return iofs.SkipAll
	}
	return errors.WithStack(err)
}

// objectRefForWalkEntry converts one walk entry into an ObjectRef,
// reporting ok=false for entries that are not objects (directories,
// sockets, and the in-progress ".put-*" temp files PutObject creates).
// Symlinks fail the complete-listing contract closed instead of disappearing
// from GC's live-set scan.
func (s *LocalStore) objectRefForWalkEntry(walkPath string, entry os.DirEntry) (ObjectRef, bool, error) {
	if entry.Type()&os.ModeSymlink != 0 {
		return ObjectRef{}, false, errors.Wrapf(ErrIntegrity,
			"object listing encountered symlink %s", walkPath)
	}
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
	return ObjectRef{
		Key:       path.Clean(filepath.ToSlash(walkPath)),
		Size:      info.Size(),
		UpdatedAt: info.ModTime(),
	}, true, nil
}

// listRootForPrefix resolves the directory a listing should walk.
//
// cleanObjectPrefix preserves ".." segments, so passing it blindly to the
// root-relative walker would attempt to escape the store. Every other
// local-store operation rejects the equivalent key through pathForKey.
func (s *LocalStore) listRootForPrefix(prefix string) (string, error) {
	cleaned := cleanObjectPrefix(prefix)
	if cleaned == "." {
		return ".", nil
	}
	// cleanObjectPrefix uses path (slash) semantics, while os.Root interprets
	// the platform separator too. On Windows a prefix like `..\sibling`
	// would survive a slash-only check, so reject the native form as well.
	if !objectPathSegmentIsSafe(cleaned) {
		return "", errors.Wrapf(ErrInvalidOptions, "invalid object prefix %q", prefix)
	}
	return cleaned, nil
}

// DeleteObjectIfUnmodified removes key only when it still matches
// cond. See the RetentionStore contract for why the condition matters.
func (s *LocalStore) DeleteObjectIfUnmodified(ctx context.Context, key string, cond DeletePrecondition) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	relPath, err := s.relPathForKey(key)
	if err != nil {
		return err
	}
	root, err := s.pinnedRoot()
	if err != nil {
		return err
	}
	if err := rejectSymlinkComponents(root, relPath); err != nil {
		return err
	}

	s.deleteMu.Lock()
	defer s.deleteMu.Unlock()

	exists, err := validateDeletePreconditionWithinRoot(root, key, relPath, cond)
	if err != nil || !exists {
		return err
	}
	removed, err := removeWithinRoot(root, relPath)
	if err != nil {
		return errors.Wrapf(err, "delete object %s", key)
	}
	if !removed {
		return nil
	}
	// Persist the unlink before reporting success. Without the
	// directory sync a crash can resurrect an object GC already
	// counted as reclaimed.
	return syncDirWithinRoot(root, filepath.Dir(relPath))
}

func validateDeletePreconditionWithinRoot(
	root *os.Root, key, relPath string, cond DeletePrecondition,
) (bool, error) {
	stat, err := root.Stat(relPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Already gone: the caller's intent is satisfied.
			return false, nil
		}
		return false, errors.Wrapf(err, "stat object %s", key)
	}
	if stat.Size() != cond.Size || !stat.ModTime().Equal(cond.UpdatedAt) {
		return false, errors.Wrapf(ErrObjectModified,
			"object %s changed since it was validated for deletion", key)
	}
	return true, nil
}

// DeleteObject removes one object. A already-absent object is not an
// error: GC must be idempotent across retries and a concurrent
// reclamation of the same key is a benign race.
func (s *LocalStore) DeleteObject(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	relPath, err := s.relPathForKey(key)
	if err != nil {
		return err
	}
	root, err := s.pinnedRoot()
	if err != nil {
		return err
	}
	if err := rejectSymlinkComponents(root, relPath); err != nil {
		return err
	}
	removed, err := removeWithinRoot(root, relPath)
	if err != nil {
		return errors.Wrapf(err, "delete object %s", key)
	}
	if !removed {
		return nil
	}
	// Persist the unlink before reporting success. Without the
	// directory sync a crash can resurrect an object GC already
	// counted as reclaimed.
	return syncDirWithinRoot(root, filepath.Dir(relPath))
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

	// rootDir is the configured directory, opened by NewLocalStore and kept
	// for the life of the store. See pinnedRoot.
	rootDir *os.Root
}

const (
	localStoreDirPerm            = 0o755
	localStoreFilePerm           = 0o600
	localStoreTempTokenBytes     = 16
	localStoreTempCreateAttempts = 10
)

func NewLocalStore(root string) (*LocalStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.Wrap(ErrInvalidOptions, "local store root is required")
	}
	cleaned := filepath.Clean(root)
	if err := os.MkdirAll(cleaned, localStoreDirPerm); err != nil {
		return nil, errors.Wrapf(err, "create local store root %s", cleaned)
	}
	rootDir, err := os.OpenRoot(cleaned)
	if err != nil {
		return nil, errors.Wrapf(err, "open local store root %s", cleaned)
	}
	return &LocalStore{root: cleaned, rootDir: rootDir}, nil
}

func (s *LocalStore) PutObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
	if err := validatePutOptions(opts); err != nil {
		return ObjectInfo{}, err
	}
	relPath, err := s.relPathForKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	root, err := s.pinnedRoot()
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := rejectSymlinkComponents(root, relPath); err != nil {
		return ObjectInfo{}, err
	}
	if err := root.MkdirAll(filepath.Dir(relPath), localStoreDirPerm); err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	tmpPath, info, err := writeLocalObjectTempWithinRoot(ctx, root, key, body, opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer func() { _ = root.Remove(tmpPath) }()
	return commitTempObjectWithinRoot(root, key, tmpPath, relPath, info)
}

func (s *LocalStore) RefreshObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
	if err := validatePutOptions(opts); err != nil {
		return ObjectInfo{}, err
	}
	relPath, err := s.relPathForKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	root, err := s.pinnedRoot()
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := rejectSymlinkComponents(root, relPath); err != nil {
		return ObjectInfo{}, err
	}
	tmpPath, info, err := writeLocalObjectTempWithinRoot(ctx, root, key, body, opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer func() { _ = root.Remove(tmpPath) }()
	// Held across the replace so an in-process reclamation of this
	// same payload cannot land between a validating stat and the
	// unlink. See DeleteObjectIfUnmodified.
	s.deleteMu.Lock()
	defer s.deleteMu.Unlock()
	return replaceObjectWithinRoot(root, key, tmpPath, relPath, info)
}

func (s *LocalStore) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectInfo{}, errors.WithStack(err)
	}
	relPath, err := s.relPathForKey(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	root, err := s.pinnedRoot()
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if err := rejectSymlinkComponents(root, relPath); err != nil {
		return nil, ObjectInfo{}, err
	}
	file, err := root.Open(relPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ObjectInfo{}, errors.Wrapf(ErrObjectNotFound, "object %s", key)
		}
		return nil, ObjectInfo{}, errors.WithStack(err)
	}
	info, err := objectInfoFromFile(key, file)
	if err != nil {
		_ = file.Close()
		return nil, ObjectInfo{}, err
	}
	return file, info, nil
}

func (s *LocalStore) HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, false, errors.WithStack(err)
	}
	relPath, err := s.relPathForKey(key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	root, err := s.pinnedRoot()
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if err := rejectSymlinkComponents(root, relPath); err != nil {
		return ObjectInfo{}, false, err
	}
	stat, err := root.Stat(relPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, false, nil
		}
		return ObjectInfo{}, false, errors.WithStack(err)
	}
	info, err := objectInfoFromStat(key, stat)
	return info, err == nil, err
}

func (s *LocalStore) pathForKey(key string) (string, error) {
	if s == nil {
		return "", errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	normalized := normalizeObjectKey(key)
	if !objectPathSegmentIsSafe(normalized) {
		return "", errors.Wrapf(ErrInvalidOptions, "invalid object key %q", key)
	}
	joined := filepath.Join(s.root, filepath.FromSlash(normalized))
	if !objectPathWithinRoot(s.root, joined) {
		return "", errors.Wrapf(ErrInvalidOptions, "object key %q resolves outside the store root", key)
	}
	return joined, nil
}

// objectPathSegmentIsSafe reports whether a normalized key or prefix
// stays inside the store root once joined.
//
// The backslash check is not redundant on the slash-only forms:
// normalizeObjectKey uses path (slash) semantics, so `..\victim`
// survives every "/"-based test, and filepath.Join then interprets the
// backslash on Windows and resolves outside the root. Both the key
// path (Get/Head/Put/Delete) and the prefix path (ListObjects) route
// through this, so a fix here cannot be applied to one and missed on
// the other.
func objectPathSegmentIsSafe(normalized string) bool {
	switch {
	case normalized == "", normalized == ".", normalized == "..":
		return false
	case strings.HasPrefix(normalized, "../"):
		return false
	case strings.ContainsRune(normalized, '\\'):
		return false
	case strings.HasPrefix(normalized, "/"):
		// normalizeObjectKey trims ONE leading slash, so `//victim`
		// arrives here as `/victim`: relative by none of the checks
		// above, but rooted. filepath.Join("C:", "/victim") resolves to
		// `C:\victim` against a drive-relative root, outside the store.
		// A rooted key is never legitimate -- object keys are relative
		// to the root by definition -- so reject rather than re-trim.
		return false
	case volumeQualified(normalized):
		return false
	default:
		return true
	}
}

// volumeQualified reports whether the first path segment carries a
// Windows volume, as in `C:` or `C:foo`.
//
// filepath.VolumeName is deliberately not used: it returns "" on
// non-Windows, so a test running on Linux or macOS would pass against a
// key that escapes on Windows. The check is spelled out so it behaves
// identically on every platform.
func volumeQualified(normalized string) bool {
	first := normalized
	if idx := strings.IndexByte(first, '/'); idx >= 0 {
		first = first[:idx]
	}
	idx := strings.IndexByte(first, ':')
	if idx < 0 {
		return false
	}
	// A single letter before the colon is a drive designator. Anything
	// else containing a colon is still refused below by the caller's
	// containment check, but keys like `a:b` are not volume-qualified.
	return idx == 1 && isASCIILetter(first[0])
}

func isASCIILetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// removeWithinRoot unlinks relPath RELATIVE TO the store root, through a
// descriptor that cannot escape it.
//
// objectPathWithinRoot is lexical, so it cannot see a symlink: if any ancestor
// under the root is a symlink -- or is swapped for one between the listing and
// the delete -- os.Remove follows it and unlinks a file outside the configured
// root. The character checks cannot close that, because the path they
// validated is still the path being passed; it is the RESOLUTION that differs.
//
// os.Root resolves every component against the opened root descriptor and
// refuses to traverse out of it, so the check and the operation can no longer
// disagree.
func removeWithinRoot(root *os.Root, relPath string) (bool, error) {
	if err := root.Remove(relPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "remove %s within store root", relPath)
	}
	return true, nil
}

// pinnedRoot returns the descriptor NewLocalStore opened for the configured
// directory. Opening it during construction is essential: delaying the open
// until the first operation would let a rename-and-symlink swap redirect that
// first operation before the descriptor was pinned.
//
// Reopening per operation re-resolves a MUTABLE PATHNAME. os.OpenRoot follows
// symlinks in its own argument, so a process that can write the root's PARENT
// can rename the root away and leave a symlink in its place between two
// operations; the next open then anchors the descriptor outside the configured
// directory, and a descendant-relative Remove unlinks an external file at the
// corresponding key. That is a different hole from a descendant symlink, which
// os.Root already refuses to traverse -- here the escape is in the root
// argument itself, before any resolution the descriptor governs.
//
// Holding the descriptor means every later operation resolves against the
// directory this store was configured with, whatever the pathname comes to
// point at afterwards. The descriptor lives as long as the store, which is the
// process: that is the point of pinning, not a leak.
func (s *LocalStore) pinnedRoot() (*os.Root, error) {
	if s == nil || s.rootDir == nil {
		return nil, errors.Wrap(ErrInvalidOptions, "object store root is required")
	}
	return s.rootDir, nil
}

// relPathForKey is pathForKey's root-relative half, for the
// descriptor-relative operations that resolve against the root themselves.
func (s *LocalStore) relPathForKey(key string) (string, error) {
	full, err := s.pathForKey(key)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(filepath.Clean(s.root), full)
	if err != nil {
		return "", errors.Wrapf(ErrInvalidOptions, "object key %q is not under the store root", key)
	}
	return rel, nil
}

func rejectSymlinkComponents(root *os.Root, relPath string) error {
	cleaned := filepath.Clean(relPath)
	if cleaned == "." {
		return nil
	}
	current := ""
	for _, component := range strings.Split(cleaned, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := root.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return errors.Wrapf(err, "inspect object path component %s", current)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.Wrapf(ErrIntegrity, "object path component %s is a symlink", current)
		}
	}
	return nil
}

// objectPathWithinRoot reports whether joined actually resolves inside
// root.
//
// This is the backstop for the character-level checks above: those
// enumerate the escapes we know about, and this one states the property
// we actually need. filepath.Rel answers it using the platform's own
// separator and volume rules, so an escape neither check anticipated
// still fails here instead of reaching the filesystem.
//
// It is deliberately unreachable today: every escape currently known is
// refused earlier by objectPathSegmentIsSafe, so no input reaches the
// store and trips this instead. That is the intended relationship
// between the two layers, not a missing case -- a test that exercised
// this through pathForKey would mean the character checks had a hole.
// TestObjectPathWithinRootIsTheBackstop therefore covers the function
// directly; if it ever starts firing in production, the character
// checks need a new case rather than this one being relaxed.
func objectPathWithinRoot(root, joined string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(joined))
	if err != nil {
		return false
	}
	if rel == "." || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func objectInfoFromStat(key string, stat os.FileInfo) (ObjectInfo, error) {
	if !stat.Mode().IsRegular() {
		return ObjectInfo{}, errors.Wrapf(ErrInvalidOptions, "object %s is not a regular file", key)
	}
	return ObjectInfo{
		Key:       normalizeObjectKey(key),
		Size:      stat.Size(),
		UpdatedAt: stat.ModTime(),
	}, nil
}

func objectInfoFromFile(key string, file *os.File) (ObjectInfo, error) {
	stat, err := file.Stat()
	if err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	return objectInfoFromStat(key, stat)
}

func hashedObjectInfoFromFile(key string, file *os.File) (ObjectInfo, error) {
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

func writeLocalObjectTempWithinRoot(
	ctx context.Context,
	root *os.Root,
	key string,
	body io.Reader,
	opts PutOptions,
) (string, ObjectInfo, error) {
	tmpPath, tmp, err := createLocalTempWithinRoot(root)
	if err != nil {
		return "", ObjectInfo{}, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = tmp.Close()
			_ = root.Remove(tmpPath)
		}
	}()
	info, err := writeLocalObjectContents(ctx, tmp, key, body, opts)
	if err != nil {
		return "", ObjectInfo{}, err
	}
	if err := tmp.Close(); err != nil {
		return "", ObjectInfo{}, errors.WithStack(err)
	}
	keep = true
	return tmpPath, info, nil
}

func createLocalTempWithinRoot(root *os.Root) (string, *os.File, error) {
	for range localStoreTempCreateAttempts {
		var token [localStoreTempTokenBytes]byte
		if _, err := rand.Read(token[:]); err != nil {
			return "", nil, errors.Wrap(err, "generate local object temp name")
		}
		name := ".put-" + hex.EncodeToString(token[:])
		file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, localStoreFilePerm)
		if err == nil {
			return name, file, nil
		}
		if !os.IsExist(err) {
			return "", nil, errors.Wrap(err, "create local object temp file")
		}
	}
	return "", nil, errors.Wrap(ErrObjectConflict, "exhausted local object temp names")
}

func writeLocalObjectContents(
	ctx context.Context,
	tmp *os.File,
	key string,
	body io.Reader,
	opts PutOptions,
) (ObjectInfo, error) {
	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, sum), contextReader{ctx: ctx, reader: body})
	if err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	gotSHA := hex.EncodeToString(sum.Sum(nil))
	if n != opts.Size {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "object %s wrote %d bytes, expected %d", key, n, opts.Size)
	}
	if gotSHA != opts.SHA256 {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "object %s wrote sha256 %s, expected %s", key, gotSHA, opts.SHA256)
	}
	if err := tmp.Sync(); err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	return ObjectInfo{Key: normalizeObjectKey(key), Size: opts.Size, SHA256: opts.SHA256}, nil
}

func commitTempObjectWithinRoot(
	root *os.Root,
	key string,
	tmpPath string,
	finalPath string,
	expected ObjectInfo,
) (ObjectInfo, error) {
	if err := root.Link(tmpPath, finalPath); err != nil {
		if !os.IsExist(err) {
			return ObjectInfo{}, errors.WithStack(err)
		}
		return verifyExistingObjectWithinRoot(root, key, finalPath, expected)
	}
	finalDir := filepath.Dir(finalPath)
	if err := syncDirWithinRoot(root, finalDir); err != nil {
		if removeErr := root.Remove(finalPath); removeErr != nil && !os.IsNotExist(removeErr) {
			err = errors.CombineErrors(err, errors.WithStack(removeErr))
		}
		return ObjectInfo{}, errors.WithStack(err)
	}
	return verifyExistingObjectWithinRoot(root, key, finalPath, expected)
}

func replaceObjectWithinRoot(
	root *os.Root,
	key string,
	tmpPath string,
	finalPath string,
	expected ObjectInfo,
) (ObjectInfo, error) {
	if err := root.Rename(tmpPath, finalPath); err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	if err := syncDirWithinRoot(root, filepath.Dir(finalPath)); err != nil {
		return ObjectInfo{}, err
	}
	info, err := hashedObjectInfoWithinRoot(root, key, finalPath)
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.Size == expected.Size && info.SHA256 == expected.SHA256 {
		return info, nil
	}
	return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "object %s changed during refresh", key)
}

func syncDirWithinRoot(root *os.Root, dir string) error {
	file, err := root.Open(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = file.Close() }()
	if err := file.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func hashedObjectInfoWithinRoot(root *os.Root, key, objectPath string) (ObjectInfo, error) {
	file, err := root.Open(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, errors.Wrapf(ErrObjectNotFound, "object %s", key)
		}
		return ObjectInfo{}, errors.WithStack(err)
	}
	defer func() { _ = file.Close() }()
	return hashedObjectInfoFromFile(key, file)
}

func verifyExistingObjectWithinRoot(
	root *os.Root, key, finalPath string, expected ObjectInfo,
) (ObjectInfo, error) {
	info, err := hashedObjectInfoWithinRoot(root, key, finalPath)
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
