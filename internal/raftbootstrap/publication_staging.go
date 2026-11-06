package raftbootstrap

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

type publicationStaging struct {
	data, temporary         string
	dataInfo, temporaryInfo os.FileInfo
}

// NewStoreWithStagingDirectory selects an existing operator-owned staging
// directory outside dataDir. Caller must retain its offline process fence and
// directory ownership for the Store's lifetime. Neither directory is created.
// Only temporary placement changes: publication validation, file sync, atomic
// rename, destination directory sync and error semantics remain unchanged.
//
// A cross-filesystem rename fails closed; there is no copy fallback. A process
// crash can leave staged files outside the source image. They are not ignored
// or treated as publication evidence, and this constructor never removes them.
func NewStoreWithStagingDirectory(dataDir, stagingDir string) (*Store, error) {
	data, dataInfo, err := publicationRealDirectory(dataDir)
	if err != nil {
		return nil, err
	}
	temporary, temporaryInfo, err := publicationRealDirectory(stagingDir)
	if err != nil {
		return nil, err
	}
	if os.SameFile(dataInfo, temporaryInfo) || publicationDirectoriesOverlap(data, temporary) {
		return nil, errors.Wrap(ErrInvalid, "publication staging must be separate from the data tree")
	}
	staging := &publicationStaging{data: data, temporary: temporary, dataInfo: dataInfo, temporaryInfo: temporaryInfo}
	store := NewStore(data)
	store.staging = staging
	store.ops.rename = staging.rename
	return store, nil
}

func publicationRealDirectory(path string) (string, os.FileInfo, error) {
	if path == "" {
		return "", nil, errors.Wrap(ErrInvalid, "missing publication directory")
	}
	path = filepath.Clean(path)
	info, err := publicationDirectoryInfo(path)
	if err != nil {
		return "", nil, errors.WithStack(err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, errors.Wrap(ErrInvalid, "publication requires a real nonsymlink directory")
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", nil, errors.WithStack(err)
	}
	absolute, err := filepath.Abs(real)
	return absolute, info, errors.WithStack(err)
}

func publicationDirectoriesOverlap(a, b string) bool {
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err != nil || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
			return true
		}
	}
	return false
}

func (s *Store) createPublicationTemp(name string) (durableFile, error) {
	dir := s.dir
	if s.staging != nil {
		if err := s.staging.validate(); err != nil {
			return nil, err
		}
		dir = s.staging.temporary
	}
	return s.ops.createTemp(dir, "."+name+"-*.tmp")
}

func publicationDirectoryInfo(path string) (_ os.FileInfo, err error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(root.Close())) }()
	info, err := root.Lstat(filepath.Base(path))
	return info, errors.WithStack(err)
}

func removePublicationTemp(path string) (err error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(root.Close())) }()
	return errors.WithStack(root.Remove(filepath.Base(path)))
}

func (s *publicationStaging) validate() error {
	for _, pinned := range []struct {
		path string
		info os.FileInfo
	}{{s.data, s.dataInfo}, {s.temporary, s.temporaryInfo}} {
		info, err := publicationDirectoryInfo(pinned.path)
		if err != nil {
			return errors.WithStack(err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, pinned.info) {
			return errors.Wrap(ErrInvalid, "publication directory identity changed")
		}
	}
	return nil
}

func (s *publicationStaging) rename(from, to string) (err error) {
	if err := s.validate(); err != nil {
		return err
	}
	if filepath.Dir(from) != s.temporary || filepath.Dir(to) != s.data {
		return errors.Wrap(ErrInvalid, "publication rename outside pinned directories")
	}
	common, err := publicationCommonDirectory(s.data, s.temporary)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(common)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(root.Close())) }()
	oldName, err := filepath.Rel(common, from)
	if err != nil {
		return errors.WithStack(err)
	}
	newName, err := filepath.Rel(common, to)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(root.Rename(oldName, newName))
}

func publicationCommonDirectory(a, b string) (string, error) {
	for parent := filepath.Dir(a); ; parent = filepath.Dir(parent) {
		relative, err := filepath.Rel(parent, b)
		if err != nil {
			return "", errors.WithStack(err)
		}
		if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return parent, nil
		}
	}
}
