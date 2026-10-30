package raftbootstrap

import (
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

const filePerm = 0o600

// Only filesystem boundaries are injectable; tests still use real files and
// directory syncs. No global hooks or alternate in-memory filesystem.
type durableFile interface {
	io.Writer
	Name() string
	Chmod(os.FileMode) error
	Sync() error
	Close() error
}
type syncDirectory interface {
	Sync() error
	Close() error
}
type fileOps struct {
	createTemp func(string, string) (durableFile, error)
	rename     func(string, string) error
	openDir    func(string) (syncDirectory, error)
}

func realFileOps() fileOps {
	return fileOps{
		createTemp: createExclusiveTemp,
		rename:     renameLocal,
		openDir:    openDirectory,
	}
}

func createExclusiveTemp(dir, pattern string) (_ durableFile, err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(root.Close())) }()
	name := strings.Replace(pattern, "*", rand.Text(), 1)
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}

func renameLocal(from, to string) (err error) {
	if filepath.Dir(from) != filepath.Dir(to) {
		return ErrInvalid
	}
	root, err := os.OpenRoot(filepath.Dir(to))
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(root.Close())) }()
	return errors.WithStack(root.Rename(filepath.Base(from), filepath.Base(to)))
}

func openDirectory(path string) (_ syncDirectory, err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(root.Close())) }()
	d, err := root.Open(".")
	return d, errors.WithStack(err)
}

func (s *Store) replace(name string, data []byte) (err error) {
	f, err := s.ops.createTemp(s.dir, "."+name+"-*.tmp")
	if err != nil {
		return errors.Wrap(err, "bootstrap temporary creation")
	}
	closed, renamed := false, false
	defer func() {
		if !closed {
			err = errors.CombineErrors(err, errors.WithStack(f.Close()))
		}
		if !renamed {
			err = errors.CombineErrors(err, errors.WithStack(os.Remove(f.Name())))
		}
	}()
	if err := f.Chmod(filePerm); err != nil {
		return errors.Wrap(err, "bootstrap file mode")
	}
	if err := writeAll(f, data); err != nil {
		return errors.Wrap(err, "bootstrap write")
	}
	if err := f.Sync(); err != nil {
		return errors.Wrap(err, "bootstrap file sync")
	}
	closed = true
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "bootstrap file close")
	}
	if err := s.ops.rename(f.Name(), filepath.Join(s.dir, name)); err != nil {
		return errors.Wrap(err, "bootstrap rename")
	}
	renamed = true
	d, err := s.ops.openDir(s.dir)
	if err != nil {
		return errors.Wrap(err, "bootstrap directory open")
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	return errors.Wrap(errors.CombineErrors(syncErr, closeErr), "bootstrap directory sync/close")
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return errors.WithStack(io.ErrShortWrite)
		}
		if err != nil {
			return errors.WithStack(err)
		}
		if n == 0 {
			return errors.WithStack(io.ErrShortWrite)
		}
		data = data[n:]
	}
	return nil
}
