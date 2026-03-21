package store

import (
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
)

type fileSnapshot struct {
	file *os.File
	path string
	once sync.Once
	err  error
}

func newFileSnapshot(file *os.File, path string) Snapshot {
	return &fileSnapshot{
		file: file,
		path: path,
	}
}

func (s *fileSnapshot) WriteTo(w io.Writer) (int64, error) {
	if s == nil || s.file == nil {
		return 0, errors.New("snapshot file is not available")
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return 0, errors.WithStack(err)
	}
	n, err := io.Copy(w, s.file)
	return n, errors.WithStack(err)
}

func (s *fileSnapshot) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		if s.file != nil {
			if err := s.file.Close(); err != nil {
				s.err = errors.WithStack(err)
			}
			s.file = nil
		}
		if s.path != "" {
			if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.err = errors.CombineErrors(s.err, errors.WithStack(err))
			}
			s.path = ""
		}
	})
	return s.err
}
