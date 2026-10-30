//go:build linux || darwin

package kv

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

const (
	tsoClaimSeedFileMode      = 0o600
	tsoClaimSeedDirectoryMode = 0o700
)

func persistTSOClaimSeed(path string, raw []byte) (retErr error) {
	dir, base, err := openTSOClaimSeedDirectory(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, errors.Wrap(dir.Close(), "close claim seed directory")) }()
	file, name, err := createTSOClaimSeedTemp(dir)
	if err != nil {
		return err
	}
	defer func() {
		err := unix.Unlinkat(int(dir.Fd()), name, 0)
		if err != nil && !errors.Is(err, unix.ENOENT) {
			retErr = errors.Join(retErr, errors.Wrap(err, "remove claim seed temporary file"))
		}
	}()
	if err := writeTSOClaimSeedTemp(file, raw); err != nil {
		return err
	}
	if err := publishTSOClaimSeed(int(dir.Fd()), name, base); err != nil {
		return err
	}
	return errors.Wrap(dir.Sync(), "sync published claim seed directory")
}

func readTSOClaimSeed(path string) (raw [tsoClaimSeedFileBytes]byte, retErr error) {
	dir, base, err := openTSOClaimSeedDirectory(path)
	if err != nil {
		return raw, err
	}
	defer func() { retErr = errors.Join(retErr, errors.Wrap(dir.Close(), "close claim seed directory")) }()
	fd, err := unix.Openat(int(dir.Fd()), base, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return raw, errors.Wrap(err, "open existing claim seed")
	}
	file, err := tsoClaimSeedFile(fd, path)
	if err != nil {
		return raw, err
	}
	defer func() { retErr = errors.Join(retErr, errors.Wrap(file.Close(), "close claim seed")) }()
	if err := validateTSOClaimSeedFile(file); err != nil {
		return raw, err
	}
	if _, err := io.ReadFull(file, raw[:]); err != nil {
		return raw, errors.Wrap(err, "read claim seed")
	}
	var extra [1]byte
	if _, err := file.Read(extra[:]); !errors.Is(err, io.EOF) {
		return raw, errors.WithStack(errors.Join(ErrTSOClaimSeedInvalid, err))
	}
	return raw, nil
}

func openTSOClaimSeedDirectory(path string) (*os.File, string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, "", errors.WithStack(ErrTSOClaimSeedUnsafe)
	}
	directory, base := filepath.Dir(path), filepath.Base(path)
	const flags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd, err := unix.Open(string(filepath.Separator), flags, 0)
	if err != nil {
		return nil, "", errors.Wrap(err, "open claim seed root directory")
	}
	for _, component := range strings.Split(strings.TrimPrefix(directory, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		next, openErr := unix.Openat(fd, component, flags, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return nil, "", errors.Wrap(openErr, "open claim seed directory component without symlinks")
		}
		fd = next
	}
	dir, err := tsoClaimSeedFile(fd, directory)
	if err != nil {
		return nil, "", err
	}
	if err := validateTSOClaimSeedDirectory(dir); err != nil {
		_ = dir.Close()
		return nil, "", err
	}
	return dir, base, nil
}

func tsoClaimSeedFile(fd int, name string) (*os.File, error) {
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.WithStack(ErrTSOClaimSeedUnsafe)
	}
	return file, nil
}

func validateTSOClaimSeedDirectory(dir *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &stat); err != nil {
		return errors.Wrap(err, "stat claim seed directory")
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o7777 != tsoClaimSeedDirectoryMode || int64(stat.Uid) != int64(os.Geteuid()) {
		return errors.WithStack(ErrTSOClaimSeedUnsafe)
	}
	return nil
}

func validateTSOClaimSeedFile(file *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return errors.Wrap(err, "stat claim seed")
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o7777 != tsoClaimSeedFileMode || int64(stat.Uid) != int64(os.Geteuid()) || stat.Nlink != 1 {
		return errors.WithStack(ErrTSOClaimSeedUnsafe)
	}
	if stat.Size != int64(tsoClaimSeedFileBytes) {
		return errors.WithStack(ErrTSOClaimSeedInvalid)
	}
	return nil
}

func createTSOClaimSeedTemp(dir *os.File) (*os.File, string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, "", errors.Wrap(err, "name claim seed temporary file")
	}
	name := ".tso-claim-seed-" + hex.EncodeToString(nonce[:]) + ".tmp"
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, tsoClaimSeedFileMode)
	if err != nil {
		return nil, "", errors.Wrap(err, "create claim seed temporary file")
	}
	file, err := tsoClaimSeedFile(fd, name)
	return file, name, err
}

func writeTSOClaimSeedTemp(file *os.File, raw []byte) (retErr error) {
	defer func() { retErr = errors.Join(retErr, errors.Wrap(file.Close(), "close claim seed temporary file")) }()
	if err := file.Chmod(tsoClaimSeedFileMode); err != nil {
		return errors.Wrap(err, "set claim seed permissions")
	}
	if _, err := file.Write(raw); err != nil {
		return errors.Wrap(err, "write claim seed temporary file")
	}
	return errors.Wrap(file.Sync(), "sync claim seed temporary file")
}
