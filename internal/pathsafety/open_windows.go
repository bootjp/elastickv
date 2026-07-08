//go:build windows

package pathsafety

import (
	"os"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/windows"
)

func OpenNoFollowRead(path string) (*os.File, error) {
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	h, err := windows.CreateFile(
		pathp,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var byHandle windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &byHandle); err != nil {
		_ = windows.CloseHandle(h)
		return nil, errors.WithStack(err)
	}
	if byHandle.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(h)
		return nil, errors.Wrapf(ErrSymlink, "%s", path)
	}
	f := os.NewFile(uintptr(h), path)
	if f == nil {
		_ = windows.CloseHandle(h)
		return nil, errors.New("path safety: os.NewFile returned nil")
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, errors.WithStack(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		_ = f.Close()
		return nil, errors.Wrapf(ErrSymlink, "%s", path)
	}
	return f, nil
}
