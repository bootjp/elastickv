package fuseadapter

import (
	"syscall"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const groupPermissionShift = 3

func canOpenExisting(input *fuse.CreateIn, stat filesystem.Stat) bool {
	if input.Uid == 0 {
		return true
	}
	mask := existingOpenMask(input.Flags)
	bits := stat.Mode
	switch {
	case input.Uid == stat.UID:
		bits >>= 6
	case input.Gid == stat.GID:
		bits >>= groupPermissionShift
	case (stat.Mode>>groupPermissionShift)&mask != stat.Mode&mask:
		member, known := callerInGroup(input.Caller, stat.GID)
		if !known {
			return false
		}
		if member {
			bits >>= groupPermissionShift
		}
	}
	return bits&mask == mask
}

func existingOpenMask(flags uint32) uint32 {
	var mask uint32
	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		mask = 4
	case syscall.O_WRONLY:
		mask = 2
	default:
		mask = 6
	}
	if flags&syscall.O_TRUNC != 0 {
		mask |= 2
	}
	return mask
}

func existingTruncateAttrs(uid, mode uint32) (filesystem.SetAttrMask, filesystem.SetAttr) {
	mask := filesystem.SetAttrMask{Size: true}
	attrs := filesystem.SetAttr{}
	if uid != 0 {
		cleared := mode &^ uint32(syscall.S_ISUID)
		if mode&syscall.S_IXGRP != 0 {
			cleared &^= syscall.S_ISGID
		}
		if cleared != mode {
			mask.Mode = true
			attrs.Mode = cleared
		}
	}
	return mask, attrs
}
