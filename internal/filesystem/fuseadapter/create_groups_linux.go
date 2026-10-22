//go:build linux

package fuseadapter

import (
	"os"
	"strconv"
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// FUSE carries only the primary GID. Use the live caller's groups, never
// account-database membership, and fail closed if credentials changed.
func callerInGroup(caller fuse.Caller, gid uint32) (member, known bool) {
	data, err := os.ReadFile("/proc/" + strconv.FormatUint(uint64(caller.Pid), 10) + "/status")
	if err != nil {
		return false, false
	}
	var uidOK, gidOK, groupsOK bool
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "Uid:":
			uidOK = len(fields) == 5 && fields[4] == strconv.FormatUint(uint64(caller.Uid), 10)
		case "Gid:":
			gidOK = len(fields) == 5 && fields[4] == strconv.FormatUint(uint64(caller.Gid), 10)
		case "Groups:":
			groupsOK = true
			for _, field := range fields[1:] {
				if field == strconv.FormatUint(uint64(gid), 10) {
					member = true
				}
			}
		}
	}
	return member, uidOK && gidOK && groupsOK
}
