package etcd

import "github.com/bootjp/elastickv/internal/raftengine"

func (e *Engine) readRoleAllowed(recovery bool) bool {
	role := e.State()
	if !recovery {
		return role == raftengine.StateLeader
	}
	return e.nativeAdmission != nil && (role == raftengine.StateLeader || role == raftengine.StateFollower)
}
