package server

import (
	"flag"
	"strings"
)

// raftBootstrapFlags configure how this node starts: bootstrapping a fresh
// cluster (single-group members or per-group peers), joining an existing
// single-group cluster as a learner, or opening protected groups from
// prepared peer configurations.
//
// peerConfigDir is an opt-in that requires explicit accepted-baseline
// preparation for every group.
type raftBootstrapFlags struct {
	bootstrap        bool
	bootstrapMembers string
	groupPeers       string
	joinMembers      string
	joinAsLearner    bool
	peerConfigDir    string
}

func (f *raftBootstrapFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&f.bootstrap, "raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	fs.StringVar(&f.bootstrapMembers, "raftBootstrapMembers", "", "Comma-separated bootstrap raft members (raftID=host:port,...)")
	fs.StringVar(&f.groupPeers, "raftGroupPeers", "", "Semicolon-separated per-group bootstrap members (groupID=raftID@host:port,...)")
	fs.StringVar(&f.joinMembers, "raftJoinMembers", "", "Comma-separated raft members used only for transport discovery while this fresh node joins an existing single-group cluster (raftID=host:port,...); requires --raftJoinAsLearner")
	fs.BoolVar(&f.joinAsLearner, "raftJoinAsLearner", false, "Local node expects to join an existing cluster as a learner; if a post-apply ConfState lists this node as a voter instead, an ERROR-level alarm fires (the node keeps running -- the flag is an operator alarm, not a consensus veto). See docs/design/2026_04_26_implemented_raft_learner.md §4.5.")
	fs.StringVar(&f.peerConfigDir, "raftBootstrapPeerConfigDir", "", "Directory of prepared protected Raft group peer configurations")
}

// joining reports whether --raftJoinMembers was set.
func (f raftBootstrapFlags) joining() bool {
	return strings.TrimSpace(f.joinMembers) != ""
}
