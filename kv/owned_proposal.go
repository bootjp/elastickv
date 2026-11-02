package kv

import "github.com/bootjp/elastickv/internal/raftengine"

func requiresOwnedProposalContext(proposer raftengine.Proposer) bool {
	capability, ok := proposer.(raftengine.OwnedProposalContextRequired)
	return ok && capability.RequiresOwnedProposalContext()
}

func (p *dynamicWrappedProposer) RequiresOwnedProposalContext() bool {
	return requiresOwnedProposalContext(p.inner)
}
