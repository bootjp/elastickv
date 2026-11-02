package raftengine

// OwnedProposalContextRequired is a scheduling capability, not admission
// authority. Protected proposals must retain their receiver-owned context;
// independent caller handles cannot be replaced by a Background batch context.
type OwnedProposalContextRequired interface {
	RequiresOwnedProposalContext() bool
}
