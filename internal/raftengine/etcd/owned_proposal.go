package etcd

func (e *Engine) RequiresOwnedProposalContext() bool {
	return e != nil && e.nativeAdmission != nil
}
