package main

// Activation must not infer readiness from a missing runtime callback. The
// legacy public gate permits that shape, but a fence changes Raft apply verdicts.
func commitTSFenceRuntimesReady(runtimes []*raftGroupRuntime) bool {
	if len(runtimes) == 0 {
		return false
	}
	domain := bootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return false
	}
	for _, runtime := range runtimes {
		if runtime == nil || runtime.bootstrapDomain != domain || runtime.publicReady == nil || !runtime.publicReady() {
			return false
		}
	}
	return true
}
