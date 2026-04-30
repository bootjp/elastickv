//go:build !unix && !windows

package backup

// isDiskFullError is the fallback for non-unix/non-windows targets
// (js, wasip1, plan9). Those runtimes either do not surface a
// disk-full errno through Go's syscall package or do not have a
// meaningful disk concept (wasm with no host filesystem, plan9 with
// its own error vocabulary). Returning false matches the documented
// helper contract: callers treat unrecognised errors as
// non-retriable, which is the safe default. Codex P2 round 10.
func isDiskFullError(err error) bool {
	_ = err
	return false
}
