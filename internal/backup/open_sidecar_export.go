package backup

import "os"

// OpenSidecarFile is the exported wrapper around the per-platform
// openSidecarFile. It opens path for write while refusing symlink,
// hard-link, FIFO, socket, and other non-regular-file clobber
// attacks via the platform-appropriate primitives (O_NOFOLLOW +
// O_NONBLOCK + Nlink check on unix; Lstat-then-OpenFile on Windows;
// a stricter Lstat-then-OpenFile fallback on other platforms).
//
// Use this whenever a writer creates or replaces a "sidecar" style
// file at a deterministic path inside an operator-supplied
// directory — the path is predictable to an attacker who can pre-
// create the entry, so the open MUST refuse to follow a symlink or
// truncate a hard-linked / non-regular file (codex P2 v25 #904
// extended this from in-package adapter writers to the
// cmd/elastickv-snapshot-encode CLI's ENCODE_INFO.json writer).
func OpenSidecarFile(path string) (*os.File, error) {
	return openSidecarFile(path)
}
