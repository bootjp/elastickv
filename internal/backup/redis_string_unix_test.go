//go:build unix

package backup

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestRedisDB_OpenJSONLRefusesFIFO is the regression for Codex P2
// round 11: a pre-existing FIFO at strings_ttl.jsonl / hll_ttl.jsonl
// would block the first TTL write indefinitely (POSIX: opening a
// reader-less FIFO with O_WRONLY blocks until a reader attaches).
// O_NONBLOCK turns that into an immediate ENXIO; the post-open
// Stat() check then refuses any non-regular file (FIFO with reader,
// socket, device). The symlink and hard-link guards alone do not
// catch this — mkfifo produces nlink=1 and is not a symlink.
//
// Lives in a unix-only test file because syscall.Mkfifo is undefined
// on Windows and js/wasm.
func TestRedisDB_OpenJSONLRefusesFIFO(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	dir := filepath.Join(root, "redis", "db_0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	fifoPath := filepath.Join(dir, redisStringsTTLFile)
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Skipf("mkfifo not supported on this platform: %v", err)
	}
	err := db.HandleString([]byte("k"), encodeNewStringValue(t, []byte("v"), fixedExpireMs))
	if err == nil {
		t.Fatalf("expected refusal of FIFO sidecar, got nil")
	}
	// Either ENXIO ("FIFO at <path>") on platforms that surface it
	// at open, or "non-regular file" if a (rare) reader is around
	// to make the open succeed. Both are acceptable as long as the
	// open does not hang and the encoder refuses to truncate the
	// pipe target.
	msg := err.Error()
	if !strings.Contains(msg, "FIFO at") && !strings.Contains(msg, "non-regular file") {
		t.Fatalf("expected FIFO refusal message, got %v", err)
	}
}
