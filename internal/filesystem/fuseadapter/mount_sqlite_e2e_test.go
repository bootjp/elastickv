//go:build linux

package fuseadapter_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sqliteWriters          = 2
	sqliteWritesPerWriter  = 3
	sqliteCommandTimeout   = 20 * time.Second
	sqliteBusyTimeoutMilli = 5000
)

// TestMountSQLiteConcurrentProcesses uses the real Linux kernel mount. Each
// UPDATE runs in a separate sqlite3 process without an application retry loop.
func TestMountSQLiteConcurrentProcesses(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		if os.Getenv("ELASTICKV_REQUIRE_SQLITE") != "" {
			t.Fatal("sqlite3 is required for the mounted SQLite test")
		}
		t.Skip("sqlite3 is unavailable")
	}
	svc := newMemoryServiceForMountWithChunkSize(t, 4096)
	for _, mode := range []string{"PERSIST", "DELETE"} {
		t.Run(mode, func(t *testing.T) {
			mountPoint := mountMemoryService(t, svc, "sqlite-"+mode)
			path := filepath.Join(mountPoint, strings.ToLower(mode)+".db")
			got := runSQLite(t, path, "PRAGMA journal_mode="+mode+"; PRAGMA synchronous=FULL; CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);")
			require.Equal(t, strings.ToLower(mode), strings.TrimSpace(got))

			results := make(chan error, sqliteWriters*sqliteWritesPerWriter)
			var writers sync.WaitGroup
			for range sqliteWriters {
				writers.Add(1)
				go func() {
					defer writers.Done()
					for range sqliteWritesPerWriter {
						actualMode, err := sqliteCommand(t.Context(), path,
							"PRAGMA busy_timeout="+strconv.Itoa(sqliteBusyTimeoutMilli)+"; PRAGMA journal_mode="+mode+"; PRAGMA synchronous=FULL; BEGIN IMMEDIATE; UPDATE counter SET value=value+1; COMMIT;")
						fields := strings.Fields(actualMode)
						if err == nil && (len(fields) == 0 || fields[len(fields)-1] != strings.ToLower(mode)) {
							err = fmt.Errorf("writer journal mode output %q, expected %s", strings.TrimSpace(actualMode), mode)
						}
						results <- err
					}
				}()
			}
			writers.Wait()
			close(results)
			for err := range results {
				require.NoError(t, err)
			}
			require.Equal(t, strconv.Itoa(sqliteWriters*sqliteWritesPerWriter), strings.TrimSpace(runSQLite(t, path, "SELECT value FROM counter;")))
			require.Equal(t, "ok", strings.TrimSpace(runSQLite(t, path, "PRAGMA integrity_check;")))
		})
	}
}

func runSQLite(t *testing.T, path, sql string) string {
	t.Helper()
	out, err := sqliteCommand(t.Context(), path, sql)
	require.NoError(t, err)
	return out
}

func sqliteCommand(parent context.Context, path, sql string) (string, error) {
	ctx, cancel := context.WithTimeout(parent, sqliteCommandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, "sqlite3", "-batch", "-noheader", path, sql)
	out, err := command.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("sqlite3 %s: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}
