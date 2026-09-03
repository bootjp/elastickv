package backup

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
)

// orphanTTLRecords builds TTL scan-index records whose value key is absent --
// ordinary cleanup leftovers. The encoder drops them, so they land in the raw
// streamed tally but not in the finalized retained-record tally.
func orphanTTLRecords(n int) []*pb.BackupKV {
	out := make([]*pb.BackupKV, 0, n)
	for i := range n {
		value := binary.BigEndian.AppendUint64(nil, uint64(1_700_000_000_000))
		out = append(out, &pb.BackupKV{
			Key:   []byte(RedisTTLPrefix + fmt.Sprintf("orphan-%04d", i)),
			Value: value,
		})
	}
	return out
}

// A v2 server counts its baseline through the same encoders as the dump
// (LiveScopeCounter), so the finalized retained-record totals are the right
// numerator. A v1 server counts every raw scoped key, leftovers included, and
// the default all-adapter dump does not require v2 -- beginBackupRequestIsScoped
// is false for it -- so a v1 server is reachable on that path. Comparing
// finalized totals to a raw baseline understates the dump by however many
// leftovers the cluster holds and fails a stream that was complete.
func TestRunLiveBackupAgainstV1ServerBaseline(t *testing.T) {
	t.Parallel()

	const orphans = 200
	newRPC := func(version uint32, baseline uint64) *fakeLiveBackupRPC {
		rpc := successfulLiveBackupRPC()
		rpc.begin.BackupProtocolVersion = version
		rpc.begin.ExpectedKeys[0].KeyCount = baseline
		rpc.records = append(rpc.records, orphanTTLRecords(orphans)...)
		return rpc
	}
	run := func(t *testing.T, rpc *fakeLiveBackupRPC) error {
		t.Helper()
		_, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
			OutputRoot: filepath.Join(t.TempDir(), "dump"),
			Adapters:   AllAdapters(),
			TTL:        time.Minute,
			// The default CLI dump asks for every adapter and no scope, which
			// is exactly the request a v1 server answers without complaint.
			ElastickvVersion: "test",
		})
		return err
	}

	t.Run("v1 raw baseline accepts a complete stream", func(t *testing.T) {
		t.Parallel()
		// 1 string + 200 orphan TTL records = 201 raw keys.
		if err := run(t, newRPC(1, orphans+1)); err != nil {
			t.Fatalf("complete stream against a v1 server: %v", err)
		}
	})

	t.Run("v2 retained baseline accepts the same stream", func(t *testing.T) {
		t.Parallel()
		// A v2 server's counter drops the same orphans: 1 retained record.
		if err := run(t, newRPC(backupProtocolVersionScopedBaseline, 1)); err != nil {
			t.Fatalf("complete stream against a v2 server: %v", err)
		}
	})

	t.Run("a real shortfall still fails against a v1 server", func(t *testing.T) {
		t.Parallel()
		rpc := newRPC(1, 10_000)
		if err := run(t, rpc); !errors.Is(err, ErrCompactionDuringDump) {
			t.Fatalf("err=%v, want ErrCompactionDuringDump", err)
		}
	})
}
