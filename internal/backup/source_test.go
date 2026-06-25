package backup

import (
	"testing"

	"github.com/cockroachdb/errors"
)

func TestSnapshotIndexFromPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		path    string
		wantIdx uint64
		wantErr bool
	}{
		{"live-writer zero-padded", "/data/fsm-snap/0000000000000064.fsm", 64, false},
		{"unpadded hand-rolled", "42.fsm", 42, false},
		{"max uint64", "18446744073709551615.fsm", 18446744073709551615, false},
		{"missing .fsm suffix", "snapshot.bin", 0, true},
		{"empty stem", ".fsm", 0, true},
		{"non-numeric stem", "snapshot-001.fsm", 0, true},
		{"negative stem", "-1.fsm", 0, true},
		{"hex stem", "0xff.fsm", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idx, err := SnapshotIndexFromPath(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got idx=%d", idx)
				}
				if !errors.Is(err, ErrSnapshotIndexUnparseable) {
					t.Fatalf("err = %v, want ErrSnapshotIndexUnparseable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if idx != tc.wantIdx {
				t.Fatalf("idx = %d, want %d", idx, tc.wantIdx)
			}
		})
	}
}
