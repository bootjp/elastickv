package server

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/bootjp/elastickv/internal/fsmount"
	"github.com/stretchr/testify/require"
)

func TestFilesystemStartConfigFromFlags(t *testing.T) {
	tests := []struct {
		name          string
		mount         string
		clientID      string
		raftID        string
		mode          uint
		uid           uint
		gid           uint
		want          fsmount.Config
		wantErr       string
		strictRecover bool
	}{
		{
			name:  "disabled when mount point is blank",
			mount: "   ",
			mode:  0o7777 + 1,
			want:  fsmount.Config{},
		},
		{
			name:   "client id defaults to raft id",
			mount:  " /mnt/fs ",
			raftID: " n1 ",
			mode:   0o750,
			uid:    1000,
			gid:    1001,
			want:   fsmount.Config{MountPoint: "/mnt/fs", ClientID: "n1", RootMode: 0o750, RootUID: 1000, RootGID: 1001},
		},
		{
			name:     "explicit client id wins",
			mount:    "/mnt/fs",
			clientID: "mount-a",
			raftID:   "n1",
			mode:     0o755,
			want:     fsmount.Config{MountPoint: "/mnt/fs", ClientID: "mount-a", RootMode: 0o755},
		},
		{
			name:   "journal mode is unconditional",
			mount:  "/mnt/fs",
			raftID: "n1",
			mode:   0o755,
			want:   fsmount.Config{MountPoint: "/mnt/fs", ClientID: "n1", RootMode: 0o755},
		},
		{
			name:          "recovery works without a mode flag",
			mount:         "/mnt/fs",
			raftID:        "n1",
			mode:          0o755,
			strictRecover: true,
			want:          fsmount.Config{MountPoint: "/mnt/fs", ClientID: "n1", StrictRecover: true, RootMode: 0o755},
		},
		{
			name:    "client id required",
			mount:   "/mnt/fs",
			mode:    0o755,
			wantErr: "client ID is required",
		},
		{
			name:    "root mode out of range",
			mount:   "/mnt/fs",
			raftID:  "n1",
			mode:    0o7777 + 1,
			wantErr: "out of range",
		},
		{
			name:    "root uid out of range",
			mount:   "/mnt/fs",
			raftID:  "n1",
			mode:    0o755,
			uid:     math.MaxUint32 + 1,
			wantErr: "out of range",
		},
		{
			name:    "root gid out of range",
			mount:   "/mnt/fs",
			raftID:  "n1",
			mode:    0o755,
			gid:     math.MaxUint32 + 1,
			wantErr: "out of range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := defaultFlags[filesystemFlags]()
			f.mount, f.clientID = tt.mount, tt.clientID
			f.strictRecover = tt.strictRecover
			f.rootMode, f.rootUID, f.rootGID = tt.mode, tt.uid, tt.gid

			got, err := filesystemStartConfig(f, tt.raftID)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, fsmount.Config{}, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStartFilesystemIfEnabledIsNoopWithoutMountPoint(t *testing.T) {
	f := defaultFlags[filesystemFlags]()
	f.mount = ""

	require.NoError(t, startFilesystemIfEnabled(context.Background(), nil, nil, nil, nil, nil, f, "n1"))
}

func TestStartFilesystemIfEnabledRequiresLifecycle(t *testing.T) {
	f := defaultFlags[filesystemFlags]()
	f.mount = t.TempDir()

	err := startFilesystemIfEnabled(context.Background(), nil, nil, nil, nil, nil, f, "n1")
	require.ErrorContains(t, err, "filesystem lifecycle is required")
}

func TestFilesystemStrictOnlyFlag(t *testing.T) {
	for _, args := range [][]string{nil, {"--filesystemStrict"}, {"--filesystemStrict=true"}, {"--filesystemStrict=1"}} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			f := RegisterFlags(fs)
			require.NoError(t, fs.Parse(args))
			require.Equal(t, "true", fs.Lookup("filesystemStrict").Value.String())
			f.filesystem.mount = "/mnt/fs"
			_, err := filesystemStartConfig(f.filesystem, "n1")
			require.NoError(t, err)
		})
	}
	for _, value := range []string{"false", "0", "FALSE"} {
		t.Run(value, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			RegisterFlags(fs)
			require.ErrorContains(t, fs.Parse([]string{"--filesystemStrict=" + value}), "legacy filesystem mode is no longer supported")
		})
	}
}
