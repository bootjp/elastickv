//go:build unix

package etcd

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestEnginePipelineConfigurationRealPeerMetadataIODoesNotBlockOwner(t *testing.T) {
	h, _, gate, _ := newPipelineConfigurationRuntime(t, "record", nil, false)
	proposeNativePipelineConfiguration(t, h, true)
	index := asyncStorageAwait(t, h, gate.entered, "configuration before disk peer metadata")
	path := peersFilePath(h.engine.dataDir)
	previous, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))
	require.NoError(t, unix.Mkfifo(path, 0o600))
	entered, finished, release := holdConfigurationPeerPipe(path, previous)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		select {
		case err := <-finished:
			if !errors.Is(err, context.Canceled) {
				require.NoError(t, err)
			}
		case <-time.After(5 * time.Second):
			t.Error("peer metadata pipe writer did not join")
		}
	})
	gate.unblock()
	// A blocking FIFO writer opens only when the actual disk actor opens the
	// metadata reader. Withhold bytes so the real peer-file read remains held.
	require.NoError(t, asyncStorageAwait(t, h, entered, "actual peer metadata reader"))
	h.verifyBlockedHeartbeats(t, func() {
		require.Less(t, h.engine.AppliedIndex(), index)
		require.Less(t, h.engine.currentConfigIndex(), index)
		require.True(t, h.engine.LastQuorumAck().IsZero())
	})
	unblock()
	result := asyncStorageAwait(t, h, h.propose([]byte("after-configuration")), "tail after real peer-file I/O")
	require.NoError(t, result.err)
	require.Equal(t, index, h.engine.currentConfigIndex())
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular(), "the actual atomic peer-file replacement must complete")
}

func holdConfigurationPeerPipe(path string, previous []byte) (<-chan error, <-chan error, chan struct{}) {
	entered, finished := make(chan error, 1), make(chan error, 1)
	release := make(chan struct{})
	go func() {
		writer, err := openConfigurationPeerPipe(path, release)
		entered <- err
		if err != nil {
			finished <- err
			return
		}
		<-release
		_, err = io.Copy(writer, bytes.NewReader(previous))
		finished <- errors.CombineErrors(err, writer.Close())
	}()
	return entered, finished, release
}

func openConfigurationPeerPipe(path string, release <-chan struct{}) (*os.File, error) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-release:
			return nil, context.Canceled
		default:
		}
		writer, err := os.OpenFile(path, os.O_WRONLY|unix.O_NONBLOCK, 0)
		if !errors.Is(err, unix.ENXIO) {
			return writer, err
		}
		select {
		case <-release:
			return nil, context.Canceled
		case <-ticker.C:
		}
	}
}

func TestEnginePipelineConfigurationPeerPipeJoinsAfterEarlyFailure(t *testing.T) {
	failure := errors.New("configuration record failed before peer metadata reader")
	h, _, gate, _ := newPipelineConfigurationRuntime(t, "record", failure, false)
	proposeNativePipelineConfiguration(t, h, true)
	asyncStorageAwait(t, h, gate.entered, "actual configuration record before reader")
	path := peersFilePath(h.engine.dataDir)
	previous, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))
	require.NoError(t, unix.Mkfifo(path, 0o600))
	_, finished, release := holdConfigurationPeerPipe(path, previous)
	joined := false
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		if joined {
			return
		}
		// A failed regression must also join the original uncancellable helper.
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK, 0)
		require.NoError(t, err)
		defer unix.Close(fd)
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("peer pipe rescue did not join writer")
		}
	})
	gate.unblock()
	select {
	case <-h.engine.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("configuration failure did not join before peer-file reader phase")
	}
	require.ErrorIs(t, h.engine.Err(), failure)
	unblock()
	select {
	case err := <-finished:
		joined = true
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Error("peer pipe writer did not join after cancellation before reader open")
	}
}
