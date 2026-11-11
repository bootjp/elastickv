package etcd

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEnginePipelineNativeRaftEnvelopeInvalidApplyFailsClosed(t *testing.T) {
	for _, mode := range []string{"plaintext", "tampered-tag", "missing-cipher"} {
		t.Run(mode, func(t *testing.T) { testEnvelopeNativeInvalidApply(t, mode) })
	}
}

func testEnvelopeNativeInvalidApply(t *testing.T, mode string) {
	t.Helper()
	c := newEnvelopeNativeCluster(t, mode == "missing-cipher")
	e := c.nodes[0].engine
	c.wait(t, func() bool { return e.AppliedIndex() >= 2 && e.AppliedIndex() == e.Status().CommitIndex }, "initial committed native leader no-op")
	before := e.AppliedIndex()
	if mode == "plaintext" {
		c.group.SetRaftPayloadWrap(nil)
	}
	if mode == "tampered-tag" {
		c.group.SetRaftPayloadWrap(func(payload []byte) ([]byte, error) {
			wrapped, err := c.wrap(payload)
			if err != nil {
				return nil, err
			}
			wrapped[len(wrapped)-1] ^= 0xff
			return wrapped, nil
		})
	}
	c.fatal = true
	done := c.write([]byte("invalid-envelope-must-not-apply"), []byte("refuse-this-write"), 300)
	testutil.Closed(t, e.Done(), 30*time.Second, "invalid committed envelope did not halt native owner/actors")
	require.True(t, errors.Is(e.Err(), ErrRaftUnwrapFailed), "unexpected apply failure: %v", e.Err())
	result := testutil.Recv(t, done, time.Second, "invalid envelope client did not return after real fatal join")
	require.True(t, errors.Is(result.err, ErrRaftUnwrapFailed), "unexpected client failure: %v", result.err)
	require.Nil(t, result.response, "a durable but refused entry is not a successful client receipt")
	require.Equal(t, before, e.AppliedIndex())
	durable, present, err := c.fsms[0].LastAppliedIndex()
	require.NoError(t, err)
	if present {
		require.LessOrEqual(t, durable, before)
	}
	_, err = c.stores[0].GetAt(t.Context(), []byte("invalid-envelope-must-not-apply"), 300)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	selected, err := readWALRestoreState(zap.NewNop(), filepath.Join(e.dataDir, walDirName), filepath.Join(e.dataDir, snapDirName))
	require.NoError(t, err)
	defer func() { require.NoError(t, selected.w.Close()) }()
	require.Greater(t, selected.hardState.GetCommit(), before, "exercise a genuinely committed entry, not a synthetic apply input")
	var refused uint64
	for _, entry := range selected.entries {
		if _, _, ok := decodeProposalEnvelope(entry.GetData()); ok && entry.GetIndex() > before {
			refused = entry.GetIndex()
		}
	}
	require.Greater(t, refused, before, "real WAL must retain the refused entry for recovery")
	t.Logf("%s: real committed WAL index=%d, published applied remains=%d", mode, refused, before)
}
