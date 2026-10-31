package etcd

import (
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type entryIdentityFSM struct {
	identity raftengine.AppliedEntryIdentity
	payload  []byte
	clears   int
}

func (f *entryIdentityFSM) SetApplyEntryIdentity(identity raftengine.AppliedEntryIdentity) {
	f.identity = identity
}

func (f *entryIdentityFSM) ClearApplyEntryIdentity() { f.clears++ }

func (f *entryIdentityFSM) Apply(payload []byte) any {
	f.payload = append([]byte(nil), payload...)
	return nil
}

func (*entryIdentityFSM) Snapshot() (Snapshot, error) { return nil, nil }
func (*entryIdentityFSM) Restore(io.Reader) error     { return nil }

func TestApplyNormalEntryDeliversPlaintextIdentity(t *testing.T) {
	const index, term = uint64(17), uint64(5)
	payload := []byte("canonical plaintext")
	fsm := &entryIdentityFSM{}
	engine := newTestEngine(fsm, nil, nil)
	entry := raftpb.Entry{
		Index: uint64Ptr(index),
		Term:  uint64Ptr(term),
		Type:  entryTypePtr(raftpb.EntryNormal),
		Data:  encodeProposalEnvelope(9, payload),
	}

	response, err := engine.applyNormalEntry(entry, false)
	require.NoError(t, err)
	require.Nil(t, response)
	require.Equal(t, payload, fsm.payload)
	require.Equal(t, raftengine.NewAppliedEntryIdentity(index, term, int32(raftpb.EntryNormal), payload), fsm.identity)
	require.Equal(t, 1, fsm.clears)
}

func TestApplyNormalEntryDoesNotDeliverIdentityForDroppedDuplicate(t *testing.T) {
	fsm := &entryIdentityFSM{}
	engine := newTestEngine(fsm, nil, nil)
	entry := raftpb.Entry{
		Index: uint64Ptr(17),
		Term:  uint64Ptr(5),
		Type:  entryTypePtr(raftpb.EntryNormal),
		Data:  encodeProposalEnvelope(9, []byte("persistent")),
	}

	response, err := engine.applyNormalEntry(entry, true)
	require.NoError(t, err)
	require.Nil(t, response)
	require.Empty(t, fsm.payload)
	require.Equal(t, raftengine.AppliedEntryIdentity{}, fsm.identity)
}

func TestAppliedEntryIdentityFramesTypeAndPayload(t *testing.T) {
	base := raftengine.NewAppliedEntryIdentity(1, 2, 3, []byte("ab"))
	require.NotEqual(t, base.Digest, raftengine.NewAppliedEntryIdentity(1, 2, 4, []byte("ab")).Digest)
	require.NotEqual(t, base.Digest, raftengine.NewAppliedEntryIdentity(1, 2, 3, []byte("a")).Digest)
	require.NotEqual(t, base.Digest, raftengine.NewAppliedEntryIdentity(1, 2, 3, []byte("ab\x00")).Digest)
}

func TestDeliverApplyIdentityCoversConfigurationEntryContext(t *testing.T) {
	for _, entryType := range []raftpb.EntryType{raftpb.EntryConfChange, raftpb.EntryConfChangeV2} {
		t.Run(entryType.String(), func(t *testing.T) {
			payload := []byte("canonical committed configuration context")
			entry := raftpb.Entry{Index: uint64Ptr(21), Term: uint64Ptr(8), Type: entryTypePtr(entryType), Data: payload}
			fsm := &entryIdentityFSM{}
			engine := newTestEngine(fsm, nil, nil)

			clear := engine.deliverApplyIdentity(entry, entry.GetData())
			require.Equal(t, raftengine.NewAppliedEntryIdentity(21, 8, int32(entryType), payload), fsm.identity)
			require.Zero(t, fsm.clears)
			clear()
			require.Equal(t, 1, fsm.clears)
		})
	}
}
