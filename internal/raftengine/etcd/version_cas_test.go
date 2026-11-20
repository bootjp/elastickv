package etcd

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func TestVersionCASConfigurationCheckedAtAppend(t *testing.T) {
	engine, err := Open(t.Context(), OpenConfig{
		NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7001",
		DataDir: t.TempDir(), Bootstrap: true, StateMachine: &testStateMachine{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	st := engine.Status()
	require.NotZero(t, st.ConfigurationIndex)
	payloads := [][]byte{[]byte(raftengine.MigrationVersionIdentityFrame), []byte(raftengine.VersionCASFrame + "test")}
	for i, payload := range payloads {
		_, err := engine.Propose(t.Context(), payload)
		require.ErrorIs(t, err, raftengine.ErrVersionCASUnavailable)
		for _, proof := range []raftengine.VersionCASProof{
			{Term: st.Term + 1, ConfigurationIndex: st.ConfigurationIndex},
			{Term: st.Term, ConfigurationIndex: st.ConfigurationIndex + 1},
		} {
			_, err := engine.Propose(raftengine.WithVersionCASProof(t.Context(), proof), payload)
			require.ErrorIs(t, err, raftengine.ErrVersionCASUnavailable)
		}

		proof := raftengine.VersionCASProof{Term: st.Term, ConfigurationIndex: st.ConfigurationIndex}
		result, err := engine.Propose(raftengine.WithVersionCASProof(t.Context(), proof), payload)
		require.NoError(t, err)
		require.Equal(t, st.LastLogIndex+uint64(i)+1, result.CommitIndex, "only a matching proof may append")
	}
}
