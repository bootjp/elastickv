package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/kv/kvtest"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSQSFIFODelayCapabilityGate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		caps  []string
		allow bool
	}{
		{name: "old-peer"},
		{name: "htfifo-only", caps: []string{sqsCapabilityHTFIFO}},
		{name: "new-peer", caps: []string{sqsCapabilityHTFIFO, sqsCapabilityFifoRetroactiveDelay}, allow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, 1, 0)
			id := sendDelayTestMessage(t, srv, "A", "g")
			_, peer := newSQSHealthServer(t, sqsHealthBody{Status: "ok", Capabilities: tc.caps})
			srv.leaderSQS = map[string]string{"peer": peer}
			err := srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"})
			if tc.allow {
				require.NoError(t, err)
			} else {
				requireDelayUnavailable(t, err)
			}
			meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
			require.NoError(t, err)
			require.Nil(t, meta.DelayReindex)
			require.Equal(t, tc.allow, meta.FifoOrderIndexed)
			want := int64(0)
			if tc.allow {
				want = 900
			}
			require.Equal(t, want, meta.DelaySeconds)
			rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
			require.Equal(t, rec.SendTimestampMillis+want*1000, rec.VisibleAtMillis)
		})
	}
}

type fifoDelayMembershipFixture struct {
	kv.Coordinator
	members []kv.RaftMember
	after   []kv.RaftMember
	err     error
	calls   int
}

func (c *fifoDelayMembershipFixture) RaftMembers(context.Context) ([]kv.RaftMember, error) {
	c.calls++
	members := c.members
	if c.calls > 1 && c.after != nil {
		members = c.after
	}
	return append([]kv.RaftMember(nil), members...), c.err
}

func (c *fifoDelayMembershipFixture) RaftMembersForKey(ctx context.Context, _ []byte) ([]kv.RaftMember, error) {
	return c.RaftMembers(ctx)
}

func TestSQSFIFODelayRequiresCompleteActualMembership(t *testing.T) {
	local := kv.RaftMember{NodeID: "local", Address: "raft-local", Suffrage: "voter"}
	remote := kv.RaftMember{NodeID: "remote", Address: "raft-remote", Suffrage: "learner"}
	for _, tc := range []struct {
		name    string
		members []kv.RaftMember
		after   []kv.RaftMember
		err     error
		mapped  []string
		allow   bool
	}{
		{name: "proven-standalone", members: []kv.RaftMember{local}, allow: true},
		{name: "empty-membership"},
		{name: "membership-error", err: errors.New("configuration unavailable")},
		{name: "blank-member-address", members: []kv.RaftMember{{NodeID: "local"}}},
		{name: "multi-node-empty-map", members: []kv.RaftMember{local, remote}},
		{name: "multi-node-omitted-learner", members: []kv.RaftMember{local, remote}, mapped: []string{local.Address}},
		{name: "multi-node-complete-map", members: []kv.RaftMember{local, remote}, mapped: []string{local.Address, remote.Address}, allow: true},
		{name: "membership-changes-during-poll", members: []kv.RaftMember{local}, after: []kv.RaftMember{local, remote}, mapped: []string{local.Address, remote.Address}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, 1, 0)
			memberCoord := &fifoDelayMembershipFixture{Coordinator: srv.coordinator, members: tc.members, after: tc.after, err: tc.err}
			srv.coordinator = memberCoord
			_, peer := newSQSHealthServer(t, sqsHealthBody{Status: "ok", Capabilities: []string{sqsCapabilityHTFIFO, sqsCapabilityFifoRetroactiveDelay}})
			srv.leaderSQS = map[string]string{}
			for _, address := range tc.mapped {
				srv.leaderSQS[address] = peer
			}
			err := srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"})
			if tc.allow {
				require.NoError(t, err)
			} else {
				requireDelayUnavailable(t, err)
			}
			meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
			require.NoError(t, err)
			require.Nil(t, meta.DelayReindex)
			require.Equal(t, tc.allow, meta.FifoOrderIndexed)
		})
	}
}

func TestSQSFIFODelayRejectsUnknownCoordinatorMembership(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 1, 0)
	unknown := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{OCC: kvtest.OCCTransactions, ValidateReadKeys: true, AdvanceCommitTS: true})
	unknown.Clock().Observe(st.LastCommitTS())
	srv.coordinator = unknown
	err := srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"})
	requireDelayUnavailable(t, err)
}

func TestSQSFIFODelayRejectsIncompletePartitionCoverage(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 4, 0)
	srv.partitionResolver = NewSQSPartitionResolver(map[string][]uint64{fifoDelayTestQueue: {10, 11}})
	err := srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"})
	var api *sqsAPIError
	require.ErrorAs(t, err, &api)
	require.Equal(t, sqsErrInvalidAttributeValue, api.errorType)
	meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
	require.NoError(t, err)
	require.Nil(t, meta.DelayReindex)
	require.False(t, meta.FifoOrderIndexed)
	require.Zero(t, meta.DelaySeconds)
}

func TestSQSFIFODelayRejectsOldPeerAfterActivation(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 1, 0)
	id := sendDelayTestMessage(t, srv, "A", "g")
	require.NoError(t, srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "0"}))
	_, peer := newSQSHealthServer(t, sqsHealthBody{Status: "ok", Capabilities: []string{sqsCapabilityHTFIFO}})
	srv.leaderSQS = map[string]string{"old-peer": peer}
	err := srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"})
	requireDelayUnavailable(t, err)
	meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
	require.NoError(t, err)
	require.True(t, meta.FifoOrderIndexed)
	require.Nil(t, meta.DelayReindex)
	require.Zero(t, meta.DelaySeconds)
	rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
	require.Equal(t, rec.SendTimestampMillis, rec.VisibleAtMillis)
}
