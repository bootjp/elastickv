package raftadmission

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

func preparedAdmissionProbe(t *testing.T) raftbootstrap.BaselineProbe {
	t.Helper()
	_, signer, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	require.NoError(t, err)
	configuration := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1}},
		Members: []raftbootstrap.Member{{ID: 1, Address: "local:9000", PublicKeyDigest: sha256.Sum256(der)}}}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	require.NoError(t, err)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{
		BaselineContext: raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
			Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1}, OperationID: raftbootstrap.OperationID{1},
			Configuration: configuration, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
			CommitIndex: 5, CommitTerm: 1, DataDigest: raftbootstrap.Digest{1}},
		Evidence: raftbootstrap.BaselineMemberEvidence{MemberID: 1, CommitIndex: 5, CommitTerm: 1,
			AppliedIndex: 5, LastLogIndex: 5, ConfigurationDigest: digest,
			NativeStateDigest: raftbootstrap.Digest{2}, DataDigest: raftbootstrap.Digest{1}},
	}, signer)
	require.NoError(t, err)
	return probe
}

func TestPreparedAdmissionOpensIdentityOnly(t *testing.T) {
	a, err := New(Config{Cluster: raftbootstrap.UUID{1}, DefaultGroup: 1, Groups: []uint64{1},
		ValidatePayload: func(Kind, uint64, []byte) error { return nil }})
	require.NoError(t, err)
	t.Cleanup(a.Close)
	observeAdmission(t, a, 1, 1)
	ticket, err := a.Ticket(1)
	require.NoError(t, err)
	probe := preparedAdmissionProbe(t)
	err = a.PublishPreparedWith(ticket, probe, raftbootstrap.Digest{3}, func(stage peersession.Stage, generation uint64) (func(), error) {
		require.Equal(t, peersession.IdentityReady, stage)
		require.Equal(t, uint64(1), generation)
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, peersession.IdentityReady, a.Stage(1))
	for _, kind := range []Kind{Data, Control, Catalog, Allocator, HLC, Maintenance} {
		_, _, err := a.Begin(t.Context(), kind)
		require.ErrorIs(t, err, ErrClosed)
	}
	_, op, err := a.Begin(t.Context(), Recovery)
	require.NoError(t, err)
	op.CancelAndJoin()
	observeAdmission(t, a, 1, 2)
	require.ErrorIs(t, a.PublishPreparedWith(ticket, probe, raftbootstrap.Digest{3}, func(peersession.Stage, uint64) (func(), error) {
		t.Fatal("stale native ticket cannot publish prepared identity")
		return nil, nil
	}), ErrStale)
}
