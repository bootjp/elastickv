package raftbootstrap

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func fixture(t testing.TB) (Intent, Evidence) {
	t.Helper()
	id, err := ParseUUID("b06f6f94-fd19-4f88-8c88-a2fd26de3427")
	require.NoError(t, err)
	in := Intent{
		Identity: Identity{Cluster: id, Group: 0}, BaselineKind: Accepted,
		Minimum: Versions{Entry: 1, Snapshot: 1, Session: 1},
		Baseline: Configuration{Index: 7, State: ConfState{Voters: []uint64{3, 1}, Learners: []uint64{2}}, Members: []Member{
			{ID: 3, Address: "node3:9000", PublicKeyDigest: sha256.Sum256([]byte("key3"))},
			{ID: 2, Address: "[::1]:9002", PublicKeyDigest: sha256.Sum256([]byte("key2"))},
			{ID: 1, Address: "node1:9000", PublicKeyDigest: sha256.Sum256([]byte("key1"))},
		}},
	}
	digest, err := ConfigurationDigest(in.Baseline)
	require.NoError(t, err)
	identity := in.Identity
	return in, Evidence{Identity: &identity, BaselineDigest: &digest, RaftState: Intact}
}

func TestIntentCanonicalRoundTrip(t *testing.T) {
	in, _ := fixture(t)
	b, err := MarshalIntent(in)
	require.NoError(t, err)
	parsed, err := ParseIntent(b)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3}, parsed.Baseline.State.Voters)
	require.Equal(t, []uint64{3, 1}, in.Baseline.State.Voters, "must not mutate input")
	require.Equal(t, uint64(1), parsed.Baseline.Members[0].ID)
	again, err := MarshalIntent(parsed)
	require.NoError(t, err)
	require.Equal(t, b, again)
	a, err := ConfigurationDigest(in.Baseline)
	require.NoError(t, err)
	c, err := ConfigurationDigest(parsed.Baseline)
	require.NoError(t, err)
	require.Equal(t, a, c)
}

func rechecksum(b []byte) {
	sum := sha256.Sum256(b[:len(b)-checksumSize])
	copy(b[len(b)-checksumSize:], sum[:])
}

func TestIntentStrictFrame(t *testing.T) {
	in, _ := fixture(t)
	valid, err := MarshalIntent(in)
	require.NoError(t, err)
	for n := 0; n < len(valid); n++ {
		_, err := ParseIntent(valid[:n])
		require.Error(t, err, "truncation at %d", n)
	}
	for n := range valid {
		b := bytes.Clone(valid)
		b[n] ^= 1
		_, err := ParseIntent(b)
		require.Error(t, err, "corruption at %d", n)
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
		want   error
	}{
		{"trailing", func(b []byte) []byte { return append(b, 0) }, ErrInvalid},
		{"oversized", func(_ []byte) []byte { return make([]byte, MaxFileSize+1) }, ErrInvalid},
		{"magic", func(b []byte) []byte { b[0] = 'X'; rechecksum(b); return b }, ErrUnsupported},
		{"version", func(b []byte) []byte { binary.BigEndian.PutUint32(b[12:], 2); rechecksum(b); return b }, ErrUnsupported},
		{"state", func(b []byte) []byte { binary.BigEndian.PutUint32(b[16:], 0); rechecksum(b); return b }, ErrUnsupported},
		{"length-overflow", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:], ^uint32(0)); rechecksum(b); return b }, ErrInvalid},
		// Header 12 + version/state 8 + UUID 16 + group 8 + kind 4 + versions 12 + floor 16 + index 8 + bool 1.
		{"count-overflow", func(b []byte) []byte { binary.BigEndian.PutUint32(b[85:], ^uint32(0)); rechecksum(b); return b }, ErrInvalid},
		{"unsorted", func(b []byte) []byte {
			binary.BigEndian.PutUint64(b[89:], 3)
			binary.BigEndian.PutUint64(b[97:], 1)
			rechecksum(b)
			return b
		}, ErrInvalid},
		{"duplicate-member", func(b []byte) []byte { binary.BigEndian.PutUint64(b[97:], 1); rechecksum(b); return b }, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseIntent(tc.mutate(bytes.Clone(valid)))
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestIntentValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Intent)
		want   error
	}{
		{"uuid", func(i *Intent) { i.Identity.Cluster = UUID{} }, ErrInvalid},
		{"zero-version", func(i *Intent) { i.Minimum.Session = 0 }, ErrInvalid},
		{"future-version", func(i *Intent) { i.Minimum.Entry = 2 }, ErrUnsupported},
		{"unknown-baseline", func(i *Intent) { i.BaselineKind = 9 }, ErrUnsupported},
		{"seed-with-index", func(i *Intent) { i.BaselineKind = Seed }, ErrInvalid},
		{"accepted-without-index", func(i *Intent) { i.Baseline.Index = 0 }, ErrInvalid},
		{"partial-floor", func(i *Intent) { i.Floor.Generation = 1 }, ErrInvalid},
		{"pre-baseline-floor", func(i *Intent) { i.Floor = Floor{ActivationIndex: 7, Generation: 1} }, ErrInvalid},
		{"duplicate-voter", func(i *Intent) { i.Baseline.State.Voters = []uint64{1, 1} }, ErrInvalid},
		{"zero-id", func(i *Intent) { i.Baseline.State.Voters[0] = 0 }, ErrInvalid},
		{"empty-voters", func(i *Intent) { i.Baseline.State.Voters = nil }, ErrInvalid},
		{"role-overlap", func(i *Intent) { i.Baseline.State.Learners = []uint64{1} }, ErrInvalid},
		{"learners-next", func(i *Intent) { i.Baseline.State.LearnersNext = []uint64{2} }, ErrInvalid},
		{"auto-leave", func(i *Intent) { i.Baseline.State.AutoLeave = true }, ErrInvalid},
		{"missing-roster", func(i *Intent) { i.Baseline.Members = i.Baseline.Members[:2] }, ErrInvalid},
		{"duplicate-roster", func(i *Intent) { i.Baseline.Members[0] = i.Baseline.Members[1] }, ErrInvalid},
		{"zero-key", func(i *Intent) { i.Baseline.Members[0].PublicKeyDigest = Digest{} }, ErrInvalid},
		{"invalid-address", func(i *Intent) { i.Baseline.Members[0].Address = "host:0" }, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, _ := fixture(t)
			tc.change(&in)
			_, err := MarshalIntent(in)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestUUIDStrict(t *testing.T) {
	for _, s := range []string{"", "00000000-0000-0000-0000-000000000000", "B06f6f94-fd19-4f88-8c88-a2fd26de3427", "b06f6f94fd19-4f88-8c88-a2fd26de3427", "g06f6f94-fd19-4f88-8c88-a2fd26de3427"} {
		_, err := ParseUUID(s)
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestJointConfigurationCanonicalization(t *testing.T) {
	in, _ := fixture(t)
	in.Baseline.State = ConfState{Voters: []uint64{1}, VotersOutgoing: []uint64{3, 1}, Learners: []uint64{2}, LearnersNext: []uint64{3}}
	b, err := MarshalIntent(in)
	require.NoError(t, err)
	parsed, err := ParseIntent(b)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3}, parsed.Baseline.State.VotersOutgoing)
	in.Baseline.State.LearnersNext = []uint64{2}
	_, err = MarshalIntent(in)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestMarkerStrict(t *testing.T) {
	for _, tc := range []struct {
		data  string
		state MarkerState
		err   error
	}{
		{MarkerV1, MarkerRequired, nil}, {"etcd", MarkerLegacy, nil}, {"etcd\n", MarkerLegacy, nil},
		{"", MarkerCorrupt, ErrInvalid}, {"etcd-bootstrap-v1", MarkerUnknown, ErrUnsupported},
		{"etcd-bootstrap-v1\n\n", MarkerUnknown, ErrUnsupported}, {"ETCD", MarkerUnknown, ErrUnsupported},
		{" etcd\n", MarkerUnknown, ErrUnsupported}, {"etcd-bootstrap-v2\n", MarkerUnknown, ErrUnsupported},
	} {
		state, err := ParseMarker([]byte(tc.data))
		require.Equal(t, tc.state, state)
		require.ErrorIs(t, err, tc.err)
	}
}

func FuzzParseIntent(f *testing.F) {
	f.Add([]byte(intentMagic))
	in, _ := fixture(f)
	valid, err := MarshalIntent(in)
	require.NoError(f, err)
	f.Add(valid)
	f.Fuzz(func(t *testing.T, b []byte) {
		in, err := ParseIntent(b)
		if err == nil {
			encoded, err := MarshalIntent(in)
			require.NoError(t, err)
			require.Equal(t, b, encoded)
		}
	})
}
