package raftbootstrap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func sharedLargeAddressIntent(t *testing.T) Intent {
	t.Helper()
	in, _ := fixture(t)
	address := strings.Repeat("a", (2<<20)-5) + ":9000"
	in.Baseline.State = ConfState{}
	in.Baseline.Members = nil
	for id := uint64(1); id <= 10; id++ {
		in.Baseline.State.Voters = append(in.Baseline.State.Voters, id)
		in.Baseline.Members = append(in.Baseline.Members, Member{ID: id, Address: address, PublicKeyDigest: Digest{byte(id)}})
	}
	return in
}

func TestEncoderRejectsSharedInputAmplification(t *testing.T) {
	in := sharedLargeAddressIntent(t)
	w := &encoder{}
	encodeConfiguration(w, in.Baseline)
	require.LessOrEqual(t, w.Len(), MaxFileSize)
	require.ErrorIs(t, w.err, ErrInvalid)
	b, err := MarshalIntent(in)
	require.ErrorIs(t, err, ErrInvalid)
	require.Nil(t, b)
	digest, err := ConfigurationDigest(in.Baseline)
	require.ErrorIs(t, err, ErrInvalid)
	require.Zero(t, digest)
}

func TestEncoderErrorLatchesAllWrites(t *testing.T) {
	in, _ := fixture(t)
	w := &encoder{}
	w.length(MaxFileSize + 1)
	require.ErrorIs(t, w.err, ErrInvalid)
	encodeConfiguration(w, in.Baseline)
	require.Zero(t, w.Len())
}

func TestEncoderIntentExactFrameLimit(t *testing.T) {
	in, _ := fixture(t)
	in.Baseline.State = ConfState{Voters: []uint64{1}}
	in.Baseline.Members = []Member{{ID: 1, Address: "host:9000", PublicKeyDigest: Digest{1}}}
	size, err := configurationEncodedSize(in.Baseline)
	require.NoError(t, err)
	addressSize := len(in.Baseline.Members[0].Address) + configurationLimit - size
	in.Baseline.Members[0].Address = strings.Repeat("a", addressSize-5) + ":9000"
	raw, err := MarshalIntent(in)
	require.NoError(t, err)
	require.Len(t, raw, MaxFileSize)
	parsed, err := ParseIntent(raw)
	require.NoError(t, err)
	require.Equal(t, in.Baseline.Members[0].Address, parsed.Baseline.Members[0].Address)
	_, err = ConfigurationDigest(in.Baseline)
	require.NoError(t, err)
	in.Baseline.Members[0].Address = "a" + in.Baseline.Members[0].Address
	raw, err = MarshalIntent(in)
	require.ErrorIs(t, err, ErrInvalid)
	require.Nil(t, raw)
	digest, err := ConfigurationDigest(in.Baseline)
	require.ErrorIs(t, err, ErrInvalid)
	require.Zero(t, digest)
}
