package raftbootstrap

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func baselineFixture(t testing.TB) (BaselineManifest, []crypto.Signer) {
	t.Helper()
	m := BaselineManifest{BaselineContext: BaselineContext{
		Version: BaselineVersion, Identity: Identity{Cluster: UUID{1}}, OperationID: OperationID{2},
		Minimum: Versions{1, 1, 1}, CommitIndex: 12, CommitTerm: 3, Watermark: 9, DataDigest: Digest{4},
		Configuration: Configuration{Index: 8, State: ConfState{
			Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 3}, Learners: []uint64{4}, LearnersNext: []uint64{3},
		}},
	}}
	signers := make([]crypto.Signer, 4)
	for i := range signers {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		signers[i] = key
		der, err := x509.MarshalPKIXPublicKey(key.Public())
		require.NoError(t, err)
		m.Configuration.Members = append(m.Configuration.Members, Member{ID: uint64(i + 1), Address: "localhost:1234", PublicKeyDigest: sha256.Sum256(der)})
	}
	for i, signer := range signers {
		m.Members = append(m.Members, signBaselineFixture(t, m.BaselineContext, uint64(i+1), signer))
	}
	return m, signers
}

func signBaselineFixture(t testing.TB, c BaselineContext, id uint64, signer crypto.Signer) BaselineMemberEvidence {
	t.Helper()
	digest, err := ConfigurationDigest(c.Configuration)
	require.NoError(t, err)
	p, err := SignBaselineProbe(BaselineProbe{BaselineContext: c, Evidence: BaselineMemberEvidence{
		MemberID: id, CommitIndex: c.CommitIndex, CommitTerm: c.CommitTerm, AppliedIndex: c.CommitIndex,
		LastLogIndex: c.CommitIndex, ConfigurationDigest: digest, NativeStateDigest: Digest{byte(id)},
		DataDigest: c.DataDigest, Watermark: c.Watermark,
	}}, signer)
	require.NoError(t, err)
	return p.Evidence
}

func TestBaselineRoundTrip(t *testing.T) {
	t.Parallel()
	m, _ := baselineFixture(t)
	raw, err := MarshalBaselineManifest(m)
	require.NoError(t, err)
	decoded, err := ParseBaselineManifest(raw)
	require.NoError(t, err)
	require.Equal(t, m, decoded)
	require.NoError(t, VerifyBaselineManifest(decoded, m.BaselineContext))
	digest, err := BaselineManifestDigest(m)
	require.NoError(t, err)
	require.Equal(t, Digest(sha256.Sum256(raw)), digest)
	for _, e := range m.Members {
		p := BaselineProbe{BaselineContext: m.BaselineContext, Evidence: e}
		b, err := MarshalBaselineProbe(p)
		require.NoError(t, err)
		parsed, err := ParseBaselineProbe(b)
		require.NoError(t, err)
		require.Equal(t, p, parsed)
		require.NoError(t, VerifyBaselineProbe(parsed, m.BaselineContext))
		_, err = ParseBaselineManifest(b)
		require.Error(t, err)
	}
	// Input ordering is immaterial; the output and signatures stay canonical.
	m.Members[0], m.Members[3] = m.Members[3], m.Members[0]
	m.Configuration.State.Voters = []uint64{2, 1}
	b, err := MarshalBaselineManifest(m)
	require.NoError(t, err)
	require.Equal(t, raw, b)
	require.Equal(t, uint64(4), m.Members[0].MemberID)
}

func TestBaselineRejectsMissingDuplicateUnknownAndDivergence(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*BaselineManifest){
		"missing voter":         func(m *BaselineManifest) { m.Members = m.Members[1:] },
		"missing outgoing next": func(m *BaselineManifest) { m.Members = append(m.Members[:2], m.Members[3:]...) },
		"missing learner":       func(m *BaselineManifest) { m.Members = m.Members[:3] },
		"duplicate":             func(m *BaselineManifest) { m.Members[1] = m.Members[0] },
		"unknown":               func(m *BaselineManifest) { m.Members[0].MemberID = 99 },
		"commit index":          func(m *BaselineManifest) { m.Members[0].CommitIndex++ },
		"commit term":           func(m *BaselineManifest) { m.Members[0].CommitTerm++ },
		"unapplied":             func(m *BaselineManifest) { m.Members[0].AppliedIndex-- },
		"pending tail":          func(m *BaselineManifest) { m.Members[0].LastLogIndex++ },
		"data":                  func(m *BaselineManifest) { m.Members[0].DataDigest[0]++ },
		"watermark":             func(m *BaselineManifest) { m.Members[0].Watermark++ },
		"configuration digest":  func(m *BaselineManifest) { m.Members[0].ConfigurationDigest[0]++ },
		"native zero":           func(m *BaselineManifest) { m.Members[0].NativeStateDigest = Digest{} },
		"native tamper":         func(m *BaselineManifest) { m.Members[0].NativeStateDigest[0]++ },
		"key":                   func(m *BaselineManifest) { m.Members[0].PublicKeyDER = m.Members[1].PublicKeyDER },
		"signature":             func(m *BaselineManifest) { m.Members[0].Signature[0]++ },
		"no signature":          func(m *BaselineManifest) { m.Members[0].Signature = nil },
		"no key":                func(m *BaselineManifest) { m.Members[0].PublicKeyDER = nil },
		"future config":         func(m *BaselineManifest) { m.Configuration.Index = m.CommitIndex + 1 },
		"auto leave":            func(m *BaselineManifest) { m.Configuration.State.AutoLeave = true },
		"zero operation":        func(m *BaselineManifest) { m.OperationID = OperationID{} },
		"prior floor":           func(m *BaselineManifest) { m.Floor = Floor{1, 1} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			m, _ := baselineFixture(t)
			mutate(&m)
			_, err := MarshalBaselineManifest(m)
			require.Error(t, err)
		})
	}
}

func TestBaselineContextReplay(t *testing.T) {
	t.Parallel()
	changes := map[string]func(*BaselineContext){
		"identity":      func(c *BaselineContext) { c.Identity.Cluster[0]++ },
		"group":         func(c *BaselineContext) { c.Identity.Group++ },
		"operation":     func(c *BaselineContext) { c.OperationID[0]++ },
		"configuration": func(c *BaselineContext) { c.Configuration.Index++ },
		"minimum":       func(c *BaselineContext) { c.Minimum.Entry++ },
		"version":       func(c *BaselineContext) { c.Version++ },
		"floor":         func(c *BaselineContext) { c.Floor = Floor{1, 1} },
		"cut":           func(c *BaselineContext) { c.CommitIndex++ },
		"term":          func(c *BaselineContext) { c.CommitTerm++ },
		"data":          func(c *BaselineContext) { c.DataDigest[0]++ },
		"watermark":     func(c *BaselineContext) { c.Watermark++ },
	}
	for name, mutate := range changes {
		t.Run(name, func(t *testing.T) {
			m, _ := baselineFixture(t)
			expected := m.BaselineContext
			mutate(&m.BaselineContext)
			require.Error(t, VerifyBaselineManifest(m, expected))
			p := BaselineProbe{BaselineContext: m.BaselineContext, Evidence: m.Members[0]}
			require.Error(t, VerifyBaselineProbe(p, expected))
			// Also reject a validly signed old frame against a different expected ID.
			m, _ = baselineFixture(t)
			expected = m.BaselineContext
			mutate(&expected)
			require.Error(t, VerifyBaselineManifest(m, expected))
		})
	}
}

func TestBaselineStandardSigners(t *testing.T) {
	t.Parallel()
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	for name, signer := range map[string]crypto.Signer{"ecdsa": ec, "rsa": rsaKey} {
		t.Run(name, func(t *testing.T) {
			m, _ := baselineFixture(t)
			der, err := x509.MarshalPKIXPublicKey(signer.Public())
			require.NoError(t, err)
			m.Configuration.Members[0].PublicKeyDigest = sha256.Sum256(der)
			e := signBaselineFixture(t, m.BaselineContext, 1, signer)
			p := BaselineProbe{BaselineContext: m.BaselineContext, Evidence: e}
			raw, err := MarshalBaselineProbe(p)
			require.NoError(t, err)
			_, err = ParseBaselineProbe(raw)
			require.NoError(t, err)
			require.NoError(t, VerifyBaselineProbe(p, m.BaselineContext))
		})
	}
}

func baselineRechecksum(b []byte) []byte {
	sum := sha256.Sum256(b[:len(b)-checksumSize])
	copy(b[len(b)-checksumSize:], sum[:])
	return b
}

func TestBaselineStrictFrames(t *testing.T) {
	t.Parallel()
	m, _ := baselineFixture(t)
	raw, err := MarshalBaselineManifest(m)
	require.NoError(t, err)
	for n := 0; n < len(raw); n++ {
		_, err := ParseBaselineManifest(raw[:n])
		require.Error(t, err)
	}
	for _, offset := range []int{0, 8, 12, 16, len(raw) - 1} {
		b := bytes.Clone(raw)
		b[offset]++
		_, err := ParseBaselineManifest(b)
		require.Error(t, err)
	}
	// A checksum is not authentication, even when repaired by an attacker.
	b := bytes.Clone(raw)
	b[16+4+24]++
	_, err = ParseBaselineManifest(baselineRechecksum(b))
	require.Error(t, err)
	w := &encoder{limit: MaxFileSize}
	encodeBaselineContext(w, m.BaselineContext)
	w.length(len(m.Members))
	for i := len(m.Members) - 1; i >= 0; i-- {
		encodeBaselineEvidence(w, m.Members[i], true)
	}
	b, err = marshalBaselineFrame(baselineManifestMagic, w)
	require.NoError(t, err)
	_, err = ParseBaselineManifest(b)
	require.Error(t, err)
	w = &encoder{limit: MaxFileSize}
	encodeBaselineContext(w, m.BaselineContext)
	w.fixed(^uint32(0))
	b, err = marshalBaselineFrame(baselineManifestMagic, w)
	require.NoError(t, err)
	_, err = ParseBaselineManifest(b)
	require.Error(t, err)
	b = append(bytes.Clone(raw[:len(raw)-checksumSize]), 0)
	b = append(b, make([]byte, checksumSize)...)
	binary.BigEndian.PutUint32(b[12:16], uint32(len(b)-baselineFrameOverhead))
	_, err = ParseBaselineManifest(baselineRechecksum(b))
	require.Error(t, err)
	_, err = ParseBaselineManifest(make([]byte, MaxFileSize+1))
	require.Error(t, err)
}

func TestBaselineAggregateBound(t *testing.T) {
	t.Parallel()
	m, _ := baselineFixture(t)
	m.Members[0].PublicKeyDER = make([]byte, baselineKeyLimit+1)
	_, err := MarshalBaselineManifest(m)
	require.Error(t, err)
	m, _ = baselineFixture(t)
	m.Members[0].Signature = make([]byte, baselineSignatureLimit+1)
	_, err = MarshalBaselineManifest(m)
	require.Error(t, err)
	m, _ = baselineFixture(t)
	shared := string(bytes.Repeat([]byte{'a'}, MaxFileSize/3))
	for i := range m.Configuration.Members {
		m.Configuration.Members[i].Address = shared
	}
	_, err = MarshalBaselineManifest(m)
	require.Error(t, err)
	m, _ = baselineFixture(t)
	sharedEvidence := BaselineMemberEvidence{PublicKeyDER: make([]byte, baselineKeyLimit), Signature: make([]byte, baselineSignatureLimit)}
	members := make([]BaselineMemberEvidence, 600)
	for i := range members {
		members[i] = sharedEvidence
	}
	require.Error(t, baselineSize(m.BaselineContext, members))
}

func TestBaselineSignatureDomainAndTrustedRoster(t *testing.T) {
	t.Parallel()
	m, signers := baselineFixture(t)
	p := BaselineProbe{BaselineContext: m.BaselineContext, Evidence: m.Members[0]}
	message := baselineSigningMessage(baselineContextDigest(p.BaselineContext), p.Evidence)
	message[0] ^= 1
	key, ok := signers[0].(ed25519.PrivateKey)
	require.True(t, ok)
	p.Evidence.Signature = ed25519.Sign(key, message)
	require.Error(t, VerifyBaselineProbe(p, m.BaselineContext))

	// Even newly signed evidence cannot authorize a different operator's cut.
	expected := m.BaselineContext
	m.OperationID[0]++
	for i, signer := range signers {
		m.Members[i] = signBaselineFixture(t, m.BaselineContext, uint64(i+1), signer)
	}
	_, err := MarshalBaselineManifest(m)
	require.NoError(t, err)
	require.ErrorIs(t, VerifyBaselineManifest(m, expected), ErrInconsistent)

	m, _ = baselineFixture(t)
	expected = m.BaselineContext
	expected.Configuration.Members = append([]Member(nil), expected.Configuration.Members...)
	expected.Configuration.Members[0].PublicKeyDigest[0]++
	require.ErrorIs(t, VerifyBaselineManifest(m, expected), ErrInconsistent)
}

func FuzzBaselineParse(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte(baselineManifestMagic))
	m, _ := baselineFixture(f)
	raw, err := MarshalBaselineManifest(m)
	require.NoError(f, err)
	f.Add(raw)
	raw, err = MarshalBaselineProbe(BaselineProbe{BaselineContext: m.BaselineContext, Evidence: m.Members[0]})
	require.NoError(f, err)
	f.Add(raw)
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := ParseBaselineManifest(b)
		if err == nil {
			canonical, err := MarshalBaselineManifest(m)
			require.NoError(t, err)
			require.Equal(t, b, canonical)
		}
		p, err := ParseBaselineProbe(b)
		if err == nil {
			canonical, err := MarshalBaselineProbe(p)
			require.NoError(t, err)
			require.Equal(t, b, canonical)
		}
	})
}
