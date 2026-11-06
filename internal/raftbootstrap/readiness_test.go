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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func readinessFixture(t testing.TB) (PreparedCertificate, BaselineManifest, []crypto.Signer) {
	t.Helper()
	m, signers := baselineFixture(t)
	digest, err := BaselineManifestDigest(m)
	require.NoError(t, err)
	cert := PreparedCertificate{ManifestDigest: digest}
	for i, signer := range signers {
		ack, err := SignPreparedMemberAck(PreparedMemberAck{MemberID: uint64(i + 1), ManifestDigest: digest,
			Minimum: m.Minimum, PreparedStateDigest: Digest{byte(i + 1)}}, m, signer)
		require.NoError(t, err)
		cert.Members = append(cert.Members, ack)
	}
	return cert, m, signers
}

func TestPreparedCertificateRoundTrip(t *testing.T) {
	cert, manifest, _ := readinessFixture(t)
	raw, err := MarshalPreparedCertificate(cert, manifest)
	require.NoError(t, err)
	parsed, err := ParsePreparedCertificate(raw, manifest)
	require.NoError(t, err)
	require.Equal(t, cert, parsed)
	require.NoError(t, VerifyPreparedCertificate(parsed, manifest))
	cert.Members[0], cert.Members[3] = cert.Members[3], cert.Members[0]
	reordered, err := MarshalPreparedCertificate(cert, manifest)
	require.NoError(t, err)
	require.Equal(t, raw, reordered)
	require.Equal(t, uint64(4), cert.Members[0].MemberID)
	// Parsing owns its slices, rather than borrowing caller wire storage.
	clear(raw)
	require.NoError(t, VerifyPreparedCertificate(parsed, manifest))
}

func TestPreparedCertificateRequiresAllMembersAndExactFields(t *testing.T) {
	tests := map[string]func(*PreparedCertificate){
		"quorum only":        func(c *PreparedCertificate) { c.Members = c.Members[:2] },
		"outgoing next":      func(c *PreparedCertificate) { c.Members = append(c.Members[:2], c.Members[3:]...) },
		"learner":            func(c *PreparedCertificate) { c.Members = c.Members[:3] },
		"duplicate":          func(c *PreparedCertificate) { c.Members[1] = c.Members[0] },
		"unknown":            func(c *PreparedCertificate) { c.Members[0].MemberID = 99 },
		"extra":              func(c *PreparedCertificate) { c.Members = append(c.Members, c.Members[0]) },
		"certificate digest": func(c *PreparedCertificate) { c.ManifestDigest[0]++ },
		"ack digest":         func(c *PreparedCertificate) { c.Members[0].ManifestDigest[0]++ },
		"entry floor":        func(c *PreparedCertificate) { c.Members[0].Minimum.Entry++ },
		"snapshot floor":     func(c *PreparedCertificate) { c.Members[0].Minimum.Snapshot++ },
		"session floor":      func(c *PreparedCertificate) { c.Members[0].Minimum.Session++ },
		"zero state":         func(c *PreparedCertificate) { c.Members[0].PreparedStateDigest = Digest{} },
		"changed state":      func(c *PreparedCertificate) { c.Members[0].PreparedStateDigest[0]++ },
		"wrong pin":          func(c *PreparedCertificate) { c.Members[0].PublicKeyDER = c.Members[1].PublicKeyDER },
		"no key":             func(c *PreparedCertificate) { c.Members[0].PublicKeyDER = nil },
		"no signature":       func(c *PreparedCertificate) { c.Members[0].Signature = nil },
		"changed signature":  func(c *PreparedCertificate) { c.Members[0].Signature = bytes.Repeat([]byte{1}, 64) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cert, manifest, _ := readinessFixture(t)
			mutate(&cert)
			require.Error(t, VerifyPreparedCertificate(cert, manifest))
			_, err := MarshalPreparedCertificate(cert, manifest)
			require.Error(t, err)
		})
	}
}

func TestPreparedCertificateSignatureDomainAndManifestBinding(t *testing.T) {
	cert, manifest, signers := readinessFixture(t)
	cert.Members[0].Signature = manifest.Members[0].Signature
	require.Error(t, VerifyPreparedCertificate(cert, manifest), "baseline signature is not a ready ack")
	ack, err := SignPreparedMemberAck(cert.Members[0], manifest, signers[0])
	require.NoError(t, err)
	cert.Members[0] = ack
	// A new valid baseline for a different attempt cannot consume the old ACKs.
	manifest.OperationID[0]++
	for i, signer := range signers {
		manifest.Members[i] = signBaselineFixture(t, manifest.BaselineContext, uint64(i+1), signer)
	}
	require.Error(t, VerifyPreparedCertificate(cert, manifest))
	_, err = SignPreparedMemberAck(ack, manifest, signers[0])
	require.Error(t, err)
	_, err = SignPreparedMemberAck(ack, manifest, nil)
	require.Error(t, err)
}

func TestPreparedMemberAckCanonicalRosterAndInvalidSigningInputs(t *testing.T) {
	cert, manifest, signers := readinessFixture(t)
	slices.Reverse(manifest.Configuration.Members)
	slices.Reverse(manifest.Members)
	ack, err := SignPreparedMemberAck(cert.Members[0], manifest, signers[0])
	require.NoError(t, err)
	cert.Members[0] = ack
	require.NoError(t, VerifyPreparedCertificate(cert, manifest))
	for _, mutate := range []func(*PreparedMemberAck){
		func(a *PreparedMemberAck) { a.Minimum.Entry++ },
		func(a *PreparedMemberAck) { a.MemberID = 99 },
		func(a *PreparedMemberAck) { a.PreparedStateDigest = Digest{} },
		func(a *PreparedMemberAck) { a.Signature = make([]byte, baselineSignatureLimit+1) },
	} {
		invalid := ack
		mutate(&invalid)
		_, err := SignPreparedMemberAck(invalid, manifest, signers[0])
		require.Error(t, err)
	}
}

func TestPreparedMemberAckSignsEveryField(t *testing.T) {
	cert, _, signers := readinessFixture(t)
	ack := cert.Members[0]
	mutations := []func(*PreparedMemberAck){
		func(a *PreparedMemberAck) { a.MemberID++ },
		func(a *PreparedMemberAck) { a.ManifestDigest[0]++ },
		func(a *PreparedMemberAck) { a.Minimum.Entry++ },
		func(a *PreparedMemberAck) { a.Minimum.Snapshot++ },
		func(a *PreparedMemberAck) { a.Minimum.Session++ },
		func(a *PreparedMemberAck) { a.PreparedStateDigest[0]++ },
		func(a *PreparedMemberAck) { a.PublicKeyDER = []byte{1} },
	}
	for _, mutate := range mutations {
		changed := ack
		mutate(&changed)
		require.Error(t, verifyBaselineSignature(signers[0].Public(), preparedSigningMessage(changed), ack.Signature))
	}
}

func TestPreparedCertificateWireBoundsAndCanonicalOrder(t *testing.T) {
	cert, manifest, _ := readinessFixture(t)
	raw, err := MarshalPreparedCertificate(cert, manifest)
	require.NoError(t, err)
	for n := range len(raw) {
		_, err := ParsePreparedCertificate(raw[:n], manifest)
		require.Error(t, err)
	}
	corrupt := bytes.Clone(raw)
	corrupt[len(corrupt)-1]++
	_, err = ParsePreparedCertificate(corrupt, manifest)
	require.Error(t, err)
	_, err = ParsePreparedCertificate(append(raw, 0), manifest)
	require.Error(t, err)
	_, err = ParsePreparedCertificate(make([]byte, MaxFileSize+1), manifest)
	require.Error(t, err)
	w := &encoder{limit: MaxFileSize}
	w.fixed(cert.ManifestDigest)
	w.length(len(cert.Members))
	for i := len(cert.Members) - 1; i >= 0; i-- {
		encodePreparedMemberAck(w, cert.Members[i], true)
	}
	noncanonical, err := marshalBaselineFrame(preparedCertificateMagic, w)
	require.NoError(t, err)
	_, err = ParsePreparedCertificate(noncanonical, manifest)
	require.Error(t, err)
	// A checksum-valid hostile count must be rejected before allocating members.
	w = &encoder{limit: MaxFileSize}
	w.fixed(cert.ManifestDigest)
	w.fixed(^uint32(0))
	oversized, err := marshalBaselineFrame(preparedCertificateMagic, w)
	require.NoError(t, err)
	_, err = ParsePreparedCertificate(oversized, manifest)
	require.Error(t, err)
	// Nor can a checksum-valid oversized variable-length field force a copy.
	corrupt = bytes.Clone(raw)
	binary.BigEndian.PutUint32(corrupt[16+32+4+8+32+12+32:], baselineKeyLimit+1)
	sum := sha256.Sum256(corrupt[:len(corrupt)-checksumSize])
	copy(corrupt[len(corrupt)-checksumSize:], sum[:])
	_, err = ParsePreparedCertificate(corrupt, manifest)
	require.Error(t, err)
}

func TestPreparedCertificateAggregateBounds(t *testing.T) {
	cert, manifest, signers := readinessFixture(t)
	cert.Members[0].PublicKeyDER = make([]byte, baselineKeyLimit+1)
	_, err := MarshalPreparedCertificate(cert, manifest)
	require.Error(t, err)
	_, err = SignPreparedMemberAck(cert.Members[0], manifest, signers[0])
	require.Error(t, err)
	ack := PreparedMemberAck{PublicKeyDER: make([]byte, baselineKeyLimit), Signature: make([]byte, baselineSignatureLimit)}
	cert.Members = make([]PreparedMemberAck, MaxFileSize/(baselineKeyLimit+baselineSignatureLimit)+1)
	for i := range cert.Members {
		cert.Members[i] = ack
	}
	require.Error(t, preparedCertificateSize(cert))
	_, err = MarshalPreparedCertificate(cert, manifest)
	require.Error(t, err)
}

func TestPreparedMemberAckSigningAlgorithms(t *testing.T) {
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	for _, signer := range []crypto.Signer{ecdsaKey, rsaKey} {
		manifest, signers := baselineFixture(t)
		signers[0] = signer
		der, err := x509.MarshalPKIXPublicKey(signer.Public())
		require.NoError(t, err)
		manifest.Configuration.Members[0].PublicKeyDigest = sha256.Sum256(der)
		for i, key := range signers {
			manifest.Members[i] = signBaselineFixture(t, manifest.BaselineContext, uint64(i+1), key)
		}
		digest, err := BaselineManifestDigest(manifest)
		require.NoError(t, err)
		ack, err := SignPreparedMemberAck(PreparedMemberAck{MemberID: 1, ManifestDigest: digest,
			Minimum: manifest.Minimum, PreparedStateDigest: Digest{1}}, manifest, signer)
		require.NoError(t, err)
		require.NoError(t, verifyPreparedMemberAck(ack, digest, manifest.Minimum, manifest.Configuration.Members[0]))
		_, wrongKey, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		_, err = SignPreparedMemberAck(ack, manifest, wrongKey)
		require.Error(t, err)
	}
}

func FuzzParsePreparedCertificate(f *testing.F) {
	cert, manifest, _ := readinessFixture(f)
	raw, err := MarshalPreparedCertificate(cert, manifest)
	require.NoError(f, err)
	f.Add(raw)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		parsed, err := ParsePreparedCertificate(data, manifest)
		if err != nil {
			return
		}
		canonical, err := MarshalPreparedCertificate(parsed, manifest)
		require.NoError(t, err)
		require.Equal(t, data, canonical)
	})
}
