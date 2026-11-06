package raftbootstrap

import "slices"

// PreparedMemberAck attests final durable preparation, not historical data
// correctness. PreparedStateDigest is an opaque, nonzero local preparation hash.
type PreparedMemberAck struct {
	MemberID            uint64
	ManifestDigest      Digest
	Minimum             Versions
	PreparedStateDigest Digest
	PublicKeyDER        []byte
	Signature           []byte
}

// PreparedCertificate requires every baseline member, not merely a Raft quorum.
// Consumers must independently bind the manifest to their trusted local prep.
type PreparedCertificate struct {
	ManifestDigest Digest
	Members        []PreparedMemberAck
}

func canonicalPreparedCertificate(cert PreparedCertificate, manifest BaselineManifest) (PreparedCertificate, error) {
	if err := preparedCertificateSize(cert); err != nil {
		return PreparedCertificate{}, err
	}
	m, err := canonicalBaselineManifest(manifest)
	if err != nil {
		return PreparedCertificate{}, err
	}
	digest, err := BaselineManifestDigest(m)
	if err != nil {
		return PreparedCertificate{}, err
	}
	if cert.ManifestDigest != digest || len(cert.Members) != len(m.Configuration.Members) {
		return PreparedCertificate{}, ErrInvalid
	}
	cert.Members = slices.Clone(cert.Members)
	slices.SortFunc(cert.Members, func(a, b PreparedMemberAck) int { return compareID(a.MemberID, b.MemberID) })
	for i, ack := range cert.Members {
		member := m.Configuration.Members[i]
		if ack.MemberID != member.ID {
			return PreparedCertificate{}, ErrInvalid
		}
		if err := verifyPreparedMemberAck(ack, digest, m.Minimum, member); err != nil {
			return PreparedCertificate{}, err
		}
	}
	return cert, nil
}

// VerifyPreparedCertificate validates all signatures and the exact pinned
// baseline roster, including learners, outgoing voters, and learners-next.
// It does not inspect disk or prove that a signer retained its prepared state.
func VerifyPreparedCertificate(cert PreparedCertificate, manifest BaselineManifest) error {
	_, err := canonicalPreparedCertificate(cert, manifest)
	return err
}

// VerifyPreparedMemberAck authenticates one import artifact. It is not an
// all-member gate; only VerifyPreparedCertificate establishes complete coverage.
func VerifyPreparedMemberAck(ack PreparedMemberAck, manifest BaselineManifest) error {
	if err := preparedCertificateSize(PreparedCertificate{Members: []PreparedMemberAck{ack}}); err != nil {
		return err
	}
	m, err := canonicalBaselineManifest(manifest)
	if err != nil {
		return err
	}
	digest, err := BaselineManifestDigest(m)
	if err != nil {
		return err
	}
	member, ok := configurationMember(m.Configuration, ack.MemberID)
	if !ok {
		return ErrInvalid
	}
	return verifyPreparedMemberAck(ack, digest, m.Minimum, member)
}
