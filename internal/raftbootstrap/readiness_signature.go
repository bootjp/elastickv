package raftbootstrap

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"crypto/x509"
)

const preparedSignatureDomain = "elastickv/bootstrap/prepared-member-ack/v1\x00"

// SignPreparedMemberAck must only be called after durable native migration,
// intent/required-marker publication, and store preparation have been verified.
// This pure signer performs no I/O and does not establish those prerequisites.
func SignPreparedMemberAck(ack PreparedMemberAck, manifest BaselineManifest, signer crypto.Signer) (PreparedMemberAck, error) {
	if signer == nil {
		return PreparedMemberAck{}, ErrInvalid
	}
	if err := preparedCertificateSize(PreparedCertificate{Members: []PreparedMemberAck{ack}}); err != nil {
		return PreparedMemberAck{}, err
	}
	manifest, err := canonicalBaselineManifest(manifest)
	if err != nil {
		return PreparedMemberAck{}, err
	}
	digest, err := BaselineManifestDigest(manifest)
	if err != nil {
		return PreparedMemberAck{}, err
	}
	member, ok := configurationMember(manifest.Configuration, ack.MemberID)
	if !ok {
		return PreparedMemberAck{}, ErrInvalid
	}
	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return PreparedMemberAck{}, ErrInvalid
	}
	ack.PublicKeyDER, ack.Signature = der, nil
	if err := validatePreparedMemberAck(ack, digest, manifest.Minimum, member, false); err != nil {
		return PreparedMemberAck{}, err
	}
	ack.Signature, err = signBaselineMessage(signer, preparedSigningMessage(ack))
	if err != nil {
		return PreparedMemberAck{}, err
	}
	if err := verifyPreparedMemberAck(ack, digest, manifest.Minimum, member); err != nil {
		return PreparedMemberAck{}, err
	}
	return ack, nil
}

func validatePreparedMemberAck(ack PreparedMemberAck, digest Digest, minimum Versions, member Member, signed bool) error {
	if err := preparedCertificateSize(PreparedCertificate{Members: []PreparedMemberAck{ack}}); err != nil {
		return err
	}
	if ack.MemberID != member.ID || ack.ManifestDigest != digest || ack.Minimum != minimum ||
		ack.PreparedStateDigest == (Digest{}) || len(ack.PublicKeyDER) == 0 ||
		(signed && len(ack.Signature) == 0) || sha256.Sum256(ack.PublicKeyDER) != member.PublicKeyDigest {
		return ErrInvalid
	}
	return nil
}

func preparedSigningMessage(ack PreparedMemberAck) []byte {
	w := &encoder{limit: MaxFileSize}
	w.appendString(preparedSignatureDomain)
	encodePreparedMemberAck(w, ack, false)
	return w.Bytes()
}

func verifyPreparedMemberAck(ack PreparedMemberAck, digest Digest, minimum Versions, member Member) error {
	if err := validatePreparedMemberAck(ack, digest, minimum, member, true); err != nil {
		return err
	}
	key, err := x509.ParsePKIXPublicKey(ack.PublicKeyDER)
	if err != nil {
		return ErrInvalid
	}
	canonical, err := x509.MarshalPKIXPublicKey(key)
	if err != nil || !bytes.Equal(canonical, ack.PublicKeyDER) {
		return ErrInvalid
	}
	return verifyBaselineSignature(key, preparedSigningMessage(ack), ack.Signature)
}
