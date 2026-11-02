package raftbootstrap

import "bytes"

const (
	preparedCertificateMagic    = "EKVPR001"
	preparedMemberAckSize       = 8 + 32 + 12 + 32 + 4 + 4
	preparedCertificateSizeBase = baselineFrameOverhead + 32 + 4
)

// Check aggregate bounds before cloning members or copying variable-length data.
func preparedCertificateSize(cert PreparedCertificate) error {
	size := preparedCertificateSizeBase
	if len(cert.Members) > (MaxFileSize-size)/preparedMemberAckSize {
		return ErrInvalid
	}
	for _, ack := range cert.Members {
		if len(ack.PublicKeyDER) > baselineKeyLimit || len(ack.Signature) > baselineSignatureLimit {
			return ErrInvalid
		}
		n := preparedMemberAckSize + len(ack.PublicKeyDER) + len(ack.Signature)
		if n > MaxFileSize-size {
			return ErrInvalid
		}
		size += n
	}
	return nil
}

func encodePreparedMemberAck(w *encoder, ack PreparedMemberAck, signature bool) {
	w.fixed(ack.MemberID)
	w.fixed(ack.ManifestDigest)
	w.fixed(ack.Minimum)
	w.fixed(ack.PreparedStateDigest)
	w.length(len(ack.PublicKeyDER))
	w.appendBytes(ack.PublicKeyDER)
	if signature {
		w.length(len(ack.Signature))
		w.appendBytes(ack.Signature)
	}
}

func MarshalPreparedCertificate(cert PreparedCertificate, manifest BaselineManifest) ([]byte, error) {
	cert, err := canonicalPreparedCertificate(cert, manifest)
	if err != nil {
		return nil, err
	}
	w := &encoder{limit: MaxFileSize - baselineFrameOverhead}
	w.fixed(cert.ManifestDigest)
	w.length(len(cert.Members))
	for _, ack := range cert.Members {
		encodePreparedMemberAck(w, ack, true)
	}
	return marshalBaselineFrame(preparedCertificateMagic, w)
}

func ParsePreparedCertificate(data []byte, manifest BaselineManifest) (PreparedCertificate, error) {
	r, err := parseBaselineFrame(data, preparedCertificateMagic)
	if err != nil {
		return PreparedCertificate{}, err
	}
	var cert PreparedCertificate
	r.fixed(&cert.ManifestDigest)
	n := r.count(preparedMemberAckSize)
	if r.err != nil {
		return PreparedCertificate{}, ErrInvalid
	}
	cert.Members = make([]PreparedMemberAck, n)
	for i := range cert.Members {
		ack := &cert.Members[i]
		r.fixed(&ack.MemberID)
		r.fixed(&ack.ManifestDigest)
		r.fixed(&ack.Minimum)
		r.fixed(&ack.PreparedStateDigest)
		ack.PublicKeyDER = baselineBlob(r)
		ack.Signature = baselineBlob(r)
	}
	if r.err != nil || r.Len() != 0 {
		return PreparedCertificate{}, ErrInvalid
	}
	canonical, err := MarshalPreparedCertificate(cert, manifest)
	if err != nil {
		return PreparedCertificate{}, err
	}
	if !bytes.Equal(canonical, data) {
		return PreparedCertificate{}, ErrInvalid
	}
	return cert, nil
}
