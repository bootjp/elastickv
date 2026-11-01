package raftbootstrap

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
)

const (
	baselineManifestMagic = "EKVBM001"
	baselineProbeMagic    = "EKVBQ001"
	baselineFrameOverhead = 8 + 4 + 4 + checksumSize
	baselineContextSize   = 4 + 24 + 16 + 12 + 16 + 24 + 32
	baselineEvidenceSize  = 8*6 + 32*3 + 4*2
	baselineCountSize     = 4
)

// Bound the aggregate wire size before cloning or signing. The fixed-depth
// binary schema has no recursive values or unbounded nested envelopes.
func baselineSize(c BaselineContext, members []BaselineMemberEvidence) error {
	size, err := configurationEncodedSize(c.Configuration)
	if err != nil {
		return err
	}
	size += baselineFrameOverhead + baselineContextSize + baselineCountSize
	if size > MaxFileSize || len(members) > (MaxFileSize-size)/baselineEvidenceSize {
		return ErrInvalid
	}
	for _, e := range members {
		if len(e.PublicKeyDER) > baselineKeyLimit || len(e.Signature) > baselineSignatureLimit {
			return ErrInvalid
		}
		n := baselineEvidenceSize + len(e.PublicKeyDER) + len(e.Signature)
		if n > MaxFileSize-size {
			return ErrInvalid
		}
		size += n
	}
	return nil
}

func encodeBaselineContext(w *encoder, c BaselineContext) {
	w.fixed(c.Version)
	w.fixed(c.Identity)
	w.fixed(c.OperationID)
	encodeConfiguration(w, c.Configuration)
	w.fixed(c.Minimum)
	w.fixed(c.Floor)
	w.fixed(c.CommitIndex)
	w.fixed(c.CommitTerm)
	w.fixed(c.Watermark)
	w.fixed(c.DataDigest)
}

func encodeBaselineEvidence(w *encoder, e BaselineMemberEvidence, signature bool) {
	w.fixed(e.MemberID)
	w.fixed(e.CommitIndex)
	w.fixed(e.CommitTerm)
	w.fixed(e.AppliedIndex)
	w.fixed(e.LastLogIndex)
	w.fixed(e.ConfigurationDigest)
	w.fixed(e.NativeStateDigest)
	w.fixed(e.DataDigest)
	w.fixed(e.Watermark)
	w.length(len(e.PublicKeyDER))
	w.appendBytes(e.PublicKeyDER)
	if signature {
		w.length(len(e.Signature))
		w.appendBytes(e.Signature)
	}
}

func marshalBaselineFrame(magic string, w *encoder) ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	f := &encoder{limit: MaxFileSize}
	f.appendString(magic)
	f.fixed(BaselineVersion)
	f.length(w.Len())
	f.appendBytes(w.Bytes())
	sum := sha256.Sum256(f.Bytes())
	f.appendBytes(sum[:])
	if f.err != nil {
		return nil, f.err
	}
	return bytes.Clone(f.Bytes()), nil
}

func MarshalBaselineManifest(m BaselineManifest) ([]byte, error) {
	m, err := canonicalBaselineManifest(m)
	if err != nil {
		return nil, err
	}
	w := &encoder{limit: MaxFileSize - baselineFrameOverhead}
	encodeBaselineContext(w, m.BaselineContext)
	w.length(len(m.Members))
	for _, e := range m.Members {
		encodeBaselineEvidence(w, e, true)
	}
	return marshalBaselineFrame(baselineManifestMagic, w)
}

func MarshalBaselineProbe(p BaselineProbe) ([]byte, error) {
	if err := VerifyBaselineProbe(p, p.BaselineContext); err != nil {
		return nil, err
	}
	c, err := canonicalBaselineContext(p.BaselineContext)
	if err != nil {
		return nil, err
	}
	w := &encoder{limit: MaxFileSize - baselineFrameOverhead}
	encodeBaselineContext(w, c)
	encodeBaselineEvidence(w, p.Evidence, true)
	return marshalBaselineFrame(baselineProbeMagic, w)
}

func parseBaselineFrame(data []byte, magic string) (*decoder, error) {
	if len(data) < baselineFrameOverhead || len(data) > MaxFileSize {
		return nil, ErrInvalid
	}
	if string(data[:8]) != magic || binary.BigEndian.Uint32(data[8:12]) != BaselineVersion {
		return nil, ErrUnsupported
	}
	if int64(binary.BigEndian.Uint32(data[12:16])) != int64(len(data))-baselineFrameOverhead {
		return nil, ErrInvalid
	}
	end := len(data) - checksumSize
	sum := sha256.Sum256(data[:end])
	if !bytes.Equal(sum[:], data[end:]) {
		return nil, ErrInvalid
	}
	return &decoder{Reader: bytes.NewReader(data[16:end])}, nil
}

func decodeBaselineContext(r *decoder) BaselineContext {
	var c BaselineContext
	r.fixed(&c.Version)
	r.fixed(&c.Identity)
	r.fixed(&c.OperationID)
	c.Configuration = decodeConfiguration(r)
	r.fixed(&c.Minimum)
	r.fixed(&c.Floor)
	r.fixed(&c.CommitIndex)
	r.fixed(&c.CommitTerm)
	r.fixed(&c.Watermark)
	r.fixed(&c.DataDigest)
	return c
}

func baselineBlob(r *decoder) []byte {
	n := r.count(1)
	if r.err != nil || n > baselineBlobLimit {
		r.err = ErrInvalid
		return nil
	}
	b := make([]byte, n)
	_, r.err = io.ReadFull(r.Reader, b)
	return b
}

func decodeBaselineEvidence(r *decoder) BaselineMemberEvidence {
	var e BaselineMemberEvidence
	r.fixed(&e.MemberID)
	r.fixed(&e.CommitIndex)
	r.fixed(&e.CommitTerm)
	r.fixed(&e.AppliedIndex)
	r.fixed(&e.LastLogIndex)
	r.fixed(&e.ConfigurationDigest)
	r.fixed(&e.NativeStateDigest)
	r.fixed(&e.DataDigest)
	r.fixed(&e.Watermark)
	e.PublicKeyDER = baselineBlob(r)
	e.Signature = baselineBlob(r)
	return e
}

// Parse verifies internal signatures, not freshness or authorization. Consumers
// must additionally Verify against their independently retained expected context.
func ParseBaselineManifest(data []byte) (BaselineManifest, error) {
	r, err := parseBaselineFrame(data, baselineManifestMagic)
	if err != nil {
		return BaselineManifest{}, err
	}
	m := BaselineManifest{BaselineContext: decodeBaselineContext(r)}
	n := r.count(baselineEvidenceSize)
	m.Members = make([]BaselineMemberEvidence, n)
	for i := range m.Members {
		m.Members[i] = decodeBaselineEvidence(r)
	}
	if r.err != nil || r.Len() != 0 {
		return BaselineManifest{}, ErrInvalid
	}
	canonical, err := MarshalBaselineManifest(m)
	if err != nil {
		return BaselineManifest{}, err
	}
	if !bytes.Equal(canonical, data) {
		return BaselineManifest{}, ErrInvalid
	}
	return m, nil
}

func ParseBaselineProbe(data []byte) (BaselineProbe, error) {
	r, err := parseBaselineFrame(data, baselineProbeMagic)
	if err != nil {
		return BaselineProbe{}, err
	}
	p := BaselineProbe{BaselineContext: decodeBaselineContext(r), Evidence: decodeBaselineEvidence(r)}
	if r.err != nil || r.Len() != 0 {
		return BaselineProbe{}, ErrInvalid
	}
	canonical, err := MarshalBaselineProbe(p)
	if err != nil {
		return BaselineProbe{}, err
	}
	if !bytes.Equal(canonical, data) {
		return BaselineProbe{}, ErrInvalid
	}
	return p, nil
}

func BaselineManifestDigest(m BaselineManifest) (Digest, error) {
	b, err := MarshalBaselineManifest(m)
	if err != nil {
		return Digest{}, err
	}
	return sha256.Sum256(b), nil
}
