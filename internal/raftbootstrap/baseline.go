package raftbootstrap

import (
	"bytes"
	"crypto/sha256"
	"slices"
)

const BaselineVersion uint32 = 1

// BaselineContext identifies an explicitly agreed new cut, not proof of past
// correctness. The operator must generate a fresh OperationID for each attempt.
// Initial enablement has no prior activation floor; Minimum binds format floors.
type BaselineContext struct {
	Version                            uint32
	Identity                           Identity
	OperationID                        OperationID
	Configuration                      Configuration
	Minimum                            Versions
	Floor                              Floor
	CommitIndex, CommitTerm, Watermark uint64
	DataDigest                         Digest
}

type BaselineMemberEvidence struct {
	MemberID                                            uint64
	CommitIndex, CommitTerm, AppliedIndex, LastLogIndex uint64
	ConfigurationDigest, NativeStateDigest, DataDigest  Digest
	Watermark                                           uint64
	PublicKeyDER, Signature                             []byte
}

// BaselineProbe is signed only after the offline reader rejects pending tails
// and old prepared transactions. NativeStateDigest is node-specific and must be
// rechecked against the selected WAL/snapshot and HardState at local prepare.
type BaselineProbe struct {
	BaselineContext
	Evidence BaselineMemberEvidence
}

type BaselineManifest struct {
	BaselineContext
	Members []BaselineMemberEvidence
}

func canonicalBaselineContext(c BaselineContext) (BaselineContext, error) {
	if c.Version != BaselineVersion {
		return BaselineContext{}, ErrUnsupported
	}
	if !validBaselineCut(c) {
		return BaselineContext{}, ErrInvalid
	}
	if err := validateVersions(c.Minimum); err != nil {
		return BaselineContext{}, err
	}
	configuration, err := canonicalConfiguration(c.Configuration)
	if err != nil {
		return BaselineContext{}, err
	}
	// Auto-leave can change the configuration without an explicit new cut.
	if configuration.State.AutoLeave {
		return BaselineContext{}, ErrInvalid
	}
	c.Configuration = configuration
	return c, nil
}

func validBaselineCut(c BaselineContext) bool {
	return c.Identity.Cluster != (UUID{}) && c.OperationID != (OperationID{}) &&
		c.Configuration.Index != 0 && c.Configuration.Index <= c.CommitIndex &&
		c.CommitIndex != 0 && c.CommitTerm != 0 && c.DataDigest != (Digest{}) &&
		c.Floor == (Floor{}) && c.Watermark != ^uint64(0)
}

func validateBaselineEvidence(c BaselineContext, e BaselineMemberEvidence, signed bool) error {
	digest, err := ConfigurationDigest(c.Configuration)
	if err != nil {
		return err
	}
	return validateBaselineEvidenceDigest(c, e, signed, digest)
}

func validateBaselineEvidenceDigest(c BaselineContext, e BaselineMemberEvidence, signed bool, digest Digest) error {
	member, ok := configurationMember(c.Configuration, e.MemberID)
	if !ok || !baselineEvidenceMatchesCut(c, e, digest) ||
		len(e.PublicKeyDER) == 0 || len(e.PublicKeyDER) > baselineKeyLimit ||
		len(e.Signature) > baselineSignatureLimit || (signed && len(e.Signature) == 0) ||
		sha256.Sum256(e.PublicKeyDER) != member.PublicKeyDigest {
		return ErrInvalid
	}
	return nil
}

func baselineEvidenceMatchesCut(c BaselineContext, e BaselineMemberEvidence, digest Digest) bool {
	return e.CommitIndex == c.CommitIndex && e.CommitTerm == c.CommitTerm &&
		e.AppliedIndex == c.CommitIndex && e.LastLogIndex == c.CommitIndex &&
		e.ConfigurationDigest == digest && e.NativeStateDigest != (Digest{}) &&
		e.DataDigest == c.DataDigest && e.Watermark == c.Watermark
}

func canonicalBaselineManifest(m BaselineManifest) (BaselineManifest, error) {
	if err := baselineSize(m.BaselineContext, m.Members); err != nil {
		return BaselineManifest{}, err
	}
	c, err := canonicalBaselineContext(m.BaselineContext)
	if err != nil {
		return BaselineManifest{}, err
	}
	if len(m.Members) != len(c.Configuration.Members) {
		return BaselineManifest{}, ErrInvalid
	}
	m.BaselineContext = c
	m.Members = slices.Clone(m.Members)
	slices.SortFunc(m.Members, func(a, b BaselineMemberEvidence) int { return compareID(a.MemberID, b.MemberID) })
	configurationDigest, err := ConfigurationDigest(c.Configuration)
	if err != nil {
		return BaselineManifest{}, err
	}
	contextDigest := baselineContextDigest(c)
	for i, e := range m.Members {
		if e.MemberID != c.Configuration.Members[i].ID {
			return BaselineManifest{}, ErrInvalid
		}
		if err := verifyBaselineEvidenceDigests(c, e, configurationDigest, contextDigest); err != nil {
			return BaselineManifest{}, err
		}
	}
	return m, nil
}

// VerifyBaselineManifest requires independent trusted context, including the
// fresh preparation ID and pinned roster; a self-consistent foreign frame is
// not authorization. The caller owns ID freshness and local native rechecks.
func VerifyBaselineManifest(m BaselineManifest, expected BaselineContext) error {
	m, err := canonicalBaselineManifest(m)
	if err != nil {
		return err
	}
	return matchBaselineContext(m.BaselineContext, expected)
}

func VerifyBaselineProbe(p BaselineProbe, expected BaselineContext) error {
	if err := baselineSize(p.BaselineContext, []BaselineMemberEvidence{p.Evidence}); err != nil {
		return err
	}
	c, err := canonicalBaselineContext(p.BaselineContext)
	if err != nil {
		return err
	}
	if err := matchBaselineContext(c, expected); err != nil {
		return err
	}
	return verifyBaselineEvidence(c, p.Evidence)
}

func matchBaselineContext(actual, expected BaselineContext) error {
	expected, err := canonicalBaselineContext(expected)
	if err != nil {
		return err
	}
	a, b := &encoder{limit: MaxFileSize}, &encoder{limit: MaxFileSize}
	encodeBaselineContext(a, actual)
	encodeBaselineContext(b, expected)
	if a.err != nil || b.err != nil || !bytes.Equal(a.Bytes(), b.Bytes()) {
		return ErrInconsistent
	}
	return nil
}
