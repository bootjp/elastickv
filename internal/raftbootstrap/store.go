package raftbootstrap

import (
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
)

type RaftState uint8

const (
	Unverified      RaftState = iota
	Intact                    // Caller verified HardState, WAL-selected snapshot/log and rollback lineage.
	Lost                      // Missing/corrupt WAL, lost term/vote or unverifiable rollback.
	ProvisionedSeed           // Caller proved no existing data/identity and explicit cold provisioning.
)

// Policy is a projection of recovered authoritative P, never inferred from the
// local marker. BaselineDigest identifies the original seed/accepted baseline,
// not a later current membership configuration.
type Policy struct {
	Identity       Identity
	BaselineDigest Digest
	Minimum        Versions
	Floor          Floor
}

// Evidence must come from the startup owner's independent WAL/snapshot/policy
// recovery. Nil identity/digest or Unverified state cannot yield Required.
type Evidence struct {
	Identity       *Identity
	BaselineDigest *Digest
	RaftState      RaftState
	Policy         *Policy
}

type Classification string

const (
	Absent       Classification = "absent" // Not proof of an empty/fresh store.
	Legacy       Classification = "legacy" // Explicit rollout/baseline decision required.
	Recovery     Classification = "recovery-only"
	Required     Classification = "required-format" // File format only, NOT runtime readiness.
	LostState    Classification = "lost-state"
	Corrupt      Classification = "corrupt"
	Unsupported  Classification = "unsupported"
	Inconsistent Classification = "inconsistent"
)

type MarkerState uint8

const (
	MarkerMissing MarkerState = iota
	MarkerLegacy
	MarkerRequired
	MarkerCorrupt
	MarkerUnknown
)

type Inspection struct {
	Class  Classification
	Marker MarkerState
	Intent *Intent
}

// Store requires an existing directory and a single startup owner. Its mutex
// serializes calls through this instance only; other processes/instances must
// not publish concurrently. It never creates directories or opens a Raft engine.
type Store struct {
	dir string
	mu  sync.Mutex
	ops fileOps
}

func NewStore(dir string) *Store { return &Store{dir: dir, ops: realFileOps()} }

func ParseMarker(data []byte) (MarkerState, error) {
	switch string(data) {
	case MarkerV1:
		return MarkerRequired, nil
	case "etcd", "etcd\n":
		return MarkerLegacy, nil
	case "":
		return MarkerCorrupt, ErrInvalid
	default:
		return MarkerUnknown, ErrUnsupported
	}
}

// Inspect performs no repairs. All partial pairs remain closed. File validity
// alone says nothing about voting eligibility, sessions or data admission.
func (s *Store) Inspect(e Evidence) (Inspection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inspect(e)
}

func (s *Store) inspect(e Evidence) (Inspection, error) {
	r := Inspection{Class: Recovery}
	b, present, err := readBounded(s.dir, MarkerFile)
	if err != nil {
		return classifiedError(r, err)
	}
	if present {
		r.Marker, err = ParseMarker(b)
		if err != nil {
			return classifiedError(r, err)
		}
	}
	b, present, err = readBounded(s.dir, IntentFile)
	if err != nil {
		return classifiedError(r, err)
	}
	if present {
		in, parseErr := ParseIntent(b)
		if parseErr != nil {
			return classifiedError(r, parseErr)
		}
		r.Intent = &in
		if err := checkEvidence(in, e); err != nil {
			return classifiedError(r, err)
		}
	}
	return classifyPair(r, e), nil
}

func classifiedError(r Inspection, err error) (Inspection, error) {
	switch {
	case errors.Is(err, ErrUnsupported):
		r.Class = Unsupported
	case errors.Is(err, ErrInconsistent):
		r.Class = Inconsistent
	default:
		r.Class = Corrupt
	}
	return r, err
}

func classifyPair(r Inspection, e Evidence) Inspection {
	if e.RaftState == Lost {
		r.Class = LostState
		return r
	}
	if r.Intent == nil {
		r.Class = classifyWithoutIntent(r.Marker, e.Policy)
		return r
	}
	if r.Marker != MarkerRequired || e.Identity == nil || e.BaselineDigest == nil {
		return r
	}
	if !verifiedRaft(e.RaftState) {
		return r
	}
	if !floorMirrored(r.Intent.Floor, e.Policy) {
		return r
	}
	r.Class = Required
	return r
}

func verifiedRaft(state RaftState) bool { return state == Intact || state == ProvisionedSeed }

func classifyWithoutIntent(marker MarkerState, p *Policy) Classification {
	if p != nil || marker == MarkerRequired {
		return Recovery
	}
	if marker == MarkerLegacy {
		return Legacy
	}
	return Absent
}

func floorMirrored(f Floor, p *Policy) bool {
	if p == nil {
		return f == (Floor{})
	}
	return p.Floor == f
}

func checkEvidence(in Intent, e Evidence) error {
	if e.RaftState == ProvisionedSeed && in.BaselineKind != Seed {
		return ErrInconsistent
	}
	digest, err := ConfigurationDigest(in.Baseline)
	if err != nil {
		return err
	}
	if e.Identity != nil && *e.Identity != in.Identity {
		return ErrInconsistent
	}
	if e.BaselineDigest != nil && *e.BaselineDigest != digest {
		return ErrInconsistent
	}
	return checkPolicy(in, digest, e.Policy)
}

func checkPolicy(in Intent, digest Digest, p *Policy) error {
	if p == nil {
		return nil
	}
	if err := validateVersions(p.Minimum); err != nil {
		return err
	}
	if p.Identity != in.Identity || p.BaselineDigest != digest || p.Minimum != in.Minimum {
		return ErrInconsistent
	}
	if !validFloor(p.Floor) || p.Floor.ActivationIndex == 0 {
		return ErrInconsistent
	}
	return checkFloor(in.Floor, p.Floor)
}

func checkFloor(local, authoritative Floor) error {
	if local.ActivationIndex != 0 && local.ActivationIndex != authoritative.ActivationIndex {
		return ErrInconsistent
	}
	if local.Generation > authoritative.Generation {
		return ErrInconsistent
	}
	return nil
}

// Publish durably publishes intent BEFORE the incompatible marker, including
// marker repair when recovered P requires it. Nil means both directory syncs
// succeeded, not that policy, sessions, catalog or runtime admission are ready.
// The caller must keep every relevant admission lane closed on any error,
// including an indeterminate error after rename. Retry reconciles actual files.
// Unknown/corrupt files are never overwritten. Recovery cannot manufacture WAL
// history or authorize an established identity to use the cold seed path.
func (s *Store) Publish(in Intent, e Evidence) error {
	return s.publish(in, e, validatePublication)
}

// RepairRequired mirrors independently recovered, retained P while admission
// stays closed. Unlike Publish it also permits lost/unverified WAL: repairing
// the downgrade barrier cannot restore lost term/vote history. Inspect still
// reports LostState/Recovery afterwards. The caller must not interpret nil as
// voting, registration, transport, policy/session or data readiness.
func (s *Store) RepairRequired(in Intent, e Evidence) error {
	return s.publish(in, e, validateRepair)
}

func validateRepair(in Intent, e Evidence) error {
	if e.Policy == nil {
		return ErrInconsistent
	}
	return validatePublicationEvidence(in, e)
}

func (s *Store) publish(in Intent, e Evidence, validate func(Intent, Evidence) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := MarshalIntent(in)
	if err != nil {
		return err
	}
	if err := validate(in, e); err != nil {
		return err
	}
	old, err := s.inspect(e)
	if err != nil {
		return err
	}
	if err := validateExisting(old, in, e); err != nil {
		return err
	}
	if err := s.replace(IntentFile, data); err != nil {
		return err
	}
	return s.replace(MarkerFile, []byte(MarkerV1))
}

func validateExisting(old Inspection, in Intent, e Evidence) error {
	if err := validateSeedOrigin(old, in, e); err != nil {
		return err
	}
	if old.Intent != nil {
		if err := monotoneIntent(*old.Intent, in); err != nil {
			return err
		}
	}
	if old.Marker == MarkerRequired && old.Intent == nil && e.Policy == nil {
		return ErrInconsistent
	}
	return validateColdSeedPair(old, in, e)
}

func validateColdSeedPair(old Inspection, in Intent, e Evidence) error {
	if e.RaftState == ProvisionedSeed && (old.Marker == MarkerLegacy || e.Policy != nil || in.Floor != (Floor{})) {
		return ErrInconsistent
	}
	return nil
}

func validateSeedOrigin(old Inspection, in Intent, e Evidence) error {
	if in.BaselineKind != Seed || old.Intent != nil || e.Policy != nil {
		return nil
	}
	if e.RaftState != ProvisionedSeed {
		return ErrInconsistent
	}
	return nil
}

func validatePublication(in Intent, e Evidence) error {
	if !verifiedRaft(e.RaftState) {
		return ErrInconsistent
	}
	return validatePublicationEvidence(in, e)
}

func validatePublicationEvidence(in Intent, e Evidence) error {
	if e.Identity == nil || e.BaselineDigest == nil {
		return ErrInconsistent
	}
	if err := checkEvidence(in, e); err != nil {
		return err
	}
	if !floorMirrored(in.Floor, e.Policy) {
		return ErrInconsistent
	}
	return nil
}

func monotoneIntent(old, next Intent) error {
	a, err := ConfigurationDigest(old.Baseline)
	if err != nil {
		return err
	}
	b, err := ConfigurationDigest(next.Baseline)
	if err != nil {
		return err
	}
	if old.Identity != next.Identity || old.BaselineKind != next.BaselineKind || a != b || old.Minimum != next.Minimum {
		return ErrInconsistent
	}
	if old.Floor.Generation > next.Floor.Generation {
		return ErrInconsistent
	}
	if old.Floor.ActivationIndex != 0 && old.Floor.ActivationIndex != next.Floor.ActivationIndex {
		return ErrInconsistent
	}
	return nil
}

func readBounded(dir, name string) (_ []byte, present bool, err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(root.Close())) }()
	info, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != filePerm || info.Size() > MaxFileSize {
		return nil, true, ErrInvalid
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, true, errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, errors.WithStack(f.Close())) }()
	b, err := io.ReadAll(io.LimitReader(f, MaxFileSize+1))
	if err != nil {
		return nil, true, errors.WithStack(err)
	}
	if len(b) > MaxFileSize {
		return nil, true, ErrInvalid
	}
	return b, true, nil
}
