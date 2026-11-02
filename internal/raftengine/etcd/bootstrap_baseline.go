package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"path/filepath"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// BaselineStoreProof must come from an independent, read-only store snapshot.
// Missing ordered evidence requires a complete independently replayable WAL
// configuration history; a snapshot's data index is never a replacement.
type BaselineStoreProof struct {
	AppliedIndex, ConfigurationIndex, Watermark uint64
	RaftEnvelopeCutoverIndex                    uint64
	EncryptionContextRequired                   bool
	DataDigest                                  raftbootstrap.Digest
	ConfigurationMembers                        []uint64
	PreparedTransactions                        uint64
}

type BaselineStoreProbe func(context.Context) (BaselineStoreProof, error)

// The caller MUST hold exclusive offline/quiescence ownership throughout every
// call (including the callback). These APIs never stop a running server.
type OfflineBaselineOptions struct {
	Crypto      *OfflineBaselineCrypto
	Ownership   io.Closer
	Preparation *BaselinePreparationCallbacks
	DataDir     string
	MemberID    uint64
	Expected    raftbootstrap.BaselineContext
	ProbeStore  BaselineStoreProbe
}

type OfflineBaselineInspection struct {
	Evidence           raftbootstrap.BaselineMemberEvidence
	HardState          *raftpb.HardState
	Snapshot           *raftpb.Snapshot
	ConfigurationIndex uint64
	RetainedEntries    uint64
}

type baselineInspection struct {
	public    OfflineBaselineInspection
	wal       *baselineWALImage
	auxDigest raftbootstrap.Digest
}

var ErrOfflineBaseline = errors.New("offline baseline verification failed")

func InspectOfflineBaseline(ctx context.Context, options OfflineBaselineOptions) (OfflineBaselineInspection, error) {
	inspection, err := inspectOfflineBaseline(ctx, options, nil)
	if err != nil {
		return OfflineBaselineInspection{}, err
	}
	return inspection.public, nil
}

func inspectOfflineBaseline(ctx context.Context, o OfflineBaselineOptions, identity *WALIdentity) (*baselineInspection, error) {
	if err := validateOfflineBaselineOptions(o); err != nil {
		return nil, err
	}
	image, snapshot, proof, err := readBaselineInput(ctx, o, identity)
	if err != nil {
		return nil, err
	}
	configuration, err := baselineConfiguration(o, image, snapshot, proof)
	if err != nil {
		return nil, err
	}
	proof.ConfigurationIndex = configuration.Index
	if err := validateBaselineCut(o.Expected, image, snapshot, proof); err != nil {
		return nil, err
	}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	expected, err := raftbootstrap.ConfigurationDigest(o.Expected.Configuration)
	if err != nil || digest != expected {
		return nil, errors.Wrap(ErrOfflineBaseline, "configuration mismatch")
	}
	aux, err := baselineTreeDigest(ctx, o.DataDir, walDirName)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/offline-baseline/native/v1\x00"))
	_, _ = h.Write(image.fileDigest[:])
	_, _ = h.Write(aux[:])
	var native raftbootstrap.Digest
	copy(native[:], h.Sum(nil))
	evidence := raftbootstrap.BaselineMemberEvidence{MemberID: o.MemberID, CommitIndex: image.hardState.GetCommit(),
		CommitTerm: o.Expected.CommitTerm, AppliedIndex: proof.AppliedIndex, LastLogIndex: image.lastIndex(),
		ConfigurationDigest: digest, NativeStateDigest: native, DataDigest: proof.DataDigest, Watermark: proof.Watermark}
	hs, _ := proto.Clone(image.hardState).(*raftpb.HardState)
	return &baselineInspection{public: OfflineBaselineInspection{Evidence: evidence, HardState: hs,
		Snapshot: snapshot, ConfigurationIndex: proof.ConfigurationIndex, RetainedEntries: uint64(len(image.entries))}, wal: image, auxDigest: aux}, nil
}

func readBaselineInput(ctx context.Context, o OfflineBaselineOptions, identity *WALIdentity) (*baselineWALImage, *raftpb.Snapshot, BaselineStoreProof, error) {
	var proof BaselineStoreProof
	if _, err := verifyBaselineOwnership(o); err != nil {
		return nil, nil, proof, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, proof, errors.WithStack(err)
	}
	image, err := readOfflineBaselineWAL(ctx, filepath.Join(o.DataDir, walDirName), o.Crypto, false)
	if err != nil {
		return nil, nil, proof, err
	}
	if err := verifyWALIdentity(image.metadata, identity); err != nil {
		return nil, nil, proof, err
	}
	snapshot, err := selectBaselineSnapshot(o.DataDir, image)
	if err != nil {
		return nil, nil, proof, err
	}
	proof, err = o.ProbeStore(ctx)
	if err != nil {
		return nil, nil, proof, errors.WithStack(err)
	}
	if err := validateOfflineBaselineCryptoCut(o.Crypto, proof, image.hardState.GetCommit()); err != nil {
		return nil, nil, proof, err
	}
	if err := validateOfflineBaselineSnapshot(ctx, o.DataDir, snapshot, o.Crypto); err != nil {
		return nil, nil, proof, err
	}
	if !baselineReplayable(o.Expected, image, snapshot) {
		return nil, nil, proof, errors.Wrap(ErrOfflineBaseline, "unreplayable committed prefix")
	}
	return image, snapshot, proof, nil
}

func baselineReplayable(c raftbootstrap.BaselineContext, image *baselineWALImage, snapshot *raftpb.Snapshot) bool {
	commit, index := image.hardState.GetCommit(), snapshot.GetMetadata().GetIndex()
	return commit == c.CommitIndex && index <= commit && commit-index <= uint64(len(image.entries))
}

func validateOfflineBaselineOptions(o OfflineBaselineOptions) error {
	if _, err := baselineTransformationDigest(o); err != nil {
		return err
	}
	if !baselineHasInputs(o) {
		return errors.Wrap(ErrOfflineBaseline, "explicit directory, member and independent store probe required")
	}
	if _, err := raftbootstrap.ConfigurationDigest(o.Expected.Configuration); err != nil {
		return errors.WithStack(err)
	}
	c := o.Expected
	if !baselinePinnedIdentityValid(c) || !baselinePinnedCutValid(c) || !baselinePinnedVersionsValid(c) {
		return errors.Wrap(ErrOfflineBaseline, "invalid pinned cut")
	}
	for _, member := range c.Configuration.Members {
		if member.ID == o.MemberID {
			return nil
		}
	}
	return errors.Wrap(ErrOfflineBaseline, "member absent from pinned roster")
}

func baselineHasInputs(o OfflineBaselineOptions) bool {
	return o.DataDir != "" && o.ProbeStore != nil && o.MemberID != 0
}

func baselinePinnedIdentityValid(c raftbootstrap.BaselineContext) bool {
	return c.Version == raftbootstrap.BaselineVersion && c.Identity.Cluster != (raftbootstrap.UUID{}) && c.OperationID != (raftbootstrap.OperationID{}) && c.Floor == (raftbootstrap.Floor{})
}

func baselinePinnedCutValid(c raftbootstrap.BaselineContext) bool {
	return c.CommitIndex != 0 && c.CommitIndex != ^uint64(0) && c.CommitTerm != 0 && c.DataDigest != (raftbootstrap.Digest{}) &&
		c.Configuration.Index != 0 && c.Configuration.Index <= c.CommitIndex && !c.Configuration.State.AutoLeave
}

func baselinePinnedVersionsValid(c raftbootstrap.BaselineContext) bool {
	return c.Minimum == (raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}) && c.Watermark != ^uint64(0)
}

func validateBaselineCut(c raftbootstrap.BaselineContext, image *baselineWALImage, snapshot *raftpb.Snapshot, proof BaselineStoreProof) error {
	commit := image.hardState.GetCommit()
	if !baselineWALCutMatches(c, image) || !baselineStoreCutMatches(c, proof) {
		return errors.Wrap(ErrOfflineBaseline, "cut, pending tail, prepared transaction or store proof mismatch")
	}
	if !baselineCommitTermMatches(c, image, snapshot) {
		return errors.Wrap(ErrOfflineBaseline, "commit term is not independently proven")
	}
	for index := snapshot.GetMetadata().GetIndex() + 1; index <= commit; index++ {
		if image.entries[index] == nil {
			return errors.Wrap(ErrOfflineBaseline, "missing committed WAL entry")
		}
	}
	if !baselineStoreMembersMatch(c, proof) {
		return errors.Wrap(ErrOfflineBaseline, "ordered store membership mismatch")
	}
	return nil
}

func baselineWALCutMatches(c raftbootstrap.BaselineContext, image *baselineWALImage) bool {
	return image.hardState.GetTerm() != 0 && image.hardState.GetCommit() == c.CommitIndex && image.lastIndex() == c.CommitIndex
}

func baselineStoreCutMatches(c raftbootstrap.BaselineContext, proof BaselineStoreProof) bool {
	return proof.AppliedIndex == c.CommitIndex && proof.Watermark == c.Watermark && proof.DataDigest == c.DataDigest &&
		proof.PreparedTransactions == 0 && proof.ConfigurationIndex != 0 && proof.ConfigurationIndex == c.Configuration.Index
}

func baselineCommitTermMatches(c raftbootstrap.BaselineContext, image *baselineWALImage, snapshot *raftpb.Snapshot) bool {
	term := uint64(0)
	if snapshot.GetMetadata().GetIndex() == c.CommitIndex {
		term = snapshot.GetMetadata().GetTerm()
	}
	if entry := image.entries[c.CommitIndex]; entry != nil {
		if term != 0 && term != entry.GetTerm() {
			return false
		}
		term = entry.GetTerm()
	}
	return term != 0 && term == c.CommitTerm && term <= image.hardState.GetTerm()
}

func baselineStoreMembersMatch(c raftbootstrap.BaselineContext, proof BaselineStoreProof) bool {
	members := namespaceApplyMembers(raftpb.ConfState{Voters: c.Configuration.State.Voters, VotersOutgoing: c.Configuration.State.VotersOutgoing,
		Learners: c.Configuration.State.Learners, LearnersNext: c.Configuration.State.LearnersNext})
	return len(proof.ConfigurationMembers) == 0 || slices.Equal(proof.ConfigurationMembers, members)
}

func baselineAppendUint64(data []byte, value uint64) []byte {
	return binary.BigEndian.AppendUint64(data, value)
}
