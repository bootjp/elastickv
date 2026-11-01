package etcd

import (
	"context"
	"io"
	"path/filepath"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// The roster, identity, fresh operation ID and minimum are independently pinned
// operator inputs. No cut, data digest, epoch or timestamp is accepted as input.
type OfflineBaselineObservationOptions struct {
	DataDir     string
	Ownership   io.Closer
	MemberID    uint64
	Identity    raftbootstrap.Identity
	OperationID raftbootstrap.OperationID
	Minimum     raftbootstrap.Versions
	Members     []raftbootstrap.Member
	ProbeStore  BaselineStoreProbe
}

// ObserveOfflineBaseline measures an UNSIGNED initial legacy cut. This is not
// historical correctness, quorum agreement, accepted preparation or readiness.
// Caller may sign only after matching its provisioned certificate/member key.
func ObserveOfflineBaseline(ctx context.Context, options OfflineBaselineObservationOptions) (raftbootstrap.BaselineProbe, error) {
	var empty raftbootstrap.BaselineProbe
	o := OfflineBaselineOptions{DataDir: options.DataDir, Ownership: options.Ownership, MemberID: options.MemberID, ProbeStore: options.ProbeStore}
	if !baselineHasInputs(o) {
		return empty, errors.Wrap(ErrOfflineBaseline, "observation requires independent offline store probe and member")
	}
	if _, err := verifyBaselineOwnership(o); err != nil {
		return empty, err
	}
	if err := baselineObservationFormat(o.DataDir); err != nil {
		return empty, err
	}
	image, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	if err != nil {
		return empty, err
	}
	if err := verifyWALIdentity(image.metadata, nil); err != nil {
		return empty, err
	}
	snapshot, err := selectBaselineSnapshot(o.DataDir, image)
	if err != nil {
		return empty, err
	}
	proof, err := options.ProbeStore(ctx)
	if err != nil {
		return empty, errors.WithStack(err)
	}
	return finishObservedBaseline(ctx, options, o, image, snapshot, proof)
}

func baselineObservationFormat(dir string) error {
	inspection, err := raftbootstrap.NewStore(dir).Inspect(raftbootstrap.Evidence{})
	if err != nil {
		return errors.WithStack(err)
	}
	if inspection.Intent != nil {
		return errors.Wrap(ErrOfflineBaseline, "an existing intent must resume its original preparation")
	}
	if inspection.Marker != raftbootstrap.MarkerMissing && inspection.Marker != raftbootstrap.MarkerLegacy {
		return errors.Wrap(ErrOfflineBaseline, "initial observation requires an unprepared legacy source")
	}
	return nil
}

func finishObservedBaseline(ctx context.Context, options OfflineBaselineObservationOptions, o OfflineBaselineOptions, image *baselineWALImage, snapshot *raftpb.Snapshot, proof BaselineStoreProof) (raftbootstrap.BaselineProbe, error) {
	var empty raftbootstrap.BaselineProbe
	commit := image.hardState.GetCommit()
	if !baselineSourceCutComplete(image, snapshot.GetMetadata().GetIndex(), commit) {
		return empty, errors.Wrap(ErrOfflineBaseline, "observed source has pending or missing committed state")
	}
	o.Expected.Configuration.Members = slices.Clone(options.Members)
	configuration, err := baselineConfiguration(o, image, snapshot, proof)
	if err != nil {
		return empty, err
	}
	term, err := baselineObservedCommitTerm(image, snapshot)
	if err != nil {
		return empty, err
	}
	o.Expected = raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion, Identity: options.Identity, OperationID: options.OperationID,
		Minimum: options.Minimum, Configuration: configuration, CommitIndex: commit, CommitTerm: term, DataDigest: proof.DataDigest, Watermark: proof.Watermark}
	// A second strict read checks the measured cut rather than normalizing a
	// moving source. All pending/applied/namespace/data checks remain shared.
	inspection, err := InspectOfflineBaseline(ctx, o)
	if err != nil {
		return empty, err
	}
	return raftbootstrap.BaselineProbe{BaselineContext: o.Expected, Evidence: inspection.Evidence}, nil
}

func baselineObservedCommitTerm(image *baselineWALImage, snapshot *raftpb.Snapshot) (uint64, error) {
	commit := image.hardState.GetCommit()
	term := uint64(0)
	if snapshot.GetMetadata().GetIndex() == commit {
		term = snapshot.GetMetadata().GetTerm()
	}
	if entry := image.entries[commit]; entry != nil {
		if term != 0 && term != entry.GetTerm() {
			return 0, errors.Wrap(ErrOfflineBaseline, "conflicting observed commit term")
		}
		term = entry.GetTerm()
	}
	if term == 0 || term > image.hardState.GetTerm() {
		return 0, errors.Wrap(ErrOfflineBaseline, "unverifiable observed commit term")
	}
	return term, nil
}
