package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

// OfflineBaselinePreparedEvidence is signer input, not an all-member certificate
// and not authorization to start serving. Produce it only while every current
// member remains quiesced. The caller signs/aggregates it before P activation.
type OfflineBaselinePreparedEvidence struct {
	ManifestDigest                                                       raftbootstrap.Digest
	MemberID                                                             uint64
	Identity                                                             WALIdentity
	Minimum                                                              raftbootstrap.Versions
	CommitIndex, CommitTerm, AppliedIndex, ConfigurationIndex, Watermark uint64
	TargetDataDigest, TargetAuxDigest, NativeWALDigest, BackupDigest     raftbootstrap.Digest
	DurableProofDigest                                                   raftbootstrap.Digest
}

// VerifyBoundOfflineBaseline returns the PreparedMemberAck.PreparedStateDigest.
// Baseline signatures and phase "prepared" alone cannot produce this proof.
func VerifyBoundOfflineBaseline(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest) (raftbootstrap.Digest, error) {
	evidence, err := VerifyOfflineBaselinePrepared(ctx, o, manifest)
	return evidence.DurableProofDigest, err
}

func VerifyOfflineBaselinePrepared(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest) (OfflineBaselinePreparedEvidence, error) {
	var evidence OfflineBaselinePreparedEvidence
	if err := validateBaselineAckOptions(o, manifest); err != nil {
		return evidence, err
	}
	paths := baselineMigrationPaths(o)
	journal, err := readBaselineJournal(paths.Directory)
	if err != nil {
		return evidence, err
	}
	if err := verifyPreparedBaselineJournal(o, manifest, journal); err != nil {
		return evidence, err
	}
	if err := verifyCompleteBaselineBackup(ctx, o, manifest, paths, journal); err != nil {
		return evidence, err
	}
	identity, err := baselineDestinationIdentity(o, journal.manifest)
	if err != nil {
		return evidence, err
	}
	inspection, _, err := verifyBaselinePreparedTarget(ctx, o, manifest, paths, journal, identity)
	if err != nil {
		return evidence, err
	}
	if inspection.wal.fileDigest != journal.newWAL {
		return evidence, errors.Wrap(ErrOfflineBaseline, "PreparedAck requires the exact exchanged WAL")
	}
	intent, err := verifyBaselineRequiredIntent(o, identity)
	if err != nil {
		return evidence, err
	}
	evidence = OfflineBaselinePreparedEvidence{ManifestDigest: journal.manifest, MemberID: o.MemberID, Identity: *identity, Minimum: o.Expected.Minimum,
		CommitIndex: inspection.public.Evidence.CommitIndex, CommitTerm: inspection.public.Evidence.CommitTerm, AppliedIndex: inspection.public.Evidence.AppliedIndex,
		ConfigurationIndex: inspection.public.ConfigurationIndex, Watermark: inspection.public.Evidence.Watermark,
		TargetDataDigest: journal.preparedData, TargetAuxDigest: journal.preparedAux, NativeWALDigest: journal.newWAL, BackupDigest: journal.backup}
	evidence.DurableProofDigest, err = baselinePreparedEvidenceDigest(evidence, intent)
	return evidence, err
}

func validateBaselineAckOptions(o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest) error {
	if o.Preparation == nil {
		return errors.Wrap(ErrOfflineBaseline, "exact prepared transformation verifier required for PreparedAck")
	}
	if err := raftbootstrap.VerifyBaselineManifest(manifest, o.Expected); err != nil {
		return errors.WithStack(err)
	}
	if _, err := verifyBaselineOwnership(o); err != nil {
		return err
	}
	return nil
}

func verifyPreparedBaselineJournal(o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, journal baselineMigrationJournal) error {
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	if err != nil {
		return errors.WithStack(err)
	}
	transformation, err := baselineTransformationDigest(o)
	if err != nil {
		return err
	}
	if journal.phase != baselineBound || journal.manifest != digest || journal.transformation != transformation {
		return errors.Wrap(ErrOfflineBaseline, "PreparedAck requires a completed matching migration")
	}
	return nil
}

func verifyBaselineRequiredIntent(o OfflineBaselineOptions, identity *WALIdentity) ([]byte, error) {
	files := raftbootstrap.NewStore(o.DataDir)
	inspection, err := files.Inspect(raftbootstrap.Evidence{Identity: &identity.Identity, BaselineDigest: &identity.BaselineDigest, RaftState: raftbootstrap.Intact})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if inspection.Marker != raftbootstrap.MarkerRequired || inspection.Intent == nil {
		return nil, errors.Wrap(ErrOfflineBaseline, "PreparedAck requires durable required marker and intent")
	}
	expected, err := raftbootstrap.MarshalIntent(raftbootstrap.Intent{Identity: o.Expected.Identity, BaselineKind: raftbootstrap.Accepted, Baseline: o.Expected.Configuration, Minimum: o.Expected.Minimum, Floor: raftbootstrap.Floor{}})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	actual, err := raftbootstrap.MarshalIntent(*inspection.Intent)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !bytes.Equal(actual, expected) {
		return nil, errors.Wrap(ErrOfflineBaseline, "prepared intent is not the accepted baseline")
	}
	for _, name := range []string{raftbootstrap.IntentFile, raftbootstrap.MarkerFile} {
		if err := baselineSyncFile(filepath.Join(o.DataDir, name)); err != nil {
			return nil, err
		}
	}
	return actual, baselineSyncDirectory(o.DataDir)
}

func baselineSyncFile(path string) error {
	if err := baselineRegularFile(path); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	return errors.WithStack(f.Sync())
}

func baselinePreparedEvidenceDigest(e OfflineBaselinePreparedEvidence, intent []byte) (raftbootstrap.Digest, error) {
	identity, err := encodeWALIdentity(&e.Identity)
	if err != nil {
		return raftbootstrap.Digest{}, err
	}
	data := []byte("elastickv/offline-baseline/prepared-member/v1\x00")
	data = append(data, e.ManifestDigest[:]...)
	for _, v := range []uint64{e.MemberID, uint64(len(identity)), uint64(e.Minimum.Entry), uint64(e.Minimum.Snapshot), uint64(e.Minimum.Session), e.CommitIndex, e.CommitTerm, e.AppliedIndex, e.ConfigurationIndex, e.Watermark} {
		data = baselineAppendUint64(data, v)
	}
	data = append(data, identity...)
	for _, digest := range []raftbootstrap.Digest{e.TargetDataDigest, e.TargetAuxDigest, e.NativeWALDigest, e.BackupDigest} {
		data = append(data, digest[:]...)
	}
	data = baselineAppendUint64(data, uint64(len(intent)))
	data = append(data, intent...)
	return sha256.Sum256(data), nil
}
