package main

import (
	"context"
	"io"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const bootstrapResumePhaseLimit = 3

func inspectBootstrapNativeInput(ctx context.Context, request bootstrapBaselineCLIRequest, expected raftbootstrap.BaselineContext, manifest raftbootstrap.BaselineManifest) (err error) {
	ownership, err := etcd.AcquireOfflineBaselineOwnership(request.RaftDataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, ownership.Close()) }()
	options, err := bootstrapOfflineOptions(request, expected, manifest, ownership)
	if err != nil {
		return err
	}
	observation, err := etcd.InspectOfflineBaseline(ctx, options)
	if err != nil {
		return errors.WithStack(err)
	}
	prepared, err := bootstrapLocalPreparation(request.MemberID, manifest)
	if err != nil {
		return err
	}
	evidence := observation.Evidence
	evidence.PublicKeyDER = prepared.Probe.Evidence.PublicKeyDER
	evidence.Signature = prepared.Probe.Evidence.Signature
	return errors.WithStack(raftbootstrap.VerifyBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: expected, Evidence: evidence}, expected))
}

func bootstrapOfflineOptions(request bootstrapBaselineCLIRequest, expected raftbootstrap.BaselineContext, manifest raftbootstrap.BaselineManifest, ownership io.Closer) (etcd.OfflineBaselineOptions, error) {
	prepared, err := bootstrapLocalPreparation(request.MemberID, manifest)
	if err != nil {
		return etcd.OfflineBaselineOptions{}, err
	}
	data, err := bootstrapMigrationDataCallbacks(prepared)
	if err != nil {
		return etcd.OfflineBaselineOptions{}, err
	}
	options := etcd.OfflineBaselineOptions{DataDir: request.RaftDataDir, MemberID: request.MemberID,
		Expected: expected, Ownership: ownership, Preparation: bootstrapOfflineFormatPreparation(data)}
	options.ProbeStore = func(ctx context.Context) (etcd.BaselineStoreProof, error) {
		if manifest.Identity.Group == dedicatedTSORaftGroupID {
			_, proof, err := probeOfflineTSOBaseline(ctx, request.RaftDataDir, ownership)
			return proof, err
		}
		return bootstrapOfflineDataStoreProbe(ctx, request.RaftDataDir)
	}
	return options, nil
}

func bootstrapMigrationDataCallbacks(prepared kv.PreparedBootstrapActivation) (*etcd.BaselinePreparationCallbacks, error) {
	if prepared.Probe.Identity.Group == dedicatedTSORaftGroupID {
		return bootstrapOfflineTSOPreparation(prepared)
	}
	return bootstrapOfflineDataPreparation(prepared)
}

func bootstrapLocalPreparation(member uint64, manifest raftbootstrap.BaselineManifest) (kv.PreparedBootstrapActivation, error) {
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	if err != nil {
		return kv.PreparedBootstrapActivation{}, errors.WithStack(err)
	}
	for _, evidence := range manifest.Members {
		if evidence.MemberID == member {
			return kv.PreparedBootstrapActivation{ManifestDigest: digest,
				Probe: raftbootstrap.BaselineProbe{BaselineContext: manifest.BaselineContext, Evidence: evidence}}, nil
		}
	}
	return kv.PreparedBootstrapActivation{}, errors.WithStack(raftbootstrap.ErrInconsistent)
}

func executeBootstrapMigration(ctx context.Context, request bootstrapBaselineCLIRequest, expected raftbootstrap.BaselineContext, manifest raftbootstrap.BaselineManifest) (result etcd.OfflineBaselineMigration, err error) {
	if err := raftbootstrap.VerifyBaselineManifest(manifest, expected); err != nil {
		return result, errors.WithStack(err)
	}
	ownership, err := etcd.AcquireOfflineBaselineOwnership(request.RaftDataDir)
	if err != nil {
		return result, errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, ownership.Close()) }()
	options, err := bootstrapOfflineOptions(request, expected, manifest, ownership)
	if err != nil {
		return result, err
	}
	if request.Command == "prepare" {
		result, err = etcd.PrepareOfflineBaseline(ctx, options, manifest)
		return result, errors.WithStack(err)
	}
	if request.Command != "resume" {
		return result, errors.WithStack(raftbootstrap.ErrInvalid)
	}
	for range bootstrapResumePhaseLimit {
		result, err = etcd.ResumeOfflineBaseline(ctx, options, manifest)
		if err != nil {
			return result, errors.WithStack(err)
		}
		if result.Phase == "bound" {
			return result, nil
		}
	}
	return result, errors.New("baseline resume did not reach the bound phase")
}
