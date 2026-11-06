package main

import (
	"context"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func bootstrapRuntimeIdentity(config *bootstrapPeerConfig, files *raftbootstrap.Store, intent raftbootstrap.Intent, st store.MVCCStore, cfg raftengine.FactoryConfig, transport *etcd.GRPCTransport) (etcd.WALIdentity, error) {
	identity, err := validateBootstrapStartup(config, intent, cfg)
	if err != nil {
		return identity, err
	}
	if err := validateBootstrapStartupDependencies(files, st, transport); err != nil {
		return identity, err
	}
	metadata, ok := st.(store.RaftControlMetadataStore)
	if !ok {
		return identity, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	prepared, err := kv.ReadPreparedBootstrapActivation(context.Background(), metadata, identity.Identity)
	if errors.Is(err, store.ErrKeyNotFound) && intent.Floor.ActivationIndex != 0 {
		// Existing activated v1 WAL; native recovery still independently verifies P.
		return identity, validateBootstrapActivationRequest(config.activation, identity)
	}
	if err != nil {
		return identity, errors.WithStack(err)
	}
	identity.PreparationDigest = prepared.ManifestDigest
	if err := validateRuntimePreparedBootstrapIdentity(prepared, identity, config.activation); err != nil {
		return identity, err
	}
	return identity, validateBootstrapActivationRequest(config.activation, identity)
}

func validatePreparedBootstrapIdentity(prepared kv.PreparedBootstrapActivation, identity etcd.WALIdentity) error {
	digest, err := raftbootstrap.ConfigurationDigest(prepared.Probe.Configuration)
	if err != nil {
		return errors.WithStack(err)
	}
	if prepared.Probe.Identity != identity.Identity || prepared.Probe.Evidence.MemberID != identity.NodeID || digest != identity.BaselineDigest {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

func publishPreparedBootstrapRecovery(admission *raftadmission.Arbiter, ticket raftadmission.RecoveryTicket, owner *peersession.Owner, files *raftbootstrap.Store, st store.MVCCStore, identity etcd.WALIdentity, state etcd.RecoveredState, activation []byte) error {
	actual, verified := state.Identity()
	if !verified || actual != identity {
		return errors.WithStack(etcd.ErrWALIdentity)
	}
	metadata, ok := st.(store.RaftControlMetadataStore)
	if !ok {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	prepared, err := kv.ReadPreparedBootstrapActivation(context.Background(), metadata, identity.Identity)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := state.VerifyPreparation(prepared.ManifestDigest); err != nil {
		return errors.WithStack(err)
	}
	if err := validateRuntimePreparedBootstrapIdentity(prepared, identity, activation); err != nil {
		return err
	}
	inspection, err := files.Inspect(raftbootstrap.Evidence{Identity: &identity.Identity, BaselineDigest: &identity.BaselineDigest, RaftState: raftbootstrap.Intact})
	if err != nil {
		return errors.WithStack(err)
	}
	if !preparedInspectionMatches(inspection, prepared, state) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	probe := prepared.Probe
	projection := peersession.Group{ID: identity.Identity.Group, Configuration: probe.Configuration,
		Policy: prepared.ManifestDigest, Minimum: probe.Minimum, State: peersession.Intact,
		Stage: peersession.IdentityReady, Pending: peersession.PendingClear, MarkerReady: true}
	policy := raftbootstrap.PolicyRecord{Configuration: probe.Configuration, LastConfigurationIndex: probe.Configuration.Index}
	if err := validateBootstrapRecoveredConfiguration(policy, state); err != nil {
		return err
	}
	return errors.WithStack(admission.PublishPreparedWith(ticket, probe, prepared.ManifestDigest,
		func(stage peersession.Stage, generation uint64) (func(), error) {
			projection.Stage, projection.Generation = stage, generation
			return owner.UpdateGroupProjection(projection)
		}))
}

func preparedInspectionMatches(inspection raftbootstrap.Inspection, prepared kv.PreparedBootstrapActivation, state etcd.RecoveredState) bool {
	return inspection.Class == raftbootstrap.Required && inspection.Intent != nil &&
		inspection.Intent.BaselineKind == raftbootstrap.Accepted &&
		inspection.Intent.Floor == (raftbootstrap.Floor{}) && inspection.Intent.Minimum == prepared.Probe.Minimum &&
		state.AppliedIndex() >= prepared.Probe.CommitIndex
}

func mirrorRecoveredBootstrapFloor(files *raftbootstrap.Store, policy raftbootstrap.PolicyRecord) error {
	intent, present, err := files.LoadIntent()
	if err != nil {
		return errors.WithStack(err)
	}
	if !present {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	floor := raftbootstrap.Floor{ActivationIndex: policy.ActivationIndex, Generation: policy.Generation}
	if intent.Floor == floor {
		return nil
	}
	intent.Floor = floor
	authoritative := raftbootstrap.Policy{Identity: policy.Identity, BaselineDigest: policy.BaselineDigest, Minimum: policy.Minimum, Floor: floor}
	return errors.WithStack(files.RepairRequired(intent, raftbootstrap.Evidence{Identity: &policy.Identity,
		BaselineDigest: &policy.BaselineDigest, RaftState: raftbootstrap.Intact, Policy: &authoritative}))
}
