package clusterbootstrap

import (
	"bytes"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// A replicated snapshot can contain another member's original preparation row.
// That row is source evidence, never proof of this node's offline migration.
// Before Open, expected is only the desired WAL identity; Open must verify it.
// Before publishing readiness, the caller must separately compare the actual
// RecoveredState identity and VerifyPreparation. Never rewrite J or the WAL.
func ValidateRuntimePreparedBootstrapIdentity(prepared kv.PreparedBootstrapActivation, expected etcd.WALIdentity, activation []byte) error {
	if prepared.Probe.Evidence.MemberID == expected.NodeID {
		if err := ValidatePreparedBootstrapIdentity(prepared, expected); err != nil {
			return err
		}
		if expected.PreparationDigest != (raftbootstrap.Digest{}) && expected.PreparationDigest != prepared.ManifestDigest {
			return errors.WithStack(raftbootstrap.ErrInconsistent)
		}
		return nil
	}
	return validateBootstrapSnapshotSource(prepared, expected, activation)
}

func validateBootstrapSnapshotSource(prepared kv.PreparedBootstrapActivation, expected etcd.WALIdentity, activation []byte) error {
	command, matched, err := raftbootstrap.ParseActivationCommand(activation)
	if err != nil || !matched {
		return errors.Join(raftbootstrap.ErrInconsistent, err)
	}
	// Parsing checks ALL baseline signatures and ALL prepared acknowledgments.
	// This artifact is independently configured, not supplied by the snapshot.
	if err := raftbootstrap.VerifyBaselineManifest(command.Manifest, prepared.Probe.BaselineContext); err != nil {
		return errors.WithStack(err)
	}
	digest, err := raftbootstrap.BaselineManifestDigest(command.Manifest)
	if err != nil {
		return errors.WithStack(err)
	}
	if digest != prepared.ManifestDigest || digest != expected.PreparationDigest || expected.PreparationDigest == (raftbootstrap.Digest{}) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	if err := validateBootstrapSnapshotSourceIdentity(prepared, expected); err != nil {
		return err
	}
	return matchBootstrapSnapshotSourceEvidence(prepared.Probe, command.Manifest)
}

func validateBootstrapSnapshotSourceIdentity(prepared kv.PreparedBootstrapActivation, expected etcd.WALIdentity) error {
	configuration, err := raftbootstrap.ConfigurationDigest(prepared.Probe.Configuration)
	if err != nil {
		return errors.WithStack(err)
	}
	if prepared.Probe.Identity != expected.Identity || configuration != expected.BaselineDigest || !bootstrapBaselineHasMember(prepared.Probe.Configuration, expected.NodeID) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

func matchBootstrapSnapshotSourceEvidence(probe raftbootstrap.BaselineProbe, manifest raftbootstrap.BaselineManifest) error {
	actual, err := raftbootstrap.MarshalBaselineProbe(probe)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, evidence := range manifest.Members {
		if evidence.MemberID != probe.Evidence.MemberID {
			continue
		}
		expected, err := raftbootstrap.MarshalBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: manifest.BaselineContext, Evidence: evidence})
		if err != nil {
			return errors.WithStack(err)
		}
		if !bytes.Equal(actual, expected) {
			return errors.WithStack(raftbootstrap.ErrInconsistent)
		}
		return nil
	}
	return errors.WithStack(raftbootstrap.ErrInconsistent)
}
