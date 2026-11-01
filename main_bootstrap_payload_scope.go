package main

import (
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func bootstrapPayloadValidator(cluster raftbootstrap.UUID) func(raftadmission.Kind, uint64, []byte) error {
	return func(kind raftadmission.Kind, group uint64, payload []byte) error {
		if err := validateBootstrapAdmissionPayload(kind, payload); err != nil {
			return err
		}
		return validateBootstrapControlScope(cluster, group, payload)
	}
}

func validateBootstrapControlScope(cluster raftbootstrap.UUID, group uint64, payload []byte) error {
	if !raftbootstrap.IsReservedControlPayload(payload) {
		return nil
	}
	activation, matched, err := raftbootstrap.ParseActivationCommand(payload)
	if matched {
		if err != nil {
			return errors.WithStack(err)
		}
		return bootstrapWitnessScope(cluster, group, activation.Witness)
	}
	command, matched, err := kv.ParseGuardedBootstrapCommand(payload)
	if err != nil || !matched {
		return errors.Join(raftbootstrap.ErrInvalid, err)
	}
	return bootstrapWitnessScope(cluster, group, command.Witness)
}

func bootstrapWitnessScope(cluster raftbootstrap.UUID, group uint64, witness raftbootstrap.Witness) error {
	if witness.Cluster != cluster || witness.Group != group {
		return errors.WithStack(raftadmission.ErrClosed)
	}
	return nil
}
