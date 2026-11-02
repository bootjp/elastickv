package kv

import (
	"context"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// GuardedTSOStateMachine adds bootstrap control to a durably backed TSO. The
// native owner supplies committed identities and independent ordered proof;
// payloads, preparation records and local readiness are never that authority.
type GuardedTSOStateMachine struct {
	*TSOStateMachine
	identity                   raftbootstrap.Identity
	entry                      raftengine.AppliedEntryIdentity
	proof                      func(context.Context) (BootstrapActivationNativeProof, error)
	pendingNativeConfiguration *raftengine.NativeConfiguration
}

func NewGuardedTSOStateMachine(state *TSOStateMachine, identity raftbootstrap.Identity, proof func(context.Context) (BootstrapActivationNativeProof, error)) (*GuardedTSOStateMachine, error) {
	if state == nil || state.backing == nil || identity.Cluster == (raftbootstrap.UUID{}) || identity.Group != 0 {
		return nil, errors.WithStack(ErrTSODurableBacking)
	}
	return &GuardedTSOStateMachine{TSOStateMachine: state, identity: identity, proof: proof}, nil
}

func (f *GuardedTSOStateMachine) NativeGroupIdentity() raftbootstrap.Identity { return f.identity }

func (f *GuardedTSOStateMachine) SetApplyIndex(index uint64) {
	f.entry = raftengine.AppliedEntryIdentity{}
	f.ClearApplyNativeConfiguration()
	f.TSOStateMachine.SetApplyIndex(index)
}

func (f *GuardedTSOStateMachine) SetApplyEntryIdentity(entry raftengine.AppliedEntryIdentity) {
	f.entry = entry
	f.TSOStateMachine.SetApplyIndex(entry.Index)
}

func (f *GuardedTSOStateMachine) ClearApplyEntryIdentity() {
	f.entry = raftengine.AppliedEntryIdentity{}
	f.ClearApplyNativeConfiguration()
	f.TSOStateMachine.SetApplyIndex(0)
}

// The native owner delivers this image BEFORE entry identity on its serial
// loop, including cold replay before a runtime Engine pointer exists.
func (f *GuardedTSOStateMachine) SetApplyNativeConfiguration(configuration raftengine.NativeConfiguration) {
	configuration.Voters = slices.Clone(configuration.Voters)
	configuration.VotersOutgoing = slices.Clone(configuration.VotersOutgoing)
	configuration.Learners = slices.Clone(configuration.Learners)
	configuration.LearnersNext = slices.Clone(configuration.LearnersNext)
	configuration.Members = slices.Clone(configuration.Members)
	f.pendingNativeConfiguration = &configuration
}

func (f *GuardedTSOStateMachine) ClearApplyNativeConfiguration() { f.pendingNativeConfiguration = nil }

func (f *GuardedTSOStateMachine) activationNativeProof(ctx context.Context, metadata store.RaftControlMetadataStore) (BootstrapActivationNativeProof, error) {
	if f.pendingNativeConfiguration != nil {
		prepared, err := ReadPreparedBootstrapActivation(ctx, metadata, f.identity)
		if err != nil {
			return BootstrapActivationNativeProof{}, err
		}
		return BootstrapActivationProofFromNative(f.identity, *f.pendingNativeConfiguration, prepared)
	}
	if f.proof == nil {
		return BootstrapActivationNativeProof{}, errors.WithStack(ErrBootstrapActivationNativeProof)
	}
	proof, err := f.proof(ctx)
	return proof, errors.Wrap(err, "read TSO activation native proof")
}

// SetBootstrapActivationNativeProof is startup wiring only. The getter must
// not wait on the serial native loop that is currently invoking Apply.
func (f *GuardedTSOStateMachine) SetBootstrapActivationNativeProof(proof func(context.Context) (BootstrapActivationNativeProof, error)) {
	f.proof = proof
}

func (f *GuardedTSOStateMachine) committedIdentity(payload []byte) (raftbootstrap.EntryIdentity, error) {
	e := f.entry
	if e.Index == 0 || e.Term == 0 || e.Type != 0 || e != raftengine.NewAppliedEntryIdentity(e.Index, e.Term, e.Type, payload) {
		return raftbootstrap.EntryIdentity{}, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
	}
	return raftbootstrap.EntryIdentity{Cluster: f.identity.Cluster, Group: f.identity.Group,
		Index: e.Index, Term: e.Term, Digest: raftbootstrap.Digest(e.Digest)}, nil
}

func (f *GuardedTSOStateMachine) Apply(payload []byte) any {
	if len(payload) == 0 {
		return haltErr(errors.WithStack(ErrTSOStateMachineInvalidEntry))
	}
	identity, err := f.committedIdentity(payload)
	if err != nil {
		return guardedBootstrapHalt(err, "bind TSO committed entry")
	}
	if !raftbootstrap.IsReservedControlPayload(payload) {
		// Existing TSO entries retain their codecs and native admission lane.
		// Only a protected active policy permits these fresh scalar transitions.
		return f.ApplyWithControl(context.Background(), identity.Index, payload, func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
			_, err := readAppliedBootstrapPolicy(ctx, metadata, f.identity, identity.Index-1)
			return err
		})
	}
	result, err := f.applyBootstrapControl(context.Background(), identity, payload)
	if err != nil {
		return guardedBootstrapHalt(err, "apply TSO bootstrap control")
	}
	return result
}

func (f *GuardedTSOStateMachine) readReceipt(ctx context.Context, identity raftbootstrap.EntryIdentity, payload []byte) (any, bool, error) {
	key, err := raftbootstrap.ReceiptKey(identity)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	raw, err := f.backing.ReadRaftControlMetadata(ctx, key)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return matchBootstrapReceiptVariant(raw, identity, payload)
}

func (f *GuardedTSOStateMachine) applyBootstrapControl(ctx context.Context, identity raftbootstrap.EntryIdentity, payload []byte) (any, error) {
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	if receipt, found, err := f.readReceipt(ctx, identity, payload); err != nil || found {
		return receipt, err
	}
	activation, matched, err := raftbootstrap.ParseActivationCommand(payload)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if matched {
		return f.applyActivation(ctx, identity, activation)
	}
	command, _, err := ParseGuardedBootstrapCommand(payload)
	if err != nil {
		return nil, err
	}
	if command.Kind != raftbootstrap.ControlGuardedWatermark {
		return nil, errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	return f.applyWatermark(ctx, identity, command)
}

func (f *GuardedTSOStateMachine) applyActivation(ctx context.Context, identity raftbootstrap.EntryIdentity, command raftbootstrap.ActivationCommand) (any, error) {
	candidate := f.controlCandidate()
	var receipt raftbootstrap.ActivationReceipt
	err := f.commitControlState(ctx, identity.Index, candidate, f.configurationIndex.Load(), func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
		_, active, err := activationExistingPolicy(ctx, metadata, identity)
		if err != nil {
			return err
		}
		var proof BootstrapActivationNativeProof
		if !active {
			proof, err = f.activationNativeProof(ctx, metadata)
			if err != nil {
				return errors.Join(ErrBootstrapActivationNativeProof, err)
			}
			if command.Manifest.Configuration.Index != f.AppliedConfigurationIndex() {
				return errors.WithStack(ErrBootstrapActivationNativeProof)
			}
		}
		receipt, err = StageBootstrapActivation(ctx, metadata, identity, command, proof, candidate.AllocationFloor())
		return err
	})
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

// commitControlState is the same synced batch/publication boundary used by
// ApplyWithControl. The candidate here must be changed INSIDE the callback:
// witness/reservation rejection must precede both floor and ceiling effects.
func (f *GuardedTSOStateMachine) applyWatermark(ctx context.Context, identity raftbootstrap.EntryIdentity, command raftbootstrap.GuardedCommand) (any, error) {
	if command.Witness.Cluster != f.identity.Cluster || command.Witness.Group != f.identity.Group {
		return nil, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
	}
	candidate := f.controlCandidate()
	var receipt raftbootstrap.GuardedReceipt
	err := f.commitControlState(ctx, identity.Index, candidate, f.configurationIndex.Load(), func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
		policy, err := readAppliedBootstrapPolicy(ctx, metadata, f.identity, identity.Index-1)
		if err != nil {
			return err
		}
		receipt, err = guardedBootstrapVerdict(identity, command, policy)
		if err != nil {
			return err
		}
		if receipt.Verdict == raftbootstrap.GuardedReceiptApplied {
			if err := candidate.applyTSOBootstrapWatermark(receipt.RequestedWatermark); err != nil {
				return err
			}
			receipt.ReturnedWatermark = candidate.AllocationFloor()
		}
		return stageGuardedBootstrapReceipt(ctx, metadata, receipt)
	})
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

func (f *TSOStateMachine) applyTSOBootstrapWatermark(ts uint64) error {
	if response := f.applyLegacy(marshalTSOAllocationFloor(ts)); response != nil {
		if halt, ok := response.(*haltApplyResponse); ok {
			return halt.HaltApply()
		}
		return errors.WithStack(ErrTSOStateMachineInvalidEntry)
	}
	f.applyLeaseCeiling(clampUint64ToInt64(ts >> hlcLogicalBits))
	return nil
}

var _ raftengine.ApplyEntryIdentityAware = (*GuardedTSOStateMachine)(nil)
var _ raftengine.ApplyNativeConfigurationAware = (*GuardedTSOStateMachine)(nil)
var _ raftengine.StateMachine = (*GuardedTSOStateMachine)(nil)
