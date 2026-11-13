package clusterbootstrap

import (
	"bytes"
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const bootstrapActivationRetryInterval = time.Second

func validateBootstrapActivationRequest(payload []byte, identity etcd.WALIdentity) error {
	if len(payload) == 0 {
		return nil
	}
	command, matched, err := raftbootstrap.ParseActivationCommand(payload)
	if err != nil || !matched {
		return errors.Join(raftbootstrap.ErrInvalid, err)
	}
	digest, err := raftbootstrap.BaselineManifestDigest(command.Manifest)
	if err != nil {
		return errors.WithStack(err)
	}
	if command.Manifest.Identity != identity.Identity || digest != identity.PreparationDigest || identity.PreparationDigest == (raftbootstrap.Digest{}) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

func (d *BootstrapDomain) registerActivation(id uint64, payload []byte) {
	if len(payload) == 0 {
		return
	}
	copy := bytes.Clone(payload)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.activations == nil {
		d.activations = make(map[uint64][]byte)
	}
	d.activations[id] = copy
}

func StartBootstrapActivationWorkers(ctx context.Context, eg *errgroup.Group, runtimes []*GroupRuntime, groups map[uint64]*kv.ShardGroup) {
	for _, runtime := range runtimes {
		if runtime == nil || runtime.BootstrapDomain == nil {
			continue
		}
		domain := runtime.BootstrapDomain
		domain.mu.Lock()
		payload := bytes.Clone(domain.activations[runtime.Spec.ID])
		domain.mu.Unlock()
		if len(payload) == 0 {
			continue
		}
		eg.Go(func() error { return runBootstrapActivation(ctx, runtime, groups, payload) })
	}
}

func runBootstrapActivation(ctx context.Context, runtime *GroupRuntime, groups map[uint64]*kv.ShardGroup, payload []byte) error {
	ticker := time.NewTicker(bootstrapActivationRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !runtime.BootstrapDomain.Admission.NeedsActivation(runtime.Spec.ID) {
				continue
			}
			attemptBootstrapActivation(ctx, runtime, groups, payload)
		}
	}
}

func attemptBootstrapActivation(ctx context.Context, runtime *GroupRuntime, groups map[uint64]*kv.ShardGroup, payload []byte) {
	owned, finish, err := runtime.BootstrapDomain.Admission.Enter(ctx, raftadmission.Recovery, runtime.Spec.ID)
	if err != nil {
		return
	}
	completed := false
	defer func() {
		if !completed {
			err = raftadmission.ErrClosed
		}
		_ = finish(err)
	}()
	// The terminal native check, not this scheduling loop, determines leadership.
	// Activation may cancel its own old-generation reply after committing P. The
	// next iteration observes independently recovered P rather than assuming ACK.
	proposer := bootstrapActivationProposer(runtime, groups)
	if proposer == nil {
		err = raftadmission.ErrClosed
		completed = true
		return
	}
	_, err = proposer.ProposeAdmin(owned, payload)
	completed = true
}

func bootstrapActivationProposer(runtime *GroupRuntime, groups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	if group := groups[runtime.Spec.ID]; group != nil {
		return group.Proposer()
	}
	return runtime.snapshotEngine()
}
