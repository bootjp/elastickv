package main

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

func (d *bootstrapDomain) registerActivation(id uint64, payload []byte) {
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

func startBootstrapActivationWorkers(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, groups map[uint64]*kv.ShardGroup) {
	for _, runtime := range runtimes {
		if runtime == nil || runtime.bootstrapDomain == nil {
			continue
		}
		domain := runtime.bootstrapDomain
		domain.mu.Lock()
		payload := bytes.Clone(domain.activations[runtime.spec.id])
		domain.mu.Unlock()
		if len(payload) == 0 {
			continue
		}
		eg.Go(func() error { return runBootstrapActivation(ctx, runtime, groups, payload) })
	}
}

func runBootstrapActivation(ctx context.Context, runtime *raftGroupRuntime, groups map[uint64]*kv.ShardGroup, payload []byte) error {
	ticker := time.NewTicker(bootstrapActivationRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !runtime.bootstrapDomain.admission.NeedsActivation(runtime.spec.id) {
				continue
			}
			attemptBootstrapActivation(ctx, runtime, groups, payload)
		}
	}
}

func attemptBootstrapActivation(ctx context.Context, runtime *raftGroupRuntime, groups map[uint64]*kv.ShardGroup, payload []byte) {
	owned, finish, err := runtime.bootstrapDomain.admission.Enter(ctx, raftadmission.Recovery, runtime.spec.id)
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

func bootstrapActivationProposer(runtime *raftGroupRuntime, groups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	if group := groups[runtime.spec.id]; group != nil {
		return group.Proposer()
	}
	return runtime.snapshotEngine()
}
