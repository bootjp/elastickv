package main

import (
	"context"
	"slices"
	"sync"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type bootstrapStartupFactory = func(raftengine.FactoryConfig, *etcd.GRPCTransport) (*etcd.BootstrapStartup, error)

// The parent supplies the exact public method list for this shared listener.
func newBootstrapStartupForGroup(config *bootstrapPeerConfig, files *raftbootstrap.Store, intent raftbootstrap.Intent, st store.MVCCStore, publicMethods []string) bootstrapStartupFactory {
	publicMethods = slices.Clone(publicMethods)
	return func(cfg raftengine.FactoryConfig, transport *etcd.GRPCTransport) (*etcd.BootstrapStartup, error) {
		identity, err := validateBootstrapStartup(config, intent, cfg)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if err := validateBootstrapStartupDependencies(files, st, transport); err != nil {
			return nil, err
		}
		owner, err := peersession.NewOwner(peersession.Config{
			Local: config.local, Provisioned: config.provisions, Versions: intent.Minimum,
			PublicMethods: publicMethods,
			MethodFactory: func(owner *peersession.Owner) ([]peersession.Method, error) {
				return etcd.PeerSessionMethodsForOwner(cfg.GroupID, owner), nil
			},
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}
		options, err := owner.ServerOptions(config.serverTLS)
		if err != nil {
			owner.Close()
			return nil, errors.WithStack(err)
		}
		nodes := bootstrapSessionNodes(config, cfg.GroupID)
		if err := transport.ConfigurePeerSessions(owner, cfg.GroupID, config.clientTLS, nodes); err != nil {
			owner.Close()
			return nil, errors.WithStack(err)
		}
		var mu sync.Mutex
		var generation uint64
		return &etcd.BootstrapStartup{
			NativeIdentity: &identity, ServerOptions: options, Register: owner.Register, Close: owner.Close,
			PublicReady: func() bool {
				group, ok := owner.GroupProjection(cfg.GroupID)
				return ok && group.Stage == peersession.DataReady && group.Pending == peersession.PendingClear
			},
			AdmissionChanged: func() {
				mu.Lock()
				defer mu.Unlock()
				owner.SuspendGroup(cfg.GroupID)
			},
			RecoveredAdmission: func(state etcd.RecoveredState) error {
				mu.Lock()
				defer mu.Unlock()
				// Any failed re-publication permanently closes this owner rather than
				// retaining an earlier projection's authority.
				if err := publishBootstrapRecovery(owner, files, st, identity, state, generation+1); err != nil {
					owner.Close()
					return err
				}
				generation++
				return nil
			},
		}, nil
	}
}

func validateBootstrapStartupDependencies(files *raftbootstrap.Store, st store.MVCCStore, transport *etcd.GRPCTransport) error {
	// TSO has no MVCC policy projection yet. Do not fabricate recovery evidence.
	if st == nil || files == nil || transport == nil {
		return errors.New("bootstrap startup requires policy store and transport; TSO policy recovery is not implemented")
	}
	return nil
}

func bootstrapSessionNodes(config *bootstrapPeerConfig, group uint64) map[uint64]peersession.Node {
	nodes := make(map[uint64]peersession.Node)
	for _, provision := range config.provisions {
		if slices.Contains(provision.Groups, group) {
			nodes[provision.Node.Member] = provision.Node
		}
	}
	return nodes
}

func bootstrapLocalMatches(config *bootstrapPeerConfig, intent raftbootstrap.Intent, cfg raftengine.FactoryConfig) bool {
	return config != nil && cfg.LocalID != "" && cfg.GroupID == intent.Identity.Group &&
		config.local.Cluster == intent.Identity.Cluster && config.local.Member == etcd.DeriveNodeID(cfg.LocalID) &&
		config.local.Address == cfg.LocalAddress
}

func validateBootstrapStartup(config *bootstrapPeerConfig, intent raftbootstrap.Intent, cfg raftengine.FactoryConfig) (etcd.WALIdentity, error) {
	var zero etcd.WALIdentity
	if !bootstrapLocalMatches(config, intent, cfg) {
		return zero, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	if _, err := raftbootstrap.MarshalIntent(intent); err != nil {
		return zero, errors.WithStack(err)
	}
	known := false
	for _, p := range config.provisions {
		if p.Node == config.local && slices.Contains(p.Groups, cfg.GroupID) {
			known = true
		}
	}
	if !known {
		return zero, errors.WithStack(peersession.ErrDenied)
	}
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	if err != nil {
		return zero, errors.WithStack(err)
	}
	return etcd.WALIdentity{Identity: intent.Identity, NodeID: config.local.Member, BaselineDigest: digest}, nil
}

func publishBootstrapRecovery(owner *peersession.Owner, files *raftbootstrap.Store, st store.MVCCStore, expected etcd.WALIdentity, state etcd.RecoveredState, generation uint64) error {
	actual, verified := state.Identity()
	if !verified || actual != expected || generation == 0 {
		return errors.WithStack(etcd.ErrWALIdentity)
	}
	policy, err := kv.LoadAppliedBootstrapPolicy(context.Background(), st, expected.Identity, state.AppliedIndex())
	if err != nil {
		return errors.WithStack(err)
	}
	if policy.BaselineDigest != expected.BaselineDigest {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	if err := validateBootstrapRecoveredConfiguration(policy, state); err != nil {
		return err
	}
	return errors.WithStack(owner.PublishRecoveredPolicy(files, expected.Identity.Group, policy, raftbootstrap.Intact, generation))
}

func validateBootstrapRecoveredConfiguration(policy raftbootstrap.PolicyRecord, state etcd.RecoveredState) error {
	conf := state.ConfState()
	if conf == nil || policy.LastConfigurationIndex != state.ConfigurationIndex() {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	configuration := policy.Configuration
	configuration.State = raftbootstrap.ConfState{
		Voters: conf.GetVoters(), VotersOutgoing: conf.GetVotersOutgoing(), Learners: conf.GetLearners(),
		LearnersNext: conf.GetLearnersNext(), AutoLeave: conf.GetAutoLeave(),
	}
	if err := validateBootstrapPeerAddresses(configuration, state.Peers()); err != nil {
		return err
	}
	actual, err := raftbootstrap.ConfigurationDigest(configuration)
	if err != nil {
		return errors.WithStack(err)
	}
	expected, err := raftbootstrap.ConfigurationDigest(policy.Configuration)
	if err != nil || actual != expected {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

func validateBootstrapPeerAddresses(configuration raftbootstrap.Configuration, peers []etcd.Peer) error {
	if len(peers) != len(configuration.Members) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	addresses := make(map[uint64]string, len(peers))
	for _, peer := range peers {
		id := peer.NodeID
		if id == 0 {
			id = etcd.DeriveNodeID(peer.ID)
		}
		if _, exists := addresses[id]; exists {
			return errors.WithStack(raftbootstrap.ErrInconsistent)
		}
		addresses[id] = peer.Address
	}
	for _, member := range configuration.Members {
		if addresses[member.ID] != member.Address {
			return errors.WithStack(raftbootstrap.ErrInconsistent)
		}
	}
	return nil
}
