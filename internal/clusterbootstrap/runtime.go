package clusterbootstrap

import (
	"context"
	"slices"
	"sync"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type bootstrapStartupFactory = func(raftengine.FactoryConfig, *etcd.GRPCTransport) (*etcd.BootstrapStartup, error)

// The parent supplies the exact public method list for this shared listener.
func newBootstrapStartupForGroup(config *BootstrapPeerConfig, files *raftbootstrap.Store, intent raftbootstrap.Intent, st store.MVCCStore, publicMethods []string, domains ...*BootstrapDomain) bootstrapStartupFactory {
	publicMethods = slices.Clone(publicMethods)
	return func(cfg raftengine.FactoryConfig, transport *etcd.GRPCTransport) (*etcd.BootstrapStartup, error) {
		identity, err := bootstrapRuntimeIdentity(config, files, intent, st, cfg, transport)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(domains) != 1 || domains[0] == nil {
			return nil, errors.New("protected startup requires the process-wide admission arbiter")
		}
		domain := domains[0]
		admission := domain.Admission
		owner, err := peersession.NewOwner(peersession.Config{
			Local: config.Local, Provisioned: config.Provisions, Versions: intent.Minimum,
			PublicMethods: publicMethods,
			MethodFactory: func(owner *peersession.Owner) ([]peersession.Method, error) {
				methods := append(etcd.PeerSessionMethodsForOwner(cfg.GroupID, owner), BootstrapSharedGRPCMethods(cfg.GroupID)...)
				return append(methods, TimestampAdmissionPeerMethods(cfg.GroupID)...), nil
			},
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}
		options, err := owner.ServerOptions(config.ServerTLS)
		if err != nil {
			owner.Close()
			return nil, errors.WithStack(err)
		}
		nodes := bootstrapSessionNodes(config, cfg.GroupID)
		if err := transport.ConfigurePeerSessions(owner, cfg.GroupID, config.ClientTLS, nodes); err != nil {
			owner.Close()
			return nil, errors.WithStack(err)
		}
		if err := domain.register(cfg.GroupID, owner); err != nil {
			owner.Close()
			return nil, err
		}
		domain.registerActivation(cfg.GroupID, config.Activation)
		var mu sync.Mutex
		return &etcd.BootstrapStartup{
			NativeAdmission: admission,
			NativeIdentity:  &identity, ServerOptions: options, Register: owner.Register, Close: owner.Close,
			PublicReady: func() bool {
				return bootstrapPublicReady(admission, owner, cfg.GroupID)
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
				if err := publishBootstrapAdmissionRecovery(admission, owner, files, st, identity, state, config.Activation, domain.encryptionRequest.Configured); err != nil {
					admission.LatchFatal(err)
					owner.Close()
					return err
				}
				return domain.Refresh()
			},
		}, nil
	}
}

func bootstrapPublicReady(admission *raftadmission.Arbiter, owner *peersession.Owner, id uint64) bool {
	group, ok := owner.GroupProjection(id)
	return admission.Stage(id) == peersession.DataReady && ok && group.Stage == peersession.DataReady && group.Pending == peersession.PendingClear
}

// encryptionConfigured is noderuntime.BootstrapEncryptionRequest.Configured: without it
// the recovered registration is published as not configured.
func publishBootstrapAdmissionRecovery(admission *raftadmission.Arbiter, owner *peersession.Owner, files *raftbootstrap.Store, st store.MVCCStore, expected etcd.WALIdentity, state etcd.RecoveredState, activation []byte, encryptionConfigured bool) error {
	ticket, err := admission.Ticket(expected.Identity.Group)
	if err != nil {
		return errors.WithStack(err)
	}
	policy, err := loadVerifiedBootstrapPolicy(files, st, expected, state)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return PublishPreparedBootstrapRecovery(admission, ticket, owner, files, st, expected, state, activation)
		}
		return errors.WithStack(err)
	}
	projection, err := owner.RecoverPolicyProjection(files, expected.Identity.Group, policy, raftbootstrap.Intact)
	if err != nil {
		return errors.WithStack(err)
	}
	// Configured encryption remains closed until independent epoch/wrapper
	// inspection publishes its evidence; process-wide "Done" is not proof.
	registration := raftadmission.RegistrationUnknown
	if !encryptionConfigured {
		registration = raftadmission.RegistrationNotConfigured
	}
	return errors.WithStack(admission.PublishRecoveryWith(ticket, policy, registration,
		func(stage peersession.Stage, generation uint64) (func(), error) {
			projection.Stage, projection.Generation = stage, generation
			return owner.UpdateGroupProjection(projection)
		}))
}

func loadVerifiedBootstrapPolicy(files *raftbootstrap.Store, st store.MVCCStore, expected etcd.WALIdentity, state etcd.RecoveredState) (raftbootstrap.PolicyRecord, error) {
	actual, verified := state.Identity()
	if !verified || actual != expected {
		return raftbootstrap.PolicyRecord{}, errors.WithStack(etcd.ErrWALIdentity)
	}
	policy, err := kv.LoadAppliedBootstrapPolicy(context.Background(), st, expected.Identity, state.AppliedIndex())
	if err != nil {
		return policy, errors.WithStack(err)
	}
	if policy.BaselineDigest != expected.BaselineDigest {
		return policy, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	if err := validateBootstrapRecoveredConfiguration(policy, state); err != nil {
		return policy, err
	}
	return policy, MirrorRecoveredBootstrapFloor(files, policy)
}

func validateBootstrapStartupDependencies(files *raftbootstrap.Store, st store.MVCCStore, transport *etcd.GRPCTransport) error {
	// TSO has no MVCC policy projection yet. Do not fabricate recovery evidence.
	if st == nil || files == nil || transport == nil {
		return errors.New("bootstrap startup requires policy store and transport; TSO policy recovery is not implemented")
	}
	return nil
}

func bootstrapSessionNodes(config *BootstrapPeerConfig, group uint64) map[uint64]peersession.Node {
	nodes := make(map[uint64]peersession.Node)
	for _, provision := range config.Provisions {
		if slices.Contains(provision.Groups, group) {
			nodes[provision.Node.Member] = provision.Node
		}
	}
	return nodes
}

func bootstrapLocalMatches(config *BootstrapPeerConfig, intent raftbootstrap.Intent, cfg raftengine.FactoryConfig) bool {
	return config != nil && cfg.LocalID != "" && cfg.GroupID == intent.Identity.Group &&
		config.Local.Cluster == intent.Identity.Cluster && config.Local.Member == etcd.DeriveNodeID(cfg.LocalID) &&
		config.Local.Address == cfg.LocalAddress
}

func validateBootstrapStartup(config *BootstrapPeerConfig, intent raftbootstrap.Intent, cfg raftengine.FactoryConfig) (etcd.WALIdentity, error) {
	var zero etcd.WALIdentity
	if !bootstrapLocalMatches(config, intent, cfg) {
		return zero, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	if _, err := raftbootstrap.MarshalIntent(intent); err != nil {
		return zero, errors.WithStack(err)
	}
	known := false
	for _, p := range config.Provisions {
		if p.Node == config.Local && slices.Contains(p.Groups, cfg.GroupID) {
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
	return etcd.WALIdentity{Identity: intent.Identity, NodeID: config.Local.Member, BaselineDigest: digest}, nil
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
