package adapter

import (
	"context"
	"slices"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

const namespaceProbeTimeout = 2 * time.Second

type MigrationNamespaceProbeClient interface {
	ProbeMigrationState(context.Context, *pb.ProbeMigrationStateRequest, ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error)
}

type NamespaceAdmissionPeer struct {
	MemberID uint64
	Address  string
	Client   MigrationNamespaceProbeClient
}

type NamespaceAdmissionSnapshot struct {
	ConfigurationIndex uint64
	Members            []NamespaceAdmissionPeer
}

// EnsureMigrationNamespaceAdmission precedes partition/ownership controls. It
// must receive the FULL apply-member inventory, not the historical voter-only
// subset. The FSM independently compares this roster to committed ConfState.
func EnsureMigrationNamespaceAdmission(ctx context.Context, client SplitMigrationClient, members []SplitMigrationVoter) error {
	if client == nil {
		return errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	snapshot := func(ctx context.Context) (NamespaceAdmissionSnapshot, error) {
		state := NamespaceAdmissionSnapshot{}
		for _, member := range members {
			if member.ID == "" {
				return state, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
			}
			peer := NamespaceAdmissionPeer{MemberID: etcd.DeriveNodeID(member.ID), Address: member.Address, Client: member.Client}
			response, err := probeNamespacePeer(ctx, peer, 0, false)
			if err != nil {
				return state, err
			}
			index := response.GetAppliedConfigurationIndex()
			if index == 0 || (state.ConfigurationIndex != 0 && state.ConfigurationIndex != index) {
				return state, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
			}
			state.ConfigurationIndex = index
			state.Members = append(state.Members, peer)
		}
		return state, nil
	}
	activate := func(ctx context.Context, command kv.NamespaceAdmissionCommand) error {
		request, err := kv.NewNamespaceAdmissionRequest(1, command)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = client.CleanupMigration(ctx, request)
		return errors.WithStack(err)
	}
	return EnsureMigrationNamespaceAdmissionWithSnapshot(ctx, snapshot, activate, nil)
}

// EnsureMigrationNamespaceAdmissionWithSnapshot also supports durable prejoin
// registration. It never infers a peer's version from the local binary. Static
// support is independent of Ready so this can bootstrap an inactive group.
func EnsureMigrationNamespaceAdmissionWithSnapshot(ctx context.Context,
	snapshot func(context.Context) (NamespaceAdmissionSnapshot, error),
	activate func(context.Context, kv.NamespaceAdmissionCommand) error,
	joining []NamespaceAdmissionPeer,
) error {
	if snapshot == nil || activate == nil {
		return errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	before, err := snapshot(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := activateNamespaceWithSnapshot(ctx, snapshot, before, activate, joining); err != nil {
		return err
	}
	for _, member := range before.Members {
		if _, err := probeNamespacePeer(ctx, member, before.ConfigurationIndex, true); err != nil {
			return err
		}
	}
	return revalidateNamespaceSnapshot(ctx, snapshot, before)
}

func activateNamespaceWithSnapshot(ctx context.Context,
	snapshot func(context.Context) (NamespaceAdmissionSnapshot, error), before NamespaceAdmissionSnapshot,
	activate func(context.Context, kv.NamespaceAdmissionCommand) error, joining []NamespaceAdmissionPeer,
) error {
	command, alreadyActive, err := namespaceNegotiation(ctx, before, joining)
	if err != nil {
		return err
	}
	if err := revalidateNamespaceSnapshot(ctx, snapshot, before); err != nil {
		return err
	}
	if !alreadyActive || len(joining) != 0 {
		if err := activate(ctx, command); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func namespaceNegotiation(ctx context.Context, snapshot NamespaceAdmissionSnapshot, joining []NamespaceAdmissionPeer) (kv.NamespaceAdmissionCommand, bool, error) {
	command := kv.NamespaceAdmissionCommand{Version: kv.MigrationNamespaceVersion, ConfigurationIndex: snapshot.ConfigurationIndex}
	if snapshot.ConfigurationIndex == 0 || len(snapshot.Members) == 0 {
		return command, false, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	alreadyActive := true
	addresses := make(map[string]bool)
	ids := make(map[uint64]bool)
	for _, member := range snapshot.Members {
		if ids[member.MemberID] || addresses[member.Address] {
			return command, false, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
		}
		response, err := probeNamespacePeer(ctx, member, snapshot.ConfigurationIndex, false)
		if err != nil {
			return command, false, err
		}
		alreadyActive = alreadyActive && response.GetReady()
		ids[member.MemberID], addresses[member.Address] = true, true
		command.ApplyMembers = append(command.ApplyMembers, member.MemberID)
		command.Members = append(command.Members, kv.NamespaceMemberCapability{MemberID: member.MemberID, Version: response.GetNamespaceAdmissionVersion(), ConfigurationIndex: snapshot.ConfigurationIndex})
	}
	var err error
	command.Joining, err = probeNamespaceJoining(ctx, snapshot.ConfigurationIndex, joining, ids, addresses)
	if err != nil {
		return command, false, err
	}
	slices.Sort(command.ApplyMembers)
	return command, alreadyActive, nil
}

func probeNamespaceJoining(ctx context.Context, configuration uint64, joining []NamespaceAdmissionPeer, ids map[uint64]bool, addresses map[string]bool) ([]kv.NamespaceMemberCapability, error) {
	var capabilities []kv.NamespaceMemberCapability
	for _, member := range joining {
		if ids[member.MemberID] || addresses[member.Address] {
			return nil, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
		}
		// A candidate does not yet have a source-group configuration. Only its
		// installed support is probed; authorization is bound to source cfg.
		response, err := probeNamespacePeer(ctx, member, 0, false)
		if err != nil {
			return nil, err
		}
		ids[member.MemberID], addresses[member.Address] = true, true
		capabilities = append(capabilities, kv.NamespaceMemberCapability{MemberID: member.MemberID, Version: response.GetNamespaceAdmissionVersion(), ConfigurationIndex: configuration})
	}
	return capabilities, nil
}

func probeNamespacePeer(ctx context.Context, member NamespaceAdmissionPeer, configuration uint64, activated bool) (*pb.ProbeMigrationStateResponse, error) {
	if member.MemberID == 0 || member.Address == "" || member.Client == nil {
		return nil, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	attempt, cancel := context.WithTimeout(ctx, namespaceProbeTimeout)
	defer cancel()
	response, err := member.Client.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{JobId: 1,
		Kind:                      pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION,
		RequireConfigurationIndex: configuration != 0, ExpectedConfigurationIndex: configuration})
	if err != nil {
		return nil, errors.Wrapf(err, "namespace member %d", member.MemberID)
	}
	if response.GetNamespaceAdmissionVersion() < kv.MigrationNamespaceVersion ||
		(configuration != 0 && response.GetAppliedConfigurationIndex() != configuration) || (activated && !response.GetReady()) {
		return nil, errors.Wrapf(kv.ErrMigrationNamespaceProtocol, "namespace member %d support/configuration/activation unproven", member.MemberID)
	}
	return response, nil
}

func revalidateNamespaceSnapshot(ctx context.Context, snapshot func(context.Context) (NamespaceAdmissionSnapshot, error), before NamespaceAdmissionSnapshot) error {
	after, err := snapshot(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	canonical := func(members []NamespaceAdmissionPeer) []NamespaceAdmissionPeer {
		members = slices.Clone(members)
		slices.SortFunc(members, func(a, b NamespaceAdmissionPeer) int {
			if a.MemberID < b.MemberID {
				return -1
			}
			if a.MemberID > b.MemberID {
				return 1
			}
			return 0
		})
		return members
	}
	if before.ConfigurationIndex != after.ConfigurationIndex || !slices.EqualFunc(canonical(before.Members), canonical(after.Members), func(a, b NamespaceAdmissionPeer) bool { return a.MemberID == b.MemberID && a.Address == b.Address }) {
		return errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	return nil
}
