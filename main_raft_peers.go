package main

import (
	"maps"
	"slices"
	"strings"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

type raftBootstrapConfig struct {
	legacyServers []raftengine.Server
	groupServers  map[uint64][]raftengine.Server
	joinServers   []raftengine.Server
}

func (c raftBootstrapConfig) anyBootstrapServers() bool {
	return len(c.legacyServers) > 0 || len(c.groupServers) > 0
}

func (c raftBootstrapConfig) serversForGroup(groupID uint64) []raftengine.Server {
	if len(c.joinServers) != 0 {
		return cloneRaftServers(c.joinServers)
	}
	if len(c.groupServers) != 0 {
		return cloneRaftServers(c.groupServers[groupID])
	}
	return cloneRaftServers(c.legacyServers)
}

func (c raftBootstrapConfig) bootstrapsGroup(groupID uint64) bool {
	if len(c.groupServers) != 0 {
		return len(c.groupServers[groupID]) > 0
	}
	return len(c.legacyServers) > 0
}

func (c raftBootstrapConfig) bootstrapSeedForGroup(groupID uint64) []raftengine.Server {
	if len(c.groupServers) == 0 {
		return nil
	}
	return cloneRaftServers(c.groupServers[groupID])
}

func (c raftBootstrapConfig) adminSeed(defaultGroup uint64) []raftengine.Server {
	if len(c.joinServers) != 0 {
		return cloneRaftServers(c.joinServers)
	}
	if len(c.groupServers) != 0 {
		return cloneRaftServers(c.groupServers[defaultGroup])
	}
	return cloneRaftServers(c.legacyServers)
}

func cloneRaftServers(in []raftengine.Server) []raftengine.Server {
	if len(in) == 0 {
		return nil
	}
	return append([]raftengine.Server(nil), in...)
}

func shardGroupIDs(groups map[uint64]*kv.ShardGroup) []uint64 {
	ids := make([]uint64, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

var (
	ErrBootstrapMembersRequireSingleGroup = errors.New("flag --raftBootstrapMembers requires exactly one raft group")
	ErrBootstrapMembersMissingLocalNode   = errors.New("flag --raftBootstrapMembers must include local --raftId")
	ErrBootstrapMembersLocalAddrMismatch  = errors.New("flag --raftBootstrapMembers local address must match local raft group address")
	ErrNoBootstrapMembersConfigured       = errors.New("no bootstrap members configured")
	ErrRaftGroupPeersMutuallyExclusive    = errors.New("flags --raftBootstrapMembers and --raftGroupPeers are mutually exclusive")
	ErrRaftGroupPeersUnknownGroup         = errors.New("flag --raftGroupPeers references unknown raft group")
	ErrRaftGroupPeersMissingGroup         = errors.New("flag --raftGroupPeers must include every raft group")
	ErrRaftGroupPeersMissingLocalNode     = errors.New("flag --raftGroupPeers group must include local --raftId")
	ErrRaftGroupPeersLocalAddrMismatch    = errors.New("flag --raftGroupPeers local address must match local raft group address")
	ErrRaftGroupPeersTooFewVoters         = errors.New("flag --raftGroupPeers group must contain at least two voters")
	ErrRaftGroupPeersHeterogeneous        = errors.New("flag --raftGroupPeers requires identical raft IDs across groups")
	ErrNoRaftGroupPeersConfigured         = errors.New("no raft group peers configured")
	ErrJoinMembersRequireSingleGroup      = errors.New("flag --raftJoinMembers requires exactly one raft group")
	ErrJoinMembersRequireLearner          = errors.New("flag --raftJoinMembers requires --raftJoinAsLearner")
	ErrJoinMembersConflictBootstrap       = errors.New("flag --raftJoinMembers cannot be combined with raft bootstrap flags")
	ErrJoinMembersMissingLocalNode        = errors.New("flag --raftJoinMembers must include local --raftId")
	ErrJoinMembersLocalAddrMismatch       = errors.New("flag --raftJoinMembers local address must match local raft group address")
	ErrJoinMembersTooFewMembers           = errors.New("flag --raftJoinMembers must include the local node and at least one existing member")
	ErrNoJoinMembersConfigured            = errors.New("no raft join members configured")
)

const (
	minRaftGroupPeerVoters = 2
	minRaftJoinMembers     = 2
)

func resolveBootstrapConfig(
	raftID string,
	groups []groupSpec,
	bootstrapMembers string,
	groupPeersRaw string,
) (raftBootstrapConfig, error) {
	if strings.TrimSpace(groupPeersRaw) == "" {
		servers, err := resolveBootstrapServers(raftID, groups, bootstrapMembers)
		if err != nil {
			return raftBootstrapConfig{}, err
		}
		return raftBootstrapConfig{legacyServers: servers}, nil
	}
	if strings.TrimSpace(bootstrapMembers) != "" {
		return raftBootstrapConfig{}, errors.WithStack(ErrRaftGroupPeersMutuallyExclusive)
	}
	groupServers, err := resolveRaftGroupPeers(raftID, groups, groupPeersRaw)
	if err != nil {
		return raftBootstrapConfig{}, err
	}
	return raftBootstrapConfig{groupServers: groupServers}, nil
}

func resolveRaftPeerConfig(
	raftID string,
	groups []groupSpec,
	bootstrapMembers string,
	groupPeersRaw string,
	joinMembers string,
	joinAsLearner bool,
	explicitBootstrap bool,
) (raftBootstrapConfig, error) {
	if strings.TrimSpace(joinMembers) == "" {
		return resolveBootstrapConfig(raftID, groups, bootstrapMembers, groupPeersRaw)
	}
	if !joinAsLearner {
		return raftBootstrapConfig{}, errors.WithStack(ErrJoinMembersRequireLearner)
	}
	if explicitBootstrap || strings.TrimSpace(bootstrapMembers) != "" || strings.TrimSpace(groupPeersRaw) != "" {
		return raftBootstrapConfig{}, errors.WithStack(ErrJoinMembersConflictBootstrap)
	}
	servers, err := resolveJoinServers(raftID, groups, joinMembers)
	if err != nil {
		return raftBootstrapConfig{}, err
	}
	return raftBootstrapConfig{joinServers: servers}, nil
}

func resolveJoinServers(raftID string, groups []groupSpec, joinMembers string) ([]raftengine.Server, error) {
	if len(groups) != 1 {
		return nil, errors.WithStack(ErrJoinMembersRequireSingleGroup)
	}
	servers, err := parseRaftBootstrapMembers(joinMembers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse raft join members")
	}
	if len(servers) == 0 {
		return nil, errors.WithStack(ErrNoJoinMembersConfigured)
	}
	if len(servers) < minRaftJoinMembers {
		return nil, errors.WithStack(ErrJoinMembersTooFewMembers)
	}

	localAddr := groups[0].address
	for _, server := range servers {
		if server.ID != raftID {
			continue
		}
		if server.Address != localAddr {
			return nil, errors.Wrapf(ErrJoinMembersLocalAddrMismatch, "expected %q got %q", localAddr, server.Address)
		}
		return servers, nil
	}
	return nil, errors.Wrapf(ErrJoinMembersMissingLocalNode, "raftId=%q", raftID)
}

func resolveBootstrapServers(raftID string, groups []groupSpec, bootstrapMembers string) ([]raftengine.Server, error) {
	if strings.TrimSpace(bootstrapMembers) == "" {
		return nil, nil
	}
	if len(groups) != 1 {
		return nil, errors.WithStack(ErrBootstrapMembersRequireSingleGroup)
	}

	servers, err := parseRaftBootstrapMembers(bootstrapMembers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse raft bootstrap members")
	}
	if len(servers) == 0 {
		return nil, errors.WithStack(ErrNoBootstrapMembersConfigured)
	}

	localAddr := groups[0].address
	for _, s := range servers {
		if s.ID != raftID {
			continue
		}
		if s.Address != localAddr {
			return nil, errors.Wrapf(ErrBootstrapMembersLocalAddrMismatch, "expected %q got %q", localAddr, s.Address)
		}
		return servers, nil
	}
	return nil, errors.Wrapf(ErrBootstrapMembersMissingLocalNode, "raftId=%q", raftID)
}

func resolveRaftGroupPeers(raftID string, groups []groupSpec, raw string) (map[uint64][]raftengine.Server, error) {
	parsed, err := parseRaftGroupPeers(raw)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse raft group peers")
	}
	if len(parsed) == 0 {
		return nil, errors.WithStack(ErrNoRaftGroupPeersConfigured)
	}
	groupByID := make(map[uint64]groupSpec, len(groups))
	for _, g := range groups {
		groupByID[g.id] = g
	}
	if err := validateRaftGroupPeerCoverage(parsed, groupByID); err != nil {
		return nil, err
	}
	if err := validateRaftGroupPeerLocalNode(raftID, parsed, groupByID); err != nil {
		return nil, err
	}
	if err := validateRaftGroupPeerHomogeneity(parsed); err != nil {
		return nil, err
	}
	return parsed, nil
}

func validateRaftGroupPeerCoverage(parsed map[uint64][]raftengine.Server, groupByID map[uint64]groupSpec) error {
	for _, groupID := range slices.Sorted(maps.Keys(parsed)) {
		if _, ok := groupByID[groupID]; !ok {
			return errors.Wrapf(ErrRaftGroupPeersUnknownGroup, "group %d", groupID)
		}
		if len(parsed[groupID]) < minRaftGroupPeerVoters {
			return errors.Wrapf(ErrRaftGroupPeersTooFewVoters, "group %d", groupID)
		}
	}
	for _, groupID := range slices.Sorted(maps.Keys(groupByID)) {
		if _, ok := parsed[groupID]; !ok {
			return errors.Wrapf(ErrRaftGroupPeersMissingGroup, "group %d", groupID)
		}
	}
	return nil
}

func validateRaftGroupPeerLocalNode(raftID string, parsed map[uint64][]raftengine.Server, groupByID map[uint64]groupSpec) error {
	for _, groupID := range slices.Sorted(maps.Keys(parsed)) {
		localAddr := groupByID[groupID].address
		found := false
		for _, server := range parsed[groupID] {
			if server.ID != raftID {
				continue
			}
			found = true
			if server.Address != localAddr {
				return errors.Wrapf(ErrRaftGroupPeersLocalAddrMismatch,
					"group %d expected %q got %q", groupID, localAddr, server.Address)
			}
		}
		if !found {
			return errors.Wrapf(ErrRaftGroupPeersMissingLocalNode, "group %d raftId=%q", groupID, raftID)
		}
	}
	return nil
}

func validateRaftGroupPeerHomogeneity(parsed map[uint64][]raftengine.Server) error {
	var canonical []string
	var canonicalGroup uint64
	for _, groupID := range slices.Sorted(maps.Keys(parsed)) {
		ids := raftServerIDs(parsed[groupID])
		if canonical == nil {
			canonical = ids
			canonicalGroup = groupID
			continue
		}
		if !slices.Equal(canonical, ids) {
			return errors.Wrapf(ErrRaftGroupPeersHeterogeneous,
				"group %d ids %v differ from group %d ids %v",
				groupID, ids, canonicalGroup, canonical)
		}
	}
	return nil
}

func raftServerIDs(servers []raftengine.Server) []string {
	ids := make([]string, 0, len(servers))
	for _, server := range servers {
		ids = append(ids, server.ID)
	}
	slices.Sort(ids)
	return ids
}
