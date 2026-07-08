package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestChooseLeaderBalanceMove_SeedsZeroCountVoters(t *testing.T) {
	now := time.Unix(100, 0)
	obs := leaderBalanceObservation{
		counts: map[string]int{"n1": 3, "n2": 0, "n3": 0},
		voters: map[string]raftengine.Server{
			"n1": {ID: "n1", Address: "127.0.0.1:5001", Suffrage: "voter"},
			"n2": {ID: "n2", Address: "127.0.0.1:5002", Suffrage: "voter"},
			"n3": {ID: "n3", Address: "127.0.0.1:5003", Suffrage: "voter"},
		},
		groups: []leaderBalanceGroupSnapshot{
			balanceGroup(1, "127.0.0.1:5001", "n1", "n2", "n3"),
			balanceGroup(2, "127.0.0.1:5001", "n1", "n2", "n3"),
			balanceGroup(3, "127.0.0.1:5001", "n1", "n2", "n3"),
		},
	}
	decision := chooseLeaderBalanceMove(obs, leaderBalanceTestConfig(), nil, time.Time{}, time.Time{}, now)
	require.Empty(t, decision.reason)
	require.Equal(t, uint64(2), decision.move.groupID, "default group 1 should be considered after non-default groups")
	require.Equal(t, "n2", decision.move.chosenTarget.ID)
	require.Equal(t, []raftengine.TransferTarget{
		{ID: "n2", Address: "127.0.0.1:5002"},
		{ID: "n3", Address: "127.0.0.1:5003"},
	}, decision.move.candidates)
}

func TestChooseLeaderBalanceMove_PartialObservationSkips(t *testing.T) {
	now := time.Unix(100, 0)
	obs := leaderBalanceObservation{
		partial: true,
		counts:  map[string]int{"n1": 2, "n2": 0},
		voters:  map[string]raftengine.Server{"n1": {ID: "n1"}, "n2": {ID: "n2"}},
		groups:  []leaderBalanceGroupSnapshot{balanceGroup(2, "a", "n1", "n2")},
	}
	decision := chooseLeaderBalanceMove(obs, leaderBalanceTestConfig(), nil, time.Time{}, time.Time{}, now)
	require.Equal(t, "partial_observation", decision.reason)
}

func TestChooseLeaderBalanceMove_RespectsCooldownAndPins(t *testing.T) {
	now := time.Unix(100, 0)
	cfg := leaderBalanceTestConfig()
	cfg.pinnedGroups = map[uint64]bool{2: true}
	obs := leaderBalanceObservation{
		counts: map[string]int{"n1": 3, "n2": 0},
		voters: map[string]raftengine.Server{
			"n1": {ID: "n1", Address: "a", Suffrage: "voter"},
			"n2": {ID: "n2", Address: "b", Suffrage: "voter"},
		},
		groups: []leaderBalanceGroupSnapshot{
			balanceGroup(1, "a", "n1", "n2"),
			balanceGroup(2, "a", "n1", "n2"),
			balanceGroup(3, "a", "n1", "n2"),
		},
	}
	cooldowns := map[uint64]time.Time{3: now.Add(time.Minute)}
	decision := chooseLeaderBalanceMove(obs, cfg, cooldowns, time.Time{}, time.Time{}, now)
	require.Equal(t, uint64(1), decision.move.groupID, "default group is admitted only after pinned/cooling non-default groups are ineligible")
}

func TestChooseLeaderBalanceMove_PrioritizesMostOverloadedSource(t *testing.T) {
	now := time.Unix(100, 0)
	obs := leaderBalanceObservation{
		counts: map[string]int{"n1": 100, "n2": 2, "n3": 0},
		voters: map[string]raftengine.Server{
			"n1": {ID: "n1", Address: "127.0.0.1:5001", Suffrage: "voter"},
			"n2": {ID: "n2", Address: "127.0.0.1:5002", Suffrage: "voter"},
			"n3": {ID: "n3", Address: "127.0.0.1:5003", Suffrage: "voter"},
		},
		groups: []leaderBalanceGroupSnapshot{
			balanceGroupForLeader(2, "n2", "127.0.0.1:5002", "n1", "n2", "n3"),
			balanceGroupForLeader(3, "n1", "127.0.0.1:5001", "n1", "n2", "n3"),
		},
	}
	decision := chooseLeaderBalanceMove(obs, leaderBalanceTestConfig(), nil, time.Time{}, time.Time{}, now)
	require.Empty(t, decision.reason)
	require.Equal(t, uint64(3), decision.move.groupID)
	require.Equal(t, "n1", decision.move.sourceID)
	require.Equal(t, "n3", decision.move.chosenTarget.ID)
}

func TestLeaderBalanceConfigNormalizeClampsOneCountThreshold(t *testing.T) {
	cfg := leaderBalanceConfig{imbalanceThreshold: 1}
	cfg.normalize()
	require.Equal(t, defaultLeaderBalanceImbalanceThreshold, cfg.imbalanceThreshold)
}

func TestLeaderBalanceTargetCandidatesRequireObjectiveImprovement(t *testing.T) {
	group := balanceGroupForLeader(2, "n1", "127.0.0.1:5001", "n1", "n2")
	candidates := leaderBalanceTargetCandidates(group, map[string]int{"n1": 1, "n2": 0}, 1)
	require.Empty(t, candidates)
}

func TestLeaderBalanceKillSwitchFailsClosedOnAmbiguousStatError(t *testing.T) {
	badPath := filepath.Join(t.TempDir(), "kill-switch"+strings.Repeat("\x00", 1))
	loop := &leaderBalanceLoop{cfg: leaderBalanceConfig{
		killSwitchFile: badPath,
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	require.True(t, loop.killSwitchActive())

	missingFile := filepath.Join(t.TempDir(), "kill-switch")
	loop.cfg.killSwitchFile = missingFile
	require.False(t, loop.killSwitchActive())
	require.NoError(t, os.WriteFile(missingFile, []byte("stop"), 0o600))
	require.True(t, loop.killSwitchActive())
}

func TestBuildLeaderBalanceTransferRequestFailsClosedForOldServers(t *testing.T) {
	req := buildLeaderBalanceTransferRequest([]raftengine.TransferTarget{
		{ID: "n2", Address: "127.0.0.1:5002"},
		{ID: "n3", Address: "127.0.0.1:5003"},
	}, 42)
	require.True(t, req.Gated)
	require.Equal(t, uint64(42), req.MaxLag)
	require.Empty(t, req.TargetId, "legacy target_id must stay empty so older servers reject instead of running ungated")
	require.Equal(t, "127.0.0.1:5002", req.TargetAddress)
	require.Equal(t, []*pb.TransferTarget{
		{TargetId: "n2", TargetAddress: "127.0.0.1:5002"},
		{TargetId: "n3", TargetAddress: "127.0.0.1:5003"},
	}, req.TargetCandidates)
}

func TestChooseLeaderBalanceMove_StartupGraceSkips(t *testing.T) {
	now := time.Unix(100, 0)
	obs := leaderBalanceObservation{
		counts: map[string]int{"n1": 2, "n2": 0},
		voters: map[string]raftengine.Server{"n1": {ID: "n1"}, "n2": {ID: "n2"}},
		groups: []leaderBalanceGroupSnapshot{balanceGroup(2, "a", "n1", "n2")},
	}
	decision := chooseLeaderBalanceMove(obs, leaderBalanceTestConfig(), nil, time.Time{}, now.Add(time.Second), now)
	require.Equal(t, "startup_grace", decision.reason)
}

func leaderBalanceTestConfig() leaderBalanceConfig {
	cfg := leaderBalanceConfig{
		enabled:            true,
		localNodeID:        "n1",
		defaultGroupID:     1,
		imbalanceThreshold: 2,
	}
	cfg.normalize()
	return cfg
}

func balanceGroup(groupID uint64, leaderAddr string, voters ...string) leaderBalanceGroupSnapshot {
	return balanceGroupForLeader(groupID, "n1", leaderAddr, voters...)
}

func balanceGroupForLeader(groupID uint64, leaderID, leaderAddr string, voters ...string) leaderBalanceGroupSnapshot {
	servers := make([]raftengine.Server, 0, len(voters))
	for i, id := range voters {
		servers = append(servers, raftengine.Server{
			ID:       id,
			Address:  fmt.Sprintf("127.0.0.1:%d", 5001+i),
			Suffrage: "voter",
		})
	}
	for i := range servers {
		if servers[i].ID == leaderID {
			servers[i].Address = leaderAddr
		}
	}
	return leaderBalanceGroupSnapshot{
		groupID: groupID,
		status: raftengine.Status{
			State:  raftengine.StateLeader,
			Leader: raftengine.LeaderInfo{ID: leaderID, Address: leaderAddr},
		},
		leader:      raftengine.LeaderInfo{ID: leaderID, Address: leaderAddr},
		voters:      servers,
		localLeader: leaderID == "n1",
	}
}
