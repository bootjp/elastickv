package etcd

import (
	"hash/fnv"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

var (
	errPeerAddressRequired = errors.New("etcd raft peer address is required")
	errPeerIDRequired      = errors.New("etcd raft peer id is required")
	errPeerNodeIDConflict  = errors.New("etcd raft peer node id collides")
	errLocalPeerMissing    = errors.New("etcd raft local peer is missing from cluster config")
	errPeerFormatInvalid   = errors.New("etcd raft peer format is invalid")
)

const peerSpecParts = 2

type Peer struct {
	NodeID  uint64
	ID      string
	Address string
}

func DeriveNodeID(id string) uint64 {
	if id == "" {
		return 0
	}
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(id))
	nodeID := sum.Sum64()
	if nodeID == 0 {
		return 1
	}
	return nodeID
}

func ParsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	peers := make([]Peer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idAddr := strings.SplitN(part, "=", peerSpecParts)
		if len(idAddr) != peerSpecParts {
			return nil, errors.Wrapf(errPeerFormatInvalid, "%q", part)
		}
		id := strings.TrimSpace(idAddr[0])
		addr := strings.TrimSpace(idAddr[1])
		if id == "" || addr == "" {
			return nil, errors.Wrapf(errPeerFormatInvalid, "%q", part)
		}
		peers = append(peers, Peer{ID: id, Address: addr})
	}
	return peers, nil
}

func normalizePeers(localNodeID uint64, localID string, localAddress string, peers []Peer) (Peer, []Peer, error) {
	local := Peer{NodeID: localNodeID, ID: localID, Address: localAddress}
	if len(peers) == 0 {
		normalizedLocal, err := normalizeLocalPeer(local)
		if err != nil {
			return Peer{}, nil, err
		}
		return normalizedLocal, []Peer{normalizedLocal}, nil
	}

	normalized, resolvedLocal, err := normalizePeerList(local, peers)
	if err != nil {
		return Peer{}, nil, err
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].NodeID < normalized[j].NodeID })
	return resolvedLocal, normalized, nil
}

func normalizeLocalPeer(local Peer) (Peer, error) {
	if local.NodeID == 0 {
		local.NodeID = DeriveNodeID(local.ID)
	}
	if local.ID == "" {
		return Peer{}, errors.WithStack(errPeerIDRequired)
	}
	if local.Address == "" {
		return Peer{}, errors.WithStack(errPeerAddressRequired)
	}
	return local, nil
}

func normalizePeerList(local Peer, peers []Peer) ([]Peer, Peer, error) {
	normalized := make([]Peer, 0, len(peers))
	seenNodeIDs := make(map[uint64]string, len(peers))
	seenIDs := make(map[string]struct{}, len(peers))
	resolvedLocal := local
	foundLocal := false

	for _, peer := range peers {
		normalizedPeer, err := normalizePeer(peer)
		if err != nil {
			return nil, Peer{}, err
		}
		if err := ensureUniquePeer(normalizedPeer, seenNodeIDs, seenIDs); err != nil {
			return nil, Peer{}, err
		}
		normalized = append(normalized, normalizedPeer)
		if localMatchesPeer(local, normalizedPeer) {
			resolvedLocal = normalizedPeer
			foundLocal = true
		}
	}
	if !foundLocal {
		return nil, Peer{}, errors.WithStack(errLocalPeerMissing)
	}
	return normalized, resolvedLocal, nil
}

func ensureUniquePeer(peer Peer, seenNodeIDs map[uint64]string, seenIDs map[string]struct{}) error {
	if existing, ok := seenNodeIDs[peer.NodeID]; ok && existing != peer.ID {
		return errors.Wrapf(errPeerNodeIDConflict, "%q and %q map to %d", existing, peer.ID, peer.NodeID)
	}
	if _, ok := seenIDs[peer.ID]; ok {
		return errors.Wrapf(errPeerNodeIDConflict, "duplicate peer id %q", peer.ID)
	}
	seenNodeIDs[peer.NodeID] = peer.ID
	seenIDs[peer.ID] = struct{}{}
	return nil
}

func normalizePeer(peer Peer) (Peer, error) {
	if peer.ID == "" && peer.NodeID == 0 {
		return Peer{}, errors.WithStack(errPeerIDRequired)
	}
	if peer.Address == "" {
		return Peer{}, errors.WithStack(errPeerAddressRequired)
	}
	if peer.NodeID == 0 {
		peer.NodeID = DeriveNodeID(peer.ID)
	}
	if peer.ID == "" {
		peer.ID = peer.Address
	}
	return peer, nil
}

func localMatchesPeer(local Peer, peer Peer) bool {
	switch {
	case local.NodeID != 0 && peer.NodeID == local.NodeID:
		return true
	case local.ID != "" && peer.ID == local.ID:
		return true
	case local.Address != "" && peer.Address == local.Address:
		return true
	default:
		return false
	}
}

func confStateForPeers(peers []Peer) raftpb.ConfState {
	voters := make([]uint64, 0, len(peers))
	for _, peer := range peers {
		voters = append(voters, peer.NodeID)
	}
	return raftpb.ConfState{Voters: voters}
}

func clonePeerMap(peers map[uint64]Peer) map[uint64]Peer {
	if len(peers) == 0 {
		return map[uint64]Peer{}
	}
	cloned := make(map[uint64]Peer, len(peers))
	for nodeID, peer := range peers {
		cloned[nodeID] = peer
	}
	return cloned
}

func upsertPeerInMap(peers map[uint64]Peer, peer Peer) {
	if peer.NodeID == 0 {
		peer.NodeID = DeriveNodeID(peer.ID)
	}
	if peer.ID == "" {
		peer.ID = peer.Address
	}
	peers[peer.NodeID] = peer
}

func hasPeerInMap(peers map[uint64]Peer, nodeID uint64) bool {
	_, ok := peers[nodeID]
	return ok
}

func removePeerFromMap(peers map[uint64]Peer, nodeID uint64) {
	delete(peers, nodeID)
}

func sortedPeerList(peers map[uint64]Peer) []Peer {
	if len(peers) == 0 {
		return nil
	}
	out := make([]Peer, 0, len(peers))
	for _, peer := range peers {
		normalizedPeer, err := normalizePeer(peer)
		if err != nil {
			continue
		}
		out = append(out, normalizedPeer)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}
