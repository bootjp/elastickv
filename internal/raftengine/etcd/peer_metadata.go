package etcd

import (
	"bufio"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
)

const (
	peersFileName       = "etcd-raft-peers.bin"
	peersFileVersionV1  = uint32(1)
	peersFileVersionV2  = uint32(2) // adds per-peer suffrage byte
	peersFileVersionV3  = uint32(3) // adds bootstrap seed + active marker
	peersFileVersion    = peersFileVersionV3
	maxPersistedPeers   = uint32(1 << 10)
	maxPersistedPeerStr = uint32(1 << 20)
)

// Per-peer suffrage byte values used in v2 of the peers file.
const (
	persistedSuffrageVoter   uint8 = 0
	persistedSuffrageLearner uint8 = 1
)

var peersFileMagic = [4]byte{'E', 'K', 'V', 'P'}

type persistedPeers struct {
	Index               uint64
	Peers               []Peer
	BootstrapSeed       []Peer
	BootstrapSeedActive bool
}

func peersFilePath(dataDir string) string {
	return filepath.Join(dataDir, peersFileName)
}

func LoadPersistedPeers(dataDir string) ([]Peer, bool, error) {
	state, ok, err := loadPersistedPeersState(dataDir)
	if err != nil || !ok {
		return nil, ok, err
	}
	return append([]Peer(nil), state.Peers...), true, nil
}

func loadPersistedPeersState(dataDir string) (persistedPeers, bool, error) {
	state, err := readPersistedPeersFile(peersFilePath(dataDir))
	if err != nil {
		if os.IsNotExist(errors.UnwrapAll(err)) {
			return persistedPeers{}, false, nil
		}
		return persistedPeers{}, false, err
	}
	return state, true, nil
}

func savePersistedPeers(dataDir string, index uint64, peers []Peer) error {
	return savePersistedPeersWithBootstrapSeed(dataDir, index, peers, nil)
}

func savePersistedPeersWithBootstrapSeed(dataDir string, index uint64, peers []Peer, bootstrapSeed []Peer) error {
	current, ok, err := loadPersistedPeersState(dataDir)
	if err != nil {
		return err
	}
	if ok && current.Index > index {
		return nil
	}
	normalized, err := normalizePersistedPeers(peers)
	if err != nil {
		return err
	}
	state := persistedPeers{
		Index: index,
		Peers: normalized,
	}
	if ok {
		state.BootstrapSeed = current.BootstrapSeed
		state.BootstrapSeedActive = current.BootstrapSeedActive
	} else if len(bootstrapSeed) > 0 {
		normalizedSeed, err := normalizePersistedPeers(bootstrapSeed)
		if err != nil {
			return err
		}
		state.BootstrapSeed = normalizedSeed
		state.BootstrapSeedActive = true
	}
	return writePersistedPeersFile(peersFilePath(dataDir), state)
}

func writeCurrentPersistedPeers(dataDir string, index uint64, peers []Peer) error {
	current, ok, err := loadPersistedPeersState(dataDir)
	if err != nil {
		return err
	}
	normalized, err := normalizePersistedPeers(peers)
	if err != nil {
		return err
	}
	state := persistedPeers{
		Index: index,
		Peers: normalized,
	}
	if ok {
		state.BootstrapSeed = current.BootstrapSeed
		state.BootstrapSeedActive = false
	}
	return writePersistedPeersFile(peersFilePath(dataDir), state)
}

func normalizePersistedPeers(peers []Peer) ([]Peer, error) {
	normalized := make([]Peer, 0, len(peers))
	for _, peer := range peers {
		normalizedPeer, err := normalizePersistedPeer(peer)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, normalizedPeer)
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].NodeID == normalized[j].NodeID {
			return normalized[i].ID < normalized[j].ID
		}
		return normalized[i].NodeID < normalized[j].NodeID
	})
	return normalized, nil
}

func readPersistedPeersFile(path string) (persistedPeers, error) {
	file, err := os.Open(path)
	if err != nil {
		return persistedPeers{}, errors.WithStack(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			slog.Warn("failed to close peers file", "path", path, "err", err)
		}
	}()

	reader := bufio.NewReader(file)
	version, err := readPeersFileHeader(reader)
	if err != nil {
		return persistedPeers{}, err
	}

	index, err := readU64(reader)
	if err != nil {
		return persistedPeers{}, err
	}
	peers, err := readPersistedPeerList(reader, version, "persisted peer")
	if err != nil {
		return persistedPeers{}, err
	}
	state := persistedPeers{Index: index, Peers: peers}
	if version >= peersFileVersionV3 {
		active, seed, err := readPersistedBootstrapSeed(reader)
		if err != nil {
			return persistedPeers{}, err
		}
		state.BootstrapSeedActive = active
		state.BootstrapSeed = seed
	}
	return state, nil
}

func readPersistedPeerList(reader io.Reader, version uint32, kind string) ([]Peer, error) {
	count, err := readU32(reader)
	if err != nil {
		return nil, err
	}
	if count > maxPersistedPeers {
		return nil, errors.WithStack(errors.Newf("%s count %d exceeds limit %d", kind, count, maxPersistedPeers))
	}
	peers := make([]Peer, 0, count)
	for range count {
		peer, err := readPersistedPeer(reader, version)
		if err != nil {
			return nil, err
		}
		peers = append(peers, peer)
	}
	return peers, nil
}

// readPeersFileHeader validates the magic and returns the file's
// version. v1 (legacy: no per-peer suffrage byte; all peers are voters)
// and v2 (current: per-peer suffrage byte) are accepted; any other
// value is rejected. The split from the shared readVersionedHeader
// helper is intentional — that helper accepts a single hardcoded
// version and we need to dispatch.
func readPeersFileHeader(r io.Reader) (uint32, error) {
	var actual [4]byte
	if _, err := io.ReadFull(r, actual[:]); err != nil {
		return 0, errors.WithStack(err)
	}
	if actual != peersFileMagic {
		return 0, errors.WithStack(errors.New("invalid etcd raft peers magic"))
	}
	version, err := readU32(r)
	if err != nil {
		return 0, err
	}
	switch version {
	case peersFileVersionV1, peersFileVersionV2, peersFileVersionV3:
		return version, nil
	default:
		return 0, errors.WithStack(errors.Newf("unsupported etcd raft peers version %d", version))
	}
}

func readPersistedPeer(reader io.Reader, version uint32) (Peer, error) {
	nodeID, err := readU64(reader)
	if err != nil {
		return Peer{}, err
	}
	suffrage := SuffrageVoter
	if version >= peersFileVersionV2 {
		raw, err := readU8(reader)
		if err != nil {
			return Peer{}, err
		}
		switch raw {
		case persistedSuffrageVoter:
			suffrage = SuffrageVoter
		case persistedSuffrageLearner:
			suffrage = SuffrageLearner
		default:
			return Peer{}, errors.WithStack(errors.Newf("unknown peer suffrage byte %d", raw))
		}
	}
	id, err := readString(reader, maxPersistedPeerStr, "peer id")
	if err != nil {
		return Peer{}, err
	}
	address, err := readString(reader, maxPersistedPeerStr, "peer address")
	if err != nil {
		return Peer{}, err
	}
	peer, err := normalizePersistedPeer(Peer{
		NodeID:  nodeID,
		ID:      id,
		Address: address,
	})
	if err != nil {
		return Peer{}, err
	}
	peer.Suffrage = suffrage
	return peer, nil
}

func writePersistedPeersFile(path string, state persistedPeers) error {
	return replaceFile(path, func(w io.Writer) error {
		writer := bufio.NewWriter(w)
		if err := writeVersionedHeader(writer, fileFormat{magic: peersFileMagic, version: peersFileVersion}); err != nil {
			return err
		}
		if err := writeU64(writer, state.Index); err != nil {
			return err
		}
		count, err := uint32Len(len(state.Peers))
		if err != nil {
			return err
		}
		if err := writeU32(writer, count); err != nil {
			return err
		}
		for _, peer := range state.Peers {
			if err := writePersistedPeerEntry(writer, peer); err != nil {
				return err
			}
		}
		if err := writePersistedBootstrapSeed(writer, state); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
}

func readPersistedBootstrapSeed(reader io.Reader) (bool, []Peer, error) {
	activeRaw, err := readU8(reader)
	if err != nil {
		return false, nil, err
	}
	var active bool
	switch activeRaw {
	case 0:
		active = false
	case 1:
		active = true
	default:
		return false, nil, errors.WithStack(errors.Newf("unknown bootstrap seed active byte %d", activeRaw))
	}
	seed, err := readPersistedPeerList(reader, peersFileVersionV3, "persisted bootstrap seed")
	if err != nil {
		return false, nil, err
	}
	return active, seed, nil
}

func writePersistedBootstrapSeed(w io.Writer, state persistedPeers) error {
	active := uint8(0)
	if state.BootstrapSeedActive {
		active = 1
	}
	if err := writeU8(w, active); err != nil {
		return err
	}
	count, err := uint32Len(len(state.BootstrapSeed))
	if err != nil {
		return err
	}
	if err := writeU32(w, count); err != nil {
		return err
	}
	for _, peer := range state.BootstrapSeed {
		if err := writePersistedPeerEntry(w, peer); err != nil {
			return err
		}
	}
	return nil
}

func writePersistedPeerEntry(w io.Writer, peer Peer) error {
	if err := writeU64(w, peer.NodeID); err != nil {
		return err
	}
	if err := writeU8(w, persistedSuffrageByte(peer.Suffrage)); err != nil {
		return err
	}
	if err := writeString(w, peer.ID); err != nil {
		return err
	}
	return writeString(w, peer.Address)
}

func persistedSuffrageByte(suffrage string) uint8 {
	if suffrage == SuffrageLearner {
		return persistedSuffrageLearner
	}
	return persistedSuffrageVoter
}

func readString(r io.Reader, maxSize uint32, kind string) (string, error) {
	size, err := readU32(r)
	if err != nil {
		return "", err
	}
	if size > maxSize {
		return "", errors.WithStack(errors.Newf("%s size %d exceeds limit %d", kind, size, maxSize))
	}
	if size == 0 {
		return "", nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", errors.WithStack(err)
	}
	return string(buf), nil
}

func writeString(w io.Writer, value string) error {
	size, err := uint32Len(len(value))
	if err != nil {
		return err
	}
	if err := writeU32(w, size); err != nil {
		return err
	}
	if _, err := io.WriteString(w, value); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
