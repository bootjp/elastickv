package etcd

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
)

const (
	peersFileName       = "etcd-raft-peers.bin"
	peersFileVersion    = uint32(1)
	maxPersistedPeers   = uint32(1 << 10)
	maxPersistedPeerStr = uint32(1 << 20)
)

var peersFileMagic = [4]byte{'E', 'K', 'V', 'P'}

type persistedPeers struct {
	Index uint64
	Peers []Peer
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
	current, ok, err := loadPersistedPeersState(dataDir)
	if err != nil {
		return err
	}
	if ok && current.Index > index {
		return nil
	}
	return writeCurrentPersistedPeers(dataDir, index, peers)
}

func writeCurrentPersistedPeers(dataDir string, index uint64, peers []Peer) error {
	normalized, err := normalizePersistedPeers(peers)
	if err != nil {
		return err
	}
	return writePersistedPeersFile(peersFilePath(dataDir), persistedPeers{
		Index: index,
		Peers: normalized,
	})
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
	defer file.Close()

	reader := bufio.NewReader(file)
	if err := readVersionedHeader(reader, fileFormat{magic: peersFileMagic, version: peersFileVersion}, "etcd raft peers"); err != nil {
		return persistedPeers{}, err
	}

	index, err := readU64(reader)
	if err != nil {
		return persistedPeers{}, err
	}
	count, err := readU32(reader)
	if err != nil {
		return persistedPeers{}, err
	}
	if count > maxPersistedPeers {
		return persistedPeers{}, errors.WithStack(errors.Newf("persisted peer count %d exceeds limit %d", count, maxPersistedPeers))
	}

	peers := make([]Peer, 0, count)
	for range count {
		peer, err := readPersistedPeer(reader)
		if err != nil {
			return persistedPeers{}, err
		}
		peers = append(peers, peer)
	}
	return persistedPeers{Index: index, Peers: peers}, nil
}

func readPersistedPeer(reader io.Reader) (Peer, error) {
	nodeID, err := readU64(reader)
	if err != nil {
		return Peer{}, err
	}
	id, err := readString(reader, maxPersistedPeerStr, "peer id")
	if err != nil {
		return Peer{}, err
	}
	address, err := readString(reader, maxPersistedPeerStr, "peer address")
	if err != nil {
		return Peer{}, err
	}
	return normalizePersistedPeer(Peer{
		NodeID:  nodeID,
		ID:      id,
		Address: address,
	})
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
			if err := writeU64(writer, peer.NodeID); err != nil {
				return err
			}
			if err := writeString(writer, peer.ID); err != nil {
				return err
			}
			if err := writeString(writer, peer.Address); err != nil {
				return err
			}
		}
		if err := writer.Flush(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
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
