package raftstore

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestMigrateLegacyBoltDB(t *testing.T) {
	baseDir := t.TempDir()
	logsPath := filepath.Join(baseDir, "logs.dat")
	stablePath := filepath.Join(baseDir, "stable.dat")
	destDir := filepath.Join(baseDir, "raft.db")

	require.NoError(t, writeLegacyLogsDB(logsPath, []*raft.Log{
		{
			Index:      1,
			Term:       2,
			Type:       raft.LogCommand,
			Data:       []byte("set alpha"),
			Extensions: []byte("ext-a"),
			AppendedAt: time.Unix(100, 0).UTC(),
		},
		{
			Index:      2,
			Term:       2,
			Type:       raft.LogNoop,
			Data:       []byte("noop"),
			Extensions: []byte("ext-b"),
			AppendedAt: time.Unix(200, 0).UTC(),
		},
	}))
	require.NoError(t, writeLegacyStableDB(stablePath, map[string][]byte{
		"CurrentTerm": encodeUint64(5),
		"LastVote":    []byte("n1"),
	}))

	stats, err := MigrateLegacyBoltDB(logsPath, stablePath, destDir)
	require.NoError(t, err)
	require.Equal(t, &MigrationStats{Logs: 2, StableKeys: 2}, stats)

	store, err := NewPebbleStore(destDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	var first raft.Log
	require.NoError(t, store.GetLog(1, &first))
	require.Equal(t, uint64(1), first.Index)
	require.Equal(t, uint64(2), first.Term)
	require.Equal(t, raft.LogCommand, first.Type)
	require.Equal(t, []byte("set alpha"), first.Data)
	require.Equal(t, []byte("ext-a"), first.Extensions)
	require.True(t, first.AppendedAt.Equal(time.Unix(100, 0).UTC()))

	var second raft.Log
	require.NoError(t, store.GetLog(2, &second))
	require.Equal(t, raft.LogNoop, second.Type)
	require.True(t, second.AppendedAt.Equal(time.Unix(200, 0).UTC()))

	currentTerm, err := store.Get([]byte("CurrentTerm"))
	require.NoError(t, err)
	require.Equal(t, encodeUint64(5), currentTerm)

	lastVote, err := store.Get([]byte("LastVote"))
	require.NoError(t, err)
	require.Equal(t, []byte("n1"), lastVote)
}

func writeLegacyLogsDB(path string, logs []*raft.Log) error {
	db, err := bbolt.Open(path, legacyBoltFileMode, nil)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return db.Update(func(tx *bbolt.Tx) error {
		logsBucket, err := tx.CreateBucketIfNotExists([]byte(legacyLogsBucket))
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(legacyStableBucket)); err != nil {
			return err
		}
		for _, entry := range logs {
			payload, err := encodeLegacyLog(entry)
			if err != nil {
				return err
			}
			if err := logsBucket.Put(encodeUint64(entry.Index), payload); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeLegacyStableDB(path string, values map[string][]byte) error {
	db, err := bbolt.Open(path, legacyBoltFileMode, nil)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(legacyLogsBucket)); err != nil {
			return err
		}
		stableBucket, err := tx.CreateBucketIfNotExists([]byte(legacyStableBucket))
		if err != nil {
			return err
		}
		for key, value := range values {
			if err := stableBucket.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
}

func encodeLegacyLog(entry *raft.Log) ([]byte, error) {
	var buf bytes.Buffer
	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(&buf, &handle)
	if err := encoder.Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeUint64(v uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, v)
	return out
}
