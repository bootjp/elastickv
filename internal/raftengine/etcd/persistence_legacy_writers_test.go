package etcd

import (
	"bufio"
	"bytes"
	"io"

	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Writers for the pre-WAL state files. Production only reads these formats
// (loadLegacyOrSplitState migrates them into the WAL on Open); tests use the
// writers to seed a legacy data directory.

func saveStateFile(path string, state persistedState) error {
	encoded, err := encodeStateFile(state)
	if err != nil {
		return err
	}
	return writeAndSyncFile(path, encoded)
}

func encodeStateFile(state persistedState) ([]byte, error) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	if err := writeVersionedHeader(writer, fileFormat{magic: stateFileMagic, version: stateFileVersion}); err != nil {
		return nil, err
	}
	if err := writeMessage(writer, &state.HardState); err != nil {
		return nil, err
	}
	if err := writeMessage(writer, &state.Snapshot); err != nil {
		return nil, err
	}
	entryCount, err := uint32Len(len(state.Entries))
	if err != nil {
		return nil, err
	}
	if err := writeU32(writer, entryCount); err != nil {
		return nil, err
	}
	for _, entry := range state.Entries {
		if err := writeMessage(writer, &entry); err != nil {
			return nil, err
		}
	}
	if err := writer.Flush(); err != nil {
		return nil, errors.WithStack(err)
	}
	return buf.Bytes(), nil
}

func saveMetadataFile(path string, hardState raftpb.HardState, snapshot raftpb.Snapshot) error {
	snapshot.Data = nil
	return replaceFile(path, func(w io.Writer) error {
		writer := bufio.NewWriter(w)
		if err := writeVersionedHeader(writer, fileFormat{magic: metadataFileMagic, version: metadataFileVersion}); err != nil {
			return err
		}
		if err := writeMessage(writer, &hardState); err != nil {
			return err
		}
		if err := writeMessage(writer, &snapshot); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
}

func writeMessage(w io.Writer, msg proto.Message) error {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	size, err := uint32Len(len(raw))
	if err != nil {
		return err
	}
	if err := writeU32(w, size); err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
