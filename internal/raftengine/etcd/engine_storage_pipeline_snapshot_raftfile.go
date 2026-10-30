package etcd

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	pipelineSnapshotCopyBuffer       = 32 << 10
	pipelineWireFixed32Bytes         = 4
	pipelineWireFixed64Bytes         = 8
	pipelineConfStateLastMemberField = 4
)

type pipelineWireSpan struct{ offset, length int64 }

type pipelineWireFile struct {
	bytes   map[protowire.Number][]pipelineWireSpan
	varints map[protowire.Number]uint64
}

type pipelineFileByteReader struct{ file *os.File }

func (r pipelineFileByteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r.file, b[:])
	return b[0], errors.WithStack(err)
}

// The existing .snap envelope contains CRC32C and serialized raftpb.Snapshot.
// Locate ranges without materializing the body; CRC covers the ENTIRE inner
// protobuf before a byte is published into the new verified FSM file.
type pipelineRaftFileSnapshot struct {
	path     string
	expected *raftpb.SnapshotMetadata
}

func (*pipelineRaftFileSnapshot) Close() error { return nil }

func (s *pipelineRaftFileSnapshot) WriteTo(w io.Writer) (written int64, err error) {
	file, err := os.Open(s.path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, file.Close()) }()
	data, err := verifiedPipelineRaftFile(file)
	if err != nil {
		return 0, err
	}
	inner, err := scanPipelineWireFile(file, data)
	if err != nil {
		return 0, err
	}
	if err := verifyPipelineRaftMetadata(file, inner.bytes[2], s.expected); err != nil {
		return 0, err
	}
	body, ok := lastPipelineWireSpan(inner.bytes[1])
	if !ok {
		return 0, errors.WithStack(errSnapshotRequired)
	}
	written, err = io.CopyBuffer(w, io.NewSectionReader(file, body.offset, body.length), make([]byte, pipelineSnapshotCopyBuffer))
	return written, errors.WithStack(err)
}

func verifiedPipelineRaftFile(file *os.File) (pipelineWireSpan, error) {
	info, err := file.Stat()
	if err != nil {
		return pipelineWireSpan{}, errors.WithStack(err)
	}
	outer, err := scanPipelineWireFile(file, pipelineWireSpan{length: info.Size()})
	if err != nil {
		return pipelineWireSpan{}, err
	}
	data, ok := lastPipelineWireSpan(outer.bytes[2])
	if !ok || outer.varints[1] > math.MaxUint32 {
		return pipelineWireSpan{}, errors.WithStack(errInvalidStorageWork)
	}
	if data.length == 0 || outer.varints[1] == 0 {
		return pipelineWireSpan{}, errors.WithStack(snap.ErrEmptySnapshot)
	}
	buffer := make([]byte, pipelineSnapshotCopyBuffer)
	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := io.CopyBuffer(hash, io.NewSectionReader(file, data.offset, data.length), buffer); err != nil {
		return pipelineWireSpan{}, errors.WithStack(err)
	}
	if uint64(hash.Sum32()) != outer.varints[1] {
		return pipelineWireSpan{}, errors.WithStack(snap.ErrCRCMismatch)
	}
	return data, nil
}

func lastPipelineWireSpan(spans []pipelineWireSpan) (pipelineWireSpan, bool) {
	if len(spans) == 0 {
		return pipelineWireSpan{}, false
	}
	return spans[len(spans)-1], true
}

func scanPipelineWireFile(file *os.File, span pipelineWireSpan) (pipelineWireFile, error) {
	fields := pipelineWireFile{bytes: make(map[protowire.Number][]pipelineWireSpan), varints: make(map[protowire.Number]uint64)}
	end, err := pipelineWireEnd(span)
	if err != nil {
		return fields, err
	}
	if _, err := file.Seek(span.offset, io.SeekStart); err != nil {
		return fields, errors.WithStack(err)
	}
	for {
		position, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fields, errors.WithStack(err)
		}
		if position == end {
			return fields, nil
		}
		if position > end {
			return fields, errors.WithStack(errInvalidStorageWork)
		}
		number, kind, err := readPipelineWireTag(file)
		if err != nil {
			return fields, errors.WithStack(err)
		}
		if err := scanPipelineWireValue(file, end, number, kind, &fields); err != nil {
			return fields, err
		}
	}
}

func pipelineWireEnd(span pipelineWireSpan) (int64, error) {
	if span.offset < 0 || span.length < 0 || span.offset > math.MaxInt64-span.length {
		return 0, errors.WithStack(errInvalidStorageWork)
	}
	return span.offset + span.length, nil
}

func readPipelineWireTag(file *os.File) (protowire.Number, protowire.Type, error) {
	tag, err := binary.ReadUvarint(pipelineFileByteReader{file: file})
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	number, kind := protowire.DecodeTag(tag)
	if number < 1 || number > protowire.MaxValidNumber {
		return 0, 0, errors.WithStack(errInvalidStorageWork)
	}
	return number, kind, nil
}

func scanPipelineWireValue(file *os.File, end int64, number protowire.Number, kind protowire.Type, fields *pipelineWireFile) error {
	switch kind {
	case protowire.VarintType:
		return scanPipelineWireVarint(file, number, fields)
	case protowire.BytesType:
		return scanPipelineWireBytes(file, end, number, fields)
	case protowire.Fixed32Type:
		return skipPipelineWireBytes(file, end, pipelineWireFixed32Bytes)
	case protowire.Fixed64Type:
		return skipPipelineWireBytes(file, end, pipelineWireFixed64Bytes)
	case protowire.StartGroupType:
		return skipPipelineWireGroup(file, end, number, 1)
	case protowire.EndGroupType:
		return errors.Wrap(errInvalidStorageWork, "unmatched protobuf end group")
	default:
		return errors.WithStack(errInvalidStorageWork)
	}
}

func scanPipelineWireVarint(file *os.File, number protowire.Number, fields *pipelineWireFile) error {
	value, err := binary.ReadUvarint(pipelineFileByteReader{file: file})
	if fields != nil && number <= 2 && err == nil {
		fields.varints[number] = value
	}
	return errors.WithStack(err)
}

func scanPipelineWireBytes(file *os.File, end int64, number protowire.Number, fields *pipelineWireFile) error {
	length, err := binary.ReadUvarint(pipelineFileByteReader{file: file})
	if err != nil {
		return errors.WithStack(err)
	}
	if length > math.MaxInt64 {
		return errors.WithStack(errInvalidStorageWork)
	}
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.WithStack(err)
	}
	if position > end || int64(length) > end-position {
		return errors.WithStack(errInvalidStorageWork)
	}
	if fields != nil && number <= 2 {
		if len(fields.bytes[number]) >= pipelineControlBytes/pipelineTaskOverhead {
			return errors.WithStack(errStorageWorkInvalidReservation)
		}
		fields.bytes[number] = append(fields.bytes[number], pipelineWireSpan{offset: position, length: int64(length)})
	}
	_, err = file.Seek(int64(length), io.SeekCurrent)
	return errors.WithStack(err)
}

func skipPipelineWireGroup(file *os.File, end int64, group protowire.Number, depth int) error {
	if depth > protowire.DefaultRecursionLimit {
		return errors.WithStack(errStorageWorkInvalidReservation)
	}
	for {
		position, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.WithStack(err)
		}
		if position >= end {
			return errors.WithStack(io.ErrUnexpectedEOF)
		}
		number, kind, err := readPipelineWireTag(file)
		if err != nil {
			return errors.WithStack(err)
		}
		if kind == protowire.EndGroupType {
			if number != group {
				return errors.WithStack(errInvalidStorageWork)
			}
			return nil
		}
		if kind == protowire.StartGroupType {
			err = skipPipelineWireGroup(file, end, number, depth+1)
		} else {
			err = scanPipelineWireValue(file, end, number, kind, nil)
		}
		if err != nil {
			return err
		}
	}
}

func skipPipelineWireBytes(file *os.File, end, length int64) error {
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.WithStack(err)
	}
	if position > end || length > end-position {
		return errors.WithStack(errInvalidStorageWork)
	}
	_, err = file.Seek(length, io.SeekCurrent)
	return errors.WithStack(err)
}

func verifyPipelineRaftMetadata(file *os.File, spans []pipelineWireSpan, expected *raftpb.SnapshotMetadata) error {
	var total, members uint64
	actual := &raftpb.SnapshotMetadata{}
	for _, span := range spans {
		if span.length < 0 || span.length > pipelineControlBytes {
			return errors.WithStack(errStorageWorkInvalidReservation)
		}
		total = pipelineAddBytes(total, uint64(span.length))
		if total > pipelineControlBytes {
			return errors.WithStack(errStorageWorkInvalidReservation)
		}
		data := make([]byte, int(span.length))
		if _, err := file.ReadAt(data, span.offset); err != nil {
			return errors.WithStack(err)
		}
		if err := preflightPipelineSnapshotMetadata(data, &members); err != nil {
			return err
		}
		if err := (proto.UnmarshalOptions{Merge: true}).Unmarshal(data, actual); err != nil {
			return errors.WithStack(err)
		}
	}
	// MemoryStorage.ApplySnapshot calls EnsureSnapshot, which fills absent
	// scalar defaults in the caller's selected metadata. Compare the same
	// canonical defaults while retaining every unknown field and member list.
	if !proto.Equal(raftpb.EnsureSnapshotMetadata(actual), raftpb.EnsureSnapshotMetadata(proto.CloneOf(expected))) {
		return errors.Wrap(errInvalidStorageWork, "selected Raft snapshot metadata changed")
	}
	return nil
}

func preflightPipelineSnapshotMetadata(data []byte, members *uint64) error {
	for len(data) != 0 {
		number, kind, tag := protowire.ConsumeTag(data)
		if tag < 0 {
			return errors.WithStack(protowire.ParseError(tag))
		}
		data = data[tag:]
		size := protowire.ConsumeFieldValue(number, kind, data)
		if size < 0 {
			return errors.WithStack(protowire.ParseError(size))
		}
		if number == 1 && kind == protowire.BytesType {
			state, _ := protowire.ConsumeBytes(data)
			if err := preflightPipelineConfStateWire(state, members); err != nil {
				return err
			}
		}
		data = data[size:]
	}
	return nil
}

func preflightPipelineConfStateWire(data []byte, members *uint64) error {
	for len(data) != 0 {
		number, kind, tag := protowire.ConsumeTag(data)
		if tag < 0 {
			return errors.WithStack(protowire.ParseError(tag))
		}
		data = data[tag:]
		size := protowire.ConsumeFieldValue(number, kind, data)
		if size < 0 {
			return errors.WithStack(protowire.ParseError(size))
		}
		if number <= pipelineConfStateLastMemberField {
			if err := countPipelineConfStateMembers(kind, data, members); err != nil {
				return err
			}
		}
		if *members > pipelineControlBytes/pipelineConfigurationMemberBytes {
			return errors.WithStack(errStorageWorkInvalidReservation)
		}
		data = data[size:]
	}
	return nil
}

func countPipelineConfStateMembers(kind protowire.Type, data []byte, members *uint64) error {
	if kind == protowire.VarintType {
		*members++
	}
	if kind != protowire.BytesType {
		return nil
	}
	packed, _ := protowire.ConsumeBytes(data)
	for len(packed) > 0 {
		_, n := protowire.ConsumeVarint(packed)
		if n < 0 {
			return errors.WithStack(protowire.ParseError(n))
		}
		*members++
		packed = packed[n:]
	}
	return nil
}
