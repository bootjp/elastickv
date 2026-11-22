package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

	"github.com/cockroachdb/errors"
)

const OnlineMigrationVersion = 1
const onlineMigrationPositionBytes = 2 * onlineSequenceKeyBytes
const onlineMigrationMaxBytes = 256 << 20
const onlineMigrationMaxFrames = 16384

var ErrOnlineMigrationInvalid = errors.New("online migration: capture invalidated")
var ErrOnlineMigrationSealed = errors.New("online migration: capture sealed")
var ErrOnlineMigrationIdentity = errors.New("online migration: session identity mismatch")

// OnlineMigrationSession is a durable source manifest. Sequence counts complete
// captured data entries, not timestamps or physical Raft indices.
type OnlineMigrationSession struct {
	Version            uint32
	Identity           MigrationCaptureIdentity
	StartIndex         uint64
	ConfigurationIndex uint64
	RouteStart         []byte
	RouteEnd           []byte
	Sequence           uint64
	LastIndex          uint64
	Bytes              uint64
	Sealed             bool
	Closed             bool
	Invalid            string
	CleanupSequence    uint64
	// Retain the initial ownership nonce if the tracker response is lost before ownership is recorded.
	OwnershipGeneration string `json:",omitempty"`
}

func onlineMigrationKey(kind string, job, seq uint64) []byte {
	prefix := RaftControlReceiptKeyPrefix
	if kind == "invalid" {
		prefix = ""
	}
	key := append([]byte(prefix+"online-split/v1/"+kind+"/"), make([]byte, onlineMigrationPositionBytes)...)
	binary.BigEndian.PutUint64(key[len(key)-onlineMigrationPositionBytes:], job)
	binary.BigEndian.PutUint64(key[len(key)-onlineSequenceKeyBytes:], seq)
	return key
}

func readOnlineJSON(ctx context.Context, st RaftControlMetadataStore, key []byte, value any) error {
	encoded, err := st.ReadRaftControlMetadata(ctx, key)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(encoded) > migrationCaptureMaxEncodedBytes {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return nil
}

func stageOnlineJSON(ctx context.Context, st RaftControlMetadataStore, key []byte, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(st.StageRaftControlMetadata(ctx, key, encoded))
}

func ReadOnlineMigrationSession(ctx context.Context, st MVCCStore, job uint64) (OnlineMigrationSession, error) {
	var session OnlineMigrationSession
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok {
		return session, errors.WithStack(ErrNotSupported)
	}
	if err := readOnlineJSON(ctx, metadata, onlineMigrationKey("source", job, 0), &session); err != nil {
		return session, err
	}
	if !validOnlineSessionHeader(session, job) {
		return session, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	var invalid struct {
		Identity MigrationCaptureIdentity
		Reason   string
	}
	err := readOnlineJSON(ctx, metadata, onlineMigrationKey("invalid", job, 0), &invalid)
	if err == nil {
		if invalid.Identity != session.Identity || invalid.Reason == "" {
			return session, errors.WithStack(ErrMigrationCaptureCorrupt)
		}
		session.Invalid = invalid.Reason
	} else if !errors.Is(err, ErrKeyNotFound) {
		return session, err
	}
	return session, nil
}

// StartOnlineMigrationSession must run in the same indexed transaction as the
// source tracker. An existing generation can never be reset by activation.
func StartOnlineMigrationSession(ctx context.Context, st MVCCStore, session OnlineMigrationSession) error {
	backend, ok := st.(migrationCaptureBackend)
	if !ok {
		return errors.WithStack(ErrNotSupported)
	}
	if !validOnlineSessionStart(session) {
		return errors.WithStack(ErrMigrationCaptureInput)
	}
	old, err := ReadOnlineMigrationSession(ctx, st, session.Identity.JobID)
	if err == nil {
		if !sameOnlineMigrationSource(old, session) {
			return errors.WithStack(ErrOnlineMigrationIdentity)
		}
		if old.Sealed {
			return errors.WithStack(ErrOnlineMigrationSealed)
		}
		if old.Invalid != "" || old.Closed {
			return errors.WithStack(ErrOnlineMigrationInvalid)
		}
		return nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	return stageOnlineJSON(ctx, backend, onlineMigrationKey("source", session.Identity.JobID, 0), session)
}

func sameOnlineMigrationSource(a, b OnlineMigrationSession) bool {
	x, y := a.Identity, b.Identity
	x.Index, y.Index = 0, 0
	return x == y && a.ConfigurationIndex == b.ConfigurationIndex && bytes.Equal(a.RouteStart, b.RouteStart) && bytes.Equal(a.RouteEnd, b.RouteEnd)
}

// InvalidateOnlineMigration is synchronous and deliberately does NOT advance
// applied index. It must finish before an unsupported ordinary data writer.
func InvalidateOnlineMigration(ctx context.Context, st MVCCStore, session OnlineMigrationSession, reason string) error {
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok || reason == "" {
		return errors.WithStack(ErrMigrationCaptureInput)
	}
	return errors.WithStack(metadata.ApplyRaftControlMetadata(ctx, func(tx context.Context) error {
		current, err := ReadOnlineMigrationSession(tx, st, session.Identity.JobID)
		if err != nil {
			return err
		}
		if !sameOnlineMigrationSource(current, session) {
			return errors.WithStack(ErrOnlineMigrationIdentity)
		}
		if current.Sealed {
			return errors.WithStack(ErrOnlineMigrationSealed)
		}
		if current.Invalid != "" {
			return nil
		}
		return stageOnlineJSON(tx, metadata, onlineMigrationKey("invalid", session.Identity.JobID, 0), struct {
			Identity MigrationCaptureIdentity
			Reason   string
		}{current.Identity, reason})
	}))
}

// SealOnlineMigrationSession is invoked only after the FSM has checked its
// durable source creation fence, configuration and unresolved prepare state.
func SealOnlineMigrationSession(ctx context.Context, st MVCCStore, job, configuration uint64) (OnlineMigrationSession, error) {
	session, err := ReadOnlineMigrationSession(ctx, st, job)
	if err != nil {
		return session, err
	}
	if session.Invalid != "" || session.Closed {
		return session, errors.WithStack(ErrOnlineMigrationInvalid)
	}
	if session.Sealed && configuration >= session.ConfigurationIndex {
		return session, nil
	}
	if session.ConfigurationIndex != configuration {
		return session, errors.WithStack(ErrOnlineMigrationIdentity)
	}
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok {
		return session, errors.WithStack(ErrNotSupported)
	}
	session.Sealed = true
	return session, stageOnlineJSON(ctx, metadata, onlineMigrationKey("source", job, 0), session)
}

func ReadOnlineMigrationFrame(ctx context.Context, st MVCCStore, session OnlineMigrationSession, seq uint64) (MigrationCaptureEntry, error) {
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok {
		return MigrationCaptureEntry{}, errors.WithStack(ErrNotSupported)
	}
	if seq == 0 || seq > session.Sequence {
		return MigrationCaptureEntry{}, errors.WithStack(ErrMigrationCaptureInput)
	}
	encoded, err := metadata.ReadRaftControlMetadata(ctx, onlineMigrationKey("frame", session.Identity.JobID, seq))
	if err != nil {
		return MigrationCaptureEntry{}, errors.WithStack(err)
	}
	frame, err := DecodeOnlineMigrationFrame(encoded, session)
	if err != nil {
		return frame.Entry, err
	}
	if frame.Sequence != seq {
		return frame.Entry, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return frame.Entry, nil
}

// CaptureOnlineMigrationEntry atomically appends the actual point postimages,
// source manifest and data. accept selects only the moving transfer interval.
func CaptureOnlineMigrationEntry(ctx context.Context, st MVCCStore, job, index uint64, digest [32]byte, accept func([]byte) bool, apply func(context.Context, *MigrationCaptureWriter) error) (MigrationCaptureEntry, error) {
	backend, ok := st.(migrationCaptureBackend)
	if !ok {
		return MigrationCaptureEntry{}, errors.WithStack(ErrNotSupported)
	}
	var entry MigrationCaptureEntry
	err := backend.ApplyRaftEntry(ctx, index, func(tx context.Context) error {
		var err error
		entry, err = captureOnlineMigrationInsideEntry(tx, backend, job, index, digest, accept, apply)
		return err
	})
	return entry, errors.WithStack(err)
}

func captureOnlineMigrationInsideEntry(ctx context.Context, backend migrationCaptureBackend, job, index uint64, digest [32]byte, accept func([]byte) bool, apply func(context.Context, *MigrationCaptureWriter) error) (MigrationCaptureEntry, error) {
	session, err := ReadOnlineMigrationSession(ctx, backend, job)
	if err != nil {
		return MigrationCaptureEntry{}, err
	}
	if err := onlineSourceWritable(session); err != nil {
		return MigrationCaptureEntry{}, err
	}
	if session.LastIndex >= index {
		return replayOnlineMigrationEntry(ctx, backend, session, index, digest)
	}
	if err := checkMigrationCaptureIndex(backend, index); err != nil {
		return MigrationCaptureEntry{}, err
	}
	limits, err := normalizeMigrationCaptureLimits(MigrationCaptureLimits{})
	if err != nil {
		return MigrationCaptureEntry{}, err
	}
	entry := MigrationCaptureEntry{Version: migrationCaptureVersion, Identity: session.Identity, CommandDigest: digest, Limits: limits, Complete: true}
	entry.Identity.Index = index
	if err := collectOnlineMigrationEffects(ctx, &entry, accept, apply); err != nil {
		return entry, err
	}
	return entry, publishOnlineMigrationFrame(ctx, backend, &session, &entry)
}

func onlineSourceWritable(session OnlineMigrationSession) error {
	if session.Invalid != "" || session.Closed {
		return errors.WithStack(ErrOnlineMigrationInvalid)
	}
	if session.Sealed {
		return errors.WithStack(ErrOnlineMigrationSealed)
	}
	return nil
}

func replayOnlineMigrationEntry(ctx context.Context, backend migrationCaptureBackend, session OnlineMigrationSession, index uint64, digest [32]byte) (MigrationCaptureEntry, error) {
	if session.LastIndex != index || session.Sequence == 0 {
		return MigrationCaptureEntry{}, errors.WithStack(ErrMigrationCaptureStaleIndex)
	}
	entry, err := ReadOnlineMigrationFrame(ctx, backend, session, session.Sequence)
	if err != nil {
		return entry, err
	}
	if entry.CommandDigest != digest {
		return entry, errors.WithStack(ErrMigrationCaptureConflict)
	}
	return entry, nil
}

func collectOnlineMigrationEffects(ctx context.Context, entry *MigrationCaptureEntry, accept func([]byte) bool, apply func(context.Context, *MigrationCaptureWriter) error) error {
	scope, _ := ctx.Value(raftEntryScopeKey{}).(*raftEntryScope)
	if scope == nil || scope.capture != nil || apply == nil {
		return errors.WithStack(ErrRaftControlScopeRequired)
	}
	collector := &migrationCaptureCollector{entry: entry, accept: accept}
	scope.capture = collector
	writer := &MigrationCaptureWriter{collector: collector, active: true}
	defer func() { writer.active = false }()
	if err := apply(ctx, writer); err != nil {
		return err
	}
	if writer.failed != nil {
		return writer.failed
	}
	if !validMigrationCaptureRecord(*entry) {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	_, err := encodeMigrationCaptureEntry(entry)
	return err
}

func publishOnlineMigrationFrame(ctx context.Context, backend migrationCaptureBackend, session *OnlineMigrationSession, entry *MigrationCaptureEntry) error {
	key := onlineMigrationKey("frame", session.Identity.JobID, session.Sequence+1)
	encoded, err := EncodeOnlineMigrationFrame(session.Sequence+1, *entry)
	if err != nil {
		return err
	}
	size := uint64(len(encoded) + len(key))
	if !entry.Complete || session.Sequence >= onlineMigrationMaxFrames || size > onlineMigrationMaxBytes-session.Bytes {
		session.Invalid = "capture budget exhausted"
		return stageOnlineJSON(ctx, backend, onlineMigrationKey("source", session.Identity.JobID, 0), session)
	}
	if err := backend.StageRaftControlMetadata(ctx, key, encoded); err != nil {
		return errors.WithStack(err)
	}
	session.Sequence++
	session.LastIndex = entry.Identity.Index
	session.Bytes += size
	return stageOnlineJSON(ctx, backend, onlineMigrationKey("source", session.Identity.JobID, 0), session)
}

// OnlineMigrationSupported identifies the persistent, indexed implementation.
func OnlineMigrationSupported(st MVCCStore) bool {
	s, ok := st.(*pebbleStore)
	return ok && s.cipher == nil
}

// OnlineMigrationLineage binds sessions to the ownership runtime generation.
func OnlineMigrationLineage(generation string) [32]byte { return sha256.Sum256([]byte(generation)) }

// OnlineMigrationFrame binds a sequential transfer position to its Raft effect.
type OnlineMigrationFrame struct {
	Sequence uint64
	Entry    MigrationCaptureEntry
}

func EncodeOnlineMigrationManifest(session OnlineMigrationSession) ([]byte, error) {
	encoded, err := json.Marshal(session)
	return encoded, errors.WithStack(err)
}

func DecodeOnlineMigrationManifest(encoded []byte) (OnlineMigrationSession, error) {
	var session OnlineMigrationSession
	if err := decodeOnlineTransfer(encoded, &session); err != nil {
		return session, err
	}
	if !validOnlineSessionHeader(session, session.Identity.JobID) {
		return session, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return session, nil
}

func EncodeOnlineMigrationFrame(seq uint64, entry MigrationCaptureEntry) ([]byte, error) {
	encoded, err := json.Marshal(OnlineMigrationFrame{seq, entry})
	return encoded, errors.WithStack(err)
}

func DecodeOnlineMigrationFrame(encoded []byte, session OnlineMigrationSession) (OnlineMigrationFrame, error) {
	var frame OnlineMigrationFrame
	if err := decodeOnlineTransfer(encoded, &frame); err != nil {
		return frame, err
	}
	identity := session.Identity
	identity.Index = frame.Entry.Identity.Index
	entryBytes, err := json.Marshal(frame.Entry)
	if err != nil {
		return frame, errors.WithStack(err)
	}
	entry, err := decodeMigrationCaptureEntry(entryBytes, identity)
	if err != nil {
		return frame, err
	}
	if !entry.Complete || frame.Sequence == 0 || frame.Sequence > session.Sequence || identity.Index <= session.StartIndex || identity.Index > session.LastIndex {
		return frame, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return frame, nil
}

func decodeOnlineTransfer(encoded []byte, value any) error {
	if len(encoded) == 0 || len(encoded) > migrationCaptureMaxEncodedBytes {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return nil
}

// ValidOnlineOwnershipGeneration checks that a recoverable ownership nonce
// has the protocol's length and is bound to the capture lineage.
func ValidOnlineOwnershipGeneration(generation string, lineage [32]byte) bool {
	return len(generation) >= 16 && len(generation) <= 128 && OnlineMigrationLineage(generation) == lineage
}

func validOnlineSessionIdentity(session OnlineMigrationSession, job uint64) bool {
	return session.Version == OnlineMigrationVersion && session.Identity.JobID == job && validMigrationCaptureIdentity(session.Identity) &&
		(session.OwnershipGeneration == "" || ValidOnlineOwnershipGeneration(session.OwnershipGeneration, session.Identity.Lineage))
}

func validOnlineSessionHeader(session OnlineMigrationSession, job uint64) bool {
	return validOnlineSessionIdentity(session, job) && session.StartIndex == session.Identity.Index && session.ConfigurationIndex != 0 && session.LastIndex >= session.StartIndex && session.Sequence <= onlineMigrationMaxFrames && session.Bytes <= onlineMigrationMaxBytes
}

func validOnlineSessionStart(session OnlineMigrationSession) bool {
	return validOnlineSessionHeader(session, session.Identity.JobID) && session.LastIndex == session.StartIndex && session.Sequence == 0 && session.Invalid == "" && !session.Sealed && !session.Closed
}
