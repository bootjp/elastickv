package filesystem

import (
	"bytes"
	"context"
	"crypto/rand"
	"slices"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	operationCASRetries    = 64
	maxOperationEffectKeys = 65535
	operationJournalRows   = 2
)

type operationJournalBackend interface {
	Snapshot(context.Context) (uint64, error)
	GetAt(context.Context, []byte, uint64) ([]byte, error)
	ScanAt(context.Context, []byte, []byte, int, uint64) ([]*store.KVPair, error)
	Commit(context.Context, uint64, []*kv.Elem[kv.OP], [][]byte) (*kv.CoordinateResponse, error)
	JournalRouteVersion() (uint64, bool)
	CommitAtRouteVersion(context.Context, uint64, []*kv.Elem[kv.OP], [][]byte, uint64) (*kv.CoordinateResponse, error)
	CommittedVersionAt(context.Context, []byte, uint64) (bool, error)
	CommittedVersionAtGroup(context.Context, []byte, uint64, uint64) (bool, error)
	GroupIDForKey([]byte) (uint64, error)
}

type serviceOperationBackend struct{ service *Service }

func (b serviceOperationBackend) Snapshot(ctx context.Context) (uint64, error) {
	return b.service.readTS(ctx)
}

func (b serviceOperationBackend) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	value, err := b.service.store.GetAt(ctx, key, ts)
	return value, errors.Wrap(err, "read operation journal")
}

func (b serviceOperationBackend) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	page, err := b.service.store.ScanAt(ctx, start, end, limit, ts)
	return page, errors.Wrap(err, "scan operation journal")
}

func (b serviceOperationBackend) Commit(ctx context.Context, ts uint64, elems []*kv.Elem[kv.OP], readKeys [][]byte) (*kv.CoordinateResponse, error) {
	return b.commit(ctx, ts, elems, readKeys, 0)
}

func (b serviceOperationBackend) JournalRouteVersion() (uint64, bool) {
	if source, ok := b.service.dispatch.(interface{ JournalRouteVersion() (uint64, bool) }); ok {
		version, ready := source.JournalRouteVersion()
		if ready {
			return version, true
		}
		if _, sharded := b.service.store.(*kv.ShardStore); sharded {
			return 0, false
		}
		return 0, true
	}
	_, sharded := b.service.store.(*kv.ShardStore)
	return 0, !sharded
}

func (b serviceOperationBackend) CommitAtRouteVersion(ctx context.Context, ts uint64, elems []*kv.Elem[kv.OP], readKeys [][]byte, observed uint64) (*kv.CoordinateResponse, error) {
	return b.commit(ctx, ts, elems, readKeys, observed)
}

func (b serviceOperationBackend) commit(ctx context.Context, ts uint64, elems []*kv.Elem[kv.OP], readKeys [][]byte, observed uint64) (*kv.CoordinateResponse, error) {
	var primary []byte
	for _, elem := range elems {
		if elem == nil {
			return nil, errors.WithStack(ErrInvalid)
		}
		if !fskeys.IsOperationKey(elem.Key) {
			continue
		}
		if primary != nil {
			return nil, errors.WithStack(ErrInvalid)
		}
		primary = elem.Key
	}
	return b.service.dispatchTxnWithPrimaryAtRoute(ctx, ts, elems, readKeys, true, primary, observed)
}

func (b serviceOperationBackend) CommittedVersionAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	if reader, ok := b.service.store.(interface {
		CommittedVersionAtAuthoritative(context.Context, []byte, uint64) (bool, error)
	}); ok {
		exists, err := reader.CommittedVersionAtAuthoritative(ctx, key, ts)
		return exists, errors.Wrap(err, "verify authoritative operation effect")
	}
	ok, err := b.service.store.CommittedVersionAt(ctx, key, ts)
	return ok, errors.Wrap(err, "verify operation effect")
}

func (b serviceOperationBackend) GroupIDForKey(key []byte) (uint64, error) {
	if router, ok := b.service.dispatch.(interface {
		ResolveOperationGroupForKey([]byte) (uint64, error)
	}); ok {
		groupID, err := router.ResolveOperationGroupForKey(key)
		if errors.Is(err, kv.ErrOperationGroupResolverUnavailable) {
			if _, sharded := b.service.store.(*kv.ShardStore); !sharded {
				return 0, nil
			}
		}
		return groupID, errors.Wrap(err, "route operation effect")
	}
	if _, sharded := b.service.store.(*kv.ShardStore); sharded {
		return 0, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return 0, nil
}

func (b serviceOperationBackend) CommittedVersionAtGroup(ctx context.Context, key []byte, groupID, ts uint64) (bool, error) {
	if reader, ok := b.service.store.(interface {
		CommittedVersionAtGroupAuthoritative(context.Context, []byte, uint64, uint64) (bool, error)
	}); ok {
		exists, err := reader.CommittedVersionAtGroupAuthoritative(ctx, key, groupID, ts)
		return exists, errors.Wrap(err, "verify participant receipt")
	}
	if groupID != 0 {
		return false, errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return b.CommittedVersionAt(ctx, key, ts)
}

// OperationJournal stores one logical request and its result in the same
// transaction as its effects. FUSE semantic planners will call this module in
// M3; it never infers an abort from a missing result after an uncertain RPC.
type OperationJournal struct {
	backend operationJournalBackend
}

func NewOperationJournal(service *Service) (*OperationJournal, error) {
	if service == nil {
		return nil, errors.WithStack(ErrInvalid)
	}
	return &OperationJournal{backend: serviceOperationBackend{service: service}}, nil
}

func newOperationJournalWithBackend(backend operationJournalBackend) *OperationJournal {
	return &OperationJournal{backend: backend}
}

func randomOperationToken() ([16]byte, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return token, errors.Wrap(err, "allocate operation identity")
	}
	return token, nil
}

// EnsureFilesystem returns the single Raft-replicated identity shared by all
// mounts. A conflict rereads the winner; a lost response remains unknown.
func (j *OperationJournal) EnsureFilesystem(ctx context.Context) ([16]byte, error) {
	for retry := range operationCASRetries {
		ts, err := j.backend.Snapshot(ctx)
		if err != nil {
			return [16]byte{}, errors.Wrap(err, "fence filesystem identity")
		}
		identity, exists, err := j.loadFilesystemAt(ctx, ts)
		if err != nil || exists {
			return identity, err
		}
		identity, err = randomOperationToken()
		if err != nil {
			return [16]byte{}, err
		}
		value := append([]byte{operationSessionVersion}, identity[:]...)
		key := fskeys.OperationFilesystemKey()
		_, err = j.backend.Commit(ctx, ts, []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: value}}, [][]byte{key})
		if err == nil {
			return identity, nil
		}
		if !IsWriteConflict(err) {
			return [16]byte{}, errors.Wrap(err, "register filesystem identity")
		}
		if err = waitOperationCASRetry(ctx, retry); err != nil {
			return [16]byte{}, err
		}
	}
	return [16]byte{}, errors.WithStack(ErrContentionLimit)
}

func (j *OperationJournal) loadFilesystemAt(ctx context.Context, ts uint64) ([16]byte, bool, error) {
	var identity [16]byte
	raw, err := j.backend.GetAt(ctx, fskeys.OperationFilesystemKey(), ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return identity, false, nil
	}
	if err != nil {
		return identity, false, errors.Wrap(err, "read filesystem identity")
	}
	if len(raw) != 17 || raw[0] != operationSessionVersion {
		return identity, false, errors.WithStack(ErrOperationRecordCorrupt)
	}
	copy(identity[:], raw[1:])
	if identity == [16]byte{} {
		return identity, false, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return identity, true, nil
}

func (j *OperationJournal) RegisterSession(ctx context.Context, filesystemID [16]byte, owner string) (OperationSessionID, error) {
	if filesystemID == [16]byte{} || owner == "" {
		return OperationSessionID{}, errors.WithStack(ErrInvalid)
	}
	token, err := randomOperationToken()
	if err != nil {
		return OperationSessionID{}, err
	}
	id := OperationSessionID{Filesystem: filesystemID, Session: token}
	if err = j.RegisterSessionWithID(ctx, id, owner); err != nil {
		return id, err
	}
	return id, nil
}

// RegisterSessionWithID lets a mount retain its chosen incarnation across a
// lost registration reply. It never substitutes a new identity on retry.
func (j *OperationJournal) RegisterSessionWithID(ctx context.Context, id OperationSessionID, owner string) error {
	if id.Filesystem == [16]byte{} || id.Session == [16]byte{} || owner == "" {
		return errors.WithStack(ErrInvalid)
	}
	for retry := range operationCASRetries {
		err := j.registerSessionOnce(ctx, id, owner)
		if !IsWriteConflict(err) {
			return err
		}
		if err = waitOperationCASRetry(ctx, retry); err != nil {
			return err
		}
	}
	return errors.WithStack(ErrContentionLimit)
}

func (j *OperationJournal) registerSessionOnce(ctx context.Context, id OperationSessionID, owner string) error {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return errors.Wrap(err, "fence operation session registration")
	}
	filesystemID, exists, err := j.loadFilesystemAt(ctx, ts)
	if err != nil {
		return err
	}
	if !exists || filesystemID != id.Filesystem {
		return errors.WithStack(ErrInvalid)
	}
	previous, exists, err := j.loadSessionAt(ctx, id, ts)
	if err != nil {
		return err
	}
	if exists {
		return j.verifyExistingRegistration(ctx, id, owner, previous, ts)
	}
	ownerKey := fskeys.OperationOwnerKey(id.Filesystem, []byte(owner))
	if err = j.requireOwnerAvailable(ctx, id.Filesystem, owner, ownerKey, ts); err != nil {
		return err
	}
	raw, err := encodeOperationSession(newOperationSession(id, owner))
	if err != nil {
		return err
	}
	ownerValue := append([]byte{operationSessionVersion}, id.Session[:]...)
	_, err = j.backend.Commit(ctx, ts, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: id.key(), Value: raw},
		{Op: kv.Put, Key: ownerKey, Value: ownerValue},
	}, [][]byte{id.key(), ownerKey, fskeys.OperationFilesystemKey()})
	return errors.Wrap(err, "register operation session")
}

func (j *OperationJournal) verifyExistingRegistration(ctx context.Context, id OperationSessionID, owner string, previous operationSession, ts uint64) error {
	if previous.Owner != owner {
		return errors.WithStack(ErrOperationIDReused)
	}
	if previous.Closed {
		return errors.WithStack(ErrOperationSessionClosed)
	}
	ownerSession, owned, err := j.loadOwnerAt(ctx, fskeys.OperationOwnerKey(id.Filesystem, []byte(owner)), ts)
	if err != nil {
		return err
	}
	if !owned || ownerSession != id.Session {
		return errors.WithStack(ErrOperationSessionClosed)
	}
	return nil
}

func (j *OperationJournal) requireOwnerAvailable(ctx context.Context, filesystemID [16]byte, owner string, ownerKey []byte, ts uint64) error {
	previousSession, owned, err := j.loadOwnerAt(ctx, ownerKey, ts)
	if err != nil || !owned {
		return err
	}
	previous, found, err := j.loadSessionAt(ctx, OperationSessionID{Filesystem: filesystemID, Session: previousSession}, ts)
	if err != nil {
		return err
	}
	if !found || previous.Owner != owner {
		return errors.WithStack(ErrOperationRecordCorrupt)
	}
	if !previous.Closed || previous.Pending != 0 {
		return errors.WithStack(ErrOperationPending)
	}
	return nil
}

func (j *OperationJournal) loadOwnerAt(ctx context.Context, key []byte, ts uint64) ([16]byte, bool, error) {
	var sessionID [16]byte
	raw, err := j.backend.GetAt(ctx, key, ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return sessionID, false, nil
	}
	if err != nil {
		return sessionID, false, errors.Wrap(err, "read operation owner")
	}
	if len(raw) != 17 || raw[0] != operationSessionVersion {
		return sessionID, false, errors.WithStack(ErrOperationRecordCorrupt)
	}
	copy(sessionID[:], raw[1:])
	return sessionID, true, nil
}

// Admit records the immutable intent before an effect can be proposed. A
// duplicate identity returns its original record only for identical bytes.
func (j *OperationJournal) Admit(ctx context.Context, id OperationID, intent []byte, resultLimit, effectLimit uint32) (operationRecord, error) {
	record := newOperationRecord(id, intent, resultLimit, effectLimit)
	if err := validateOperationRecord(record); err != nil {
		return operationRecord{}, err
	}
	for retry := range operationCASRetries {
		admitted, err := j.admitOnce(ctx, record)
		if IsWriteConflict(err) {
			if err = waitOperationCASRetry(ctx, retry); err != nil {
				return operationRecord{}, err
			}
			continue
		}
		return admitted, err
	}
	return operationRecord{}, errors.WithStack(ErrContentionLimit)
}

func (j *OperationJournal) admitOnce(ctx context.Context, record operationRecord) (operationRecord, error) {
	ts, err := j.backend.Snapshot(ctx)
	if err != nil {
		return operationRecord{}, errors.Wrap(err, "fence operation admission")
	}
	id := record.ID
	session, exists, err := j.loadSessionAt(ctx, OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}, ts)
	if err != nil {
		return operationRecord{}, err
	}
	if !exists {
		return operationRecord{}, errors.WithStack(ErrOperationSessionNotFound)
	}
	previous, found, err := j.loadRecordAt(ctx, id, ts)
	if err != nil {
		return operationRecord{}, err
	}
	if found {
		if previous.Fingerprint != record.Fingerprint || !bytes.Equal(previous.Intent, record.Intent) {
			return operationRecord{}, errors.WithStack(ErrOperationIDReused)
		}
		return previous, nil
	}
	if err = reserveOperation(&session, record); err != nil {
		return operationRecord{}, err
	}
	return record, j.storeRecordAndSession(ctx, ts, record, session, nil, nil)
}

func reserveOperation(session *operationSession, record operationRecord) error {
	if session.retired(record.ID.Sequence) {
		return errors.WithStack(ErrOperationRetired)
	}
	if session.Closed {
		return errors.WithStack(ErrOperationSessionClosed)
	}
	if session.RetainedCount >= session.MaxCount || record.Reservation > session.MaxBytes-session.RetainedBytes {
		return errors.WithStack(ErrOperationQuota)
	}
	session.Pending++
	session.RetainedCount++
	session.RetainedBytes += record.Reservation
	return nil
}

func (j *OperationJournal) loadSessionAt(ctx context.Context, id OperationSessionID, ts uint64) (operationSession, bool, error) {
	raw, err := j.backend.GetAt(ctx, id.key(), ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return operationSession{}, false, nil
	}
	if err != nil {
		return operationSession{}, false, errors.Wrap(err, "read operation session")
	}
	session, err := decodeOperationSession(raw)
	if err != nil || session.ID != id {
		return operationSession{}, false, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return session, true, nil
}

func (j *OperationJournal) loadRecordAt(ctx context.Context, id OperationID, ts uint64) (operationRecord, bool, error) {
	raw, err := j.backend.GetAt(ctx, id.key(), ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return operationRecord{}, false, nil
	}
	if err != nil {
		return operationRecord{}, false, errors.Wrap(err, "read operation record")
	}
	record, err := decodeOperationRecord(raw)
	if err != nil || record.ID != id {
		return operationRecord{}, false, errors.WithStack(ErrOperationRecordCorrupt)
	}
	return record, true, nil
}

func (j *OperationJournal) storeRecordAndSession(ctx context.Context, ts uint64, record operationRecord, session operationSession, extra []*kv.Elem[kv.OP], extraReadKeys [][]byte) error {
	return j.storeRecordAndSessionAtRoute(ctx, ts, record, session, extra, extraReadKeys, 0)
}

func (j *OperationJournal) storeRecordAndSessionAtRoute(ctx context.Context, ts uint64, record operationRecord, session operationSession, extra []*kv.Elem[kv.OP], extraReadKeys [][]byte, observed uint64) error {
	recordRaw, err := encodeOperationRecord(record)
	if err != nil {
		return err
	}
	sessionRaw, err := encodeOperationSession(session)
	if err != nil {
		return err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(extra)+operationJournalRows)
	elems = append(elems, extra...)
	commitTSOffset := uint64(0)
	if record.State != OperationPending {
		commitTSOffset = operationCommitTSOffset
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: record.ID.key(), Value: recordRaw, CommitTSValueOffset: commitTSOffset},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: session.ID.key(), Value: sessionRaw},
	)
	readKeys := make([][]byte, 0, len(extraReadKeys)+operationJournalRows)
	readKeys = append(readKeys, record.ID.key(), session.ID.key())
	readKeys = append(readKeys, extraReadKeys...)
	if observed != 0 {
		_, err = j.backend.CommitAtRouteVersion(ctx, ts, elems, readKeys, observed)
	} else {
		_, err = j.backend.Commit(ctx, ts, elems, readKeys)
	}
	return errors.Wrap(err, "publish operation journal")
}

func (session operationSession) retired(sequence uint64) bool {
	return sequence <= session.RejectBefore || slices.Contains(session.AckGaps, sequence)
}

func operationEffectKeys(elems []*kv.Elem[kv.OP], recordKey, sessionKey []byte) ([][]byte, error) {
	keys := make([][]byte, 0, len(elems))
	seen := make(map[string]struct{}, len(elems))
	for _, elem := range elems {
		if elem == nil || len(elem.Key) == 0 || elem.Op == kv.DelPrefix || fskeys.IsOperationMetadataKey(elem.Key) ||
			bytes.Equal(elem.Key, recordKey) || bytes.Equal(elem.Key, sessionKey) {
			return nil, errors.WithStack(ErrInvalid)
		}
		if _, exists := seen[string(elem.Key)]; exists {
			return nil, errors.WithStack(ErrInvalid)
		}
		seen[string(elem.Key)] = struct{}{}
		keys = append(keys, append([]byte(nil), elem.Key...))
	}
	if len(keys) > maxOperationEffectKeys {
		return nil, errors.WithStack(ErrInvalid)
	}
	return keys, nil
}
