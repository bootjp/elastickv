package kv

import (
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	defaultMaxActiveBackupPins = 4
	defaultBackupPinSweepEvery = time.Second
)

var (
	ErrInvalidBackupPin     = errors.New("backup pin is invalid")
	ErrTooManyActiveBackups = errors.New("too many active backup pins")
)

type BackupPinID [16]byte

func (id BackupPinID) IsZero() bool {
	return id == BackupPinID{}
}

func (id BackupPinID) String() string {
	return hex.EncodeToString(id[:])
}

type backupDeadlinePin struct {
	readTS   uint64
	deadline time.Time
}

type backupPinKey struct {
	id      BackupPinID
	groupID uint64
}

func newBackupPinKey(pinID BackupPinID, groupID uint64) backupPinKey {
	return backupPinKey{id: pinID, groupID: groupID}
}

type ActiveTimestampTrackerOption func(*ActiveTimestampTracker)

func WithActiveTimestampTrackerMaxBackupPins(maxPins int) ActiveTimestampTrackerOption {
	return func(t *ActiveTimestampTracker) {
		if maxPins > 0 {
			t.maxBackupPins = maxPins
		}
	}
}

func WithActiveTimestampTrackerSweepInterval(interval time.Duration) ActiveTimestampTrackerOption {
	return func(t *ActiveTimestampTracker) {
		t.sweepEvery = interval
	}
}

func WithActiveTimestampTrackerLogger(logger *slog.Logger) ActiveTimestampTrackerOption {
	return func(t *ActiveTimestampTracker) {
		if logger != nil {
			t.logger = logger
		}
	}
}

// ActiveTimestampTracker tracks in-flight read or transaction timestamps that
// must remain readable while background compaction is running.
type ActiveTimestampTracker struct {
	mu            sync.Mutex
	nextID        uint64
	active        map[uint64]uint64
	backupPins    map[backupPinKey]backupDeadlinePin
	maxBackupPins int
	sweepEvery    time.Duration
	sweepOnce     sync.Once
	stopCh        chan struct{}
	closeOnce     sync.Once
	logger        *slog.Logger
}

// ActiveTimestampToken releases one tracked timestamp when the owning
// operation completes.
type ActiveTimestampToken struct {
	tracker *ActiveTimestampTracker
	id      uint64
	once    sync.Once
}

func NewActiveTimestampTracker(opts ...ActiveTimestampTrackerOption) *ActiveTimestampTracker {
	t := &ActiveTimestampTracker{
		active:        make(map[uint64]uint64),
		backupPins:    make(map[backupPinKey]backupDeadlinePin),
		maxBackupPins: defaultMaxActiveBackupPins,
		sweepEvery:    defaultBackupPinSweepEvery,
		stopCh:        make(chan struct{}),
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func (t *ActiveTimestampTracker) Pin(ts uint64) *ActiveTimestampToken {
	if t == nil || ts == 0 || ts == ^uint64(0) {
		return &ActiveTimestampToken{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id := t.nextID
	t.active[id] = ts
	return &ActiveTimestampToken{
		tracker: t,
		id:      id,
	}
}

func (t *ActiveTimestampTracker) Oldest() uint64 {
	return t.oldestForGroup(0, false)
}

// OldestForGroup returns the oldest process-wide read pin or backup pin for
// groupID. Backup pins for other Raft groups do not constrain this group.
func (t *ActiveTimestampTracker) OldestForGroup(groupID uint64) uint64 {
	return t.oldestForGroup(groupID, true)
}

func (t *ActiveTimestampTracker) oldestForGroup(groupID uint64, scoped bool) uint64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	readOldest := oldestReadTimestamp(t.active)
	backupOldest := oldestBackupTimestamp(t.backupPins, groupID, scoped, time.Now())
	return oldestNonZeroTimestamp(readOldest, backupOldest)
}

func oldestReadTimestamp(active map[uint64]uint64) uint64 {
	var oldest uint64
	for _, ts := range active {
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	return oldest
}

func oldestBackupTimestamp(pins map[backupPinKey]backupDeadlinePin, groupID uint64, scoped bool, now time.Time) uint64 {
	var oldest uint64
	for key, pin := range pins {
		if scoped && key.groupID != groupID {
			continue
		}
		if !pin.deadline.After(now) {
			continue
		}
		if oldest == 0 || pin.readTS < oldest {
			oldest = pin.readTS
		}
	}
	return oldest
}

func oldestNonZeroTimestamp(a, b uint64) uint64 {
	if a == 0 || (b != 0 && b < a) {
		return b
	}
	return a
}

func (t *ActiveTimestampTracker) PinWithDeadline(pinID BackupPinID, readTS uint64, deadline time.Time) error {
	return t.PinWithDeadlineForGroup(pinID, 0, readTS, deadline)
}

func (t *ActiveTimestampTracker) PinWithDeadlineForGroup(pinID BackupPinID, groupID uint64, readTS uint64, deadline time.Time) error {
	return t.pinWithDeadlineForGroup(pinID, groupID, readTS, deadline, true)
}

func (t *ActiveTimestampTracker) ApplyPinWithDeadlineForGroup(pinID BackupPinID, groupID uint64, readTS uint64, deadline time.Time) error {
	return t.pinWithDeadlineForGroup(pinID, groupID, readTS, deadline, false)
}

func (t *ActiveTimestampTracker) pinWithDeadlineForGroup(pinID BackupPinID, groupID uint64, readTS uint64, deadline time.Time, enforceLimit bool) error {
	if t == nil {
		return nil
	}
	if !validBackupDeadlinePin(pinID, readTS, deadline) {
		return errors.WithStack(ErrInvalidBackupPin)
	}
	t.mu.Lock()
	expired := t.reapExpiredBackupPinsLocked(time.Now())
	key := newBackupPinKey(pinID, groupID)
	if enforceLimit && !t.hasBackupPinIDLocked(pinID) && t.activeBackupPinIDCountLocked() >= t.maxBackupPins {
		t.mu.Unlock()
		t.logExpiredBackupPins(expired)
		return errors.WithStack(ErrTooManyActiveBackups)
	}
	t.backupPins[key] = mergeBackupDeadlinePin(
		t.backupPins[key], backupDeadlinePin{readTS: readTS, deadline: deadline},
	)
	t.startBackupPinSweeperLocked()
	t.mu.Unlock()
	t.logExpiredBackupPins(expired)
	return nil
}

func validBackupDeadlinePin(pinID BackupPinID, readTS uint64, deadline time.Time) bool {
	return !pinID.IsZero() && readTS != 0 && readTS != ^uint64(0) && !deadline.IsZero()
}

func mergeBackupDeadlinePin(existing, requested backupDeadlinePin) backupDeadlinePin {
	if existing.readTS != 0 && existing.readTS < requested.readTS {
		requested.readTS = existing.readTS
	}
	if existing.deadline.After(requested.deadline) {
		requested.deadline = existing.deadline
	}
	return requested
}

func (t *ActiveTimestampTracker) Extend(pinID BackupPinID, deadline time.Time) error {
	return t.ExtendForGroup(pinID, 0, deadline)
}

func (t *ActiveTimestampTracker) ExtendForGroup(pinID BackupPinID, groupID uint64, deadline time.Time) error {
	return t.extendForGroup(pinID, groupID, deadline, true)
}

func (t *ActiveTimestampTracker) ApplyExtendForGroup(pinID BackupPinID, groupID uint64, deadline time.Time) error {
	return t.extendForGroup(pinID, groupID, deadline, false)
}

func (t *ActiveTimestampTracker) extendForGroup(pinID BackupPinID, groupID uint64, deadline time.Time, returnMissingExpired bool) error {
	if t == nil {
		return nil
	}
	if pinID.IsZero() || deadline.IsZero() {
		return errors.WithStack(ErrInvalidBackupPin)
	}
	t.mu.Lock()
	key := newBackupPinKey(pinID, groupID)
	pin, exists := t.backupPins[key]
	if !exists {
		t.mu.Unlock()
		if !returnMissingExpired {
			return nil
		}
		return errors.WithStack(ErrInvalidBackupPin)
	}
	if returnMissingExpired && !pin.deadline.After(time.Now()) {
		delete(t.backupPins, key)
		t.mu.Unlock()
		t.logExpiredBackupPins([]expiredBackupPin{{key: key, ts: pin.readTS}})
		return errors.WithStack(ErrInvalidBackupPin)
	}
	if deadline.After(pin.deadline) {
		pin.deadline = deadline
	}
	t.backupPins[key] = pin
	t.mu.Unlock()
	return nil
}

func (t *ActiveTimestampTracker) ReleaseBackupPin(pinID BackupPinID) {
	t.ReleaseBackupPinForGroup(pinID, 0)
}

func (t *ActiveTimestampTracker) ReleaseBackupPinForGroup(pinID BackupPinID, groupID uint64) {
	if t == nil || pinID.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.backupPins, newBackupPinKey(pinID, groupID))
}

func (t *ActiveTimestampTracker) ActiveBackupPinCount() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.backupPins)
}

func (t *ActiveTimestampTracker) BackupPinDeadline(pinID BackupPinID) (time.Time, bool) {
	return t.BackupPinDeadlineForGroup(pinID, 0)
}

func (t *ActiveTimestampTracker) BackupPinDeadlineForGroup(pinID BackupPinID, groupID uint64) (time.Time, bool) {
	if t == nil || pinID.IsZero() {
		return time.Time{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	pin, ok := t.backupPins[newBackupPinKey(pinID, groupID)]
	return pin.deadline, ok
}

func (t *ActiveTimestampTracker) startBackupPinSweeperLocked() {
	if t.sweepEvery <= 0 {
		return
	}
	t.sweepOnce.Do(func() {
		go t.sweepBackupPins()
	})
}

func (t *ActiveTimestampTracker) sweepBackupPins() {
	if t.sweepEvery <= 0 {
		return
	}
	ticker := time.NewTicker(t.sweepEvery)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			t.reapExpiredBackupPins(now)
		case <-t.stopCh:
			return
		}
	}
}

// Close stops the backup-pin sweeper goroutine. It is safe to call more than
// once and on trackers that never started the sweeper.
func (t *ActiveTimestampTracker) Close() {
	if t == nil {
		return
	}
	t.closeOnce.Do(func() {
		close(t.stopCh)
	})
}

func (t *ActiveTimestampTracker) reapExpiredBackupPins(now time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	expired := t.reapExpiredBackupPinsLocked(now)
	t.mu.Unlock()
	t.logExpiredBackupPins(expired)
}

type expiredBackupPin struct {
	key backupPinKey
	ts  uint64
}

func (t *ActiveTimestampTracker) reapExpiredBackupPinsLocked(now time.Time) []expiredBackupPin {
	expired := make([]expiredBackupPin, 0)
	for key, pin := range t.backupPins {
		if !pin.deadline.After(now) {
			expired = append(expired, expiredBackupPin{key: key, ts: pin.readTS})
			delete(t.backupPins, key)
		}
	}
	return expired
}

func (t *ActiveTimestampTracker) hasBackupPinIDLocked(pinID BackupPinID) bool {
	for key := range t.backupPins {
		if key.id == pinID {
			return true
		}
	}
	return false
}

func (t *ActiveTimestampTracker) activeBackupPinIDCountLocked() int {
	seen := make(map[BackupPinID]struct{}, len(t.backupPins))
	for key := range t.backupPins {
		seen[key.id] = struct{}{}
	}
	return len(seen)
}

func (t *ActiveTimestampTracker) logExpiredBackupPins(expired []expiredBackupPin) {
	for _, pin := range expired {
		t.logger.Warn("backup_pin_expired", "pin_id", pin.key.id.String(), "raft_group_id", pin.key.groupID, "read_ts", pin.ts)
	}
}

func (t *ActiveTimestampToken) Release() {
	if t == nil || t.tracker == nil || t.id == 0 {
		return
	}
	t.once.Do(func() {
		t.tracker.mu.Lock()
		defer t.tracker.mu.Unlock()
		delete(t.tracker.active, t.id)
	})
}
