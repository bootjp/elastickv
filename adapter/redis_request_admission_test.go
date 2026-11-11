package adapter

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type redisOwnershipTestKey struct{}

type redisTestOwnership struct {
	ctx        context.Context
	revoked    atomic.Bool
	finishes   int
	joins      int
	original   error
	onValidate func()
	onFinish   func()
}

func (o *redisTestOwnership) Context() context.Context { return o.ctx }
func (o *redisTestOwnership) Validate() error {
	if o.onValidate != nil {
		o.onValidate()
	}
	if o.revoked.Load() {
		return errRedisAdmission
	}
	return nil
}
func (o *redisTestOwnership) Finish(original error) error {
	o.finishes++
	o.original = original
	if o.onFinish != nil {
		o.onFinish()
	}
	if err := o.Validate(); err != nil {
		return errors.Join(original, err)
	}
	return original
}
func (o *redisTestOwnership) CancelAndJoin() { o.joins++ }

type redisOwnedTestConn struct {
	commandRecorder
	raw bytes.Buffer
}

func (c *redisOwnedTestConn) WriteRaw(b []byte) { _, _ = c.raw.Write(b) }

func redisOwnedTestServer(owner *redisTestOwnership) *RedisServer {
	return NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisRequestAdmission(func(context.Context) (RequestOwnership, error) { return owner, nil }))
}

func TestRedisRequestAdmissionBeforeHeavyExecution(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.WithValue(context.Background(), redisOwnershipTestKey{}, "captured")}
	s := redisOwnedTestServer(owner)
	mints := 0
	s.requestAdmission = func(context.Context) (RequestOwnership, error) {
		mints++
		require.Empty(t, s.heavyCommandLimiter.slots, "capture before heavy enqueue/slot acquisition")
		return owner, nil
	}
	owner.onValidate = func() {
		// Simulate revocation after capture but before the queued callback runs.
		owner.revoked.Store(true)
	}
	called := false
	c := &redisOwnedTestConn{}
	s.dispatchCommand(c, cmdHGetAll, func(redcon.Conn, redcon.Command) { called = true }, redcon.Command{}, time.Now())
	require.False(t, called)
	require.Equal(t, 1, mints, "stale captured work must never obtain a new owner")
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
	require.Empty(t, c.raw.String())
	require.Len(t, c.writes, 1)
	require.Equal(t, errRedisAdmission.Error(), c.writes[0].s)
}

type redisOwnershipReadStore struct {
	store.MVCCStore
	onRead func(context.Context)
}

func (s *redisOwnershipReadStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.onRead(ctx)
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *redisOwnershipReadStore) ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	s.onRead(ctx)
	return s.MVCCStore.ExistsAt(ctx, key, ts)
}

func (s *redisOwnershipReadStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.onRead(ctx)
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestRedisRequestAdmissionHashReads(t *testing.T) {
	t.Parallel()
	for _, name := range []string{cmdHGet, cmdHGetAll} {
		t.Run(name, func(t *testing.T) {
			owner := &redisTestOwnership{ctx: context.WithValue(context.Background(), redisOwnershipTestKey{}, "hash-owner")}
			base := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			key := []byte("hash")
			require.NoError(t, base.PutAt(context.Background(), store.HashFieldKey(key, []byte("f")), []byte("v"), 1, 0))
			require.NoError(t, base.PutAt(context.Background(), store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), 1, 0))
			reads := 0
			st := &redisOwnershipReadStore{MVCCStore: base, onRead: func(ctx context.Context) {
				reads++
				require.Equal(t, "hash-owner", ctx.Value(redisOwnershipTestKey{}))
			}}
			s := redisOwnedTestServer(owner)
			s.store, s.coordinator = st, newLocalAdapterCoordinator(base)
			c := &redisOwnedTestConn{}
			args := [][]byte{[]byte(name), key}
			handler := s.hgetall
			want := "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
			if name == cmdHGet {
				handler = s.hget
				args = append(args, []byte("f"))
				want = "$1\r\nv\r\n"
			}
			s.dispatchCommand(c, name, handler, redcon.Command{Args: args}, time.Now())
			require.Positive(t, reads)
			require.Equal(t, want, c.raw.String())
			require.Empty(t, c.writes)
		})
	}
}

func TestRedisRequestAdmissionDirectReadAndResponse(t *testing.T) {
	t.Parallel()
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "revoked"}[revoke], func(t *testing.T) {
			owner := &redisTestOwnership{ctx: context.WithValue(context.Background(), redisOwnershipTestKey{}, "captured")}
			base := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			require.NoError(t, base.PutAt(context.Background(), redisStrKey([]byte("k")), encodeRedisStr([]byte("secret"), nil), 1, 0))
			reads := 0
			st := &redisOwnershipReadStore{MVCCStore: base, onRead: func(ctx context.Context) {
				reads++
				require.Equal(t, "captured", ctx.Value(redisOwnershipTestKey{}))
				if revoke {
					owner.revoked.Store(true)
				}
			}}
			s := redisOwnedTestServer(owner)
			s.store, s.coordinator = st, newLocalAdapterCoordinator(base)
			// Metrics wrapping must not erase the command's context carrier.
			s.requestObserver = monitoring.NewRegistry("node", "addr").RedisObserver()
			c := &redisOwnedTestConn{}
			s.dispatchCommand(c, cmdGet, s.get, redcon.Command{Args: [][]byte{[]byte(cmdGet), []byte("k")}}, time.Now())
			require.Positive(t, reads)
			require.Equal(t, 1, owner.finishes)
			require.Equal(t, 1, owner.joins)
			if revoke {
				require.Empty(t, c.raw.String(), "do not publish data read after ownership expires")
				require.Len(t, c.writes, 1)
			} else {
				require.Equal(t, "$6\r\nsecret\r\n", c.raw.String())
				require.Empty(t, c.writes)
			}
		})
	}
}

func TestRedisRequestAdmissionFinishChecksBeforeResponse(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.Background()}
	owner.onFinish = func() { owner.revoked.Store(true) }
	s := redisOwnedTestServer(owner)
	c := &redisOwnedTestConn{}
	s.dispatchCommand(c, cmdPing, s.ping, redcon.Command{Args: [][]byte{[]byte(cmdPing)}}, time.Now())
	require.Empty(t, c.raw.String())
	require.Len(t, c.writes, 1)
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
}

func TestRedisRequestAdmissionJoinsBeforePublishing(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.Background()}
	finishing, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	owner.onFinish = func() { close(finishing); <-release }
	s := redisOwnedTestServer(owner)
	c := &redisOwnedTestConn{}
	go func() {
		defer close(done)
		s.dispatchCommand(c, cmdPing, s.ping, redcon.Command{Args: [][]byte{[]byte(cmdPing)}}, time.Now())
	}()
	<-finishing
	require.Empty(t, c.raw.String(), "Finish must join children before releasing the result")
	close(release)
	<-done
	require.Equal(t, "+PONG\r\n", c.raw.String())
	require.Equal(t, 1, owner.joins)
}

func TestRedisRequestAdmissionUnknownCommandsFailClosed(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"UNOWNED-DETACH", "FUTURE-COMMAND"} {
		t.Run(name, func(t *testing.T) {
			owner := &redisTestOwnership{ctx: context.Background()}
			s := redisOwnedTestServer(owner)
			c := &redisOwnedTestConn{}
			called := false
			s.dispatchCommand(c, name, func(redcon.Conn, redcon.Command) { called = true }, redcon.Command{}, time.Now())
			require.False(t, called, "new commands require an explicit ownership audit")
			require.Empty(t, c.raw.String())
			require.Len(t, c.writes, 1)
			require.Equal(t, errRedisOwnershipUnsupported.Error(), c.writes[0].s)
			require.Equal(t, 1, owner.finishes)
			require.Equal(t, 1, owner.joins)
		})
	}
}

func TestRedisRequestAdmissionFullSlotAndPanicRelease(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.Background()}
	s := redisOwnedTestServer(owner)
	s.heavyCommandLimiter = newRedisHeavyCommandLimiter(1)
	c := &redisOwnedTestConn{}
	require.True(t, s.heavyCommandLimiter.submit(func() {
		s.dispatchCommand(c, cmdHGetAll, func(redcon.Conn, redcon.Command) { t.Fatal("full slot executed") }, redcon.Command{Args: [][]byte{[]byte(cmdHGetAll)}}, time.Now())
	}))
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
	require.Error(t, owner.original)
	owner = &redisTestOwnership{ctx: context.Background()}
	s = redisOwnedTestServer(owner)
	require.Panics(t, func() {
		s.dispatchCommand(c, cmdGet, func(redcon.Conn, redcon.Command) { panic("test panic") }, redcon.Command{}, time.Now())
	})
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
}

func TestRedisRequestAdmissionInvalidHookAndLegacy(t *testing.T) {
	t.Parallel()
	for _, hook := range []RequestAdmission{
		func(context.Context) (RequestOwnership, error) { return nil, nil },
		func(context.Context) (RequestOwnership, error) { return nil, errRedisAdmission },
		func(context.Context) (RequestOwnership, error) { return &redisTestOwnership{}, nil },
	} {
		s := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisRequestAdmission(hook))
		c := &redisOwnedTestConn{}
		s.dispatchCommand(c, cmdGet, func(redcon.Conn, redcon.Command) { t.Fatal("invalid owner executed") }, redcon.Command{}, time.Now())
		require.Len(t, c.writes, 1)
	}
	s := NewRedisServer(nil, "", nil, nil, nil, nil)
	c := &commandRecorder{}
	s.dispatchCommand(c, cmdPing, s.ping, redcon.Command{Args: [][]byte{[]byte(cmdPing)}}, time.Now())
	require.Len(t, c.writes, 1)
	require.Equal(t, "PONG", c.writes[0].s)
}

func TestRedisRequestAdmissionDoesNotEnqueueDetachedMaintenance(t *testing.T) {
	t.Parallel()
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	compactor := NewDeltaCompactor(base, newLocalAdapterCoordinator(base))
	s := &RedisServer{compactor: compactor}
	ctx := context.WithValue(context.Background(), redisOwnedRequestKey{}, true)
	s.triggerUrgentCompactionContext(ctx, "hash", []byte("key"))
	require.Empty(t, compactor.urgentCh)
	s.triggerUrgentCompactionContext(context.Background(), "hash", []byte("key"))
	require.Len(t, compactor.urgentCh, 1, "nil-hook legacy maintenance still works")
}

func TestRedisRequestAdmissionFinishPanicIsNotRetried(t *testing.T) {
	t.Parallel()
	owner := &redisTestOwnership{ctx: context.Background(), onFinish: func() { panic("finish panic") }}
	s := redisOwnedTestServer(owner)
	c := &redisOwnedTestConn{}
	require.Panics(t, func() {
		s.dispatchCommand(c, cmdPing, s.ping, redcon.Command{Args: [][]byte{[]byte(cmdPing)}}, time.Now())
	})
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
	require.Empty(t, c.raw.String())
}

func TestRedisRequestAdmissionCancelledCaptureDoesNotExecute(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	owner := &redisTestOwnership{ctx: ctx}
	s := redisOwnedTestServer(owner)
	c := &redisOwnedTestConn{}
	s.dispatchCommand(c, cmdHGetAll, func(redcon.Conn, redcon.Command) { t.Fatal("cancelled capture executed") }, redcon.Command{}, time.Now())
	require.ErrorIs(t, owner.original, context.Canceled)
	require.Equal(t, 1, owner.finishes)
	require.Equal(t, 1, owner.joins)
	require.Empty(t, c.raw.String())
}
