package adapter

import (
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func requireWaiterSignaled(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("waiter was not signaled")
	}
}

func requireWaiterQuiet(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("waiter was unexpectedly signaled")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRedisApplyObserverSignalsStreamEntryWaiters(t *testing.T) {
	observer := NewRedisApplyObserver()
	key := []byte("stream-key")
	waiter, release := observer.streamWaiters.Register([][]byte{key})
	defer release()

	observer.OnApply(pb.Op_PUT, store.StreamEntryKey(key, 1, 0))
	requireWaiterSignaled(t, waiter.C)
}

func TestRedisApplyObserverIgnoresNonPutAndMalformedStreamKeys(t *testing.T) {
	observer := NewRedisApplyObserver()
	key := []byte("stream-key")
	waiter, release := observer.streamWaiters.Register([][]byte{key})
	defer release()

	observer.OnApply(pb.Op_DEL, store.StreamEntryKey(key, 1, 0))
	requireWaiterQuiet(t, waiter.C)

	observer.OnApply(pb.Op_PUT, []byte(store.StreamEntryPrefix+"bad"))
	requireWaiterQuiet(t, waiter.C)
}

func TestRedisApplyObserverSignalsZSetMemberWaiters(t *testing.T) {
	observer := NewRedisApplyObserver()
	key := []byte("zset-key")
	waiter, release := observer.zsetWaiters.Register([][]byte{key})
	defer release()

	observer.OnApply(pb.Op_PUT, store.ZSetMemberKey(key, []byte("member")))
	requireWaiterSignaled(t, waiter.C)

	waiter2, release2 := observer.zsetWaiters.Register([][]byte{key})
	defer release2()
	observer.OnApply(pb.Op_PUT, store.ZSetScoreKey(key, 1, []byte("member")))
	requireWaiterSignaled(t, waiter2.C)
}

func TestWithRedisApplyObserverSharesRegistries(t *testing.T) {
	observer := NewRedisApplyObserver()
	server := NewRedisServer(nil, "", nil, nil, nil, nil, WithRedisApplyObserver(observer))

	require.Same(t, observer.streamWaiters, server.streamWaiters)
	require.Same(t, observer.zsetWaiters, server.zsetWaiters)
}
