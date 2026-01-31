package adapter

import (
	"bytes"
	"context"
	"math"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

func listMetaKey(userKey []byte) []byte {
	return store.ListMetaKey(userKey)
}

func listItemKey(userKey []byte, seq int64) []byte {
	return store.ListItemKey(userKey, seq)
}

func (r *RedisServer) loadListMeta(ctx context.Context, key []byte) (store.ListMeta, bool, error) {
	return r.loadListMetaAt(ctx, key, r.readTS())
}

func (r *RedisServer) loadListMetaAt(ctx context.Context, key []byte, readTS uint64) (store.ListMeta, bool, error) {
	val, err := r.store.GetAt(ctx, store.ListMetaKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.ListMeta{}, false, nil
		}
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	meta, err := store.UnmarshalListMeta(val)
	if err != nil {
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

func (r *RedisServer) isListKey(ctx context.Context, key []byte) (bool, error) {
	_, exists, err := r.loadListMetaAt(ctx, key, r.readTS())
	return exists, err
}

func (r *RedisServer) isListKeyAt(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	_, exists, err := r.loadListMetaAt(ctx, key, readTS)
	return exists, err
}

func (r *RedisServer) buildRPushOps(meta store.ListMeta, key []byte, values [][]byte) ([]*kv.Elem[kv.OP], store.ListMeta, error) {
	if len(values) == 0 {
		return nil, meta, nil
	}

	elems := make([]*kv.Elem[kv.OP], 0, len(values)+1)
	seq := meta.Head + meta.Len
	for _, v := range values {
		vCopy := bytes.Clone(v)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listItemKey(key, seq), Value: vCopy})
		seq++
	}

	meta.Len += int64(len(values))
	meta.Tail = meta.Head + meta.Len

	b, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, meta, errors.WithStack(err)
	}

	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listMetaKey(key), Value: b})
	return elems, meta, nil
}

func (r *RedisServer) listRPush(ctx context.Context, key []byte, values [][]byte) (int64, error) {
	meta, _, err := r.loadListMeta(ctx, key)
	if err != nil {
		return 0, err
	}

	ops, newMeta, err := r.buildRPushOps(meta, key, values)
	if err != nil {
		return 0, err
	}
	if len(ops) == 0 {
		return newMeta.Len, nil
	}

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: ops}
	if _, err := r.coordinator.Dispatch(group); err != nil {
		return 0, errors.WithStack(err)
	}
	return newMeta.Len, nil
}

func (r *RedisServer) deleteList(ctx context.Context, key []byte) error {
	meta, exists, err := r.loadListMeta(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	start := listItemKey(key, math.MinInt64)
	end := listItemKey(key, math.MaxInt64)

	readTS := r.readTS()
	kvs, err := r.store.ScanAt(ctx, start, end, math.MaxInt, readTS)
	if err != nil {
		return errors.WithStack(err)
	}

	ops := make([]*kv.Elem[kv.OP], 0, len(kvs)+1)
	for _, kvp := range kvs {
		ops = append(ops, &kv.Elem[kv.OP]{Op: kv.Del, Key: kvp.Key})
	}
	// delete meta last
	ops = append(ops, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(key)})

	// ensure meta bounds consistent even if scan missed (in case of empty list)
	_ = meta

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: ops}
	_, err = r.coordinator.Dispatch(group)
	return errors.WithStack(err)
}

func (r *RedisServer) fetchListRange(ctx context.Context, key []byte, meta store.ListMeta, startIdx, endIdx int64, readTS uint64) ([]string, error) {
	if endIdx < startIdx {
		return []string{}, nil
	}

	startSeq := meta.Head + startIdx
	endSeq := meta.Head + endIdx

	startKey := listItemKey(key, startSeq)
	endKey := listItemKey(key, endSeq+1) // exclusive

	kvs, err := r.store.ScanAt(ctx, startKey, endKey, int(endIdx-startIdx+1), readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]string, 0, len(kvs))
	for _, kvp := range kvs {
		out = append(out, string(kvp.Value))
	}
	return out, nil
}

func (r *RedisServer) rangeList(key []byte, startRaw, endRaw []byte) ([]string, error) {
	readTS := r.readTS()
	if !r.coordinator.IsLeader() {
		return r.proxyLRange(key, startRaw, endRaw)
	}

	if err := r.coordinator.VerifyLeader(); err != nil {
		return nil, errors.WithStack(err)
	}

	meta, exists, err := r.loadListMetaAt(context.Background(), key, readTS)
	if err != nil {
		return nil, err
	}
	if !exists || meta.Len == 0 {
		return []string{}, nil
	}

	start, err := parseInt(startRaw)
	if err != nil {
		return nil, err
	}
	end, err := parseInt(endRaw)
	if err != nil {
		return nil, err
	}

	s, e := clampRange(start, end, int(meta.Len))
	if e < s {
		return []string{}, nil
	}

	return r.fetchListRange(context.Background(), key, meta, int64(s), int64(e), readTS)
}

func (r *RedisServer) rpush(conn redcon.Conn, cmd redcon.Command) {
	ctx := context.Background()

	var length int64
	var err error
	if r.coordinator.IsLeader() {
		length, err = r.listRPush(ctx, cmd.Args[1], cmd.Args[2:])
	} else {
		length, err = r.proxyRPush(cmd.Args[1], cmd.Args[2:])
	}

	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(length)
}

func (r *RedisServer) lrange(conn redcon.Conn, cmd redcon.Command) {
	items, err := r.rangeList(cmd.Args[1], cmd.Args[2], cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(items))
	for _, it := range items {
		conn.WriteBulkString(it)
	}
}
