package transport

import (
	"github.com/bootjp/elastickv/kv"
)

type redisTranscoder struct {
}

func newRedisTranscoder() *redisTranscoder {
	return &redisTranscoder{}
}

func (c *redisTranscoder) DeleteToRequest(key []byte) (*kv.OperationGroup[kv.OP], error) {
	return &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:  kv.Del,
				Key: key,
			},
		},
	}, nil
}

func (c *redisTranscoder) SetToRequest(key, value []byte) (*kv.OperationGroup[kv.OP], error) {
	return &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:    kv.Put,
				Key:   key,
				Value: value,
			},
		},
	}, nil
}
