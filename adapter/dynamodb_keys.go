package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (d *DynamoDBServer) scanAllByPrefix(ctx context.Context, prefix []byte) ([]*store.KVPair, error) {
	return d.scanAllByPrefixAt(ctx, prefix, snapshotTS(d.coordinator.Clock(), d.store))
}

func (d *DynamoDBServer) scanAllByPrefixAt(ctx context.Context, prefix []byte, readTS uint64) ([]*store.KVPair, error) {
	readPin := d.pinReadTS(readTS)
	defer readPin.Release()

	end := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)

	out := make([]*store.KVPair, 0, dynamoScanPageLimit)
	for {
		kvs, err := d.store.ScanAt(ctx, start, end, dynamoScanPageLimit, readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(kvs) == 0 {
			break
		}
		for _, kvp := range kvs {
			if !bytes.HasPrefix(kvp.Key, prefix) {
				return out, nil
			}
			out = append(out, kvp)
		}
		if len(kvs) < dynamoScanPageLimit {
			break
		}
		start = nextScanCursor(kvs[len(kvs)-1].Key)
		if end != nil && bytes.Compare(start, end) > 0 {
			break
		}
	}
	return out, nil
}

func nextScanCursor(lastKey []byte) []byte {
	next := make([]byte, len(lastKey)+1)
	copy(next, lastKey)
	return next
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func dynamoTableMetaKey(tableName string) []byte {
	return []byte(dynamoTableMetaPrefix + encodeDynamoSegment(tableName))
}

func dynamoTableGenerationKey(tableName string) []byte {
	return []byte(dynamoTableGenerationPrefix + encodeDynamoSegment(tableName))
}

func dynamoItemPrefixForTable(tableName string, generation uint64) []byte {
	return []byte(dynamoItemPrefix + encodeDynamoSegment(tableName) + "|" + strconv.FormatUint(generation, 10) + "|")
}

func dynamoItemHashPrefixForTable(tableName string, generation uint64, hashKey []byte) []byte {
	base := dynamoItemPrefixForTable(tableName, generation)
	return append(base, hashKey...)
}

func legacyDynamoItemHashPrefixForTable(tableName string, generation uint64, hashKey string) []byte {
	return []byte(
		dynamoItemPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(hashKey) + "|",
	)
}

func dynamoGSIPrefixForTable(tableName string, generation uint64) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|",
	)
}

func dynamoGSIIndexPrefixForTable(tableName string, generation uint64, indexName string) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(indexName) + "|",
	)
}

func dynamoGSIHashPrefixForTable(tableName string, generation uint64, indexName string, hashKey []byte) []byte {
	base := dynamoGSIIndexPrefixForTable(tableName, generation, indexName)
	return append(base, hashKey...)
}

func legacyDynamoGSIHashPrefixForTable(tableName string, generation uint64, indexName string, hashKey string) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(indexName) + "|" +
			encodeDynamoSegment(hashKey) + "|",
	)
}

func dynamoGSIKey(
	tableName string,
	generation uint64,
	indexName string,
	indexHash []byte,
	indexRange []byte,
	pkHash []byte,
	pkRange []byte,
) []byte {
	key := dynamoGSIIndexPrefixForTable(tableName, generation, indexName)
	key = append(key, indexHash...)
	key = append(key, indexRange...)
	key = append(key, pkHash...)
	key = append(key, pkRange...)
	return key
}

func legacyDynamoGSIKey(tableName string, generation uint64, indexName string, indexHash string, indexRange string, pkHash string, pkRange string) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(indexName) + "|" +
			encodeDynamoSegment(indexHash) + "|" +
			encodeDynamoSegment(indexRange) + "|" +
			encodeDynamoSegment(pkHash) + "|" +
			encodeDynamoSegment(pkRange),
	)
}

func dynamoItemKey(tableName string, generation uint64, hashKey []byte, rangeKey []byte) []byte {
	key := dynamoItemPrefixForTable(tableName, generation)
	key = append(key, hashKey...)
	key = append(key, rangeKey...)
	return key
}

func legacyDynamoItemKey(tableName string, generation uint64, hashKey, rangeKey string) []byte {
	return []byte(
		dynamoItemPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(hashKey) + "|" +
			encodeDynamoSegment(rangeKey),
	)
}

func encodeDynamoSegment(v string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(v))
}

func decodeDynamoSegment(v string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(b), nil
}

func tableNameFromMetaKey(key []byte) (string, bool) {
	enc, ok := strings.CutPrefix(string(key), dynamoTableMetaPrefix)
	if !ok || enc == "" {
		return "", false
	}
	name, err := decodeDynamoSegment(enc)
	if err != nil {
		return "", false
	}
	return name, true
}
