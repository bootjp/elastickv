package backup

import (
	"encoding/base64"
	"sort"
	"strconv"

	pb "github.com/bootjp/elastickv/proto"
)

// encode_dynamodb_gsi.go derives the global-secondary-index rows for each
// base item — the inverse of the decoder's no-op HandleGSIRow, which drops
// GSI rows precisely because they are derivable from the base item set +
// schema. It reproduces the live adapter's gsiEntryKeysForItem (V2 ordered
// path): for every GSI whose key attributes the item carries, emit
//
//	key   = !ddb|gsi|<b64(table)>|<gen>|<b64(index)>|
//	        <orderedIndexHash><orderedIndexRange><orderedPKHash><orderedPKRange>
//	value = <the base item's !ddb|item| key>   (a pointer back to the item)
//
// Sparse indexing: an item missing the index hash attribute (or a defined
// index range attribute) contributes no row for that index — matching the
// live gsiKeyValues include=false path.

// ddbGSIRow is one derived index row: the index key and the base item key
// it points to.
type ddbGSIRow struct {
	key   []byte
	value []byte
}

// ddbGSIRows derives the GSI rows for one item. itemKey is the item's
// primary !ddb|item| key, stored as each row's value.
func ddbGSIRows(tableName string, generation uint64, schema *pb.DynamoTableSchema, item *pb.DynamoItem, itemKey []byte) ([]ddbGSIRow, error) {
	gsis := schema.GetGlobalSecondaryIndexes()
	if len(gsis) == 0 {
		return nil, nil
	}
	pkHash, pkRange, err := ddbOrderedPrimaryKeySegments(schema, item)
	if err != nil {
		return nil, err
	}
	rows := make([]ddbGSIRow, 0, len(gsis))
	for _, indexName := range ddbSortedGSINames(gsis) {
		idxHash, idxRange, include, err := ddbGSIKeySegments(item, gsis[indexName].GetKeySchema())
		if err != nil {
			return nil, err
		}
		if !include {
			continue
		}
		key := ddbGSIIndexKeyPrefix(tableName, generation, indexName)
		key = append(key, idxHash...)
		key = append(key, idxRange...)
		key = append(key, pkHash...)
		key = append(key, pkRange...)
		rows = append(rows, ddbGSIRow{key: key, value: itemKey})
	}
	return rows, nil
}

func ddbSortedGSINames(gsis map[string]*pb.DynamoGlobalSecondaryIndex) []string {
	names := make([]string, 0, len(gsis))
	for name := range gsis {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ddbGSIKeySegments extracts the ordered index hash/range segments from the
// item for one GSI key schema. include=false (a sparse item that does not
// project into this index) when the item lacks the index hash attribute or
// a defined index range attribute — matching the live gsiKeyValues.
func ddbGSIKeySegments(item *pb.DynamoItem, ks *pb.DynamoKeySchema) ([]byte, []byte, bool, error) {
	attrs := item.GetAttributes()
	hashAV, ok := attrs[ks.GetHashKey()]
	if !ok {
		return nil, nil, false, nil
	}
	hashRaw, err := ddbPrimaryKeyBytes(hashAV)
	if err != nil {
		return nil, nil, false, err
	}
	hashSeg := encodeDDBOrderedKeySegment(hashRaw)
	if ks.GetRangeKey() == "" {
		return hashSeg, nil, true, nil
	}
	rangeAV, ok := attrs[ks.GetRangeKey()]
	if !ok {
		return nil, nil, false, nil
	}
	rangeRaw, err := ddbPrimaryKeyBytes(rangeAV)
	if err != nil {
		return nil, nil, false, err
	}
	return hashSeg, encodeDDBOrderedKeySegment(rangeRaw), true, nil
}

// ddbGSIIndexKeyPrefix reproduces dynamoGSIIndexPrefixForTable:
// !ddb|gsi|<base64url(table)>|<gen>|<base64url(indexName)>|
func ddbGSIIndexKeyPrefix(tableName string, generation uint64, indexName string) []byte {
	encTable := base64.RawURLEncoding.EncodeToString([]byte(tableName))
	encIndex := base64.RawURLEncoding.EncodeToString([]byte(indexName))
	gen := strconv.FormatUint(generation, 10)
	out := make([]byte, 0, len(DDBGSIPrefix)+len(encTable)+len(gen)+len(encIndex)+len("|||"))
	out = append(out, DDBGSIPrefix...)
	out = append(out, encTable...)
	out = append(out, '|')
	out = append(out, gen...)
	out = append(out, '|')
	out = append(out, encIndex...)
	out = append(out, '|')
	return out
}
