package backup

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// encode_dynamodb_items.go is the Phase 0b DynamoDB ITEM reverse encoder
// — the inverse of the per-item JSON emitter in dynamodb.go (writeDDBItem
// / itemToPublic). It reconstructs the
// !ddb|item|<base64url(table)>|<gen>|<orderedHash><orderedRange> records
// from each table's items/<pk>[/<sk>].json files.
//
// Primary keys: S (string), B (binary), and N (number) are all supported;
// the N ordered encoding lives in encode_dynamodb_numeric.go. The derived
// !ddb|gsi| index rows (which the decoder drops as a no-op because they
// are derivable from the base item set + schema) land in a later slice.
//
// Format fidelity is pinned against the live adapter write path
// (adapter/dynamodb.go: dynamoItemKey / encodeDynamoKeySegment /
// storedDDBItemMagic), not just the decode side, so the emitted .fsm
// loads into a running cluster. The ORDERED key encoding (not the legacy
// base64url-segment EncodeDDBItemKey test helper) is required because the
// restored schema stamps KeyEncodingVersion=V2.
//
// Generation: every item key embeds ddbRestoreGeneration — the SAME
// uniform value the schema proto and the !ddb|meta|gen| counter carry
// (see encode_dynamodb.go) — so the restored table's counter and its
// item keys always agree.

// Ordered-key bytes mirror adapter/dynamodb.go's dynamoKey* constants: a
// 0x00 byte in the raw key is escaped to 0x00 0xFF, and every segment is
// terminated with 0x00 0x01 so concatenated hash+range segments sort and
// parse unambiguously.
const (
	ddbKeyEscapeByte      = byte(0x00)
	ddbKeyTerminatorByte  = byte(0x01)
	ddbKeyEscapedZeroByte = byte(0xFF)
	// ddbKeySegmentOverhead is the two-byte 0x00 0x01 terminator every
	// ordered key segment carries (mirrors dynamoKeySegmentOverhead).
	ddbKeySegmentOverhead = 2
)

// ErrDDBEncodeInvalidItem is returned when an items/*.json file cannot be
// parsed into a well-formed item (bad JSON shape, unknown attribute type,
// missing primary-key attribute, a non-S/N/B primary key, or a malformed
// number literal).
var ErrDDBEncodeInvalidItem = errors.New("backup: dynamodb encode invalid item json")

// encodeItems walks <tableDir>/items/ and stages one !ddb|item| record
// per item file. A missing items/ directory is not an error (a table may
// have a schema but no rows).
func (e *DynamoDBEncoder) encodeItems(b *snapshotBuilder, root *os.Root, tableDir, tableName string, schema *pb.DynamoTableSchema) error {
	itemsRel := filepath.Join(tableDir, "items")
	linfo, err := root.Lstat(itemsRel)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return errors.WithStack(err)
	}
	if !linfo.IsDir() {
		return errors.Wrapf(ErrDDBEncodeInvalidItem, "%s is not a directory", itemsRel)
	}
	entries, err := readRootSubdirEntries(root, itemsRel)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		switch {
		case ent.IsDir():
			if err := e.encodeCompositeItemDir(b, root, itemsRel, ent.Name(), tableName, schema); err != nil {
				return err
			}
		case isJSONFilename(ent.Name()):
			if err := e.encodeOneItem(b, root, filepath.Join(itemsRel, ent.Name()), tableName, schema); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeCompositeItemDir handles the composite-key layout
// items/<hashSeg>/<rangeSeg>.json: every .json file directly under the
// hash directory is one item.
func (e *DynamoDBEncoder) encodeCompositeItemDir(b *snapshotBuilder, root *os.Root, itemsRel, hashDir, tableName string, schema *pb.DynamoTableSchema) error {
	sub := filepath.Join(itemsRel, hashDir)
	entries, err := readRootSubdirEntries(root, sub)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		// The decoder emits at most items/<hash>/<range>.json — a directory
		// nested inside a hash directory cannot come from a valid dump, so
		// fail closed rather than silently skip (which would under-count
		// restored items without any error).
		if ent.IsDir() {
			return errors.Wrapf(ErrDDBEncodeInvalidItem,
				"%s: unexpected nested directory %q (items layout is at most 2 levels)", sub, ent.Name())
		}
		if !isJSONFilename(ent.Name()) {
			continue
		}
		if err := e.encodeOneItem(b, root, filepath.Join(sub, ent.Name()), tableName, schema); err != nil {
			return err
		}
	}
	return nil
}

// encodeOneItem reads one item JSON file, rebuilds the proto, and stages
// its !ddb|item| key/value on b.
func (e *DynamoDBEncoder) encodeOneItem(b *snapshotBuilder, root *os.Root, rel, tableName string, schema *pb.DynamoTableSchema) error {
	public, err := readRootJSONItem(root, rel)
	if err != nil {
		return err
	}
	item, err := publicToItem(public)
	if err != nil {
		return errors.Wrapf(err, "%s", rel)
	}
	key, err := ddbItemKeyBytes(tableName, ddbRestoreGeneration, schema, item)
	if err != nil {
		return errors.Wrapf(err, "%s", rel)
	}
	val, err := marshalStoredDDBItem(item)
	if err != nil {
		return err
	}
	return b.Add(key, val, 0)
}

// readRootSubdirEntries lists a subdirectory THROUGH the pinned root fd
// (os.Root.Open enforces no symlink escape), rather than re-resolving the
// path with os.ReadDir.
func readRootSubdirEntries(root *os.Root, rel string) ([]os.DirEntry, error) {
	d, err := root.Open(rel)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = d.Close() }()
	entries, err := d.ReadDir(-1)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return entries, nil
}

// readRootJSONItem opens an item file within root (symlink-escape / FIFO /
// hard-link safe, like readSchema) and decodes its public attribute map.
func readRootJSONItem(root *os.Root, rel string) (map[string]any, error) {
	linfo, err := root.Lstat(rel)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !linfo.Mode().IsRegular() {
		return nil, errors.Wrapf(ErrDDBEncodeNotRegular, "%s (mode=%s)", rel, linfo.Mode())
	}
	if err := refuseHardLink(linfo, rel); err != nil {
		return nil, err
	}
	f, err := root.Open(rel)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = f.Close() }()
	var public map[string]any
	if err := decodeOneJSON(f, &public); err != nil {
		return nil, errors.Wrapf(ErrDDBEncodeInvalidItem, "%s: %v", rel, err)
	}
	return public, nil
}

func isJSONFilename(name string) bool {
	return strings.HasSuffix(name, ".json")
}

// ddbItemKeyBytes builds the ordered !ddb|item| key for an item against
// the table schema's primary-key attribute names.
func ddbItemKeyBytes(tableName string, generation uint64, schema *pb.DynamoTableSchema, item *pb.DynamoItem) ([]byte, error) {
	attrs := item.GetAttributes()
	hashName := schema.GetPrimaryKey().GetHashKey()
	hashAV, ok := attrs[hashName]
	if !ok {
		return nil, errors.Wrapf(ErrDDBEncodeInvalidItem, "item missing hash-key attribute %q", hashName)
	}
	hashRaw, err := ddbPrimaryKeyBytes(hashAV)
	if err != nil {
		return nil, err
	}
	key := ddbItemKeyPrefix(tableName, generation)
	key = append(key, encodeDDBOrderedKeySegment(hashRaw)...)
	rangeName := schema.GetPrimaryKey().GetRangeKey()
	if rangeName == "" {
		return key, nil
	}
	rangeAV, ok := attrs[rangeName]
	if !ok {
		return nil, errors.Wrapf(ErrDDBEncodeInvalidItem, "item missing range-key attribute %q", rangeName)
	}
	rangeRaw, err := ddbPrimaryKeyBytes(rangeAV)
	if err != nil {
		return nil, err
	}
	return append(key, encodeDDBOrderedKeySegment(rangeRaw)...), nil
}

// ddbItemKeyPrefix reproduces dynamoItemPrefixForTable:
// !ddb|item|<base64url(table)>|<gen>|.
func ddbItemKeyPrefix(tableName string, generation uint64) []byte {
	enc := base64.RawURLEncoding.EncodeToString([]byte(tableName))
	gen := strconv.FormatUint(generation, 10)
	out := make([]byte, 0, len(DDBItemPrefix)+len(enc)+len(gen)+len("||"))
	out = append(out, DDBItemPrefix...)
	out = append(out, enc...)
	out = append(out, '|')
	out = append(out, gen...)
	out = append(out, '|')
	return out
}

// ddbPrimaryKeyBytes extracts the raw key bytes for a primary/range key
// attribute (DynamoDB primary keys are S, N, or B). S and B contribute
// their literal bytes; N contributes its order-preserving numeric encoding
// (the segment escape/terminator is applied uniformly by the caller).
func ddbPrimaryKeyBytes(av *pb.DynamoAttributeValue) ([]byte, error) {
	switch v := av.GetValue().(type) {
	case *pb.DynamoAttributeValue_S:
		return []byte(v.S), nil
	case *pb.DynamoAttributeValue_B:
		return v.B, nil
	case *pb.DynamoAttributeValue_N:
		return ddbNumericKeyBytes(v.N)
	default:
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "primary key attribute is not S, N, or B")
	}
}

// encodeDDBOrderedKeySegment reproduces encodeDynamoKeySegment: escape
// 0x00 -> 0x00 0xFF, then append the 0x00 0x01 terminator.
func encodeDDBOrderedKeySegment(raw []byte) []byte {
	out := make([]byte, 0, len(raw)+ddbKeySegmentOverhead)
	for _, c := range raw {
		if c == ddbKeyEscapeByte {
			out = append(out, ddbKeyEscapeByte, ddbKeyEscapedZeroByte)
			continue
		}
		out = append(out, c)
	}
	return append(out, ddbKeyEscapeByte, ddbKeyTerminatorByte)
}

// marshalStoredDDBItem reproduces the live item value:
// storedDDBItemMagic + deterministic proto marshal.
func marshalStoredDDBItem(item *pb.DynamoItem) ([]byte, error) {
	body, err := gproto.MarshalOptions{Deterministic: true}.Marshal(item)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// const-capacity, zero-length start + append (magic then body): keeps
	// the len(magic)+len(body) arithmetic out of make() — which CodeQL
	// flags as a potential allocation-size overflow — and the zero length
	// satisfies makezero. Same pattern as marshalStoredDDBSchema.
	out := make([]byte, 0, len(storedDDBItemMagic))
	out = append(out, storedDDBItemMagic...)
	return append(out, body...), nil
}

// publicToItem is the inverse of itemToPublic: it rebuilds a DynamoItem
// proto from the decoded items/*.json attribute map.
func publicToItem(public map[string]any) (*pb.DynamoItem, error) {
	attrs := make(map[string]*pb.DynamoAttributeValue, len(public))
	for name, raw := range public {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.Wrapf(ErrDDBEncodeInvalidItem, "attribute %q is not an object", name)
		}
		av, err := publicToAttributeValue(m)
		if err != nil {
			return nil, errors.Wrapf(err, "attribute %q", name)
		}
		attrs[name] = av
	}
	return &pb.DynamoItem{Attributes: attrs}, nil
}

// publicToAttributeValue is the inverse of attributeValueToPublic: the
// public form is a single-key object {TYPE: value}.
func publicToAttributeValue(m map[string]any) (*pb.DynamoAttributeValue, error) {
	if len(m) != 1 {
		return nil, errors.Wrapf(ErrDDBEncodeInvalidItem,
			"attribute object must have exactly one type key, got %d", len(m))
	}
	for typ, val := range m {
		return decodePublicAV(typ, val)
	}
	return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "empty attribute object")
}

// decodePublicAV dispatches a single {TYPE: value} pair. Scalars and
// collections are split into two grouped switches so each stays well
// under the cyclomatic limit and so the package has no init-cycle from a
// var-level decoder table referencing the recursive L/M decoders.
func decodePublicAV(typ string, val any) (*pb.DynamoAttributeValue, error) {
	if av, matched, err := decodeScalarAV(typ, val); matched || err != nil {
		return av, err
	}
	if av, matched, err := decodeCollectionAV(typ, val); matched || err != nil {
		return av, err
	}
	return nil, errors.Wrapf(ErrDDBEncodeInvalidItem, "unknown attribute type %q", typ)
}

func decodeScalarAV(typ string, val any) (*pb.DynamoAttributeValue, bool, error) {
	switch typ {
	case "S":
		av, err := decodeAVString(val)
		return av, true, err
	case "N":
		av, err := decodeAVNumber(val)
		return av, true, err
	case "B":
		av, err := decodeAVBinary(val)
		return av, true, err
	case "BOOL":
		av, err := decodeAVBool(val)
		return av, true, err
	case "NULL":
		av, err := decodeAVNull(val)
		return av, true, err
	}
	return nil, false, nil
}

func decodeCollectionAV(typ string, val any) (*pb.DynamoAttributeValue, bool, error) {
	switch typ {
	case "SS":
		av, err := decodeAVStringSet(val)
		return av, true, err
	case "NS":
		av, err := decodeAVNumberSet(val)
		return av, true, err
	case "BS":
		av, err := decodeAVBinarySet(val)
		return av, true, err
	case "L":
		av, err := decodeAVList(val)
		return av, true, err
	case "M":
		av, err := decodeAVMap(val)
		return av, true, err
	}
	return nil, false, nil
}

func decodeAVString(v any) (*pb.DynamoAttributeValue, error) {
	s, ok := v.(string)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "S value is not a string")
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_S{S: s}}, nil
}

func decodeAVNumber(v any) (*pb.DynamoAttributeValue, error) {
	s, ok := v.(string)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "N value is not a string")
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_N{N: s}}, nil
}

func decodeAVBinary(v any) (*pb.DynamoAttributeValue, error) {
	raw, err := base64StdFromAny(v)
	if err != nil {
		return nil, err
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_B{B: raw}}, nil
}

func decodeAVBool(v any) (*pb.DynamoAttributeValue, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "BOOL value is not a boolean")
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_BoolValue{BoolValue: b}}, nil
}

func decodeAVNull(v any) (*pb.DynamoAttributeValue, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "NULL value is not a boolean")
	}
	// Preserve the boolean as-is: the decoder serializes NullValue
	// verbatim, so the encoder must round-trip NULL=false too (an internal
	// record may carry it). AWS-API payload validity is enforced at the
	// adapter's PutItem boundary, not on this internal-record path.
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_NullValue{NullValue: b}}, nil
}

func decodeAVStringSet(v any) (*pb.DynamoAttributeValue, error) {
	vals, err := stringSliceFromAny(v)
	if err != nil {
		return nil, err
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_Ss{Ss: &pb.DynamoStringSet{Values: vals}}}, nil
}

func decodeAVNumberSet(v any) (*pb.DynamoAttributeValue, error) {
	vals, err := stringSliceFromAny(v)
	if err != nil {
		return nil, err
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_Ns{Ns: &pb.DynamoNumberSet{Values: vals}}}, nil
}

// decodeAVBinarySet rebuilds a BS attribute. Like the SS/NS path, an
// empty array is preserved (not rejected) to round-trip the structural
// empty case the decoder serializes for nil/empty sets.
func decodeAVBinarySet(v any) (*pb.DynamoAttributeValue, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "BS value is not an array")
	}
	out := make([][]byte, 0, len(arr))
	for _, e := range arr {
		raw, err := base64StdFromAny(e)
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_Bs{Bs: &pb.DynamoBinarySet{Values: out}}}, nil
}

func decodeAVList(v any) (*pb.DynamoAttributeValue, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "L value is not an array")
	}
	out := make([]*pb.DynamoAttributeValue, 0, len(arr))
	for _, e := range arr {
		m, ok := e.(map[string]any)
		if !ok {
			return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "L element is not an object")
		}
		child, err := publicToAttributeValue(m)
		if err != nil {
			return nil, err
		}
		out = append(out, child)
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_L{L: &pb.DynamoAttributeValueList{Values: out}}}, nil
}

func decodeAVMap(v any) (*pb.DynamoAttributeValue, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "M value is not an object")
	}
	out := make(map[string]*pb.DynamoAttributeValue, len(m))
	for name, child := range m {
		cm, ok := child.(map[string]any)
		if !ok {
			return nil, errors.Wrapf(ErrDDBEncodeInvalidItem, "M[%q] is not an object", name)
		}
		av, err := publicToAttributeValue(cm)
		if err != nil {
			return nil, errors.Wrapf(err, "M[%q]", name)
		}
		out[name] = av
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_M{M: &pb.DynamoAttributeValueMap{Values: out}}}, nil
}

// stringSliceFromAny coerces a JSON array of strings (SS / NS). An empty
// array is preserved (not rejected): the decoder intentionally serializes
// nil/empty sets as [] to avoid dropping the attribute, so the encoder
// must round-trip that structural case rather than fail recovery of a
// legacy/drifted snapshot.
func stringSliceFromAny(v any) ([]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "set value is not an array")
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, ok := e.(string)
		if !ok {
			return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "set element is not a string")
		}
		out = append(out, s)
	}
	return out, nil
}

// base64StdFromAny decodes a JSON base64 string into bytes. encoding/json
// renders []byte as standard-base64 strings, so B / BS values round-trip
// through StdEncoding.
func base64StdFromAny(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, errors.Wrap(ErrDDBEncodeInvalidItem, "binary value is not a base64 string")
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.Wrapf(ErrDDBEncodeInvalidItem, "binary value is not valid base64: %v", err)
	}
	return raw, nil
}
