package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// Snapshot prefixes the DynamoDB encoder dispatches on. Mirror the live
// constants in kv/shard_key.go (DynamoTableMetaPrefix etc.) so a renamed
// prefix is caught by the dispatch tests below.
const (
	DDBTableMetaPrefix = "!ddb|meta|table|"
	DDBTableGenPrefix  = "!ddb|meta|gen|"
	DDBItemPrefix      = "!ddb|item|"
	DDBGSIPrefix       = "!ddb|gsi|"
)

// Stored value magic prefixes. Mirror adapter/dynamodb_storage_codec.go:15-16.
// Values that lack the right magic are rejected as either future-schema or
// genuinely corrupt — Phase 0a does not silently emit garbage.
var (
	storedDDBSchemaMagic = []byte{0x00, 'D', 'S', 0x01}
	storedDDBItemMagic   = []byte{0x00, 'D', 'I', 0x01}
)

// ErrDDBInvalidSchema, ErrDDBInvalidItem, ErrDDBMalformedKey are the
// typed error classes for this encoder. Surface via errors.Is.
var (
	ErrDDBInvalidSchema = errors.New("backup: invalid !ddb|meta|table value")
	ErrDDBInvalidItem   = errors.New("backup: invalid !ddb|item value")
	ErrDDBMalformedKey  = errors.New("backup: malformed DynamoDB key")
)

// DDBEncoder encodes the DynamoDB prefix family into the per-table layout
// described in docs/design/2026_04_29_proposed_snapshot_logical_decoder.md
// (Phase 0): one `_schema.json` per table and one
// `items/<pk>/<sk>.json` per item (default per-item layout).
//
// Lifecycle: Handle* per record, Finalize once. Items arrive before the
// schema in lex order ('i' < 'm' under !ddb|), so the encoder buffers
// per-encoded-table-segment and emits at Finalize once the schema is
// known.
//
// Wide-column GSI rows (!ddb|gsi|*) are NOT dumped: they are derivable
// from the base item set + schema, and replaying GSI rows on restore
// would conflict with the destination's own index maintenance.
type DDBEncoder struct {
	outRoot     string
	bundleJSONL bool

	tables map[string]*ddbTableState

	warn func(event string, fields ...any)
}

type ddbTableState struct {
	encoded string
	name    string
	schema  *pb.DynamoTableSchema
	items   []*pb.DynamoItem
}

// NewDDBEncoder constructs an encoder rooted at <outRoot>/dynamodb/.
func NewDDBEncoder(outRoot string) *DDBEncoder {
	return &DDBEncoder{
		outRoot: outRoot,
		tables:  make(map[string]*ddbTableState),
	}
}

// WithBundleJSONL switches per-table layout to `items/data-<part>.jsonl`
// (one item per line). Default is per-item files. The choice is recorded
// in MANIFEST.json (`dynamodb_layout`) by the master pipeline; the
// encoder itself only needs the flag to pick the on-disk shape.
//
// Bundle mode is a follow-up: this PR ships per-item only. Calling
// WithBundleJSONL(true) returns an error from Finalize until the bundle
// path lands.
func (d *DDBEncoder) WithBundleJSONL(on bool) *DDBEncoder {
	d.bundleJSONL = on
	return d
}

// WithWarnSink wires structured-warning emission (orphan items,
// schema-less tables, etc.).
func (d *DDBEncoder) WithWarnSink(fn func(event string, fields ...any)) *DDBEncoder {
	d.warn = fn
	return d
}

// HandleTableMeta processes a !ddb|meta|table|<encodedTable> record.
// Strips the magic prefix, proto-unmarshals into DynamoTableSchema, and
// parks it on the per-table state.
func (d *DDBEncoder) HandleTableMeta(key, value []byte) error {
	encoded, err := stripPrefixSegment(key, []byte(DDBTableMetaPrefix))
	if err != nil {
		return errors.Wrap(ErrDDBMalformedKey, err.Error())
	}
	rawName, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return errors.Wrap(ErrDDBMalformedKey, err.Error())
	}
	if !bytes.HasPrefix(value, storedDDBSchemaMagic) {
		return errors.Wrap(ErrDDBInvalidSchema, "missing magic prefix")
	}
	body := value[len(storedDDBSchemaMagic):]
	schema := &pb.DynamoTableSchema{}
	if err := gproto.Unmarshal(body, schema); err != nil {
		return errors.Wrap(ErrDDBInvalidSchema, err.Error())
	}
	st := d.tableState(encoded)
	st.name = string(rawName)
	st.schema = schema
	return nil
}

// HandleItem processes a !ddb|item|<encTable>|<gen>|<rest> record. The
// encoded table segment is parsed out of the key (everything between
// the first and second `|` after stripping `!ddb|item|`) and the item
// proto is buffered until Finalize. We do NOT parse the rest of the
// key here: every primary-key value the item could hold is also
// present in the proto's attributes map, and the schema (which arrives
// later in lex order) is what tells us which attributes are the hash
// and range keys.
func (d *DDBEncoder) HandleItem(key, value []byte) error {
	encoded, err := parseDDBItemKey(key)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(value, storedDDBItemMagic) {
		return errors.Wrap(ErrDDBInvalidItem, "missing magic prefix")
	}
	body := value[len(storedDDBItemMagic):]
	item := &pb.DynamoItem{}
	if err := gproto.Unmarshal(body, item); err != nil {
		return errors.Wrap(ErrDDBInvalidItem, err.Error())
	}
	st := d.tableState(encoded)
	st.items = append(st.items, item)
	return nil
}

// HandleGSIRow drops GSI rows by default (they are derivable from the
// base item set + schema). Exposed as a no-op so the master pipeline
// can dispatch all !ddb|* prefixes uniformly without special-casing.
func (d *DDBEncoder) HandleGSIRow(_, _ []byte) error { return nil }

// HandleTableGen drops the per-table generation counter (operational
// state, not user-visible).
func (d *DDBEncoder) HandleTableGen(_, _ []byte) error { return nil }

// Finalize emits each table's _schema.json and per-item JSON files.
// Tables with items but no schema (orphans — e.g., the schema record
// was lost or excluded) emit a warning and are skipped. Tables with
// a schema but no items emit a _schema.json and an empty items/
// directory.
func (d *DDBEncoder) Finalize() error {
	if d.bundleJSONL {
		return errors.New("backup: dynamodb_layout=jsonl not implemented in this PR")
	}
	var firstErr error
	for _, st := range d.tables {
		if st.schema == nil {
			d.emitWarn("ddb_orphan_items",
				"encoded_table", st.encoded,
				"buffered_items", len(st.items))
			continue
		}
		if err := d.flushTable(st); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (d *DDBEncoder) flushTable(st *ddbTableState) error {
	dir := filepath.Join(d.outRoot, "dynamodb", EncodeSegment([]byte(st.name)))
	itemsDir := filepath.Join(dir, "items")
	if err := os.MkdirAll(itemsDir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	if err := writeFileAtomic(filepath.Join(dir, "_schema.json"), mustMarshalIndent(schemaToPublic(st.schema))); err != nil {
		return err
	}
	hashKey := st.schema.GetPrimaryKey().GetHashKey()
	rangeKey := st.schema.GetPrimaryKey().GetRangeKey()
	for _, item := range st.items {
		if err := writeDDBItem(itemsDir, hashKey, rangeKey, item); err != nil {
			return err
		}
	}
	return nil
}

func (d *DDBEncoder) emitWarn(event string, fields ...any) {
	if d.warn == nil {
		return
	}
	d.warn(event, fields...)
}

func (d *DDBEncoder) tableState(encoded string) *ddbTableState {
	if st, ok := d.tables[encoded]; ok {
		return st
	}
	st := &ddbTableState{encoded: encoded}
	d.tables[encoded] = st
	return st
}

// parseDDBItemKey extracts the encoded table segment from
// !ddb|item|<encTable>|<gen>|<rest>. base64url does not contain `|`,
// so a strict `|` split between the prefix and the gen is unambiguous.
func parseDDBItemKey(key []byte) (string, error) {
	rest, err := stripPrefixSegment(key, []byte(DDBItemPrefix))
	if err != nil {
		return "", errors.Wrap(ErrDDBMalformedKey, err.Error())
	}
	idx := strings.IndexByte(rest, '|')
	if idx <= 0 {
		return "", errors.Wrapf(ErrDDBMalformedKey,
			"item key missing table/gen separator: %q", key)
	}
	enc := rest[:idx]
	if _, err := base64.RawURLEncoding.DecodeString(enc); err != nil {
		return "", errors.Wrap(ErrDDBMalformedKey, err.Error())
	}
	return enc, nil
}

// writeDDBItem emits one item under itemsDir/<pk>[/<sk>].json. The
// hash-only and composite-key shapes match the design's two examples.
// A missing hash-key attribute on an item is a structural error (the
// item could never have been GetItem-able without one) and surfaces
// as ErrDDBInvalidItem.
func writeDDBItem(itemsDir, hashKey, rangeKey string, item *pb.DynamoItem) error {
	attrs := item.GetAttributes()
	hashVal, ok := attrs[hashKey]
	if !ok {
		return errors.Wrapf(ErrDDBInvalidItem,
			"item missing hash-key attribute %q", hashKey)
	}
	hashFilename, err := ddbKeyAttrToSegment(hashVal)
	if err != nil {
		return err
	}
	publicItem := itemToPublic(item)
	body, err := json.MarshalIndent(publicItem, "", "  ")
	if err != nil {
		return errors.WithStack(err)
	}
	if rangeKey == "" {
		return writeFileAtomic(filepath.Join(itemsDir, hashFilename+".json"), body)
	}
	rangeVal, ok := attrs[rangeKey]
	if !ok {
		return errors.Wrapf(ErrDDBInvalidItem,
			"item missing range-key attribute %q", rangeKey)
	}
	rangeFilename, err := ddbKeyAttrToSegment(rangeVal)
	if err != nil {
		return err
	}
	dir := filepath.Join(itemsDir, hashFilename)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	return writeFileAtomic(filepath.Join(dir, rangeFilename+".json"), body)
}

// ddbKeyAttrToSegment encodes a primary-key attribute (S, N, or B) to
// a filesystem-safe segment. Per the design, S and N take the standard
// EncodeSegment path; B takes EncodeBinarySegment so binary keys never
// collide with string keys whose hex shape happens to look like
// base64.
//
// All other attribute kinds are rejected — DynamoDB primary keys can
// only be S, N, or B.
func ddbKeyAttrToSegment(av *pb.DynamoAttributeValue) (string, error) {
	switch v := av.GetValue().(type) {
	case *pb.DynamoAttributeValue_S:
		return EncodeSegment([]byte(v.S)), nil
	case *pb.DynamoAttributeValue_N:
		return EncodeSegment([]byte(v.N)), nil
	case *pb.DynamoAttributeValue_B:
		return EncodeBinarySegment(v.B), nil
	}
	return "", errors.Wrapf(ErrDDBInvalidItem,
		"primary key has unsupported attribute kind %T", av.GetValue())
}

// schemaToPublic projects DynamoTableSchema into the AWS-DescribeTable
// JSON shape documented in the design. Fields the live record carries
// for cluster-internal reasons (key_encoding_version, generation,
// migrating_from_generation) are stripped — they are not part of the
// user-visible schema and would not be re-applicable on restore.
func schemaToPublic(s *pb.DynamoTableSchema) ddbPublicSchema {
	pk := publicKeySchema{
		HashKey:  publicKeyAttribute{Name: s.GetPrimaryKey().GetHashKey()},
		RangeKey: publicKeyAttribute{Name: s.GetPrimaryKey().GetRangeKey()},
	}
	if s.GetPrimaryKey().GetRangeKey() == "" {
		pk.RangeKey = publicKeyAttribute{}
	}
	defs := make(map[string]string, len(s.GetAttributeDefinitions()))
	for k, v := range s.GetAttributeDefinitions() {
		defs[k] = v
	}
	pk.HashKey.Type = defs[pk.HashKey.Name]
	if pk.RangeKey.Name != "" {
		pk.RangeKey.Type = defs[pk.RangeKey.Name]
	}
	gsis := make([]publicGSI, 0, len(s.GetGlobalSecondaryIndexes()))
	for name, gsi := range s.GetGlobalSecondaryIndexes() {
		g := publicGSI{
			Name: name,
			KeySchema: publicKeySchema{
				HashKey:  publicKeyAttribute{Name: gsi.GetKeySchema().GetHashKey()},
				RangeKey: publicKeyAttribute{Name: gsi.GetKeySchema().GetRangeKey()},
			},
		}
		g.KeySchema.HashKey.Type = defs[g.KeySchema.HashKey.Name]
		if g.KeySchema.RangeKey.Name != "" {
			g.KeySchema.RangeKey.Type = defs[g.KeySchema.RangeKey.Name]
		} else {
			g.KeySchema.RangeKey = publicKeyAttribute{}
		}
		g.Projection.Type = gsi.GetProjection().GetProjectionType()
		g.Projection.NonKeyAttributes = append([]string{}, gsi.GetProjection().GetNonKeyAttributes()...)
		gsis = append(gsis, g)
	}
	attrDefs := make([]publicAttributeDefinition, 0, len(defs))
	for name, ty := range defs {
		attrDefs = append(attrDefs, publicAttributeDefinition{Name: name, Type: ty})
	}
	return ddbPublicSchema{
		FormatVersion:          1,
		TableName:              s.GetTableName(),
		PrimaryKey:             pk,
		AttributeDefinitions:   attrDefs,
		GlobalSecondaryIndexes: gsis,
	}
}

type ddbPublicSchema struct {
	FormatVersion          uint32                      `json:"format_version"`
	TableName              string                      `json:"table_name"`
	PrimaryKey             publicKeySchema             `json:"primary_key"`
	AttributeDefinitions   []publicAttributeDefinition `json:"attribute_definitions"`
	GlobalSecondaryIndexes []publicGSI                 `json:"global_secondary_indexes,omitempty"`
}

type publicKeySchema struct {
	HashKey  publicKeyAttribute `json:"hash_key"`
	RangeKey publicKeyAttribute `json:"range_key,omitempty"`
}

type publicKeyAttribute struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

type publicAttributeDefinition struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type publicGSI struct {
	Name       string           `json:"name"`
	KeySchema  publicKeySchema  `json:"key_schema"`
	Projection publicProjection `json:"projection"`
}

type publicProjection struct {
	Type             string   `json:"type"`
	NonKeyAttributes []string `json:"non_key_attributes,omitempty"`
}

// itemToPublic translates a DynamoItem proto into the AWS-DynamoDB-JSON
// shape: a top-level map of attribute name -> typed-attribute object.
// The attribute objects use the standard AWS keys (S, N, B, BOOL,
// NULL, SS, NS, BS, L, M).
func itemToPublic(item *pb.DynamoItem) map[string]any {
	out := make(map[string]any, len(item.GetAttributes()))
	for name, av := range item.GetAttributes() {
		out[name] = attributeValueToPublic(av)
	}
	return out
}

func attributeValueToPublic(av *pb.DynamoAttributeValue) map[string]any {
	if scalar := scalarAttributeValueToPublic(av); scalar != nil {
		return scalar
	}
	if set := setAttributeValueToPublic(av); set != nil {
		return set
	}
	if comp := compositeAttributeValueToPublic(av); comp != nil {
		return comp
	}
	// Empty oneof. AWS treats this as malformed; preserve as NULL so
	// the dump remains deserialisable rather than embedding an empty
	// object that downstream tools might reject.
	return map[string]any{"NULL": true}
}

func scalarAttributeValueToPublic(av *pb.DynamoAttributeValue) map[string]any {
	switch v := av.GetValue().(type) {
	case *pb.DynamoAttributeValue_S:
		return map[string]any{"S": v.S}
	case *pb.DynamoAttributeValue_N:
		return map[string]any{"N": v.N}
	case *pb.DynamoAttributeValue_B:
		return map[string]any{"B": v.B}
	case *pb.DynamoAttributeValue_BoolValue:
		return map[string]any{"BOOL": v.BoolValue}
	case *pb.DynamoAttributeValue_NullValue:
		return map[string]any{"NULL": v.NullValue}
	}
	return nil
}

func setAttributeValueToPublic(av *pb.DynamoAttributeValue) map[string]any {
	switch v := av.GetValue().(type) {
	case *pb.DynamoAttributeValue_Ss:
		return map[string]any{"SS": append([]string{}, v.Ss.GetValues()...)}
	case *pb.DynamoAttributeValue_Ns:
		return map[string]any{"NS": append([]string{}, v.Ns.GetValues()...)}
	case *pb.DynamoAttributeValue_Bs:
		return map[string]any{"BS": append([][]byte{}, v.Bs.GetValues()...)}
	}
	return nil
}

func compositeAttributeValueToPublic(av *pb.DynamoAttributeValue) map[string]any {
	switch v := av.GetValue().(type) {
	case *pb.DynamoAttributeValue_L:
		out := make([]map[string]any, 0, len(v.L.GetValues()))
		for _, child := range v.L.GetValues() {
			out = append(out, attributeValueToPublic(child))
		}
		return map[string]any{"L": out}
	case *pb.DynamoAttributeValue_M:
		out := make(map[string]any, len(v.M.GetValues()))
		for k, child := range v.M.GetValues() {
			out[k] = attributeValueToPublic(child)
		}
		return map[string]any{"M": out}
	}
	return nil
}

// EncodeDDBItemKey constructs a !ddb|item key for tests. Mirrors the
// live legacyDynamoItemKey constructor in adapter/dynamodb.go (string
// hash + range, simplest shape).
func EncodeDDBItemKey(tableName string, generation uint64, hashKey, rangeKey string) []byte {
	out := []byte(DDBItemPrefix)
	out = append(out, base64.RawURLEncoding.EncodeToString([]byte(tableName))...)
	out = append(out, '|')
	out = append(out, fmt.Sprintf("%d", generation)...)
	out = append(out, '|')
	out = append(out, base64.RawURLEncoding.EncodeToString([]byte(hashKey))...)
	if rangeKey != "" {
		out = append(out, '|')
		out = append(out, base64.RawURLEncoding.EncodeToString([]byte(rangeKey))...)
	}
	return out
}

// EncodeDDBTableMetaKey constructs a !ddb|meta|table key for tests.
func EncodeDDBTableMetaKey(tableName string) []byte {
	return []byte(DDBTableMetaPrefix + base64.RawURLEncoding.EncodeToString([]byte(tableName)))
}
