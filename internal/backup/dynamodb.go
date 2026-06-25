package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	encoded    string
	name       string
	schema     *pb.DynamoTableSchema
	itemsByGen map[uint64][]*pb.DynamoItem
}

func ensureItemsByGen(m map[uint64][]*pb.DynamoItem) map[uint64][]*pb.DynamoItem {
	if m == nil {
		return make(map[uint64][]*pb.DynamoItem)
	}
	return m
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
	if encoded == "" {
		// base64.RawURLEncoding.DecodeString("") succeeds with an
		// empty slice, so without this guard a truncated key like
		// `!ddb|meta|table|` would be routed under the empty table
		// name. That hides corruption (Codex P2 #117).
		return errors.Wrapf(ErrDDBMalformedKey, "empty table segment: %q", key)
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
// encoded table segment AND the item generation are parsed out of the
// key; the proto is buffered keyed by generation so Finalize can emit
// only the rows belonging to the schema's active generation.
//
// Stale-generation rows (left behind by an in-flight delete/recreate
// before async cleanup finishes) would otherwise silently leak under
// the new schema and either resurrect deleted data or fail Finalize
// when primary-key names changed across generations — Codex P1 #237.
func (d *DDBEncoder) HandleItem(key, value []byte) error {
	encoded, generation, err := parseDDBItemKey(key)
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
	st.itemsByGen = ensureItemsByGen(st.itemsByGen)
	st.itemsByGen[generation] = append(st.itemsByGen[generation], item)
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
// Tables with items but no schema (orphans) emit a warning and are
// skipped — preserving the spec's lenient handling for incomplete
// inputs. Real flush errors fail fast so corruption surfaces
// immediately rather than being attributed to a later table (Gemini
// MEDIUM #182).
func (d *DDBEncoder) Finalize() error {
	if d.bundleJSONL {
		return errors.New("backup: dynamodb_layout=jsonl not implemented in this PR")
	}
	for _, st := range d.tables {
		if st.schema == nil {
			d.emitWarn("ddb_orphan_items",
				"encoded_table", st.encoded,
				"buffered_items", totalItemsAcrossGens(st.itemsByGen))
			continue
		}
		if err := d.flushTable(st); err != nil {
			return err
		}
	}
	return nil
}

func totalItemsAcrossGens(m map[uint64][]*pb.DynamoItem) int {
	total := 0
	for _, items := range m {
		total += len(items)
	}
	return total
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
	activeGen := st.schema.GetGeneration()
	migrationSourceGen := st.schema.GetMigratingFromGeneration()
	// During a generation migration the live read path falls back to
	// migration_source_generation for items not yet copied to the new
	// generation (see adapter/dynamodb.go readLogicalItemAt). Both
	// generations therefore carry items the user can read at this
	// moment, so a backup must include both. Codex P1 #227.
	//
	// Items present in BOTH generations: the new-gen row is the
	// authoritative one (the live code prefers it on read). We emit
	// migration-source first, then active gen LAST, so writeFileAtomic's
	// tmp+rename leaves the active-gen content on disk per (pk,sk).
	emitOrder := []uint64{}
	if migrationSourceGen != 0 && migrationSourceGen != activeGen {
		emitOrder = append(emitOrder, migrationSourceGen)
	}
	emitOrder = append(emitOrder, activeGen)
	if stale := totalStaleItemsExcluding(st.itemsByGen, emitOrder); stale > 0 {
		d.emitWarn("ddb_stale_generation_items",
			"table", st.name,
			"active_generation", activeGen,
			"migration_source_generation", migrationSourceGen,
			"stale_count", stale,
			"hint", "stale-gen rows excluded; restore would otherwise emit them under the new schema")
	}
	for _, gen := range emitOrder {
		for _, item := range st.itemsByGen[gen] {
			if err := writeDDBItem(itemsDir, hashKey, rangeKey, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func totalStaleItemsExcluding(m map[uint64][]*pb.DynamoItem, included []uint64) int {
	includedSet := make(map[uint64]struct{}, len(included))
	for _, g := range included {
		includedSet[g] = struct{}{}
	}
	stale := 0
	for gen, items := range m {
		if _, ok := includedSet[gen]; !ok {
			stale += len(items)
		}
	}
	return stale
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

// parseDDBItemKey extracts the encoded table segment AND the item
// generation from !ddb|item|<encTable>|<gen>|<rest>. base64url does
// not contain `|`, so the first two `|` after the prefix are
// unambiguous separators between the table segment, the decimal
// generation, and the rest of the key (hash/range encoding).
func parseDDBItemKey(key []byte) (string, uint64, error) {
	rest, err := stripPrefixSegment(key, []byte(DDBItemPrefix))
	if err != nil {
		return "", 0, errors.Wrap(ErrDDBMalformedKey, err.Error())
	}
	tableEnd := strings.IndexByte(rest, '|')
	if tableEnd <= 0 {
		return "", 0, errors.Wrapf(ErrDDBMalformedKey,
			"item key missing table/gen separator: %q", key)
	}
	enc := rest[:tableEnd]
	if _, err := base64.RawURLEncoding.DecodeString(enc); err != nil {
		return "", 0, errors.Wrap(ErrDDBMalformedKey, err.Error())
	}
	afterTable := rest[tableEnd+1:]
	genEnd := strings.IndexByte(afterTable, '|')
	if genEnd <= 0 {
		return "", 0, errors.Wrapf(ErrDDBMalformedKey,
			"item key missing gen/rest separator: %q", key)
	}
	gen, err := strconv.ParseUint(afterTable[:genEnd], 10, 64) //nolint:mnd // 10 == decimal radix; 64 == uint64 width
	if err != nil {
		return "", 0, errors.Wrap(ErrDDBMalformedKey, err.Error())
	}
	// Item keys must carry a primary-key payload after the gen
	// separator (the encoded hash + range bytes). A bare
	// `!ddb|item|<table>|<gen>|` cannot identify any item; treating
	// such a key as valid would let a truncated record slip past
	// malformed-key detection and emit under value-side attributes
	// only, masking snapshot corruption (Codex P2 #303).
	if genEnd+1 == len(afterTable) {
		return "", 0, errors.Wrapf(ErrDDBMalformedKey,
			"item key missing primary-key payload: %q", key)
	}
	return enc, gen, nil
}

// writeDDBItem emits one item under itemsDir/<pk>[/<sk>].json. The
// hash-only and composite-key shapes match the design's two examples.
// A missing hash-key attribute on an item is a structural error (the
// item could never have been GetItem-able without one) and surfaces
// as ErrDDBInvalidItem.
//
// The encoded hash/range filename segments may legitimately be "." or
// ".." (DynamoDB S/N keys can hold any string, and EncodeSegment
// preserves both `.` chars as RFC3986-unreserved). filepath.Join
// would then either collapse `<itemsDir>/.` back to itemsDir or
// resolve `<itemsDir>/..` to the parent — letting an item like
// hash=".." range="_schema" overwrite the table-level _schema.json.
// Reject sole-dot segments at this boundary so the items/ subtree
// cannot escape via key-controlled paths. Codex P1 round 12.
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
	if err := refuseDotSegmentFilename(hashFilename, "hash"); err != nil {
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
	if err := refuseDotSegmentFilename(rangeFilename, "range"); err != nil {
		return err
	}
	dir := filepath.Join(itemsDir, hashFilename)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	return writeFileAtomic(filepath.Join(dir, rangeFilename+".json"), body)
}

// refuseDotSegmentFilename blocks hash/range segments that filepath
// resolution would collapse or escape on (".", ".."). Both are
// reachable from valid DynamoDB N/S key values.
func refuseDotSegmentFilename(seg, role string) error {
	if seg == "." || seg == ".." {
		return errors.Wrapf(ErrDDBInvalidItem,
			"%s-key segment %q is a dot path (would escape items dir)", role, seg)
	}
	return nil
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
		// DynamoDB N equality is numeric, not lexical: "1" and
		// "1.0" name the same primary-key item, and so do "5e-1"
		// and "0.5". Without canonicalisation each text form
		// produces a distinct filename, so in migration mode the
		// "active generation wins" invariant breaks (both source
		// and active rows survive at different paths and restore
		// replays duplicates). Mirror the live adapter's
		// canonicalNumberString (adapter/dynamodb.go:7651) which
		// uses big.Rat — same canonical form keeps filename
		// identity in lockstep with the live equality check.
		// Codex P1 round 9.
		return EncodeSegment([]byte(canonicalDDBNumber(v.N))), nil
	case *pb.DynamoAttributeValue_B:
		return EncodeBinarySegment(v.B), nil
	}
	return "", errors.Wrapf(ErrDDBInvalidItem,
		"primary key has unsupported attribute kind %T", av.GetValue())
}

// canonicalDDBNumber returns the canonical decimal representation of
// a DynamoDB N value. Equivalent inputs (`"1"`, `"1.0"`, `"0.5"`,
// `"5e-1"`, …) collapse to the same string; malformed inputs fall
// through to a trimmed copy so a downstream parse failure surfaces
// the original bytes rather than a silently rewritten value. The
// implementation matches adapter/dynamodb.go canonicalNumberString
// byte-for-byte so backup filenames track live equality.
func canonicalDDBNumber(v string) string {
	rat := &big.Rat{}
	if _, ok := rat.SetString(strings.TrimSpace(v)); !ok {
		return strings.TrimSpace(v)
	}
	return rat.RatString()
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
	// Build GSI list in deterministic name-sorted order. Ranging over
	// the underlying map directly produced a different array order on
	// every dump, undermining byte-for-byte reproducibility of
	// _schema.json across runs of the same snapshot. Codex P2 round 9.
	gsiNames := make([]string, 0, len(s.GetGlobalSecondaryIndexes()))
	for name := range s.GetGlobalSecondaryIndexes() {
		gsiNames = append(gsiNames, name)
	}
	sort.Strings(gsiNames)
	gsis := make([]publicGSI, 0, len(gsiNames))
	for _, name := range gsiNames {
		gsi := s.GetGlobalSecondaryIndexes()[name]
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
	// AttributeDefinitions is also sorted by attribute name for the
	// same determinism reason.
	defNames := make([]string, 0, len(defs))
	for name := range defs {
		defNames = append(defNames, name)
	}
	sort.Strings(defNames)
	attrDefs := make([]publicAttributeDefinition, 0, len(defNames))
	for _, name := range defNames {
		attrDefs = append(attrDefs, publicAttributeDefinition{Name: name, Type: defs[name]})
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
	// Ensure the destination slice is non-nil even when the source
	// is nil/empty so json.Marshal renders [] rather than null.
	// AWS DynamoDB JSON does NOT permit empty sets ([] is rejected
	// by the live API), but the dump format intentionally accepts
	// the structural empty case to avoid silently dropping a set
	// attribute whose live representation drifted to nil.
	switch v := av.GetValue().(type) {
	case *pb.DynamoAttributeValue_Ss:
		out := make([]string, 0, len(v.Ss.GetValues()))
		out = append(out, v.Ss.GetValues()...)
		return map[string]any{"SS": out}
	case *pb.DynamoAttributeValue_Ns:
		out := make([]string, 0, len(v.Ns.GetValues()))
		out = append(out, v.Ns.GetValues()...)
		return map[string]any{"NS": out}
	case *pb.DynamoAttributeValue_Bs:
		out := make([][]byte, 0, len(v.Bs.GetValues()))
		out = append(out, v.Bs.GetValues()...)
		return map[string]any{"BS": out}
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
