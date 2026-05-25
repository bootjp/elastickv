package backup

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strconv"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

// encode_dynamodb.go is the Phase 0b DynamoDB reverse encoder — the
// inverse of the DDBEncoder decoder in dynamodb.go (design:
// docs/design/2026_05_25_proposed_snapshot_logical_encoder.md
// §"DynamoDB").
//
// This commit covers TABLE SCHEMAS: it reconstructs the
// !ddb|meta|table| schema record (and the !ddb|meta|gen| generation
// counter) from each table's _schema.json. The item records
// (!ddb|item|, which need the ordered primary-key encoding) and the
// derived GSI rows land in follow-up commits on this milestone.
//
// Generation handling (the design's decision gate): _schema.json does
// not carry the table's original generation, so the encoder stamps a
// uniform restore generation (ddbRestoreGeneration) into BOTH the
// schema proto's Generation field and the !ddb|meta|gen| counter — and
// the item encoder (next commit) embeds the SAME generation in every
// !ddb|item| key, so the counter and the item-key generation always
// agree. This is the Option-B path: internally consistent for a
// single-cluster restore; the original generation number is not
// preserved (it is unrecoverable from the public dump).
//
// KeyEncodingVersion is stamped to the current ordered-key version so
// the restored table uses the same ordered primary-key encoding the
// item encoder produces.

// ddbRestoreGeneration is the uniform generation stamped into the
// schema, the generation counter, and (in the item encoder) every
// item key, so a restored table is internally consistent.
const ddbRestoreGeneration uint64 = 1

// ddbOrderedKeyEncodingV2 mirrors adapter/dynamodb.go's
// dynamoOrderedKeyEncodingV2 — the ordered primary-key encoding the
// item encoder reproduces. Stamped into the restored schema so the
// live store reads items with the matching encoder.
const ddbOrderedKeyEncodingV2 uint64 = 2

// ddbSchemaFormatVersion is the only _schema.json format_version the
// DynamoDB encoder accepts (a dedicated DDB constant rather than the
// redis collection one, even though both are 1).
const ddbSchemaFormatVersion uint32 = 1

// ErrDDBEncodeInvalidSchema is returned when a _schema.json file cannot
// be parsed into the expected shape.
var ErrDDBEncodeInvalidSchema = errors.New("backup: dynamodb encode invalid _schema.json")

// ErrDDBEncodeNotRegular is returned when a _schema.json (or, later,
// item file) is not a regular file — a symlink, FIFO, device, or
// directory. A dedicated DDB sentinel (not the redis one) so callers
// classify it correctly via errors.Is.
var ErrDDBEncodeNotRegular = errors.New("backup: dynamodb dump file is not a regular file")

// DynamoDBEncoder reconstructs the internal DynamoDB keyspace from the
// decoded dynamodb/ directory tree.
type DynamoDBEncoder struct {
	inRoot string
}

// NewDynamoDBEncoder constructs an encoder rooted at <inRoot>/dynamodb/.
func NewDynamoDBEncoder(inRoot string) *DynamoDBEncoder {
	return &DynamoDBEncoder{inRoot: inRoot}
}

func (e *DynamoDBEncoder) dynamoDir() string {
	return filepath.Join(e.inRoot, "dynamodb")
}

// Encode walks dynamodb/<table>/ and stages each table's schema +
// generation-counter records on b. A missing dynamodb/ directory is
// not an error.
func (e *DynamoDBEncoder) Encode(b *snapshotBuilder) error {
	dir := e.dynamoDir()
	if err := lstatDumpDir(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = root.Close() }()
	entries, err := readRootDirEntries(root)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		if err := e.encodeTable(b, root, ent.Name()); err != nil {
			return err
		}
	}
	return nil
}

// encodeTable reads one <table>/_schema.json and emits its
// !ddb|meta|table| schema record and !ddb|meta|gen| counter.
func (e *DynamoDBEncoder) encodeTable(b *snapshotBuilder, root *os.Root, tableDir string) error {
	schema, err := e.readSchema(root, tableDir)
	if err != nil {
		return err
	}
	tableName := schema.GetTableName()
	if tableName == "" {
		return errors.Wrapf(ErrDDBEncodeInvalidSchema, "%s/_schema.json: empty table_name", tableDir)
	}
	// A table must have a hash key; fail early here rather than letting
	// an empty primary key propagate into the item encoder (which
	// builds item keys from the hash/range attributes).
	if schema.GetPrimaryKey().GetHashKey() == "" {
		return errors.Wrapf(ErrDDBEncodeInvalidSchema, "%s/_schema.json: empty primary hash key", tableDir)
	}
	encTable := base64.RawURLEncoding.EncodeToString([]byte(tableName))

	metaKey := append([]byte(DDBTableMetaPrefix), encTable...)
	metaVal, err := marshalStoredDDBSchema(schema)
	if err != nil {
		return err
	}
	if err := b.Add(metaKey, metaVal, 0); err != nil {
		return err
	}

	genKey := append([]byte(DDBTableGenPrefix), encTable...)
	genVal := []byte(strconv.FormatUint(ddbRestoreGeneration, 10))
	return b.Add(genKey, genVal, 0)
}

// readSchema opens <table>/_schema.json within root (symlink-escape /
// FIFO / hard-link safe, like the redis sidecar reads) and rebuilds the
// DynamoTableSchema proto from its public projection.
func (e *DynamoDBEncoder) readSchema(root *os.Root, tableDir string) (*pb.DynamoTableSchema, error) {
	rel := filepath.Join(tableDir, "_schema.json")
	// Pre-open Lstat THROUGH the root fd refuses a symlink / FIFO /
	// device / directory BEFORE root.Open, so a reader-less FIFO cannot
	// block the open (the post-open fstat guard the walk paths use never
	// runs if Open itself hangs). Consistent with openDumpSidecar; the
	// Lstat is relative to the pinned root fd so it cannot escape.
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
	var pub ddbPublicSchema
	if err := decodeOneJSON(f, &pub); err != nil {
		return nil, errors.Wrapf(ErrDDBEncodeInvalidSchema, "%s: %v", rel, err)
	}
	if pub.FormatVersion != ddbSchemaFormatVersion {
		return nil, errors.Wrapf(ErrDDBEncodeInvalidSchema,
			"%s: unsupported format_version %d", rel, pub.FormatVersion)
	}
	return publicToSchema(pub), nil
}

// publicToSchema is the inverse of schemaToPublic: it rebuilds the
// DynamoTableSchema proto from its _schema.json projection. Attribute
// definitions come from the flat AttributeDefinitions list (the public
// per-key Type fields are redundant copies). KeyEncodingVersion and
// Generation are stamped for the restore (see file header).
func publicToSchema(pub ddbPublicSchema) *pb.DynamoTableSchema {
	defs := make(map[string]string, len(pub.AttributeDefinitions))
	for _, d := range pub.AttributeDefinitions {
		defs[d.Name] = d.Type
	}
	schema := &pb.DynamoTableSchema{
		TableName:            pub.TableName,
		AttributeDefinitions: defs,
		PrimaryKey:           publicKeyToProto(pub.PrimaryKey),
		KeyEncodingVersion:   ddbOrderedKeyEncodingV2,
		Generation:           ddbRestoreGeneration,
	}
	if len(pub.GlobalSecondaryIndexes) > 0 {
		gsis := make(map[string]*pb.DynamoGlobalSecondaryIndex, len(pub.GlobalSecondaryIndexes))
		for _, g := range pub.GlobalSecondaryIndexes {
			gsis[g.Name] = &pb.DynamoGlobalSecondaryIndex{
				KeySchema: publicKeyToProto(g.KeySchema),
				Projection: &pb.DynamoGSIProjection{
					ProjectionType:   g.Projection.Type,
					NonKeyAttributes: append([]string(nil), g.Projection.NonKeyAttributes...),
				},
			}
		}
		schema.GlobalSecondaryIndexes = gsis
	}
	return schema
}

// publicKeyToProto rebuilds a DynamoKeySchema (hash + optional range)
// from its public projection.
func publicKeyToProto(k publicKeySchema) *pb.DynamoKeySchema {
	return &pb.DynamoKeySchema{
		HashKey:  k.HashKey.Name,
		RangeKey: k.RangeKey.Name,
	}
}

// marshalStoredDDBSchema reproduces the live schema value:
// storedDDBSchemaMagic + deterministic proto marshal.
func marshalStoredDDBSchema(schema *pb.DynamoTableSchema) ([]byte, error) {
	body, err := gproto.MarshalOptions{Deterministic: true}.Marshal(schema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// const-capacity, zero-length start + append (magic then body):
	// keeps the `len(magic)+len(body)` arithmetic out of make() — which
	// CodeQL flags as a potential allocation-size overflow — and the
	// zero length satisfies makezero.
	out := make([]byte, 0, len(storedDDBSchemaMagic))
	out = append(out, storedDDBSchemaMagic...)
	return append(out, body...), nil
}
