package adapter

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// AdminTableSummary is the table-level information the admin dashboard
// surfaces for a single Dynamo-compatible table. It deliberately
// projects only the fields the dashboard needs so the package's
// wire-format types (dynamoTableSchema and friends) stay internal.
type AdminTableSummary struct {
	Name                   string
	PartitionKey           string
	SortKey                string
	Generation             uint64
	GlobalSecondaryIndexes []AdminGSISummary
}

// AdminGSISummary mirrors AdminTableSummary for a single GSI.
type AdminGSISummary struct {
	Name           string
	PartitionKey   string
	SortKey        string
	ProjectionType string
}

// AdminListTables returns every Dynamo-style table this server knows
// about, in the lexicographic order the metadata index produces.
// Intended for the in-process admin listener as the SigV4-free
// counterpart to the listTables HTTP handler; both share the same
// underlying lookup so the two views cannot drift.
func (d *DynamoDBServer) AdminListTables(ctx context.Context) ([]string, error) {
	return d.listTableNames(ctx)
}

// AdminDescribeTable returns a schema snapshot for name. The triple
// (result, present, error) lets admin callers distinguish a genuine
// "not found" from a storage error without sniffing sentinels: when
// the table is missing the function returns (nil, false, nil).
//
// Unlike the SigV4 describeTable handler, AdminDescribeTable does
// NOT invoke ensureLegacyTableMigration. The admin dashboard is a
// strictly read-only surface (Gemini medium review on PR #633), so
// triggering Raft-coordinated key-encoding migrations as a side
// effect of routine polling would (a) violate the read-only
// contract and (b) cause every dashboard refresh to write to the
// cluster. Migration still runs lazily on the next SigV4 read or
// write of the same table — the schema we return here is just a
// snapshot for display, not a guarantee that the table is
// up-to-date for serving.
func (d *DynamoDBServer) AdminDescribeTable(ctx context.Context, name string) (*AdminTableSummary, bool, error) {
	schema, exists, err := d.loadTableSchema(ctx, name)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	return summaryFromSchema(schema), true, nil
}

// AdminRole is the authorization tier the adapter checks against on
// every admin write entrypoint. The constants intentionally mirror
// internal/admin.Role string values so the wire / persisted role
// vocabulary stays aligned across packages, but we keep a separate
// type here so the adapter has zero dependency on internal/admin.
type AdminRole string

const (
	// AdminRoleReadOnly may issue list / describe but not create or delete.
	AdminRoleReadOnly AdminRole = "read_only"
	// AdminRoleFull may issue every admin operation.
	AdminRoleFull AdminRole = "full"
)

// canWrite reports whether the role authorises state-mutating
// operations. Kept as a method (rather than an inline check) so any
// future "delete-only" tier reads consistently across the package.
func (r AdminRole) canWrite() bool { return r == AdminRoleFull }

// AdminPrincipal is the authentication context every admin write
// entrypoint takes. The adapter re-evaluates authorisation against
// this principal *itself* — it does not trust the caller to have
// already enforced the role. That is the design's "認可の真実は常に
// adapter 側" invariant (Section 3.2): if a follower forwards a
// pre-authenticated request via the future AdminForward RPC, the
// leader must still verify before acting.
type AdminPrincipal struct {
	AccessKey string
	Role      AdminRole
}

// ErrAdminNotLeader is returned by every write entrypoint when this
// node is not the verified Raft leader. The admin HTTP handler
// translates this to 503 + Retry-After: 1 today; the future
// AdminForward RPC catches it as the trigger to forward to the
// leader instead.
var ErrAdminNotLeader = errors.New("dynamodb admin: this node is not the raft leader")

// ErrAdminForbidden is returned when the principal lacks the role
// required for the operation. Admin handlers translate this to 403
// "forbidden" without leaking which field of the principal failed
// the check.
var ErrAdminForbidden = errors.New("dynamodb admin: principal lacks required role")

// IsAdminTableAlreadyExists reports whether err is the adapter's
// "table already exists" failure (ResourceInUseException). The
// bridge in main_admin.go uses this to map the adapter's internal
// error vocabulary onto admin's HTTP-facing sentinels without
// importing the package-private dynamoAPIError type.
func IsAdminTableAlreadyExists(err error) bool {
	return adminAPIErrorTypeIs(err, dynamoErrResourceInUse)
}

// IsAdminTableNotFound is the ResourceNotFoundException counterpart
// for AdminDeleteTable / AdminDescribeTable mapped through the
// adapter's structured error chain.
func IsAdminTableNotFound(err error) bool {
	return adminAPIErrorTypeIs(err, dynamoErrResourceNotFound)
}

// IsAdminValidation reports whether err is a validation failure
// the adapter signalled via ValidationException. Admin handlers map
// this to 400 + a sanitised message.
func IsAdminValidation(err error) bool {
	return adminAPIErrorTypeIs(err, dynamoErrValidation)
}

// AdminErrorMessage extracts the human-readable message from a
// dynamoAPIError for surfacing back to the SPA. Returns "" when err
// is not a structured adapter error so callers fall back to a
// generic message instead of leaking arbitrary err.Error() output.
func AdminErrorMessage(err error) string {
	var apiErr *dynamoAPIError
	if errors.As(err, &apiErr) && apiErr != nil {
		return apiErr.message
	}
	return ""
}

func adminAPIErrorTypeIs(err error, want string) bool {
	var apiErr *dynamoAPIError
	if !errors.As(err, &apiErr) || apiErr == nil {
		return false
	}
	return apiErr.errorType == want
}

// AdminAttribute names a single primary-key or GSI key column. Type
// must be one of "S", "N", "B" — DynamoDB does not allow boolean or
// list keys and the adapter's existing schema validation enforces
// the same restriction at the next layer.
type AdminAttribute struct {
	Name string
	Type string
}

// AdminCreateGSI describes one global secondary index in an admin
// CreateTable request. SortKey is optional (hash-only GSI). When
// ProjectionType is "INCLUDE", NonKeyAttributes lists the projected
// attribute names; otherwise NonKeyAttributes is ignored.
type AdminCreateGSI struct {
	Name             string
	PartitionKey     AdminAttribute
	SortKey          *AdminAttribute
	ProjectionType   string
	NonKeyAttributes []string
}

// AdminCreateTableInput is the admin-facing CreateTable shape. The
// HTTP handler maps the design 4.2 JSON body into this struct, then
// AdminCreateTable converts it to the adapter's internal
// createTableInput. We do not pass the SigV4-flavoured wire struct
// directly because that struct's field names track AWS exactly and
// would be awkward for the admin SPA to author.
type AdminCreateTableInput struct {
	TableName    string
	PartitionKey AdminAttribute
	SortKey      *AdminAttribute
	GSI          []AdminCreateGSI
}

// AdminCreateTable creates a Dynamo-compatible table on the local
// node, after re-validating the principal's role and confirming this
// node is the verified Raft leader. The returned summary mirrors the
// shape of AdminDescribeTable on the same name so the SPA can show
// the freshly-created table without an extra describe round-trip.
//
// Errors:
//   - ErrAdminForbidden when the principal cannot write.
//   - ErrAdminNotLeader when the node is a follower.
//   - The adapter's standard dynamoAPIError chain for validation /
//     storage failures, preserved unmodified so the HTTP handler can
//     map the inner code (ValidationException, ResourceInUseException,
//     etc.) to the appropriate status without re-classifying.
func (d *DynamoDBServer) AdminCreateTable(ctx context.Context, principal AdminPrincipal, in AdminCreateTableInput) (*AdminTableSummary, error) {
	if !principal.Role.canWrite() {
		return nil, ErrAdminForbidden
	}
	if !isVerifiedDynamoLeader(d.coordinator) {
		return nil, ErrAdminNotLeader
	}
	legacy, err := buildLegacyCreateTableInput(in)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(legacy.TableName) == "" {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	unlock := d.lockTableOperations([]string{legacy.TableName})
	defer unlock()
	schema, err := buildCreateTableSchema(legacy)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if err := d.createTableWithRetry(ctx, legacy.TableName, schema); err != nil {
		return nil, err
	}
	d.observeTables(ctx, schema.TableName)
	// Reload after commit so the returned summary carries the
	// generation that createTableWithRetry actually persisted —
	// `schema` going in had generation 0 because the next number is
	// computed inside the retry loop. Reading it back also matches
	// the SPA's mental model: the response shape is identical to a
	// follow-up AdminDescribeTable call.
	stored, exists, err := d.loadTableSchema(ctx, legacy.TableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		// Should be unreachable: createTableWithRetry succeeded above
		// under the table lock, so a missing schema here means the
		// metadata index is corrupt or another writer raced through
		// the lock — both of which are server-internal failures.
		return nil, newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "table missing immediately after create")
	}
	return summaryFromSchema(stored), nil
}

// AdminDeleteTable is the SigV4-bypass counterpart to deleteTable.
// Returns the same sentinel errors as AdminCreateTable plus the
// adapter's standard dynamoErrResourceNotFound when the table is
// absent — admin handlers should map that to 404 rather than 500.
func (d *DynamoDBServer) AdminDeleteTable(ctx context.Context, principal AdminPrincipal, name string) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !isVerifiedDynamoLeader(d.coordinator) {
		return ErrAdminNotLeader
	}
	if strings.TrimSpace(name) == "" {
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	unlock := d.lockTableOperations([]string{name})
	defer unlock()
	return d.deleteTableWithRetry(ctx, name)
}

// buildLegacyCreateTableInput maps the admin-facing struct to the
// adapter's existing wire-format struct so the rest of the schema
// pipeline (buildCreateTableSchema → dispatch) can be reused as-is.
// Doing the translation here — rather than refactoring the wire
// types — keeps SigV4 path bit-exact and limits the blast radius of
// the admin feature.
func buildLegacyCreateTableInput(in AdminCreateTableInput) (createTableInput, error) {
	if strings.TrimSpace(in.TableName) == "" {
		return createTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	if strings.TrimSpace(in.PartitionKey.Name) == "" {
		return createTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing partition key name")
	}
	collector := newAttrCollector()
	out := createTableInput{TableName: in.TableName}
	if err := appendKeySchema(&out, in, collector.add); err != nil {
		return createTableInput{}, err
	}
	for _, gsi := range in.GSI {
		legacy, err := buildLegacyGSI(gsi, collector.add)
		if err != nil {
			return createTableInput{}, err
		}
		out.GlobalSecondaryIndexes = append(out.GlobalSecondaryIndexes, legacy)
	}
	out.AttributeDefinitions = collector.sorted()
	return out, nil
}

// attrCollector merges every attribute referenced by primary key
// and GSIs, rejecting conflicting type declarations for the same
// name. Pulling the bookkeeping out of the build function lets us
// share it with appendKeySchema / buildLegacyGSI without exposing a
// raw map to callers.
type attrCollector struct{ set map[string]string }

func newAttrCollector() *attrCollector { return &attrCollector{set: map[string]string{}} }

func (c *attrCollector) add(a AdminAttribute) error {
	if existing, ok := c.set[a.Name]; ok && existing != a.Type {
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation,
			"conflicting attribute type for "+a.Name)
	}
	c.set[a.Name] = a.Type
	return nil
}

// sorted emits the merged attribute definitions in lexicographic
// order so makeCreateTableRequest produces a byte-identical
// OperationGroup for inputs that differ only in map iteration
// order. Tests that assert against an already-created table can
// then compare schemas without pre-sorting on their side.
func (c *attrCollector) sorted() []createTableAttributeDefinition {
	defs := make([]createTableAttributeDefinition, 0, len(c.set))
	for name, typ := range c.set {
		defs = append(defs, createTableAttributeDefinition{
			AttributeName: name,
			AttributeType: typ,
		})
	}
	sort.Slice(defs, func(i, j int) bool {
		return defs[i].AttributeName < defs[j].AttributeName
	})
	return defs
}

// appendKeySchema writes the primary key (HASH + optional RANGE)
// into out.KeySchema and registers the same attributes with addAttr
// so AttributeDefinitions stays consistent with the key schema.
func appendKeySchema(out *createTableInput, in AdminCreateTableInput, addAttr func(AdminAttribute) error) error {
	if err := addAttr(in.PartitionKey); err != nil {
		return err
	}
	out.KeySchema = append(out.KeySchema, createTableKeySchemaElement{
		AttributeName: in.PartitionKey.Name,
		KeyType:       "HASH",
	})
	if in.SortKey == nil {
		return nil
	}
	if err := addAttr(*in.SortKey); err != nil {
		return err
	}
	out.KeySchema = append(out.KeySchema, createTableKeySchemaElement{
		AttributeName: in.SortKey.Name,
		KeyType:       "RANGE",
	})
	return nil
}

func buildLegacyGSI(gsi AdminCreateGSI, addAttr func(AdminAttribute) error) (createTableGSI, error) {
	if strings.TrimSpace(gsi.Name) == "" {
		return createTableGSI{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing GSI name")
	}
	if strings.TrimSpace(gsi.PartitionKey.Name) == "" {
		return createTableGSI{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing GSI partition key name")
	}
	if err := addAttr(gsi.PartitionKey); err != nil {
		return createTableGSI{}, err
	}
	out := createTableGSI{
		IndexName: gsi.Name,
		KeySchema: []createTableKeySchemaElement{
			{AttributeName: gsi.PartitionKey.Name, KeyType: "HASH"},
		},
		Projection: createTableProjection{ProjectionType: gsi.ProjectionType},
	}
	if gsi.SortKey != nil {
		if err := addAttr(*gsi.SortKey); err != nil {
			return createTableGSI{}, err
		}
		out.KeySchema = append(out.KeySchema, createTableKeySchemaElement{
			AttributeName: gsi.SortKey.Name,
			KeyType:       "RANGE",
		})
	}
	if strings.EqualFold(gsi.ProjectionType, "INCLUDE") {
		out.Projection.NonKeyAttributes = append([]string(nil), gsi.NonKeyAttributes...)
	}
	return out, nil
}

func summaryFromSchema(s *dynamoTableSchema) *AdminTableSummary {
	out := &AdminTableSummary{
		Name:         s.TableName,
		PartitionKey: s.PrimaryKey.HashKey,
		SortKey:      s.PrimaryKey.RangeKey,
		Generation:   s.Generation,
	}
	if len(s.GlobalSecondaryIndexes) == 0 {
		return out
	}
	names := make([]string, 0, len(s.GlobalSecondaryIndexes))
	for n := range s.GlobalSecondaryIndexes {
		names = append(names, n)
	}
	// Sort so the JSON the admin handler emits is deterministic; map
	// iteration order would otherwise produce an unstable output that
	// breaks both UI diffing and snapshot tests.
	sort.Strings(names)
	out.GlobalSecondaryIndexes = make([]AdminGSISummary, 0, len(names))
	for _, name := range names {
		gsi := s.GlobalSecondaryIndexes[name]
		out.GlobalSecondaryIndexes = append(out.GlobalSecondaryIndexes, AdminGSISummary{
			Name:           name,
			PartitionKey:   gsi.KeySchema.HashKey,
			SortKey:        gsi.KeySchema.RangeKey,
			ProjectionType: gsi.Projection.ProjectionType,
		})
	}
	return out
}
