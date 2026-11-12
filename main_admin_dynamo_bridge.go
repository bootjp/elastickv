package main

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/cockroachdb/errors"
)

// newDynamoTablesSource adapts *adapter.DynamoDBServer to the
// admin.TablesSource interface. The bridge stays in this file (rather
// than internal/admin) so the admin package stays free of the heavy
// adapter-package dependency tree (gRPC, Raft, store).
//
// Returns nil when dynamoServer is nil; admin.NewServer handles a nil
// Tables field by leaving the dynamo paths off the wire entirely,
// which is the right behaviour for builds that ship without the
// Dynamo adapter.
func newDynamoTablesSource(dynamoServer *adapter.DynamoDBServer) admin.TablesSource {
	if dynamoServer == nil {
		return nil
	}
	return &dynamoTablesBridge{server: dynamoServer}
}

// dynamoTablesBridge is the thin adapter that re-shapes the adapter's
// AdminTableSummary DTO into the admin package's DynamoTableSummary.
// The two structs are deliberately isomorphic so this translation
// does no allocation more than necessary; if a future GSI field is
// added on one side, the build breaks here, which is exactly the
// drift signal we want.
type dynamoTablesBridge struct {
	server *adapter.DynamoDBServer
}

func (b *dynamoTablesBridge) AdminListTables(ctx context.Context) ([]string, error) {
	return b.server.AdminListTables(ctx) //nolint:wrapcheck // pure pass-through; the adapter owns the error context.
}

func (b *dynamoTablesBridge) AdminDescribeTable(ctx context.Context, name string) (*admin.DynamoTableSummary, bool, error) {
	summary, exists, err := b.server.AdminDescribeTable(ctx, name)
	if err != nil {
		return nil, false, err //nolint:wrapcheck // adapter wraps internally.
	}
	if !exists {
		return nil, false, nil
	}
	return convertAdminTableSummary(summary), true, nil
}

func (b *dynamoTablesBridge) AdminCreateTable(ctx context.Context, principal admin.AuthPrincipal, in admin.CreateTableRequest) (*admin.DynamoTableSummary, error) {
	summary, err := b.server.AdminCreateTable(ctx, convertAdminPrincipal(principal), convertCreateTableInput(in))
	if err != nil {
		return nil, translateAdminTablesError(err)
	}
	return convertAdminTableSummary(summary), nil
}

func (b *dynamoTablesBridge) AdminDeleteTable(ctx context.Context, principal admin.AuthPrincipal, name string) error {
	if err := b.server.AdminDeleteTable(ctx, convertAdminPrincipal(principal), name); err != nil {
		return translateAdminTablesError(err)
	}
	return nil
}

// Phase 3a — item-level bridge methods. Each one converts the
// admin-package wire types into the adapter's parallel struct
// graph, dispatches to the adapter, and translates the adapter's
// error vocabulary onto the admin-package sentinels.

func (b *dynamoTablesBridge) AdminScanItems(ctx context.Context, principal admin.AuthPrincipal, table string, opts admin.AdminScanItemsOptions) (admin.AdminScanItemsResult, error) {
	adapterOpts := adapter.AdminScanOptions{
		Limit:          opts.Limit,
		ExclusiveStart: adminToAdapterAttributeMap(opts.ExclusiveStart),
	}
	out, err := b.server.AdminScanTable(ctx, convertAdminPrincipal(principal), table, adapterOpts)
	if err != nil {
		return admin.AdminScanItemsResult{}, translateAdminItemsError(err)
	}
	res := admin.AdminScanItemsResult{
		Items:            make([]admin.AdminItem, len(out.Items)),
		LastEvaluatedKey: adapterToAdminAttributeMap(out.LastEvaluatedKey),
	}
	for i, item := range out.Items {
		res.Items[i] = admin.AdminItem{Attributes: adapterToAdminAttributeMap(item.Attributes)}
	}
	return res, nil
}

func (b *dynamoTablesBridge) AdminGetItem(ctx context.Context, principal admin.AuthPrincipal, table string, key map[string]admin.AdminAttributeValue) (*admin.AdminItem, bool, error) {
	item, found, err := b.server.AdminGetItem(ctx, convertAdminPrincipal(principal), table, adminToAdapterAttributeMap(key))
	if err != nil {
		return nil, false, translateAdminItemsError(err)
	}
	if !found || item == nil {
		return nil, false, nil
	}
	return &admin.AdminItem{Attributes: adapterToAdminAttributeMap(item.Attributes)}, true, nil
}

func (b *dynamoTablesBridge) AdminPutItem(ctx context.Context, principal admin.AuthPrincipal, table string, key map[string]admin.AdminAttributeValue, item admin.AdminItem) error {
	adapterItem := adapter.AdminItem{Attributes: adminToAdapterAttributeMap(item.Attributes)}
	if err := b.server.AdminPutItem(ctx, convertAdminPrincipal(principal), table, adminToAdapterAttributeMap(key), adapterItem); err != nil {
		return translateAdminItemsError(err)
	}
	return nil
}

func (b *dynamoTablesBridge) AdminDeleteItem(ctx context.Context, principal admin.AuthPrincipal, table string, key map[string]admin.AdminAttributeValue) error {
	if err := b.server.AdminDeleteItem(ctx, convertAdminPrincipal(principal), table, adminToAdapterAttributeMap(key)); err != nil {
		return translateAdminItemsError(err)
	}
	return nil
}

// adminToAdapterAttributeValue / map functions translate the
// admin package's wire type into the adapter package's parallel
// struct. The two are isomorphic field-for-field; any drift here
// is a build break, which is the right signal.
func adminToAdapterAttributeValue(v admin.AdminAttributeValue) adapter.AdminAttributeValue {
	out := adapter.AdminAttributeValue{
		S: v.S, N: v.N, B: v.B, BOOL: v.BOOL, NULL: v.NULL,
		SS: v.SS, NS: v.NS, BS: v.BS,
	}
	if v.L != nil {
		out.L = make([]adapter.AdminAttributeValue, len(v.L))
		for i, e := range v.L {
			out.L[i] = adminToAdapterAttributeValue(e)
		}
	}
	if v.M != nil {
		out.M = make(map[string]adapter.AdminAttributeValue, len(v.M))
		for k, e := range v.M {
			out.M[k] = adminToAdapterAttributeValue(e)
		}
	}
	return out
}

func adapterToAdminAttributeValue(v adapter.AdminAttributeValue) admin.AdminAttributeValue {
	out := admin.AdminAttributeValue{
		S: v.S, N: v.N, B: v.B, BOOL: v.BOOL, NULL: v.NULL,
		SS: v.SS, NS: v.NS, BS: v.BS,
	}
	if v.L != nil {
		out.L = make([]admin.AdminAttributeValue, len(v.L))
		for i, e := range v.L {
			out.L[i] = adapterToAdminAttributeValue(e)
		}
	}
	if v.M != nil {
		out.M = make(map[string]admin.AdminAttributeValue, len(v.M))
		for k, e := range v.M {
			out.M[k] = adapterToAdminAttributeValue(e)
		}
	}
	return out
}

func adminToAdapterAttributeMap(in map[string]admin.AdminAttributeValue) map[string]adapter.AdminAttributeValue {
	if in == nil {
		return nil
	}
	out := make(map[string]adapter.AdminAttributeValue, len(in))
	for k, v := range in {
		out[k] = adminToAdapterAttributeValue(v)
	}
	return out
}

func adapterToAdminAttributeMap(in map[string]adapter.AdminAttributeValue) map[string]admin.AdminAttributeValue {
	if in == nil {
		return nil
	}
	out := make(map[string]admin.AdminAttributeValue, len(in))
	for k, v := range in {
		out[k] = adapterToAdminAttributeValue(v)
	}
	return out
}

// translateAdminItemsError maps the adapter's item-level error
// vocabulary onto the admin-package sentinels. Mirrors
// translateAdminTablesError's shape but uses the Items
// sentinels — kept separate so a future per-resource
// classification change (e.g., a new "rate limited" sentinel
// for the item path but not the table path) lands in one place.
func translateAdminItemsError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, adapter.ErrAdminForbidden):
		return admin.ErrItemsForbidden
	case errors.Is(err, adapter.ErrAdminNotLeader):
		return admin.ErrItemsNotLeader
	case errors.Is(err, adapter.ErrAdminDynamoNotFound):
		return admin.ErrItemsTableNotFound
	case errors.Is(err, adapter.ErrAdminDynamoValidation):
		// Wrap rather than replace so the adapter's specific
		// validation message ("table_name is required", legacy-
		// migration hint at adapter/dynamodb_admin_items.go, etc.)
		// reaches the HTTP 400 body via writeItemsError's
		// err.Error() emission. Bare sentinel return dropped the
		// operator-facing reason at the bridge boundary (Claude
		// review on PR #813 r3). errors.Is(out, ErrItemsValidation)
		// still returns true so the handler's match arm still
		// fires.
		return errors.Wrap(admin.ErrItemsValidation, err.Error())
	case isLeaderChurnError(err):
		// Mid-dispatch leadership churn looks like an internal error
		// from the kv coordinator (election finishes between the
		// adapter's isVerifiedDynamoLeader check and the actual
		// Raft propose). Map to the Items not-leader sentinel so the
		// handler returns 503 + Retry-After: 1 and the SPA's retry
		// contract stays intact (P2 review on PR #813 — the table-side
		// translator already does this; the item-side translator
		// initially shipped without the parallel branch).
		return admin.ErrItemsNotLeader
	default:
		return err //nolint:wrapcheck // forwarded so the handler logs but does not surface it.
	}
}

// convertCreateTableInput translates the admin-handler request DTO
// into the adapter's parallel input struct. We do this here — not
// in the admin package — to keep `internal/admin` free of any
// adapter import.
func convertCreateTableInput(in admin.CreateTableRequest) adapter.AdminCreateTableInput {
	out := adapter.AdminCreateTableInput{
		TableName:    in.TableName,
		PartitionKey: adapter.AdminAttribute{Name: in.PartitionKey.Name, Type: in.PartitionKey.Type},
	}
	if in.SortKey != nil {
		out.SortKey = &adapter.AdminAttribute{Name: in.SortKey.Name, Type: in.SortKey.Type}
	}
	if len(in.GSI) == 0 {
		return out
	}
	out.GSI = make([]adapter.AdminCreateGSI, len(in.GSI))
	for i, g := range in.GSI {
		gsi := adapter.AdminCreateGSI{
			Name:             g.Name,
			PartitionKey:     adapter.AdminAttribute{Name: g.PartitionKey.Name, Type: g.PartitionKey.Type},
			ProjectionType:   g.Projection.Type,
			NonKeyAttributes: append([]string(nil), g.Projection.NonKeyAttributes...),
		}
		if g.SortKey != nil {
			gsi.SortKey = &adapter.AdminAttribute{Name: g.SortKey.Name, Type: g.SortKey.Type}
		}
		out.GSI[i] = gsi
	}
	return out
}

// translateAdminTablesError maps the adapter's error vocabulary
// onto the admin-package sentinels the HTTP handler matches against.
// Anything not recognised is forwarded as-is and answered with 500
// + a sanitised body, so a future adapter error mode does not leak
// raw text to clients while we are still adding the translation.
func translateAdminTablesError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, adapter.ErrAdminForbidden):
		return admin.ErrTablesForbidden
	case errors.Is(err, adapter.ErrAdminNotLeader):
		return admin.ErrTablesNotLeader
	// Check structured adapter errors BEFORE the leader-churn
	// matcher: a ValidationException whose message contains
	// "not leader" (e.g., a user-supplied attribute name like
	// `not leader` triggering the conflicting-attribute-type
	// validator) must be classified as 400 invalid_request, not
	// 503 leader_unavailable. The kv-internal sentinel checks
	// in isLeaderChurnError still catch real leadership churn
	// because they are typed-sentinel matches, not substring.
	case adapter.IsAdminTableAlreadyExists(err):
		return admin.ErrTablesAlreadyExists
	case adapter.IsAdminTableNotFound(err):
		return admin.ErrTablesNotFound
	case adapter.IsAdminValidation(err):
		msg := adapter.AdminErrorMessage(err)
		if msg == "" {
			msg = "validation failed"
		}
		return &admin.ValidationError{Message: msg}
	case isLeaderChurnError(err):
		// Cover leader-churn that surfaces between the up-front
		// isVerifiedDynamoLeader check and createTableWithRetry's
		// dispatch — the kv coordinator can drop leadership in
		// that window and the resulting transient error should
		// surface as 503 leader_unavailable + Retry-After: 1
		// rather than a generic 500. P2 review on PR #634.
		return admin.ErrTablesNotLeader
	default:
		return err //nolint:wrapcheck // forwarded so the handler logs but does not surface it.
	}
}

func convertAdminTableSummary(in *adapter.AdminTableSummary) *admin.DynamoTableSummary {
	out := &admin.DynamoTableSummary{
		Name:         in.Name,
		PartitionKey: in.PartitionKey,
		SortKey:      in.SortKey,
		Generation:   in.Generation,
	}
	if len(in.GlobalSecondaryIndexes) == 0 {
		return out
	}
	out.GlobalSecondaryIndexes = make([]admin.DynamoGSISummary, len(in.GlobalSecondaryIndexes))
	for i, g := range in.GlobalSecondaryIndexes {
		out.GlobalSecondaryIndexes[i] = admin.DynamoGSISummary{
			Name:           g.Name,
			PartitionKey:   g.PartitionKey,
			SortKey:        g.SortKey,
			ProjectionType: g.ProjectionType,
		}
	}
	return out
}
