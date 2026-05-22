package adapter

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
)

// AdminAttributeValue mirrors the AWS DynamoDB-wire MessageAttribute
// shape. Each instance carries exactly one populated field for
// scalar types (S/N/B/BOOL/NULL), exactly one populated set
// (SS/NS/BS), or a recursive container (L, M). Wire-compatible with
// every AWS SDK and the existing SigV4 path's internal type.
type AdminAttributeValue struct {
	S    *string                        `json:"S,omitempty"`
	N    *string                        `json:"N,omitempty"`
	B    []byte                         `json:"B,omitempty"`
	BOOL *bool                          `json:"BOOL,omitempty"`
	NULL *bool                          `json:"NULL,omitempty"`
	SS   []string                       `json:"SS,omitempty"`
	NS   []string                       `json:"NS,omitempty"`
	BS   [][]byte                       `json:"BS,omitempty"`
	L    []AdminAttributeValue          `json:"L,omitempty"`
	M    map[string]AdminAttributeValue `json:"M,omitempty"`
}

// AdminItem is the admin-facing projection of one DynamoDB item.
type AdminItem struct {
	Attributes map[string]AdminAttributeValue `json:"attributes"`
}

// AdminItemKey carries the primary key of a single item; SortKey is
// nil for hash-only tables.
type AdminItemKey struct {
	PartitionKey AdminAttributeValue  `json:"partition_key"`
	SortKey      *AdminAttributeValue `json:"sort_key,omitempty"`
}

// AdminScanResult is the admin-package projection of the underlying
// scan output: the bounded items page plus a continuation key (nil
// when the scan has drained for the current MVCC snapshot).
type AdminScanResult struct {
	Items            []AdminItem
	LastEvaluatedKey map[string]AdminAttributeValue
}

// AdminScanOptions controls one AdminScanTable call. Defaults
// match the design doc §3.1.1:
//
//	Limit = 25 (clamped to [1, adminItemScanMaxLimit=100])
//	StartKey = nil (front of the table)
type AdminScanOptions struct {
	Limit          int
	ExclusiveStart map[string]AdminAttributeValue
}

const (
	adminItemScanDefaultLimit = 25
	adminItemScanMaxLimit     = 100
)

// ErrAdminDynamoNotFound is the structured "item or table not
// found" sentinel admin handlers match against to render 404. The
// describe path uses (nil, false, nil) instead; this sentinel is
// for write-path failures where the missing target is observed only
// after the retry / commit attempt.
var ErrAdminDynamoNotFound = errors.New("dynamodb admin: target not found")

// ErrAdminDynamoValidation is returned when an admin entrypoint
// receives a structurally-bad request (missing key, malformed
// attribute, blank table name).
var ErrAdminDynamoValidation = errors.New("dynamodb admin: invalid request")

// AdminScanTable returns a bounded page of items. Read-only.
//
// Sentinels:
//   - ErrAdminForbidden     — principal lacks read role
//   - ErrAdminNotLeader     — follower
//   - ErrAdminDynamoNotFound — table absent
//   - ErrAdminDynamoValidation — empty / malformed input
//
// Scan-response budget: a single underlying scan may return a
// LastEvaluatedKey before reaching Limit because of DynamoDB's
// 1 MiB response cap. The admin RPC does NOT loop internally to
// refill the page; partial pages are documented behaviour and the
// caller treats any non-nil LastEvaluatedKey as "more available".
func (d *DynamoDBServer) AdminScanTable(ctx context.Context, principal AdminPrincipal, tableName string, opts AdminScanOptions) (AdminScanResult, error) {
	if !principal.Role.canRead() {
		return AdminScanResult{}, ErrAdminForbidden
	}
	if !isVerifiedDynamoLeader(ctx, d.coordinator) {
		return AdminScanResult{}, ErrAdminNotLeader
	}
	if strings.TrimSpace(tableName) == "" {
		return AdminScanResult{}, ErrAdminDynamoValidation
	}
	limit := clampAdminScanLimit(opts.Limit)
	scanInput := scanInput{
		TableName:         tableName,
		ExclusiveStartKey: adminToInternalAttributeMap(opts.ExclusiveStart),
		Limit:             &limit,
	}
	out, err := d.scanItems(ctx, scanInput)
	if err != nil {
		return AdminScanResult{}, translateDynamoAdminError(err)
	}
	return AdminScanResult{
		Items:            adminItemsFromInternal(out.items),
		LastEvaluatedKey: internalToAdminAttributeMap(out.lastEvaluatedKey),
	}, nil
}

// AdminGetItem fetches one item by primary key. Returns
// (nil, false, nil) when the item is absent (not an error).
//
// Sentinels:
//   - ErrAdminForbidden     — principal lacks read role
//   - ErrAdminNotLeader     — follower
//   - ErrAdminDynamoNotFound — table absent
//   - ErrAdminDynamoValidation — empty / malformed input
func (d *DynamoDBServer) AdminGetItem(ctx context.Context, principal AdminPrincipal, tableName string, key map[string]AdminAttributeValue) (*AdminItem, bool, error) {
	if !principal.Role.canRead() {
		return nil, false, ErrAdminForbidden
	}
	if !isVerifiedDynamoLeader(ctx, d.coordinator) {
		return nil, false, ErrAdminNotLeader
	}
	if strings.TrimSpace(tableName) == "" {
		return nil, false, ErrAdminDynamoValidation
	}
	if len(key) == 0 {
		return nil, false, ErrAdminDynamoValidation
	}
	readTS := d.nextTxnReadTS()
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if !exists {
		return nil, false, ErrAdminDynamoNotFound
	}
	internalKey := adminToInternalAttributeMap(key)
	current, found, err := d.readLogicalItemAt(ctx, schema, internalKey, readTS)
	if err != nil {
		return nil, false, translateDynamoAdminError(err)
	}
	if !found {
		return nil, false, nil
	}
	return &AdminItem{Attributes: internalToAdminAttributeMap(current.item)}, true, nil
}

// AdminPutItem creates-or-replaces one item. Write role required.
//
// Sentinels:
//   - ErrAdminForbidden        — principal lacks write role
//   - ErrAdminNotLeader        — follower
//   - ErrAdminDynamoNotFound   — table absent
//   - ErrAdminDynamoValidation — empty / malformed input
func (d *DynamoDBServer) AdminPutItem(ctx context.Context, principal AdminPrincipal, tableName string, item AdminItem) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !isVerifiedDynamoLeader(ctx, d.coordinator) {
		return ErrAdminNotLeader
	}
	if strings.TrimSpace(tableName) == "" {
		return ErrAdminDynamoValidation
	}
	if len(item.Attributes) == 0 {
		return ErrAdminDynamoValidation
	}
	in := putItemInput{
		TableName: tableName,
		Item:      adminToInternalAttributeMap(item.Attributes),
	}
	if _, err := d.putItemWithRetry(ctx, in); err != nil {
		return translateDynamoAdminError(err)
	}
	return nil
}

// AdminDeleteItem removes one item by primary key. Write role
// required.
func (d *DynamoDBServer) AdminDeleteItem(ctx context.Context, principal AdminPrincipal, tableName string, key map[string]AdminAttributeValue) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !isVerifiedDynamoLeader(ctx, d.coordinator) {
		return ErrAdminNotLeader
	}
	if strings.TrimSpace(tableName) == "" {
		return ErrAdminDynamoValidation
	}
	if len(key) == 0 {
		return ErrAdminDynamoValidation
	}
	in := deleteItemInput{
		TableName: tableName,
		Key:       adminToInternalAttributeMap(key),
	}
	if _, err := d.deleteItemWithRetry(ctx, in); err != nil {
		return translateDynamoAdminError(err)
	}
	return nil
}

// clampAdminScanLimit folds the user-supplied Limit into the legal
// [1, adminItemScanMaxLimit] range, mapping 0 to the default 25.
func clampAdminScanLimit(limit int) int32 {
	if limit <= 0 {
		return adminItemScanDefaultLimit
	}
	if limit > adminItemScanMaxLimit {
		return adminItemScanMaxLimit
	}
	return int32(limit)
}

// translateDynamoAdminError maps the adapter's existing *dynamoAPIError
// vocabulary onto the admin sentinels. Anything we cannot match is
// forwarded as-is and the admin handler will render it as a generic
// 500 with a sanitized body.
func translateDynamoAdminError(err error) error {
	switch {
	case err == nil:
		return nil
	case adminAPIErrorTypeIs(err, dynamoErrResourceNotFound):
		return ErrAdminDynamoNotFound
	case adminAPIErrorTypeIs(err, dynamoErrValidation):
		return errors.Wrap(ErrAdminDynamoValidation, AdminErrorMessage(err))
	default:
		return errors.WithStack(err)
	}
}

// adminItemsFromInternal converts a slice of internal item rows
// (the scanItems output) into the admin-facing AdminItem slice. Nil
// in, nil out so callers see the contract directly.
func adminItemsFromInternal(items []map[string]attributeValue) []AdminItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]AdminItem, len(items))
	for i, item := range items {
		out[i] = AdminItem{Attributes: internalToAdminAttributeMap(item)}
	}
	return out
}

// internalToAdminAttributeMap converts the unexported wire type
// into the exported one. Both share identical JSON tags so the
// conversion is a per-field copy with recursive descent for L/M.
func internalToAdminAttributeMap(in map[string]attributeValue) map[string]AdminAttributeValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]AdminAttributeValue, len(in))
	for k, v := range in {
		out[k] = internalToAdminAttributeValue(v)
	}
	return out
}

func adminToInternalAttributeMap(in map[string]AdminAttributeValue) map[string]attributeValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]attributeValue, len(in))
	for k, v := range in {
		out[k] = adminToInternalAttributeValue(v)
	}
	return out
}

func internalToAdminAttributeValue(v attributeValue) AdminAttributeValue {
	out := AdminAttributeValue{
		S: v.S, N: v.N, B: v.B, BOOL: v.BOOL, NULL: v.NULL,
		SS: v.SS, NS: v.NS, BS: v.BS,
	}
	if len(v.L) > 0 {
		out.L = make([]AdminAttributeValue, len(v.L))
		for i, e := range v.L {
			out.L[i] = internalToAdminAttributeValue(e)
		}
	}
	if len(v.M) > 0 {
		out.M = make(map[string]AdminAttributeValue, len(v.M))
		for k, e := range v.M {
			out.M[k] = internalToAdminAttributeValue(e)
		}
	}
	return out
}

func adminToInternalAttributeValue(v AdminAttributeValue) attributeValue {
	out := attributeValue{
		S: v.S, N: v.N, B: v.B, BOOL: v.BOOL, NULL: v.NULL,
		SS: v.SS, NS: v.NS, BS: v.BS,
	}
	if len(v.L) > 0 {
		out.L = make([]attributeValue, len(v.L))
		for i, e := range v.L {
			out.L[i] = adminToInternalAttributeValue(e)
		}
	}
	if len(v.M) > 0 {
		out.M = make(map[string]attributeValue, len(v.M))
		for k, e := range v.M {
			out.M[k] = adminToInternalAttributeValue(e)
		}
	}
	return out
}
