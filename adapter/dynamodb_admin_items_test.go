package adapter

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// stringAttr / numberAttr / boolAttr build admin attribute values
// inline so tests stay legible. The pointer-to-string / pointer-to-bool
// pattern matches the underlying wire shape.
func stringAttr(s string) AdminAttributeValue {
	return AdminAttributeValue{S: &s}
}

func numberAttr(n int) AdminAttributeValue {
	s := strconv.Itoa(n)
	return AdminAttributeValue{N: &s}
}

func boolAttr(b bool) AdminAttributeValue {
	return AdminAttributeValue{BOOL: &b}
}

// createTableForItemTests sets up a hash-only table named "items"
// with an "id" partition key so the per-test setup stays one line.
func createTableForItemTests(t *testing.T, srv *DynamoDBServer, name string) {
	t.Helper()
	_, err := srv.AdminCreateTable(context.Background(), fullAdminPrincipal, AdminCreateTableInput{
		TableName:    name,
		PartitionKey: AdminAttribute{Name: "id", Type: "S"},
	})
	require.NoError(t, err)
}

// TestDynamoDB_AdminPutItem_HappyPath pins the basic write contract:
// PutItem followed by GetItem returns the stored attributes.
func TestDynamoDB_AdminPutItem_HappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	ctx := context.Background()
	createTableForItemTests(t, srv, "puttable")

	item := AdminItem{Attributes: map[string]AdminAttributeValue{
		"id":     stringAttr("alpha"),
		"name":   stringAttr("Alice"),
		"active": boolAttr(true),
		"age":    numberAttr(42),
	}}
	require.NoError(t, srv.AdminPutItem(ctx, fullAdminPrincipal, "puttable", item))

	got, exists, err := srv.AdminGetItem(ctx, fullAdminPrincipal, "puttable", map[string]AdminAttributeValue{"id": stringAttr("alpha")})
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "Alice", *got.Attributes["name"].S)
	require.True(t, *got.Attributes["active"].BOOL)
	require.Equal(t, "42", *got.Attributes["age"].N)
}

// TestDynamoDB_AdminGetItem_Missing returns (nil, false, nil) — not
// an error — so the admin handler can render 404 without sniffing
// sentinels.
func TestDynamoDB_AdminGetItem_Missing(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	createTableForItemTests(t, srv, "absent")

	got, exists, err := srv.AdminGetItem(context.Background(), fullAdminPrincipal, "absent", map[string]AdminAttributeValue{"id": stringAttr("no-such")})
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, got)
}

// TestDynamoDB_AdminGetItem_MissingTable surfaces the structured
// ErrAdminDynamoNotFound sentinel rather than a generic 500.
func TestDynamoDB_AdminGetItem_MissingTable(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	_, _, err := nodes[0].dynamoServer.AdminGetItem(context.Background(), fullAdminPrincipal, "nope", map[string]AdminAttributeValue{"id": stringAttr("k")})
	require.True(t, errors.Is(err, ErrAdminDynamoNotFound))
}

// TestDynamoDB_AdminDeleteItem_HappyPath confirms a delete makes
// the subsequent get return (nil, false, nil).
func TestDynamoDB_AdminDeleteItem_HappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	ctx := context.Background()
	createTableForItemTests(t, srv, "deltable")

	key := map[string]AdminAttributeValue{"id": stringAttr("k1")}
	require.NoError(t, srv.AdminPutItem(ctx, fullAdminPrincipal, "deltable", AdminItem{Attributes: key}))
	require.NoError(t, srv.AdminDeleteItem(ctx, fullAdminPrincipal, "deltable", key))

	_, exists, err := srv.AdminGetItem(ctx, fullAdminPrincipal, "deltable", key)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestDynamoDB_AdminScanTable_HappyPath puts three items and
// confirms scan returns all three. Order is not asserted (scan
// order is not part of the contract).
func TestDynamoDB_AdminScanTable_HappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	ctx := context.Background()
	createTableForItemTests(t, srv, "scantable")

	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, srv.AdminPutItem(ctx, fullAdminPrincipal, "scantable",
			AdminItem{Attributes: map[string]AdminAttributeValue{"id": stringAttr(id)}}))
	}

	res, err := srv.AdminScanTable(ctx, fullAdminPrincipal, "scantable", AdminScanOptions{})
	require.NoError(t, err)
	require.Len(t, res.Items, 3)
	seen := map[string]bool{}
	for _, it := range res.Items {
		require.NotNil(t, it.Attributes["id"].S)
		seen[*it.Attributes["id"].S] = true
	}
	require.True(t, seen["a"] && seen["b"] && seen["c"], "scan did not surface all 3 ids: %v", seen)
}

// TestDynamoDB_AdminScanTable_LimitClamping pins the [1, 100] clamp
// + default-on-zero behaviour. The default (25) is exercised
// implicitly by the happy-path test above; here we focus on the
// over-cap branch.
func TestDynamoDB_AdminScanTable_LimitClamping(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	ctx := context.Background()
	createTableForItemTests(t, srv, "limittable")

	for i := range 5 {
		require.NoError(t, srv.AdminPutItem(ctx, fullAdminPrincipal, "limittable",
			AdminItem{Attributes: map[string]AdminAttributeValue{"id": stringAttr("row-" + strconv.Itoa(i))}}))
	}

	// Limit=2 — under the cap, exact match.
	res, err := srv.AdminScanTable(ctx, fullAdminPrincipal, "limittable", AdminScanOptions{Limit: 2})
	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.NotNil(t, res.LastEvaluatedKey, "Limit < total: must return continuation key")

	// Limit=500 — clamped down to 100 (well above the 5 items in the table).
	res, err = srv.AdminScanTable(ctx, fullAdminPrincipal, "limittable", AdminScanOptions{Limit: 500})
	require.NoError(t, err)
	require.Len(t, res.Items, 5)
}

// TestDynamoDB_AdminScanTable_CursorRoundTrip drains the table
// across two pages and confirms the union covers every item
// exactly once.
func TestDynamoDB_AdminScanTable_CursorRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	ctx := context.Background()
	createTableForItemTests(t, srv, "cursortable")

	const sent = 7
	for i := range sent {
		require.NoError(t, srv.AdminPutItem(ctx, fullAdminPrincipal, "cursortable",
			AdminItem{Attributes: map[string]AdminAttributeValue{"id": stringAttr("k" + strconv.Itoa(i))}}))
	}

	pageA, err := srv.AdminScanTable(ctx, fullAdminPrincipal, "cursortable", AdminScanOptions{Limit: 3})
	require.NoError(t, err)
	require.Len(t, pageA.Items, 3)
	require.NotNil(t, pageA.LastEvaluatedKey)

	pageB, err := srv.AdminScanTable(ctx, fullAdminPrincipal, "cursortable", AdminScanOptions{Limit: 10, ExclusiveStart: pageA.LastEvaluatedKey})
	require.NoError(t, err)
	require.Len(t, pageB.Items, sent-3)
	require.Nil(t, pageB.LastEvaluatedKey, "second page drains the table")

	seen := map[string]bool{}
	for _, it := range append(pageA.Items, pageB.Items...) {
		require.NotNil(t, it.Attributes["id"].S)
		seen[*it.Attributes["id"].S] = true
	}
	require.Len(t, seen, sent)
}

// TestDynamoDB_AdminPutItem_ReadOnlyForbidden pins the write gate.
func TestDynamoDB_AdminPutItem_ReadOnlyForbidden(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	createTableForItemTests(t, srv, "rotable")

	err := srv.AdminPutItem(context.Background(), readOnlyAdminPrincipal, "rotable",
		AdminItem{Attributes: map[string]AdminAttributeValue{"id": stringAttr("x")}})
	require.True(t, errors.Is(err, ErrAdminForbidden))
}

// TestDynamoDB_AdminDeleteItem_ReadOnlyForbidden mirrors the put-side gate.
func TestDynamoDB_AdminDeleteItem_ReadOnlyForbidden(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	createTableForItemTests(t, srv, "rodel")

	err := srv.AdminDeleteItem(context.Background(), readOnlyAdminPrincipal, "rodel",
		map[string]AdminAttributeValue{"id": stringAttr("x")})
	require.True(t, errors.Is(err, ErrAdminForbidden))
}

// TestDynamoDB_AdminScanTable_NoRoleForbidden pins the lower-bound
// gate (zero-value AdminPrincipal denied even for read).
func TestDynamoDB_AdminScanTable_NoRoleForbidden(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	createTableForItemTests(t, srv, "noauth")

	_, err := srv.AdminScanTable(context.Background(), AdminPrincipal{}, "noauth", AdminScanOptions{})
	require.True(t, errors.Is(err, ErrAdminForbidden))
}

// TestDynamoDB_AdminPutItem_EmptyTableName pins the up-front
// validation guard.
func TestDynamoDB_AdminPutItem_EmptyTableName(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer

	err := srv.AdminPutItem(context.Background(), fullAdminPrincipal, "  ",
		AdminItem{Attributes: map[string]AdminAttributeValue{"id": stringAttr("x")}})
	require.True(t, errors.Is(err, ErrAdminDynamoValidation))
}

// TestDynamoDB_AdminGetItem_EmptyKey pins the up-front validation guard.
func TestDynamoDB_AdminGetItem_EmptyKey(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	createTableForItemTests(t, srv, "emptykey")

	_, _, err := srv.AdminGetItem(context.Background(), fullAdminPrincipal, "emptykey", nil)
	require.True(t, errors.Is(err, ErrAdminDynamoValidation))
}

// TestClampAdminScanLimit pins the truth table directly.
func TestClampAdminScanLimit(t *testing.T) {
	t.Parallel()
	cases := map[int]int32{
		0:                         adminItemScanDefaultLimit,
		-5:                        adminItemScanDefaultLimit,
		1:                         1,
		adminItemScanDefaultLimit: adminItemScanDefaultLimit,
		adminItemScanMaxLimit:     adminItemScanMaxLimit,
		adminItemScanMaxLimit + 1: adminItemScanMaxLimit,
		1_000_000:                 adminItemScanMaxLimit,
	}
	for in, want := range cases {
		require.Equalf(t, want, clampAdminScanLimit(in), "clampAdminScanLimit(%d)", in)
	}
}

// TestDynamoDB_AdminScanTable_EmptyTableReturnsEmptySlice pins
// Claude r1 low: a freshly-created empty table must surface as
// "items": [] on the wire (not "items": null) so the SPA's
// items.map() call does not crash.
func TestDynamoDB_AdminScanTable_EmptyTableReturnsEmptySlice(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	createTableForItemTests(t, srv, "empty")

	res, err := srv.AdminScanTable(context.Background(), fullAdminPrincipal, "empty", AdminScanOptions{})
	require.NoError(t, err)
	require.NotNil(t, res.Items, "empty-table scan must return [] not nil so JSON emits [] not null")
	require.Len(t, res.Items, 0)
}

// TestDynamoDB_AdminPutItem_MissingTable confirms the write path
// surfaces ErrAdminDynamoNotFound — the test was missing in r1
// (Claude bot finding 3).
func TestDynamoDB_AdminPutItem_MissingTable(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer

	err := srv.AdminPutItem(context.Background(), fullAdminPrincipal, "ghost",
		AdminItem{Attributes: map[string]AdminAttributeValue{"id": stringAttr("x")}})
	require.True(t, errors.Is(err, ErrAdminDynamoNotFound),
		"want ErrAdminDynamoNotFound; got %v", err)
}

// TestDynamoDB_AdminDeleteItem_MissingTable mirrors the put-side test.
func TestDynamoDB_AdminDeleteItem_MissingTable(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer

	err := srv.AdminDeleteItem(context.Background(), fullAdminPrincipal, "ghost",
		map[string]AdminAttributeValue{"id": stringAttr("x")})
	require.True(t, errors.Is(err, ErrAdminDynamoNotFound),
		"want ErrAdminDynamoNotFound; got %v", err)
}

// TestDynamoDB_AdminGetItem_MalformedKey pins Codex r1 P2: a key
// that fails schema validation (e.g. wrong attribute name) must
// surface as ErrAdminDynamoValidation rather than an opaque error
// the admin handler would render as 500.
func TestDynamoDB_AdminGetItem_MalformedKey(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	srv := nodes[0].dynamoServer
	ctx := context.Background()
	createTableForItemTests(t, srv, "malformed")

	// Wrong attribute name: schema expects "id" (string), but we pass "wrongkey".
	_, _, err := srv.AdminGetItem(ctx, fullAdminPrincipal, "malformed",
		map[string]AdminAttributeValue{"wrongkey": stringAttr("x")})
	require.True(t, errors.Is(err, ErrAdminDynamoValidation),
		"want ErrAdminDynamoValidation for wrong key shape; got %v", err)
}

// TestAdminAttributeValue_NestedRoundTrip pins the recursive L / M
// conversion: a deeply nested admin value round-trips through the
// internal type without losing any field.
func TestAdminAttributeValue_NestedRoundTrip(t *testing.T) {
	t.Parallel()
	s := "inner"
	in := AdminAttributeValue{
		M: map[string]AdminAttributeValue{
			"list": {L: []AdminAttributeValue{
				{S: &s},
				{N: func() *string { v := "7"; return &v }()},
				{M: map[string]AdminAttributeValue{"nested": {BOOL: func() *bool { b := true; return &b }()}}},
			}},
		},
	}
	internal := adminToInternalAttributeValue(in)
	roundTripped := internalToAdminAttributeValue(internal)
	require.Equal(t, "inner", *roundTripped.M["list"].L[0].S)
	require.Equal(t, "7", *roundTripped.M["list"].L[1].N)
	require.True(t, *roundTripped.M["list"].L[2].M["nested"].BOOL)
}
