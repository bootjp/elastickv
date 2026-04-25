package adapter

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// fullAdminPrincipal is the canonical "may write" principal used in
// every write-path test below. Defining it once keeps the fixture
// noise out of the assertions and means a future schema change to
// AdminPrincipal touches one spot, not 30.
var fullAdminPrincipal = AdminPrincipal{AccessKey: "AKIA_FULL", Role: AdminRoleFull}
var readOnlyAdminPrincipal = AdminPrincipal{AccessKey: "AKIA_RO", Role: AdminRoleReadOnly}

// TestDynamoDB_AdminListTables_Empty exercises the SigV4-bypass admin
// entrypoint on a server that has no Dynamo tables. The expected shape
// is an empty (non-nil) slice so the admin JSON response stays a valid
// array rather than `null`, matching the design doc 4.3 contract.
func TestDynamoDB_AdminListTables_Empty(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	got, err := nodes[0].dynamoServer.AdminListTables(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestDynamoDB_AdminListTables_Sorted verifies that the admin entrypoint
// returns table names in lexicographic order, matching the listTables
// HTTP handler so the two admin views (SigV4 and bypass) cannot drift.
func TestDynamoDB_AdminListTables_Sorted(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()

	for _, name := range []string{"zeta", "alpha", "mu"} {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   aws.String(name),
			BillingMode: ddbTypes.BillingModePayPerRequest,
			AttributeDefinitions: []ddbTypes.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			},
			KeySchema: []ddbTypes.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: ddbTypes.KeyTypeHash},
			},
		})
		require.NoError(t, err)
	}

	got, err := nodes[0].dynamoServer.AdminListTables(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "mu", "zeta"}, got)
}

// TestDynamoDB_AdminDescribeTable_Missing checks the (nil, false, nil)
// "not found" contract — admin callers must be able to tell a missing
// table apart from a storage error without sniffing sentinels.
func TestDynamoDB_AdminDescribeTable_Missing(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	summary, exists, err := nodes[0].dynamoServer.AdminDescribeTable(context.Background(), "absent")
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, summary)
}

// TestDynamoDB_AdminDescribeTable_Composite covers the simple-key happy
// path: a table with hash + range key and no GSIs. The admin summary
// must mirror the schema's primary key fields exactly.
func TestDynamoDB_AdminDescribeTable_Composite(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("orders"),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("customer"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("orderID"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("customer"), KeyType: ddbTypes.KeyTypeHash},
			{AttributeName: aws.String("orderID"), KeyType: ddbTypes.KeyTypeRange},
		},
	})
	require.NoError(t, err)

	summary, exists, err := nodes[0].dynamoServer.AdminDescribeTable(ctx, "orders")
	require.NoError(t, err)
	require.True(t, exists)
	require.NotNil(t, summary)
	require.Equal(t, "orders", summary.Name)
	require.Equal(t, "customer", summary.PartitionKey)
	require.Equal(t, "orderID", summary.SortKey)
	require.NotZero(t, summary.Generation)
	require.Empty(t, summary.GlobalSecondaryIndexes)
}

// TestDynamoDB_AdminDescribeTable_GSI_SortedDeterministic exercises the
// GSI projection path. Two indexes are added in deliberately reversed
// alphabetical order to confirm summaryFromSchema's Sort.Strings call —
// without it, map iteration order would produce a flaky output.
func TestDynamoDB_AdminDescribeTable_GSI_SortedDeterministic(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("threads"),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("threadId"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("owner"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("createdAt"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("threadId"), KeyType: ddbTypes.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []ddbTypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("zStatusIndex"),
				KeySchema: []ddbTypes.KeySchemaElement{
					{AttributeName: aws.String("status"), KeyType: ddbTypes.KeyTypeHash},
					{AttributeName: aws.String("createdAt"), KeyType: ddbTypes.KeyTypeRange},
				},
				Projection: &ddbTypes.Projection{ProjectionType: ddbTypes.ProjectionTypeAll},
			},
			{
				IndexName: aws.String("aOwnerIndex"),
				KeySchema: []ddbTypes.KeySchemaElement{
					{AttributeName: aws.String("owner"), KeyType: ddbTypes.KeyTypeHash},
				},
				Projection: &ddbTypes.Projection{ProjectionType: ddbTypes.ProjectionTypeKeysOnly},
			},
		},
	})
	require.NoError(t, err)

	summary, exists, err := nodes[0].dynamoServer.AdminDescribeTable(ctx, "threads")
	require.NoError(t, err)
	require.True(t, exists)
	require.NotNil(t, summary)
	require.Equal(t, "threadId", summary.PartitionKey)
	require.Empty(t, summary.SortKey)

	require.Len(t, summary.GlobalSecondaryIndexes, 2)
	// Names sorted lexicographically: "aOwnerIndex" < "zStatusIndex".
	require.Equal(t, "aOwnerIndex", summary.GlobalSecondaryIndexes[0].Name)
	require.Equal(t, "owner", summary.GlobalSecondaryIndexes[0].PartitionKey)
	require.Empty(t, summary.GlobalSecondaryIndexes[0].SortKey)
	require.Equal(t, string(ddbTypes.ProjectionTypeKeysOnly), summary.GlobalSecondaryIndexes[0].ProjectionType)

	require.Equal(t, "zStatusIndex", summary.GlobalSecondaryIndexes[1].Name)
	require.Equal(t, "status", summary.GlobalSecondaryIndexes[1].PartitionKey)
	require.Equal(t, "createdAt", summary.GlobalSecondaryIndexes[1].SortKey)
	require.Equal(t, string(ddbTypes.ProjectionTypeAll), summary.GlobalSecondaryIndexes[1].ProjectionType)
}

// TestDynamoDB_AdminCreateTable_HappyPath confirms the SigV4-bypass
// CreateTable round-trips through the same OperationGroup pipeline as
// the SigV4 path: the resulting table appears under AdminListTables
// and AdminDescribeTable returns the expected key shape.
func TestDynamoDB_AdminCreateTable_HappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	srv := nodes[0].dynamoServer
	ctx := context.Background()

	summary, err := srv.AdminCreateTable(ctx, fullAdminPrincipal, AdminCreateTableInput{
		TableName:    "users",
		PartitionKey: AdminAttribute{Name: "id", Type: "S"},
		SortKey:      &AdminAttribute{Name: "createdAt", Type: "N"},
	})
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, "users", summary.Name)
	require.Equal(t, "id", summary.PartitionKey)
	require.Equal(t, "createdAt", summary.SortKey)
	require.NotZero(t, summary.Generation)

	names, err := srv.AdminListTables(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"users"}, names)

	got, exists, err := srv.AdminDescribeTable(ctx, "users")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, summary.Name, got.Name)
	require.Equal(t, summary.PartitionKey, got.PartitionKey)
	require.Equal(t, summary.SortKey, got.SortKey)
	require.Equal(t, summary.Generation, got.Generation)
}

// TestDynamoDB_AdminCreateTable_WithGSI exercises a multi-GSI shape
// including INCLUDE projection. The two GSIs are added in
// reverse-alphabetical order to confirm the resulting summary's
// deterministic ordering (sort.Strings) carries through.
func TestDynamoDB_AdminCreateTable_WithGSI(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	srv := nodes[0].dynamoServer
	ctx := context.Background()

	summary, err := srv.AdminCreateTable(ctx, fullAdminPrincipal, AdminCreateTableInput{
		TableName:    "messages",
		PartitionKey: AdminAttribute{Name: "threadId", Type: "S"},
		GSI: []AdminCreateGSI{
			{
				Name:           "zStatusIndex",
				PartitionKey:   AdminAttribute{Name: "status", Type: "S"},
				SortKey:        &AdminAttribute{Name: "ts", Type: "N"},
				ProjectionType: "ALL",
			},
			{
				Name:             "aPriorityIndex",
				PartitionKey:     AdminAttribute{Name: "priority", Type: "N"},
				ProjectionType:   "INCLUDE",
				NonKeyAttributes: []string{"author"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, summary.GlobalSecondaryIndexes, 2)
	require.Equal(t, "aPriorityIndex", summary.GlobalSecondaryIndexes[0].Name)
	require.Equal(t, "zStatusIndex", summary.GlobalSecondaryIndexes[1].Name)
	require.Equal(t, "ALL", summary.GlobalSecondaryIndexes[1].ProjectionType)
	require.Equal(t, "INCLUDE", summary.GlobalSecondaryIndexes[0].ProjectionType)
}

// TestDynamoDB_AdminCreateTable_DuplicateRejected covers the
// ResourceInUseException path. The adapter must not silently
// re-create the table; admin handlers map this to 4xx for the SPA.
func TestDynamoDB_AdminCreateTable_DuplicateRejected(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	srv := nodes[0].dynamoServer
	ctx := context.Background()
	in := AdminCreateTableInput{
		TableName:    "dup",
		PartitionKey: AdminAttribute{Name: "id", Type: "S"},
	}
	_, err := srv.AdminCreateTable(ctx, fullAdminPrincipal, in)
	require.NoError(t, err)
	_, err = srv.AdminCreateTable(ctx, fullAdminPrincipal, in)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}

// TestDynamoDB_AdminCreateTable_RoleEnforcedAtAdapter is the design
// doc 3.2 invariant: even if the admin handler somehow let a
// read-only principal through, the adapter would still refuse.
func TestDynamoDB_AdminCreateTable_RoleEnforcedAtAdapter(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	_, err := nodes[0].dynamoServer.AdminCreateTable(context.Background(), readOnlyAdminPrincipal, AdminCreateTableInput{
		TableName:    "ro",
		PartitionKey: AdminAttribute{Name: "id", Type: "S"},
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAdminForbidden))
}

// TestDynamoDB_AdminCreateTable_ValidationErrors covers the cheap
// up-front checks AdminCreateTable does before touching the
// coordinator. These are the codepaths the SPA hits when a user fills
// the form incorrectly; they must surface as validation errors rather
// than 500s.
func TestDynamoDB_AdminCreateTable_ValidationErrors(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	srv := nodes[0].dynamoServer
	ctx := context.Background()

	cases := []struct {
		name string
		in   AdminCreateTableInput
	}{
		{"empty table name", AdminCreateTableInput{TableName: "  ", PartitionKey: AdminAttribute{Name: "id", Type: "S"}}},
		{"missing partition key name", AdminCreateTableInput{TableName: "t", PartitionKey: AdminAttribute{Name: "", Type: "S"}}},
		{"empty GSI name", AdminCreateTableInput{
			TableName:    "t",
			PartitionKey: AdminAttribute{Name: "id", Type: "S"},
			GSI:          []AdminCreateGSI{{Name: ""}},
		}},
		{"GSI partition key missing", AdminCreateTableInput{
			TableName:    "t",
			PartitionKey: AdminAttribute{Name: "id", Type: "S"},
			GSI:          []AdminCreateGSI{{Name: "gsi", PartitionKey: AdminAttribute{Name: ""}}},
		}},
		{"conflicting attribute types across primary and GSI", AdminCreateTableInput{
			TableName:    "t",
			PartitionKey: AdminAttribute{Name: "id", Type: "S"},
			GSI:          []AdminCreateGSI{{Name: "gsi", PartitionKey: AdminAttribute{Name: "id", Type: "N"}, ProjectionType: "ALL"}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := srv.AdminCreateTable(ctx, fullAdminPrincipal, tc.in)
			require.Error(t, err)
		})
	}
}

// TestDynamoDB_AdminDeleteTable_HappyPath checks the round trip:
// create, confirm via list, delete, confirm absence.
func TestDynamoDB_AdminDeleteTable_HappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	srv := nodes[0].dynamoServer
	ctx := context.Background()

	_, err := srv.AdminCreateTable(ctx, fullAdminPrincipal, AdminCreateTableInput{
		TableName:    "to-delete",
		PartitionKey: AdminAttribute{Name: "id", Type: "S"},
	})
	require.NoError(t, err)

	require.NoError(t, srv.AdminDeleteTable(ctx, fullAdminPrincipal, "to-delete"))

	_, exists, err := srv.AdminDescribeTable(ctx, "to-delete")
	require.NoError(t, err)
	require.False(t, exists)
}

// TestDynamoDB_AdminDeleteTable_MissingReturnsResourceNotFound
// confirms the adapter surfaces a structured error (not a generic
// 500) when the table never existed; admin handlers map this to 404.
func TestDynamoDB_AdminDeleteTable_MissingReturnsResourceNotFound(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	err := nodes[0].dynamoServer.AdminDeleteTable(context.Background(), fullAdminPrincipal, "absent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// TestDynamoDB_AdminDeleteTable_ReadOnlyForbidden mirrors the
// CreateTable role test on the delete path; both must enforce.
func TestDynamoDB_AdminDeleteTable_ReadOnlyForbidden(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	err := nodes[0].dynamoServer.AdminDeleteTable(context.Background(), readOnlyAdminPrincipal, "anything")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAdminForbidden))
}

// TestDynamoDB_AdminDeleteTable_EmptyName_Validation confirms the
// up-front guard returns a validation error rather than passing an
// empty name to the coordinator (which would either 500 or, worse,
// match a tombstone-leaning corner case).
func TestDynamoDB_AdminDeleteTable_EmptyName_Validation(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	err := nodes[0].dynamoServer.AdminDeleteTable(context.Background(), fullAdminPrincipal, "  ")
	require.Error(t, err)
}

func newDynamoClient(t *testing.T, address string) *dynamodb.Client {
	t.Helper()
	// Region is intentionally arbitrary here. The test DynamoDB
	// server does not enforce a region match in its SigV4 path —
	// every existing adapter test uses "us-west-2" as a placeholder
	// for the same reason. If the server later starts requiring a
	// specific region, source it from the same constant the server
	// reads instead of hardcoding it on each side independently.
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + address)
	})
}
