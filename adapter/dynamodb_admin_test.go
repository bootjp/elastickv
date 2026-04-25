package adapter

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

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

func newDynamoClient(t *testing.T, address string) *dynamodb.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + address)
	})
}
