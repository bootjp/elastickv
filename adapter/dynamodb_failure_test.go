package adapter

import (
	"context"
	"fmt"
	"testing"
	"time"

	raftadminpb "github.com/Jille/raftadmin/proto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDynamoDB_QueryAndScan_SurviveFollowerIsolationAndLeaderTransfer(t *testing.T) {
	t.Parallel()

	const (
		waitTimeout  = 12 * time.Second
		waitInterval = 100 * time.Millisecond
		tableName    = "events"
	)

	ctx := context.Background()
	nodes, servers := setupAddVoterJoinPathNodes(t, ctx)
	t.Cleanup(func() {
		shutdown(nodes)
		servers.AwaitNoError(t, waitTimeout)
	})

	waitForNodeListeners(t, ctx, nodes, waitTimeout, waitInterval)
	require.Eventually(t, func() bool {
		return nodes[0].raft.State() == raft.Leader
	}, waitTimeout, waitInterval)

	adminConn, err := grpc.NewClient(nodes[0].grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminConn.Close() })
	admin := raftadminpb.NewRaftAdminClient(adminConn)

	addVotersAndAwait(t, ctx, 2*time.Second, admin, nodes, []int{1, 2})
	waitForConfigReplication(t, expectedVoterConfig(nodes), nodes, waitTimeout, waitInterval)
	waitForRaftReadiness(t, nodes, waitTimeout, waitInterval)

	client0 := newDynamoTestClient(t, nodes[0].dynamoAddress)
	client1 := newDynamoTestClient(t, nodes[1].dynamoAddress)

	createCompositeKeyTable(t, ctx, client0, tableName)
	putCompositeItems(t, ctx, client0, tableName, 2)

	require.Eventually(t, func() bool {
		return queryPartitionCount(ctx, client1, tableName, "tenant") == 2 &&
			scanTableCount(ctx, client1, tableName) == 2
	}, waitTimeout, waitInterval)

	require.NoError(t, nodes[2].tm.Close())
	nodes[2].tm = nil

	putCompositeItems(t, ctx, client0, tableName, 3)
	require.Eventually(t, func() bool {
		return queryPartitionCount(ctx, client1, tableName, "tenant") == 3 &&
			scanTableCount(ctx, client1, tableName) == 3
	}, waitTimeout, waitInterval)

	require.NoError(t, nodes[0].raft.LeadershipTransferToServer(
		raft.ServerID("1"),
		raft.ServerAddress(nodes[1].raftAddress),
	).Error())
	require.Eventually(t, func() bool {
		return nodes[1].raft.State() == raft.Leader
	}, waitTimeout, waitInterval)

	putCompositeItems(t, ctx, client1, tableName, 4)
	require.Eventually(t, func() bool {
		return queryPartitionCount(ctx, client0, tableName, "tenant") == 4 &&
			scanTableCount(ctx, client0, tableName) == 4
	}, waitTimeout, waitInterval)
}

func createCompositeKeyTable(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string) {
	t.Helper()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbTypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbTypes.KeyTypeRange},
		},
	})
	require.NoError(t, err)
}

func putCompositeItems(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string, total int) {
	t.Helper()

	for i := 0; i < total; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]ddbTypes.AttributeValue{
				"pk":      &ddbTypes.AttributeValueMemberS{Value: "tenant"},
				"sk":      &ddbTypes.AttributeValueMemberS{Value: fmt.Sprintf("item-%02d", i)},
				"payload": &ddbTypes.AttributeValueMemberS{Value: fmt.Sprintf("value-%02d", i)},
			},
		})
		require.NoError(t, err)
	}
}

func queryPartitionCount(ctx context.Context, client *dynamodb.Client, tableName string, pk string) int {
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk": &ddbTypes.AttributeValueMemberS{Value: pk},
		},
	})
	if err != nil {
		return -1
	}
	return int(out.Count)
}

func scanTableCount(ctx context.Context, client *dynamodb.Client, tableName string) int {
	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return -1
	}
	return int(out.Count)
}
