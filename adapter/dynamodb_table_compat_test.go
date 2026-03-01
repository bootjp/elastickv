package adapter

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestDynamoDB_TableAPICompatibility(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	threadsTable := "threads"
	messagesTable := "messages"

	createThreadsTable := func(tb testing.TB) {
		tb.Helper()
		_, createErr := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   aws.String(threadsTable),
			BillingMode: ddbTypes.BillingModePayPerRequest,
			AttributeDefinitions: []ddbTypes.AttributeDefinition{
				{AttributeName: aws.String("threadId"), AttributeType: ddbTypes.ScalarAttributeTypeS},
				{AttributeName: aws.String("status"), AttributeType: ddbTypes.ScalarAttributeTypeS},
				{AttributeName: aws.String("createdAt"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			},
			KeySchema: []ddbTypes.KeySchemaElement{
				{AttributeName: aws.String("threadId"), KeyType: ddbTypes.KeyTypeHash},
			},
			GlobalSecondaryIndexes: []ddbTypes.GlobalSecondaryIndex{
				{
					IndexName: aws.String("statusIndex"),
					KeySchema: []ddbTypes.KeySchemaElement{
						{AttributeName: aws.String("status"), KeyType: ddbTypes.KeyTypeHash},
						{AttributeName: aws.String("createdAt"), KeyType: ddbTypes.KeyTypeRange},
					},
					Projection: &ddbTypes.Projection{ProjectionType: ddbTypes.ProjectionTypeAll},
				},
			},
		})
		require.NoError(tb, createErr)
	}

	createMessagesTable := func(tb testing.TB) {
		tb.Helper()
		_, createErr := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   aws.String(messagesTable),
			BillingMode: ddbTypes.BillingModePayPerRequest,
			AttributeDefinitions: []ddbTypes.AttributeDefinition{
				{AttributeName: aws.String("threadId"), AttributeType: ddbTypes.ScalarAttributeTypeS},
				{AttributeName: aws.String("createdAt"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			},
			KeySchema: []ddbTypes.KeySchemaElement{
				{AttributeName: aws.String("threadId"), KeyType: ddbTypes.KeyTypeHash},
				{AttributeName: aws.String("createdAt"), KeyType: ddbTypes.KeyTypeRange},
			},
		})
		require.NoError(tb, createErr)
	}

	putThread := func(tb testing.TB, threadID string, title string, createdAt string, status string, accessToken string) {
		tb.Helper()
		_, putErr := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(threadsTable),
			Item: map[string]ddbTypes.AttributeValue{
				"threadId":    &ddbTypes.AttributeValueMemberS{Value: threadID},
				"title":       &ddbTypes.AttributeValueMemberS{Value: title},
				"createdAt":   &ddbTypes.AttributeValueMemberS{Value: createdAt},
				"status":      &ddbTypes.AttributeValueMemberS{Value: status},
				"accessToken": &ddbTypes.AttributeValueMemberS{Value: accessToken},
			},
		})
		require.NoError(tb, putErr)
	}

	queryThreadsByStatus := func(tb testing.TB, status string, scanIndexForward bool) *dynamodb.QueryOutput {
		tb.Helper()
		out, queryErr := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(threadsTable),
			IndexName:              aws.String("statusIndex"),
			KeyConditionExpression: aws.String("#status = :status"),
			ExpressionAttributeNames: map[string]string{
				"#status": "status",
			},
			ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
				":status": &ddbTypes.AttributeValueMemberS{Value: status},
			},
			ScanIndexForward: aws.Bool(scanIndexForward),
		})
		require.NoError(tb, queryErr)
		return out
	}

	// Shared setup for all sub-tests so each sub-test can run in isolation with -run.
	createThreadsTable(t)
	createMessagesTable(t)

	t.Run("TableLifecycle", func(t *testing.T) {
		listAllOut, listErr := client.ListTables(ctx, &dynamodb.ListTablesInput{})
		require.NoError(t, listErr)
		require.ElementsMatch(t, []string{threadsTable, messagesTable}, listAllOut.TableNames)

		listPageOut, listPageErr := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)})
		require.NoError(t, listPageErr)
		require.Len(t, listPageOut.TableNames, 1)
		require.NotEmpty(t, aws.ToString(listPageOut.LastEvaluatedTableName))

		desc, descErr := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(threadsTable)})
		require.NoError(t, descErr)
		require.NotNil(t, desc.Table)
		require.Equal(t, threadsTable, aws.ToString(desc.Table.TableName))
	})

	t.Run("ThreadItemOperations", func(t *testing.T) {
		putThread(t, "t1", "title1", "2026-01-01T00:00:00Z", "pending", "")
		putThread(t, "t2", "title2", "2026-01-02T00:00:00Z", "pending", "")
		putThread(t, "t3", "title3", "2026-01-03T00:00:00Z", "answered", "")

		getOut, getErr := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(threadsTable),
			Key: map[string]ddbTypes.AttributeValue{
				"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
			},
		})
		require.NoError(t, getErr)
		require.NotNil(t, getOut.Item)
		threadID, ok := getOut.Item["threadId"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		require.Equal(t, "t1", threadID.Value)
		threadStatus, ok := getOut.Item["status"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		require.Equal(t, "pending", threadStatus.Value)
		title, ok := getOut.Item["title"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		require.Equal(t, "title1", title.Value)

		queryThreadsOut := queryThreadsByStatus(t, "pending", false)
		require.Len(t, queryThreadsOut.Items, 2)
		created0, ok := queryThreadsOut.Items[0]["createdAt"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		created1, ok := queryThreadsOut.Items[1]["createdAt"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		require.Equal(t, "2026-01-02T00:00:00Z", created0.Value)
		require.Equal(t, "2026-01-01T00:00:00Z", created1.Value)

		_, updateErr := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(threadsTable),
			Key: map[string]ddbTypes.AttributeValue{
				"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
			},
			UpdateExpression: aws.String("SET #status = :status"),
			ExpressionAttributeNames: map[string]string{
				"#status": "status",
			},
			ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
				":status": &ddbTypes.AttributeValueMemberS{Value: "answered"},
			},
		})
		require.NoError(t, updateErr)

		queryPendingAfterUpdate := queryThreadsByStatus(t, "pending", false)
		require.Len(t, queryPendingAfterUpdate.Items, 1)
		queryAnsweredAfterUpdate := queryThreadsByStatus(t, "answered", false)
		require.Len(t, queryAnsweredAfterUpdate.Items, 2)

		_, updateErr = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(threadsTable),
			Key: map[string]ddbTypes.AttributeValue{
				"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
			},
			UpdateExpression:    aws.String("SET #accessToken = :accessToken"),
			ConditionExpression: aws.String("attribute_exists(#threadId) AND (attribute_not_exists(#accessToken) OR #accessToken = :empty)"),
			ExpressionAttributeNames: map[string]string{
				"#threadId":    "threadId",
				"#accessToken": "accessToken",
			},
			ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
				":accessToken": &ddbTypes.AttributeValueMemberS{Value: "token1"},
				":empty":       &ddbTypes.AttributeValueMemberS{Value: ""},
			},
		})
		require.NoError(t, updateErr)

		_, updateErr = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(threadsTable),
			Key: map[string]ddbTypes.AttributeValue{
				"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
			},
			UpdateExpression:    aws.String("SET #accessToken = :accessToken"),
			ConditionExpression: aws.String("attribute_exists(#threadId) AND (attribute_not_exists(#accessToken) OR #accessToken = :empty)"),
			ExpressionAttributeNames: map[string]string{
				"#threadId":    "threadId",
				"#accessToken": "accessToken",
			},
			ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
				":accessToken": &ddbTypes.AttributeValueMemberS{Value: "token2"},
				":empty":       &ddbTypes.AttributeValueMemberS{Value: ""},
			},
		})
		require.Error(t, updateErr)
		var condErr *ddbTypes.ConditionalCheckFailedException
		require.ErrorAs(t, updateErr, &condErr)

		missingOut, missingErr := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(threadsTable),
			Key: map[string]ddbTypes.AttributeValue{
				"threadId": &ddbTypes.AttributeValueMemberS{Value: "does-not-exist"},
			},
		})
		require.NoError(t, missingErr)
		require.Empty(t, missingOut.Item)
	})

	t.Run("MessagesQuery", func(t *testing.T) {
		_, putErr := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(messagesTable),
			Item: map[string]ddbTypes.AttributeValue{
				"messageId": &ddbTypes.AttributeValueMemberS{Value: "m1"},
				"threadId":  &ddbTypes.AttributeValueMemberS{Value: "t1"},
				"content":   &ddbTypes.AttributeValueMemberS{Value: "hello"},
				"sender":    &ddbTypes.AttributeValueMemberS{Value: "user"},
				"createdAt": &ddbTypes.AttributeValueMemberS{Value: "2026-01-01T00:00:01Z"},
			},
		})
		require.NoError(t, putErr)
		_, putErr = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(messagesTable),
			Item: map[string]ddbTypes.AttributeValue{
				"messageId": &ddbTypes.AttributeValueMemberS{Value: "m2"},
				"threadId":  &ddbTypes.AttributeValueMemberS{Value: "t1"},
				"content":   &ddbTypes.AttributeValueMemberS{Value: "world"},
				"sender":    &ddbTypes.AttributeValueMemberS{Value: "admin"},
				"createdAt": &ddbTypes.AttributeValueMemberS{Value: "2026-01-01T00:00:02Z"},
			},
		})
		require.NoError(t, putErr)

		queryMessagesOut, queryErr := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(messagesTable),
			KeyConditionExpression: aws.String("threadId = :threadId"),
			ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
				":threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
			},
			ScanIndexForward: aws.Bool(true),
		})
		require.NoError(t, queryErr)
		require.Len(t, queryMessagesOut.Items, 2)
		mc0, ok := queryMessagesOut.Items[0]["createdAt"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		mc1, ok := queryMessagesOut.Items[1]["createdAt"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		require.Equal(t, "2026-01-01T00:00:01Z", mc0.Value)
		require.Equal(t, "2026-01-01T00:00:02Z", mc1.Value)
	})

	t.Run("TransactWriteItems", func(t *testing.T) {
		_, txErr := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []ddbTypes.TransactWriteItem{
				{
					Put: &ddbTypes.Put{
						TableName: aws.String(threadsTable),
						Item: map[string]ddbTypes.AttributeValue{
							"threadId":    &ddbTypes.AttributeValueMemberS{Value: "t4"},
							"title":       &ddbTypes.AttributeValueMemberS{Value: "title4"},
							"createdAt":   &ddbTypes.AttributeValueMemberS{Value: "2026-01-04T00:00:00Z"},
							"status":      &ddbTypes.AttributeValueMemberS{Value: "pending"},
							"accessToken": &ddbTypes.AttributeValueMemberS{Value: ""},
						},
					},
				},
			},
		})
		require.NoError(t, txErr)

		queryPendingAfterTransact := queryThreadsByStatus(t, "pending", false)
		require.Len(t, queryPendingAfterTransact.Items, 2)
	})

	t.Run("DeleteTable", func(t *testing.T) {
		_, deleteErr := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(messagesTable)})
		require.NoError(t, deleteErr)
	})
}

func TestDynamoDB_UpdateItem_ConditionOnMissingItemFails(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	threadsTable := "threads_missing_condition"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(threadsTable),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("threadId"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("threadId"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(threadsTable),
		Key: map[string]ddbTypes.AttributeValue{
			"threadId": &ddbTypes.AttributeValueMemberS{Value: "does-not-exist"},
		},
		UpdateExpression:    aws.String("SET #status = :status"),
		ConditionExpression: aws.String("attribute_exists(#threadId)"),
		ExpressionAttributeNames: map[string]string{
			"#status":   "status",
			"#threadId": "threadId",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "answered"},
		},
	})
	require.Error(t, err)
	var condErr *ddbTypes.ConditionalCheckFailedException
	require.ErrorAs(t, err, &condErr)
}

func TestDynamoDB_UpdateItem_ConcurrentNoLostUpdate(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	threadsTable := "threads_concurrent_update"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(threadsTable),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("threadId"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("threadId"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	for i := 0; i < 40; i++ {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(threadsTable),
			Item: map[string]ddbTypes.AttributeValue{
				"threadId":    &ddbTypes.AttributeValueMemberS{Value: "t1"},
				"status":      &ddbTypes.AttributeValueMemberS{Value: "pending"},
				"accessToken": &ddbTypes.AttributeValueMemberS{Value: ""},
			},
		})
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		errCh := make(chan error, 2)
		go func() {
			defer wg.Done()
			_, updateErr := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(threadsTable),
				Key: map[string]ddbTypes.AttributeValue{
					"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
				},
				UpdateExpression: aws.String("SET #status = :status"),
				ExpressionAttributeNames: map[string]string{
					"#status": "status",
				},
				ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
					":status": &ddbTypes.AttributeValueMemberS{Value: "answered"},
				},
			})
			errCh <- updateErr
		}()
		go func() {
			defer wg.Done()
			_, updateErr := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(threadsTable),
				Key: map[string]ddbTypes.AttributeValue{
					"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
				},
				UpdateExpression: aws.String("SET #accessToken = :token"),
				ExpressionAttributeNames: map[string]string{
					"#accessToken": "accessToken",
				},
				ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
					":token": &ddbTypes.AttributeValueMemberS{Value: "claimed"},
				},
			})
			errCh <- updateErr
		}()
		wg.Wait()
		close(errCh)
		for updateErr := range errCh {
			require.NoError(t, updateErr)
		}

		out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(threadsTable),
			Key: map[string]ddbTypes.AttributeValue{
				"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
			},
		})
		require.NoError(t, err)
		status, ok := out.Item["status"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		token, ok := out.Item["accessToken"].(*ddbTypes.AttributeValueMemberS)
		require.True(t, ok)
		require.Equal(t, "answered", status.Value)
		require.Equal(t, "claimed", token.Value)
	}
}

func TestDynamoDB_DeleteTable_RecreateNoGhostData(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "messages_ghost"
	createMessagesTable := func() {
		_, createErr := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   aws.String(table),
			BillingMode: ddbTypes.BillingModePayPerRequest,
			AttributeDefinitions: []ddbTypes.AttributeDefinition{
				{AttributeName: aws.String("threadId"), AttributeType: ddbTypes.ScalarAttributeTypeS},
				{AttributeName: aws.String("createdAt"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			},
			KeySchema: []ddbTypes.KeySchemaElement{
				{AttributeName: aws.String("threadId"), KeyType: ddbTypes.KeyTypeHash},
				{AttributeName: aws.String("createdAt"), KeyType: ddbTypes.KeyTypeRange},
			},
		})
		require.NoError(t, createErr)
	}

	createMessagesTable()
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"threadId":  &ddbTypes.AttributeValueMemberS{Value: "t1"},
			"createdAt": &ddbTypes.AttributeValueMemberS{Value: "2026-01-01T00:00:00Z"},
			"content":   &ddbTypes.AttributeValueMemberS{Value: "before-delete"},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	require.NoError(t, err)
	createMessagesTable()

	queryOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("threadId = :threadId"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, queryOut.Items, 0)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"threadId":  &ddbTypes.AttributeValueMemberS{Value: "t1"},
			"createdAt": &ddbTypes.AttributeValueMemberS{Value: "2026-01-01T00:00:01Z"},
			"content":   &ddbTypes.AttributeValueMemberS{Value: "after-recreate"},
		},
	})
	require.NoError(t, err)
	queryOut, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("threadId = :threadId"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, queryOut.Items, 1)
	content, ok := queryOut.Items[0]["content"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "after-recreate", content.Value)
}

func TestDynamoDB_UpdateItem_PrimaryKeyMutationRejected(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "threads_key_mutation"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("threadId"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("threadId"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
			"status":   &ddbTypes.AttributeValueMemberS{Value: "pending"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
		},
		UpdateExpression: aws.String("SET #threadId = :next"),
		ExpressionAttributeNames: map[string]string{
			"#threadId": "threadId",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":next": &ddbTypes.AttributeValueMemberS{Value: "t2"},
		},
	})
	require.Error(t, err)

	oldOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, oldOut.Item)
	newOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"threadId": &ddbTypes.AttributeValueMemberS{Value: "t2"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, newOut.Item)
}

func TestDynamoDB_Query_NumberRangeKeySortsNumerically(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "numbers_sort"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("seq"), AttributeType: ddbTypes.ScalarAttributeTypeN},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbTypes.KeyTypeHash},
			{AttributeName: aws.String("seq"), KeyType: ddbTypes.KeyTypeRange},
		},
	})
	require.NoError(t, err)

	for _, n := range []string{"10", "2"} {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]ddbTypes.AttributeValue{
				"pk":  &ddbTypes.AttributeValueMemberS{Value: "a"},
				"seq": &ddbTypes.AttributeValueMemberN{Value: n},
			},
		})
		require.NoError(t, err)
	}

	asc, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk": &ddbTypes.AttributeValueMemberS{Value: "a"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, asc.Items, 2)
	asc0, ok := asc.Items[0]["seq"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	asc1, ok := asc.Items[1]["seq"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "2", asc0.Value)
	require.Equal(t, "10", asc1.Value)

	desc, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk": &ddbTypes.AttributeValueMemberS{Value: "a"},
		},
		ScanIndexForward: aws.Bool(false),
	})
	require.NoError(t, err)
	require.Len(t, desc.Items, 2)
	desc0, ok := desc.Items[0]["seq"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	desc1, ok := desc.Items[1]["seq"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "10", desc0.Value)
	require.Equal(t, "2", desc1.Value)
}

func TestDynamoDB_UpdateItem_OverlappingExpressionNames(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "overlap_names"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"id":     &ddbTypes.AttributeValueMemberS{Value: "1"},
			"a":      &ddbTypes.AttributeValueMemberS{Value: "A"},
			"ab":     &ddbTypes.AttributeValueMemberS{Value: "B"},
			"status": &ddbTypes.AttributeValueMemberS{Value: "pending"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression:    aws.String("SET #s = :answered"),
		ConditionExpression: aws.String("#ab = :ab AND #a = :a"),
		ExpressionAttributeNames: map[string]string{
			"#s":  "status",
			"#a":  "a",
			"#ab": "ab",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":answered": &ddbTypes.AttributeValueMemberS{Value: "answered"},
			":a":        &ddbTypes.AttributeValueMemberS{Value: "A"},
			":ab":       &ddbTypes.AttributeValueMemberS{Value: "B"},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	status, ok := out.Item["status"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "answered", status.Value)
}

func TestDynamoDB_Query_RangeKeyConditions(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "query_range_conditions"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbTypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbTypes.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []ddbTypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("statusIndex"),
				KeySchema: []ddbTypes.KeySchemaElement{
					{AttributeName: aws.String("status"), KeyType: ddbTypes.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: ddbTypes.KeyTypeRange},
				},
				Projection: &ddbTypes.Projection{ProjectionType: ddbTypes.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	put := func(pk, sk, status string) {
		_, putErr := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]ddbTypes.AttributeValue{
				"pk":     &ddbTypes.AttributeValueMemberS{Value: pk},
				"sk":     &ddbTypes.AttributeValueMemberS{Value: sk},
				"status": &ddbTypes.AttributeValueMemberS{Value: status},
			},
		})
		require.NoError(t, putErr)
	}
	put("u1", "2026-01-01", "pending")
	put("u1", "2026-01-02", "pending")
	put("u1", "2026-02-01", "pending")
	put("u2", "2026-01-03", "pending")

	geOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk AND sk >= :from"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk":   &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":from": &ddbTypes.AttributeValueMemberS{Value: "2026-01-02"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, geOut.Items, 2)

	betweenOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk AND sk BETWEEN :from AND :to"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk":   &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":from": &ddbTypes.AttributeValueMemberS{Value: "2026-01-01"},
			":to":   &ddbTypes.AttributeValueMemberS{Value: "2026-01-31"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, betweenOut.Items, 2)

	beginsWithOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk":     &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":prefix": &ddbTypes.AttributeValueMemberS{Value: "2026-01"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, beginsWithOut.Items, 2)

	gsiOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String("statusIndex"),
		KeyConditionExpression: aws.String("status = :status AND sk BETWEEN :from AND :to"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "pending"},
			":from":   &ddbTypes.AttributeValueMemberS{Value: "2026-01-01"},
			":to":     &ddbTypes.AttributeValueMemberS{Value: "2026-01-31"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, gsiOut.Items, 3)
}

func TestDynamoDB_UpdateItem_MultipleActions(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "update_multiple_actions"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"id":      &ddbTypes.AttributeValueMemberS{Value: "1"},
			"counter": &ddbTypes.AttributeValueMemberN{Value: "1"},
			"a":       &ddbTypes.AttributeValueMemberS{Value: "old-a"},
			"b":       &ddbTypes.AttributeValueMemberS{Value: "remove-me"},
			"legacy":  &ddbTypes.AttributeValueMemberS{Value: "delete-me"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("SET #a = :a, #c = :c ADD #counter :inc REMOVE #b DELETE #legacy"),
		ExpressionAttributeNames: map[string]string{
			"#a":       "a",
			"#c":       "c",
			"#counter": "counter",
			"#b":       "b",
			"#legacy":  "legacy",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":a":   &ddbTypes.AttributeValueMemberS{Value: "new-a"},
			":c":   &ddbTypes.AttributeValueMemberS{Value: "new-c"},
			":inc": &ddbTypes.AttributeValueMemberN{Value: "2"},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Item)

	counter, ok := out.Item["counter"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "3", counter.Value)
	_, hasB := out.Item["b"]
	require.False(t, hasB)
	_, hasLegacy := out.Item["legacy"]
	require.False(t, hasLegacy)
	_, hasC := out.Item["c"]
	require.True(t, hasC)
}

func TestDynamoDB_TransactWriteItems_MixedActions(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "transact_mixed_actions"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"id":    &ddbTypes.AttributeValueMemberS{Value: "1"},
			"value": &ddbTypes.AttributeValueMemberS{Value: "old"},
		},
	})
	require.NoError(t, err)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"id":    &ddbTypes.AttributeValueMemberS{Value: "2"},
			"value": &ddbTypes.AttributeValueMemberS{Value: "keep"},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbTypes.TransactWriteItem{
			{
				ConditionCheck: &ddbTypes.ConditionCheck{
					TableName: aws.String(table),
					Key: map[string]ddbTypes.AttributeValue{
						"id": &ddbTypes.AttributeValueMemberS{Value: "2"},
					},
					ConditionExpression: aws.String("attribute_exists(#id)"),
					ExpressionAttributeNames: map[string]string{
						"#id": "id",
					},
				},
			},
			{
				Update: &ddbTypes.Update{
					TableName: aws.String(table),
					Key: map[string]ddbTypes.AttributeValue{
						"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
					},
					UpdateExpression: aws.String("SET #value = :next"),
					ExpressionAttributeNames: map[string]string{
						"#value": "value",
					},
					ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
						":next": &ddbTypes.AttributeValueMemberS{Value: "updated"},
					},
				},
			},
			{
				Delete: &ddbTypes.Delete{
					TableName: aws.String(table),
					Key: map[string]ddbTypes.AttributeValue{
						"id": &ddbTypes.AttributeValueMemberS{Value: "2"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out1, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	value1, ok := out1.Item["value"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "updated", value1.Value)

	out2, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "2"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, out2.Item)
}

func TestDynamoDB_Query_RangeKeyComparisonOperators(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "query_range_compare_ops"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("seq"), AttributeType: ddbTypes.ScalarAttributeTypeN},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbTypes.KeyTypeHash},
			{AttributeName: aws.String("seq"), KeyType: ddbTypes.KeyTypeRange},
		},
	})
	require.NoError(t, err)

	for _, seq := range []string{"1", "2", "3"} {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]ddbTypes.AttributeValue{
				"pk":  &ddbTypes.AttributeValueMemberS{Value: "u1"},
				"seq": &ddbTypes.AttributeValueMemberN{Value: seq},
			},
		})
		require.NoError(t, err)
	}

	run := func(expr string, values map[string]ddbTypes.AttributeValue) *dynamodb.QueryOutput {
		out, qErr := client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(table),
			KeyConditionExpression:    aws.String(expr),
			ExpressionAttributeValues: values,
			ScanIndexForward:          aws.Bool(true),
		})
		require.NoError(t, qErr)
		return out
	}

	ltOut := run(
		"pk = :pk AND seq < :v",
		map[string]ddbTypes.AttributeValue{
			":pk": &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":v":  &ddbTypes.AttributeValueMemberN{Value: "2"},
		},
	)
	require.Len(t, ltOut.Items, 1)
	ltSeq, ok := ltOut.Items[0]["seq"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "1", ltSeq.Value)

	leOut := run(
		"pk = :pk AND seq <= :v",
		map[string]ddbTypes.AttributeValue{
			":pk": &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":v":  &ddbTypes.AttributeValueMemberN{Value: "2"},
		},
	)
	require.Len(t, leOut.Items, 2)

	gtOut := run(
		"pk = :pk AND seq > :v",
		map[string]ddbTypes.AttributeValue{
			":pk": &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":v":  &ddbTypes.AttributeValueMemberN{Value: "2"},
		},
	)
	require.Len(t, gtOut.Items, 1)
	gtSeq, ok := gtOut.Items[0]["seq"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "3", gtSeq.Value)

	eqOut := run(
		"pk = :pk AND seq = :v",
		map[string]ddbTypes.AttributeValue{
			":pk": &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":v":  &ddbTypes.AttributeValueMemberN{Value: "2"},
		},
	)
	require.Len(t, eqOut.Items, 1)
	eqSeq, ok := eqOut.Items[0]["seq"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "2", eqSeq.Value)
}

func TestDynamoDB_TransactWriteItems_ConditionFailureRollsBack(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "transact_conditional_rollback"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"id":    &ddbTypes.AttributeValueMemberS{Value: "1"},
			"value": &ddbTypes.AttributeValueMemberS{Value: "old"},
		},
	})
	require.NoError(t, err)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"id":    &ddbTypes.AttributeValueMemberS{Value: "2"},
			"guard": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbTypes.TransactWriteItem{
			{
				Update: &ddbTypes.Update{
					TableName: aws.String(table),
					Key: map[string]ddbTypes.AttributeValue{
						"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
					},
					UpdateExpression: aws.String("SET #value = :next"),
					ExpressionAttributeNames: map[string]string{
						"#value": "value",
					},
					ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
						":next": &ddbTypes.AttributeValueMemberS{Value: "updated"},
					},
				},
			},
			{
				ConditionCheck: &ddbTypes.ConditionCheck{
					TableName: aws.String(table),
					Key: map[string]ddbTypes.AttributeValue{
						"id": &ddbTypes.AttributeValueMemberS{Value: "2"},
					},
					ConditionExpression: aws.String("#guard = :closed"),
					ExpressionAttributeNames: map[string]string{
						"#guard": "guard",
					},
					ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
						":closed": &ddbTypes.AttributeValueMemberS{Value: "closed"},
					},
				},
			},
			{
				Delete: &ddbTypes.Delete{
					TableName: aws.String(table),
					Key: map[string]ddbTypes.AttributeValue{
						"id": &ddbTypes.AttributeValueMemberS{Value: "2"},
					},
				},
			},
		},
	})
	require.Error(t, err)

	out1, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	value1, ok := out1.Item["value"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "old", value1.Value)

	out2, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "2"},
		},
	})
	require.NoError(t, err)
	guard, ok := out2.Item["guard"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "open", guard.Value)
}

func TestDynamoDB_UpdateItem_AddDecimalAndNegativeNumber(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})

	ctx := context.Background()
	table := "update_add_decimal"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbTypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbTypes.KeyTypeHash},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"id":      &ddbTypes.AttributeValueMemberS{Value: "1"},
			"counter": &ddbTypes.AttributeValueMemberN{Value: "1.5"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
		UpdateExpression: aws.String("ADD #counter :delta"),
		ExpressionAttributeNames: map[string]string{
			"#counter": "counter",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":delta": &ddbTypes.AttributeValueMemberN{Value: "-0.4"},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbTypes.AttributeValue{
			"id": &ddbTypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	counter, ok := out.Item["counter"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "1.1", counter.Value)
}
