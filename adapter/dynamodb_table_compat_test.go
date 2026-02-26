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

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
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
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
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
	require.NoError(t, err)

	listAllOut, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{threadsTable, messagesTable}, listAllOut.TableNames)
	listPageOut, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, listPageOut.TableNames, 1)
	require.NotEmpty(t, aws.ToString(listPageOut.LastEvaluatedTableName))
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(threadsTable)})
	require.NoError(t, err)
	require.NotNil(t, desc.Table)
	require.Equal(t, threadsTable, aws.ToString(desc.Table.TableName))

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(threadsTable),
		Item: map[string]ddbTypes.AttributeValue{
			"threadId":    &ddbTypes.AttributeValueMemberS{Value: "t1"},
			"title":       &ddbTypes.AttributeValueMemberS{Value: "title1"},
			"createdAt":   &ddbTypes.AttributeValueMemberS{Value: "2026-01-01T00:00:00Z"},
			"status":      &ddbTypes.AttributeValueMemberS{Value: "pending"},
			"accessToken": &ddbTypes.AttributeValueMemberS{Value: ""},
		},
	})
	require.NoError(t, err)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(threadsTable),
		Item: map[string]ddbTypes.AttributeValue{
			"threadId":    &ddbTypes.AttributeValueMemberS{Value: "t2"},
			"title":       &ddbTypes.AttributeValueMemberS{Value: "title2"},
			"createdAt":   &ddbTypes.AttributeValueMemberS{Value: "2026-01-02T00:00:00Z"},
			"status":      &ddbTypes.AttributeValueMemberS{Value: "pending"},
			"accessToken": &ddbTypes.AttributeValueMemberS{Value: ""},
		},
	})
	require.NoError(t, err)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(threadsTable),
		Item: map[string]ddbTypes.AttributeValue{
			"threadId":    &ddbTypes.AttributeValueMemberS{Value: "t3"},
			"title":       &ddbTypes.AttributeValueMemberS{Value: "title3"},
			"createdAt":   &ddbTypes.AttributeValueMemberS{Value: "2026-01-03T00:00:00Z"},
			"status":      &ddbTypes.AttributeValueMemberS{Value: "answered"},
			"accessToken": &ddbTypes.AttributeValueMemberS{Value: ""},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(threadsTable),
		Key: map[string]ddbTypes.AttributeValue{
			"threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)
	threadID, ok := getOut.Item["threadId"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "t1", threadID.Value)
	status, ok := getOut.Item["status"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "pending", status.Value)
	title, ok := getOut.Item["title"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "title1", title.Value)

	queryThreadsOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(threadsTable),
		IndexName:              aws.String("statusIndex"),
		KeyConditionExpression: aws.String("#status = :status"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "pending"},
		},
		ScanIndexForward: aws.Bool(false),
	})
	require.NoError(t, err)
	require.Len(t, queryThreadsOut.Items, 2)
	created0, ok := queryThreadsOut.Items[0]["createdAt"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	created1, ok := queryThreadsOut.Items[1]["createdAt"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "2026-01-02T00:00:00Z", created0.Value)
	require.Equal(t, "2026-01-01T00:00:00Z", created1.Value)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
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
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
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
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
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
	require.Error(t, err)
	var condErr *ddbTypes.ConditionalCheckFailedException
	require.ErrorAs(t, err, &condErr)
	getOut, err = client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(threadsTable),
		Key: map[string]ddbTypes.AttributeValue{
			"threadId": &ddbTypes.AttributeValueMemberS{Value: "does-not-exist"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, getOut.Item)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(messagesTable),
		Item: map[string]ddbTypes.AttributeValue{
			"messageId": &ddbTypes.AttributeValueMemberS{Value: "m1"},
			"threadId":  &ddbTypes.AttributeValueMemberS{Value: "t1"},
			"content":   &ddbTypes.AttributeValueMemberS{Value: "hello"},
			"sender":    &ddbTypes.AttributeValueMemberS{Value: "user"},
			"createdAt": &ddbTypes.AttributeValueMemberS{Value: "2026-01-01T00:00:01Z"},
		},
	})
	require.NoError(t, err)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(messagesTable),
		Item: map[string]ddbTypes.AttributeValue{
			"messageId": &ddbTypes.AttributeValueMemberS{Value: "m2"},
			"threadId":  &ddbTypes.AttributeValueMemberS{Value: "t1"},
			"content":   &ddbTypes.AttributeValueMemberS{Value: "world"},
			"sender":    &ddbTypes.AttributeValueMemberS{Value: "admin"},
			"createdAt": &ddbTypes.AttributeValueMemberS{Value: "2026-01-01T00:00:02Z"},
		},
	})
	require.NoError(t, err)

	queryMessagesOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(messagesTable),
		KeyConditionExpression: aws.String("threadId = :threadId"),
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":threadId": &ddbTypes.AttributeValueMemberS{Value: "t1"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, queryMessagesOut.Items, 2)
	mc0, ok := queryMessagesOut.Items[0]["createdAt"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	mc1, ok := queryMessagesOut.Items[1]["createdAt"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "2026-01-01T00:00:01Z", mc0.Value)
	require.Equal(t, "2026-01-01T00:00:02Z", mc1.Value)

	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(messagesTable)})
	require.NoError(t, err)
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
