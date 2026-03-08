package adapter

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func newDynamoTestClient(t *testing.T, address string) *dynamodb.Client {
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

func TestDynamoDB_PutDeleteItem_ReturnValuesAndConditions(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":    &ddbTypes.AttributeValueMemberS{Value: "k1"},
			"value":  &ddbTypes.AttributeValueMemberS{Value: "v1"},
			"status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)

	putOut, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":    &ddbTypes.AttributeValueMemberS{Value: "k1"},
			"value":  &ddbTypes.AttributeValueMemberS{Value: "v2"},
			"status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		ConditionExpression: aws.String("attribute_exists(#k)"),
		ExpressionAttributeNames: map[string]string{
			"#k": "key",
		},
		ReturnValues: ddbTypes.ReturnValueAllOld,
	})
	require.NoError(t, err)
	require.Contains(t, putOut.Attributes, "value")
	oldValue, ok := putOut.Attributes["value"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v1", oldValue.Value)

	deleteOut, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		ConditionExpression: aws.String("#status = :status"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		ReturnValues: ddbTypes.ReturnValueAllOld,
	})
	require.NoError(t, err)
	deletedValue, ok := deleteOut.Attributes["value"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v2", deletedValue.Value)

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, getOut.Item)
}

func TestDynamoDB_UpdateAndGetItem_ReturnValuesAndProjection(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":    &ddbTypes.AttributeValueMemberS{Value: "k1"},
			"value":  &ddbTypes.AttributeValueMemberS{Value: "v1"},
			"status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)

	updateOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		UpdateExpression: aws.String("SET #value = :value, #status = :status"),
		ExpressionAttributeNames: map[string]string{
			"#value":  "value",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":value":  &ddbTypes.AttributeValueMemberS{Value: "v2"},
			":status": &ddbTypes.AttributeValueMemberS{Value: "closed"},
		},
		ReturnValues: ddbTypes.ReturnValueUpdatedNew,
	})
	require.NoError(t, err)
	require.Len(t, updateOut.Attributes, 2)

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		ProjectionExpression: aws.String("#value"),
		ExpressionAttributeNames: map[string]string{
			"#value": "value",
		},
	})
	require.NoError(t, err)
	require.Len(t, getOut.Item, 1)
	projectedValue, ok := getOut.Item["value"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v2", projectedValue.Value)
}

func TestDynamoDB_QueryAndScan_PaginationFilterProjection(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	table := "paged_reads"
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []ddbTypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbTypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbTypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbTypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbTypes.KeyTypeRange},
		},
		BillingMode: ddbTypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	putItem := func(sk, status string) {
		_, putErr := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]ddbTypes.AttributeValue{
				"pk":     &ddbTypes.AttributeValueMemberS{Value: "u1"},
				"sk":     &ddbTypes.AttributeValueMemberS{Value: sk},
				"status": &ddbTypes.AttributeValueMemberS{Value: status},
			},
		})
		require.NoError(t, putErr)
	}
	putItem("001", "closed")
	putItem("002", "open")
	putItem("003", "open")

	queryPage1, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		FilterExpression:       aws.String("#status = :open"),
		ProjectionExpression:   aws.String("#sk, #status"),
		ExpressionAttributeNames: map[string]string{
			"#sk":     "sk",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk":   &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":open": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		Limit:            aws.Int32(2),
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, queryPage1.Items, 1)
	require.Equal(t, int32(1), queryPage1.Count)
	require.Equal(t, int32(2), queryPage1.ScannedCount)
	require.NotEmpty(t, queryPage1.LastEvaluatedKey)

	queryPage2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		FilterExpression:       aws.String("#status = :open"),
		ProjectionExpression:   aws.String("#sk, #status"),
		ExpressionAttributeNames: map[string]string{
			"#sk":     "sk",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":pk":   &ddbTypes.AttributeValueMemberS{Value: "u1"},
			":open": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		ExclusiveStartKey: queryPage1.LastEvaluatedKey,
		Limit:             aws.Int32(2),
		ScanIndexForward:  aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, queryPage2.Items, 1)
	require.Empty(t, queryPage2.LastEvaluatedKey)

	scanPage1, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:            aws.String(table),
		FilterExpression:     aws.String("#status = :open"),
		ProjectionExpression: aws.String("#sk, #status"),
		ExpressionAttributeNames: map[string]string{
			"#sk":     "sk",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":open": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		Limit: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, scanPage1.Items, 1)
	require.Equal(t, int32(1), scanPage1.Count)
	require.Equal(t, int32(2), scanPage1.ScannedCount)
	require.NotEmpty(t, scanPage1.LastEvaluatedKey)
	require.Len(t, scanPage1.Items[0], 2)

	scanPage2, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:            aws.String(table),
		FilterExpression:     aws.String("#status = :open"),
		ProjectionExpression: aws.String("#sk, #status"),
		ExpressionAttributeNames: map[string]string{
			"#sk":     "sk",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":open": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		ExclusiveStartKey: scanPage1.LastEvaluatedKey,
		Limit:             aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, scanPage2.Items, 1)
	require.Empty(t, scanPage2.LastEvaluatedKey)
}

func TestDynamoDB_BatchWriteItem_PutAndDelete(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":   &ddbTypes.AttributeValueMemberS{Value: "delete-me"},
			"value": &ddbTypes.AttributeValueMemberS{Value: "legacy"},
		},
	})
	require.NoError(t, err)

	out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbTypes.WriteRequest{
			"t": {
				{
					PutRequest: &ddbTypes.PutRequest{
						Item: map[string]ddbTypes.AttributeValue{
							"key":   &ddbTypes.AttributeValueMemberS{Value: "k1"},
							"value": &ddbTypes.AttributeValueMemberS{Value: "v1"},
						},
					},
				},
				{
					PutRequest: &ddbTypes.PutRequest{
						Item: map[string]ddbTypes.AttributeValue{
							"key":   &ddbTypes.AttributeValueMemberS{Value: "k2"},
							"value": &ddbTypes.AttributeValueMemberS{Value: "v2"},
						},
					},
				},
				{
					DeleteRequest: &ddbTypes.DeleteRequest{
						Key: map[string]ddbTypes.AttributeValue{
							"key": &ddbTypes.AttributeValueMemberS{Value: "delete-me"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Empty(t, out.UnprocessedItems)

	for _, key := range []string{"k1", "k2"} {
		getOut, getErr := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("t"),
			Key: map[string]ddbTypes.AttributeValue{
				"key": &ddbTypes.AttributeValueMemberS{Value: key},
			},
		})
		require.NoError(t, getErr)
		require.NotEmpty(t, getOut.Item)
	}

	deletedOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "delete-me"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, deletedOut.Item)
}

func TestDynamoDB_BatchWriteItem_RejectsDuplicateItemRequests(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbTypes.WriteRequest{
			"t": {
				{
					PutRequest: &ddbTypes.PutRequest{
						Item: map[string]ddbTypes.AttributeValue{
							"key":   &ddbTypes.AttributeValueMemberS{Value: "k1"},
							"value": &ddbTypes.AttributeValueMemberS{Value: "v1"},
						},
					},
				},
				{
					DeleteRequest: &ddbTypes.DeleteRequest{
						Key: map[string]ddbTypes.AttributeValue{
							"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
						},
					},
				},
			},
		},
	})
	require.Error(t, err)
	var validationErr smithy.APIError
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, dynamoErrValidation, validationErr.ErrorCode())
}

func TestDynamoDB_Query_GSIPaginationRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	table := "gsi_paged_reads"
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
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
		BillingMode: ddbTypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	for _, sk := range []string{"001", "002", "003"} {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]ddbTypes.AttributeValue{
				"pk":     &ddbTypes.AttributeValueMemberS{Value: "u1"},
				"sk":     &ddbTypes.AttributeValueMemberS{Value: sk},
				"status": &ddbTypes.AttributeValueMemberS{Value: "open"},
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String("statusIndex"),
		KeyConditionExpression: aws.String("#status = :status"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		Limit:            aws.Int32(2),
		ScanIndexForward: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, page1.Items, 2)
	require.NotEmpty(t, page1.LastEvaluatedKey)
	require.Contains(t, page1.LastEvaluatedKey, "pk")
	require.Contains(t, page1.LastEvaluatedKey, "status")
	require.Contains(t, page1.LastEvaluatedKey, "sk")

	page2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String("statusIndex"),
		KeyConditionExpression: aws.String("#status = :status"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
		ExclusiveStartKey: page1.LastEvaluatedKey,
		Limit:             aws.Int32(2),
		ScanIndexForward:  aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, page2.Items, 1)
	require.Empty(t, page2.LastEvaluatedKey)
}

func TestDynamoDB_QueryAndScan_InvalidOptionsRejected(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	_, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("t"),
		KeyConditionExpression: aws.String("#k = :key"),
		ExpressionAttributeNames: map[string]string{
			"#k": "key",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		Limit: aws.Int32(0),
	})
	require.Error(t, err)
	var validationErr smithy.APIError
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, dynamoErrValidation, validationErr.ErrorCode())

	_, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String("t"),
		Select:    ddbTypes.Select("INVALID"),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, dynamoErrValidation, validationErr.ErrorCode())
}

func TestDynamoDB_TransactWriteItems_PutConditionExpression(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":    &ddbTypes.AttributeValueMemberS{Value: "k1"},
			"value":  &ddbTypes.AttributeValueMemberS{Value: "v1"},
			"status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbTypes.TransactWriteItem{
			{
				Put: &ddbTypes.Put{
					TableName: aws.String("t"),
					Item: map[string]ddbTypes.AttributeValue{
						"key":    &ddbTypes.AttributeValueMemberS{Value: "k1"},
						"value":  &ddbTypes.AttributeValueMemberS{Value: "v2"},
						"status": &ddbTypes.AttributeValueMemberS{Value: "open"},
					},
					ConditionExpression: aws.String("#status = :status"),
					ExpressionAttributeNames: map[string]string{
						"#status": "status",
					},
					ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
						":status": &ddbTypes.AttributeValueMemberS{Value: "open"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbTypes.TransactWriteItem{
			{
				Put: &ddbTypes.Put{
					TableName: aws.String("t"),
					Item: map[string]ddbTypes.AttributeValue{
						"key":    &ddbTypes.AttributeValueMemberS{Value: "k1"},
						"value":  &ddbTypes.AttributeValueMemberS{Value: "v3"},
						"status": &ddbTypes.AttributeValueMemberS{Value: "open"},
					},
					ConditionExpression: aws.String("#status = :status"),
					ExpressionAttributeNames: map[string]string{
						"#status": "status",
					},
					ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
						":status": &ddbTypes.AttributeValueMemberS{Value: "closed"},
					},
				},
			},
		},
	})
	require.Error(t, err)

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
	})
	require.NoError(t, err)
	value, ok := getOut.Item["value"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v2", value.Value)
}

func TestApplyQueryExclusiveStartKey_AdvancesWhenStartKeyMissing(t *testing.T) {
	t.Parallel()

	schema := &dynamoTableSchema{
		TableName: "t",
		PrimaryKey: dynamoKeySchema{
			HashKey:  "pk",
			RangeKey: "sk",
		},
		Generation: 1,
	}
	items := []map[string]attributeValue{
		{
			"pk": newStringAttributeValue("u1"),
			"sk": newStringAttributeValue("001"),
		},
		{
			"pk": newStringAttributeValue("u1"),
			"sk": newStringAttributeValue("003"),
		},
	}

	remaining, err := applyQueryExclusiveStartKey(schema, map[string]attributeValue{
		"pk": newStringAttributeValue("u1"),
		"sk": newStringAttributeValue("002"),
	}, items)
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	require.Equal(t, "003", remaining[0]["sk"].stringValue())
}
