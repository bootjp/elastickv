package adapter

import (
	"bytes"
	"context"
	"slices"
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

func TestDynamoDB_PutGetItem_BinaryAndSetAttributes(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	blob := []byte{0x00, 0x01, 0x02, 0xFF}
	bs0 := []byte("x")
	bs1 := []byte{0x10, 0x20}

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":  &ddbTypes.AttributeValueMemberS{Value: "k1"},
			"blob": &ddbTypes.AttributeValueMemberB{Value: blob},
			"ss":   &ddbTypes.AttributeValueMemberSS{Value: []string{"b", "a"}},
			"ns":   &ddbTypes.AttributeValueMemberNS{Value: []string{"1.0", "2"}},
			"bs":   &ddbTypes.AttributeValueMemberBS{Value: [][]byte{bs0, bs1}},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
	})
	require.NoError(t, err)

	gotBlob, ok := out.Item["blob"].(*ddbTypes.AttributeValueMemberB)
	require.True(t, ok)
	require.True(t, bytes.Equal(blob, gotBlob.Value))

	gotSS, ok := out.Item["ss"].(*ddbTypes.AttributeValueMemberSS)
	require.True(t, ok)
	slices.Sort(gotSS.Value)
	require.Equal(t, []string{"a", "b"}, gotSS.Value)

	gotNS, ok := out.Item["ns"].(*ddbTypes.AttributeValueMemberNS)
	require.True(t, ok)
	slices.Sort(gotNS.Value)
	require.Equal(t, []string{"1.0", "2"}, gotNS.Value)

	gotBS, ok := out.Item["bs"].(*ddbTypes.AttributeValueMemberBS)
	require.True(t, ok)
	require.Len(t, gotBS.Value, 2)
	require.True(t, containsBinaryValue(gotBS.Value, bs0))
	require.True(t, containsBinaryValue(gotBS.Value, bs1))
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

func containsBinaryValue(values [][]byte, want []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, want) {
			return true
		}
	}
	return false
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

func TestDynamoDB_ConditionExpression_ExtendedOperatorsAndFunctions(t *testing.T) {
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
			"status": &ddbTypes.AttributeValueMemberS{Value: "prefix-open"},
			"num":    &ddbTypes.AttributeValueMemberN{Value: "5"},
			"tags":   &ddbTypes.AttributeValueMemberSS{Value: []string{"red", "blue"}},
			"items": &ddbTypes.AttributeValueMemberL{Value: []ddbTypes.AttributeValue{
				&ddbTypes.AttributeValueMemberS{Value: "alpha"},
				&ddbTypes.AttributeValueMemberS{Value: "beta"},
			}},
			"blob": &ddbTypes.AttributeValueMemberB{Value: []byte("abcde")},
			"meta": &ddbTypes.AttributeValueMemberM{Value: map[string]ddbTypes.AttributeValue{
				"nested": &ddbTypes.AttributeValueMemberS{Value: "value"},
			}},
		},
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":    &ddbTypes.AttributeValueMemberS{Value: "k1"},
			"status": &ddbTypes.AttributeValueMemberS{Value: "prefix-open"},
			"num":    &ddbTypes.AttributeValueMemberN{Value: "5"},
		},
		ConditionExpression: aws.String(
			"NOT (#num < :min) AND #num BETWEEN :lower AND :upper AND #num IN (:five, :seven) " +
				"AND #num >= :lower AND #num <= :upper AND #num <> :zero " +
				"AND begins_with(#status, :prefix) AND contains(#tags, :tag) AND contains(#items, :itemNeedle) " +
				"AND contains(#blob, :blobPart) AND attribute_type(#meta, :mapType) " +
				"AND size(#items) = :listSize AND #meta.#nested = :nested",
		),
		ExpressionAttributeNames: map[string]string{
			"#num":    "num",
			"#status": "status",
			"#tags":   "tags",
			"#items":  "items",
			"#blob":   "blob",
			"#meta":   "meta",
			"#nested": "nested",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":min":        &ddbTypes.AttributeValueMemberN{Value: "4"},
			":lower":      &ddbTypes.AttributeValueMemberN{Value: "5"},
			":upper":      &ddbTypes.AttributeValueMemberN{Value: "6"},
			":five":       &ddbTypes.AttributeValueMemberN{Value: "5.0"},
			":seven":      &ddbTypes.AttributeValueMemberN{Value: "7"},
			":zero":       &ddbTypes.AttributeValueMemberN{Value: "0"},
			":prefix":     &ddbTypes.AttributeValueMemberS{Value: "prefix"},
			":tag":        &ddbTypes.AttributeValueMemberS{Value: "red"},
			":itemNeedle": &ddbTypes.AttributeValueMemberS{Value: "beta"},
			":blobPart":   &ddbTypes.AttributeValueMemberB{Value: []byte("bcd")},
			":mapType":    &ddbTypes.AttributeValueMemberS{Value: "M"},
			":listSize":   &ddbTypes.AttributeValueMemberN{Value: "2"},
			":nested":     &ddbTypes.AttributeValueMemberS{Value: "value"},
		},
	})
	require.NoError(t, err)
}

func TestDynamoDB_UpdateItem_DocumentPathsFunctionsAndSetOps(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"key":        &ddbTypes.AttributeValueMemberS{Value: "k1"},
			"deprecated": &ddbTypes.AttributeValueMemberS{Value: "legacy"},
			"score":      &ddbTypes.AttributeValueMemberN{Value: "1"},
			"tags":       &ddbTypes.AttributeValueMemberSS{Value: []string{"old"}},
			"numset":     &ddbTypes.AttributeValueMemberNS{Value: []string{"1"}},
			"binset":     &ddbTypes.AttributeValueMemberBS{Value: [][]byte{[]byte("x")}},
			"profile": &ddbTypes.AttributeValueMemberM{Value: map[string]ddbTypes.AttributeValue{
				"name": &ddbTypes.AttributeValueMemberS{Value: "Alice"},
			}},
			"items": &ddbTypes.AttributeValueMemberL{Value: []ddbTypes.AttributeValue{
				&ddbTypes.AttributeValueMemberS{Value: "first"},
			}},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		UpdateExpression: aws.String(
			"SET #profile.#name = :name, #profile.#visits = if_not_exists(#profile.#visits, :zero) + :inc, " +
				"#items = list_append(#items, :more), #extra.#child = :child REMOVE #deprecated",
		),
		ExpressionAttributeNames: map[string]string{
			"#profile":    "profile",
			"#name":       "name",
			"#visits":     "visits",
			"#items":      "items",
			"#extra":      "extra",
			"#child":      "child",
			"#deprecated": "deprecated",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":name": &ddbTypes.AttributeValueMemberS{Value: "Bob"},
			":zero": &ddbTypes.AttributeValueMemberN{Value: "0"},
			":inc":  &ddbTypes.AttributeValueMemberN{Value: "2"},
			":more": &ddbTypes.AttributeValueMemberL{Value: []ddbTypes.AttributeValue{
				&ddbTypes.AttributeValueMemberS{Value: "second"},
				&ddbTypes.AttributeValueMemberS{Value: "third"},
			}},
			":child": &ddbTypes.AttributeValueMemberS{Value: "value"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		UpdateExpression: aws.String("SET #items[1] = :second REMOVE #items[0]"),
		ExpressionAttributeNames: map[string]string{
			"#items": "items",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":second": &ddbTypes.AttributeValueMemberS{Value: "second-updated"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		UpdateExpression: aws.String("ADD #score :scoreInc, #tags :tagAdds, #numset :numAdds, #binset :binAdds"),
		ExpressionAttributeNames: map[string]string{
			"#score":  "score",
			"#tags":   "tags",
			"#numset": "numset",
			"#binset": "binset",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":scoreInc": &ddbTypes.AttributeValueMemberN{Value: "3"},
			":tagAdds":  &ddbTypes.AttributeValueMemberSS{Value: []string{"new", "old"}},
			":numAdds":  &ddbTypes.AttributeValueMemberNS{Value: []string{"2.0"}},
			":binAdds":  &ddbTypes.AttributeValueMemberBS{Value: [][]byte{[]byte("y")}},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
		UpdateExpression: aws.String("DELETE #tags :tagDeletes, #numset :numDeletes, #binset :binDeletes"),
		ExpressionAttributeNames: map[string]string{
			"#tags":   "tags",
			"#numset": "numset",
			"#binset": "binset",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":tagDeletes": &ddbTypes.AttributeValueMemberSS{Value: []string{"old"}},
			":numDeletes": &ddbTypes.AttributeValueMemberNS{Value: []string{"1.0"}},
			":binDeletes": &ddbTypes.AttributeValueMemberBS{Value: [][]byte{[]byte("x")}},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]ddbTypes.AttributeValue{
			"key": &ddbTypes.AttributeValueMemberS{Value: "k1"},
		},
	})
	require.NoError(t, err)
	require.NotContains(t, out.Item, "deprecated")

	profile, ok := out.Item["profile"].(*ddbTypes.AttributeValueMemberM)
	require.True(t, ok)
	name, ok := profile.Value["name"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "Bob", name.Value)
	visits, ok := profile.Value["visits"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "2", visits.Value)

	extra, ok := out.Item["extra"].(*ddbTypes.AttributeValueMemberM)
	require.True(t, ok)
	child, ok := extra.Value["child"].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "value", child.Value)

	items, ok := out.Item["items"].(*ddbTypes.AttributeValueMemberL)
	require.True(t, ok)
	require.Len(t, items.Value, 2)
	second, ok := items.Value[0].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "second-updated", second.Value)
	third, ok := items.Value[1].(*ddbTypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "third", third.Value)

	score, ok := out.Item["score"].(*ddbTypes.AttributeValueMemberN)
	require.True(t, ok)
	require.Equal(t, "4", score.Value)

	tags, ok := out.Item["tags"].(*ddbTypes.AttributeValueMemberSS)
	require.True(t, ok)
	require.Equal(t, []string{"new"}, tags.Value)

	numset, ok := out.Item["numset"].(*ddbTypes.AttributeValueMemberNS)
	require.True(t, ok)
	require.Equal(t, []string{"2.0"}, numset.Value)

	binset, ok := out.Item["binset"].(*ddbTypes.AttributeValueMemberBS)
	require.True(t, ok)
	require.Len(t, binset.Value, 1)
	require.True(t, bytes.Equal([]byte("y"), binset.Value[0]))
}

func TestDynamoDB_GSIProjectionAndConsistentRead(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	table := "gsi_projection"
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
				Projection: &ddbTypes.Projection{
					ProjectionType:   ddbTypes.ProjectionTypeInclude,
					NonKeyAttributes: []string{"projected"},
				},
			},
		},
		BillingMode: ddbTypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbTypes.AttributeValue{
			"pk":        &ddbTypes.AttributeValueMemberS{Value: "u1"},
			"sk":        &ddbTypes.AttributeValueMemberS{Value: "001"},
			"status":    &ddbTypes.AttributeValueMemberS{Value: "open"},
			"projected": &ddbTypes.AttributeValueMemberS{Value: "yes"},
			"hidden":    &ddbTypes.AttributeValueMemberS{Value: "no"},
		},
	})
	require.NoError(t, err)

	queryOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String("statusIndex"),
		KeyConditionExpression: aws.String("#status = :status"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)
	require.Len(t, queryOut.Items, 1)
	require.Contains(t, queryOut.Items[0], "pk")
	require.Contains(t, queryOut.Items[0], "sk")
	require.Contains(t, queryOut.Items[0], "status")
	require.Contains(t, queryOut.Items[0], "projected")
	require.NotContains(t, queryOut.Items[0], "hidden")

	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String("statusIndex"),
		KeyConditionExpression: aws.String("#status = :status"),
		ProjectionExpression:   aws.String("#hidden"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
			"#hidden": "hidden",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
	})
	require.Error(t, err)
	var validationErr smithy.APIError
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, dynamoErrValidation, validationErr.ErrorCode())

	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String("statusIndex"),
		KeyConditionExpression: aws.String("#status = :status"),
		ConsistentRead:         aws.Bool(true),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddbTypes.AttributeValue{
			":status": &ddbTypes.AttributeValueMemberS{Value: "open"},
		},
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, dynamoErrValidation, validationErr.ErrorCode())

	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	require.NoError(t, err)
	require.Len(t, desc.Table.GlobalSecondaryIndexes, 1)
	require.Equal(t, ddbTypes.ProjectionTypeInclude, desc.Table.GlobalSecondaryIndexes[0].Projection.ProjectionType)
	require.Equal(t, []string{"projected"}, desc.Table.GlobalSecondaryIndexes[0].Projection.NonKeyAttributes)
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
