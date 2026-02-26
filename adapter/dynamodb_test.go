package adapter

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createSimpleKeyTable(t *testing.T, ctx context.Context, client *dynamodb.Client) {
	t.Helper()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("t"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("key"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("key"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

func TestDynamoDB_PutItem_GetItem(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})
	createSimpleKeyTable(t, context.Background(), client)

	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "test"},
			"value": &types.AttributeValueMemberS{Value: "v"},
		},
	})
	assert.NoError(t, err)

	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	assert.NoError(t, err)
	keyAttr, ok := out.Item["key"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	valueAttr, ok := out.Item["value"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	assert.Equal(t, "test", keyAttr.Value)
	assert.Equal(t, "v", valueAttr.Value)
}

func TestDynamoDB_TransactWriteItems(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})
	createSimpleKeyTable(t, context.Background(), client)

	_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("t"),
					Item: map[string]types.AttributeValue{
						"key":   &types.AttributeValueMemberS{Value: "k1"},
						"value": &types.AttributeValueMemberS{Value: "v1"},
					},
				},
			},
			{
				Put: &types.Put{
					TableName: aws.String("t"),
					Item: map[string]types.AttributeValue{
						"key":   &types.AttributeValueMemberS{Value: "k2"},
						"value": &types.AttributeValueMemberS{Value: "v2"},
					},
				},
			},
		},
	})
	assert.NoError(t, err)

	out1, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "k1"},
		},
	})
	assert.NoError(t, err)
	value1Attr, ok := out1.Item["value"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	assert.Equal(t, "v1", value1Attr.Value)

	out2, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "k2"},
		},
	})
	assert.NoError(t, err)
	value2Attr, ok := out2.Item["value"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	assert.Equal(t, "v2", value2Attr.Value)
}

func TestDynamoDB_UpdateItem_Condition(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})
	createSimpleKeyTable(t, context.Background(), client)

	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "test"},
			"value": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	assert.NoError(t, err)

	_, err = client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "test"},
		},
		UpdateExpression:    aws.String("SET #v = :val"),
		ConditionExpression: aws.String("attribute_exists(#k)"),
		ExpressionAttributeNames: map[string]string{
			"#v": "value",
			"#k": "key",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "v2"},
		},
	})
	assert.NoError(t, err)

	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	assert.NoError(t, err)
	valueAttr, ok := out.Item["value"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	assert.Equal(t, "v2", valueAttr.Value)

	_, err = client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "test"},
		},
		UpdateExpression:    aws.String("SET #v = :val"),
		ConditionExpression: aws.String("attribute_not_exists(#k)"),
		ExpressionAttributeNames: map[string]string{
			"#v": "value",
			"#k": "key",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "v3"},
		},
	})
	assert.Error(t, err)
}

func TestDynamoDB_TransactWriteItems_Concurrent(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})
	createSimpleKeyTable(t, context.Background(), client)

	wg := &sync.WaitGroup{}
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			keyPrefix := "concurrent-txn-" + strconv.Itoa(i)
			key1 := keyPrefix + "-k1"
			key2 := keyPrefix + "-k2"
			value1 := "v1-" + strconv.Itoa(i)
			value2 := "v2-" + strconv.Itoa(i)

			// Perform transaction with two put operations
			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String("t"),
							Item: map[string]types.AttributeValue{
								"key":   &types.AttributeValueMemberS{Value: key1},
								"value": &types.AttributeValueMemberS{Value: value1},
							},
						},
					},
					{
						Put: &types.Put{
							TableName: aws.String("t"),
							Item: map[string]types.AttributeValue{
								"key":   &types.AttributeValueMemberS{Value: key2},
								"value": &types.AttributeValueMemberS{Value: value2},
							},
						},
					},
				},
			})
			assert.NoError(t, err, "Transaction failed for goroutine %d", i)

			// Verify both items were written correctly
			out1, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
				TableName: aws.String("t"),
				Key: map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{Value: key1},
				},
			})
			assert.NoError(t, err, "Get failed for key1 in goroutine %d", i)
			value1Attr, ok := out1.Item["value"].(*types.AttributeValueMemberS)
			require.True(t, ok, "Type assertion failed for key1 in goroutine %d", i)
			assert.Equal(t, value1, value1Attr.Value, "Value mismatch for key1 in goroutine %d", i)

			out2, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
				TableName: aws.String("t"),
				Key: map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{Value: key2},
				},
			})
			assert.NoError(t, err, "Get failed for key2 in goroutine %d", i)
			value2Attr, ok := out2.Item["value"].(*types.AttributeValueMemberS)
			require.True(t, ok, "Type assertion failed for key2 in goroutine %d", i)
			assert.Equal(t, value2, value2Attr.Value, "Value mismatch for key2 in goroutine %d", i)
		}(i)
	}

	wg.Wait()
}

func TestDynamoDB_TransactWriteItems_Concurrent_Conflicting(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})
	createSimpleKeyTable(t, context.Background(), client)

	// Initialize some base keys that will be updated concurrently
	baseKeys := []string{"shared-key-1", "shared-key-2", "shared-key-3"}
	for _, key := range baseKeys {
		_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("t"),
			Item: map[string]types.AttributeValue{
				"key":     &types.AttributeValueMemberS{Value: key},
				"value":   &types.AttributeValueMemberS{Value: "initial"},
				"counter": &types.AttributeValueMemberN{Value: "0"},
			},
		})
		assert.NoError(t, err)
	}

	wg := &sync.WaitGroup{}
	numGoroutines := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Each goroutine attempts to update multiple shared keys in a transaction
			counterValue := strconv.Itoa(i)

			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String("t"),
							Item: map[string]types.AttributeValue{
								"key":     &types.AttributeValueMemberS{Value: baseKeys[0]},
								"value":   &types.AttributeValueMemberS{Value: "updated-by-" + counterValue},
								"counter": &types.AttributeValueMemberN{Value: counterValue},
							},
						},
					},
					{
						Put: &types.Put{
							TableName: aws.String("t"),
							Item: map[string]types.AttributeValue{
								"key":     &types.AttributeValueMemberS{Value: baseKeys[1]},
								"value":   &types.AttributeValueMemberS{Value: "updated-by-" + counterValue},
								"counter": &types.AttributeValueMemberN{Value: counterValue},
							},
						},
					},
					{
						Put: &types.Put{
							TableName: aws.String("t"),
							Item: map[string]types.AttributeValue{
								"key":     &types.AttributeValueMemberS{Value: baseKeys[2]},
								"value":   &types.AttributeValueMemberS{Value: "updated-by-" + counterValue},
								"counter": &types.AttributeValueMemberN{Value: counterValue},
							},
						},
					},
				},
			})
			assert.NoError(t, err, "Transaction failed for goroutine %d", i)
		}(i)
	}

	wg.Wait()

	// Verify that all keys have been updated and have consistent values
	// Due to the concurrent nature, we can't predict which goroutine will win,
	// but we can verify that each key has valid data
	for _, key := range baseKeys {
		out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
			TableName: aws.String("t"),
			Key: map[string]types.AttributeValue{
				"key": &types.AttributeValueMemberS{Value: key},
			},
		})
		assert.NoError(t, err, "Get failed for key %s", key)
		assert.NotNil(t, out.Item, "Item should exist for key %s", key)

		if out.Item != nil && out.Item["value"] != nil && out.Item["counter"] != nil {
			valueAttr, ok := out.Item["value"].(*types.AttributeValueMemberS)
			assert.True(t, ok, "Value type assertion failed for key %s", key)
			counterAttr, ok := out.Item["counter"].(*types.AttributeValueMemberN)
			assert.True(t, ok, "Counter type assertion failed for key %s", key)
			value := valueAttr.Value
			counter := counterAttr.Value

			// Verify that the value and counter are consistent (both from the same goroutine)
			assert.Contains(t, value, "updated-by-"+counter, "Value and counter should be consistent for key %s", key)
		}
	}
}

func TestSplitTopLevelByKeyword_HandlesTokenBoundaries(t *testing.T) {
	parts := splitTopLevelByKeyword("(attribute_exists(a))AND(attribute_exists(b))", "AND")
	require.Equal(t, []string{"(attribute_exists(a))", "(attribute_exists(b))"}, parts)
}

func TestEvalConditionExpression_LogicalKeywordWithoutSpaces(t *testing.T) {
	item := map[string]attributeValue{
		"k": {S: "v"},
	}
	values := map[string]attributeValue{
		":v": {S: "v"},
	}
	ok, err := evalConditionExpression("attribute_exists(k)AND(k = :v)", item, values)
	require.NoError(t, err)
	require.True(t, ok)
}
