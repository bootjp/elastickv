package adapter

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
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

func TestDynamoDB_DeleteItem(t *testing.T) {
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
	createSimpleKeyTable(t, context.Background(), client)

	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "delete-target"},
			"value": &types.AttributeValueMemberS{Value: "v"},
		},
	})
	require.NoError(t, err)

	delOut, err := client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "delete-target"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	require.NoError(t, err)
	require.NotEmpty(t, delOut.Attributes)
	oldValue, ok := delOut.Attributes["value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v", oldValue.Value)

	getOut, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "delete-target"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, getOut.Item)
}

func TestDynamoDB_DeleteItem_Condition(t *testing.T) {
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
	createSimpleKeyTable(t, context.Background(), client)

	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "cond-target"},
			"value": &types.AttributeValueMemberS{Value: "v"},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "cond-target"},
		},
		ConditionExpression: aws.String("attribute_not_exists(#k)"),
		ExpressionAttributeNames: map[string]string{
			"#k": "key",
		},
	})
	require.Error(t, err)
	var condErr *types.ConditionalCheckFailedException
	require.ErrorAs(t, err, &condErr)

	getOut, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "cond-target"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, getOut.Item)

	_, err = client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "cond-target"},
		},
		ConditionExpression: aws.String("attribute_exists(#k)"),
		ExpressionAttributeNames: map[string]string{
			"#k": "key",
		},
	})
	require.NoError(t, err)
}

func TestDynamoDB_DeleteItem_RequestBodyTooLarge(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	reqBody := strings.Repeat("a", dynamoMaxRequestBodyBytes+1)
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://"+nodes[0].dynamoAddress+"/",
		strings.NewReader(reqBody),
	)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", deleteItemTarget)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), dynamoErrValidation)
	require.Contains(t, string(body), "too large")
}

func TestDynamoDB_Healthz(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"http://"+nodes[0].dynamoAddress+dynamoHealthPath,
		nil,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok\n", string(body))
}

func TestDynamoDB_PutItem_RequestBodyTooLarge(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	reqBody := strings.Repeat("a", dynamoMaxRequestBodyBytes+1)
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://"+nodes[0].dynamoAddress+"/",
		strings.NewReader(reqBody),
	)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", putItemTarget)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), dynamoErrValidation)
	require.Contains(t, string(body), "too large")
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

func TestDynamoDB_UpdateItem_RejectsExpressionAttributeNameInjection(t *testing.T) {
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
	createSimpleKeyTable(t, context.Background(), client)

	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "test"},
			"value": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "test"},
		},
		UpdateExpression:    aws.String("SET #v = :val"),
		ConditionExpression: aws.String("attribute_exists(#guard)"),
		ExpressionAttributeNames: map[string]string{
			"#v":     "value",
			"#guard": "missing) OR attribute_exists(key",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "v2"},
		},
	})
	require.ErrorContains(t, err, "invalid expression attribute name")

	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{
			"key": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	require.NoError(t, err)
	valueAttr, ok := out.Item["value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v1", valueAttr.Value)
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

	for i := range numGoroutines {
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

	for i := range numGoroutines {
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
		"k": newStringAttributeValue("v"),
	}
	values := map[string]attributeValue{
		":v": newStringAttributeValue("v"),
	}
	ok, err := evalConditionExpression("attribute_exists(k)AND(k = :v)", item, values)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestReplaceNames_ValidatesExpressionAttributeNames(t *testing.T) {
	t.Parallel()

	t.Run("invalid placeholder", func(t *testing.T) {
		t.Parallel()
		_, err := replaceNames("attribute_exists(#name)", map[string]string{
			"name": "value",
		})
		require.ErrorContains(t, err, `invalid expression attribute placeholder "name"`)
	})

	t.Run("invalid placeholder character", func(t *testing.T) {
		t.Parallel()
		_, err := replaceNames("attribute_exists(#na-me)", map[string]string{
			"#na-me": "value",
		})
		require.ErrorContains(t, err, `invalid expression attribute placeholder "#na-me"`)
	})

	t.Run("invalid attribute name", func(t *testing.T) {
		t.Parallel()
		_, err := replaceNames("attribute_exists(#name)", map[string]string{
			"#name": "value OR attribute_exists(key)",
		})
		require.ErrorContains(t, err, `invalid expression attribute name "value OR attribute_exists(key)"`)
	})

	t.Run("valid replacement", func(t *testing.T) {
		t.Parallel()
		expr, err := replaceNames("attribute_exists(#name)", map[string]string{
			"#name": "value_1",
		})
		require.NoError(t, err)
		require.Equal(t, "attribute_exists(value_1)", expr)
	})

	t.Run("valid replacement with dot and hyphen", func(t *testing.T) {
		t.Parallel()
		expr, err := replaceNames("#left = :l AND #right = :r", map[string]string{
			"#left":  "data.field",
			"#right": "my-attribute",
		})
		require.NoError(t, err)
		require.Equal(t, "data.field = :l AND my-attribute = :r", expr)
	})
}

func TestValidateConditionOnItem_AttributeNameContainsLogicalKeywordSubstring(t *testing.T) {
	t.Parallel()

	item := map[string]attributeValue{
		"a-OR-b": newStringAttributeValue("ok"),
	}
	values := map[string]attributeValue{
		":v": newStringAttributeValue("ok"),
	}
	names := map[string]string{
		"#k": "a-OR-b",
	}

	err := validateConditionOnItem("#k = :v", names, values, item)
	require.NoError(t, err)
}

func TestQueryExclusiveStartKey_AppliesAfterOrdering(t *testing.T) {
	schema := &dynamoTableSchema{
		PrimaryKey: dynamoKeySchema{
			HashKey:  "pk",
			RangeKey: "sk",
		},
	}
	items := []map[string]attributeValue{
		{
			"pk": newStringAttributeValue("h"),
			"sk": newStringAttributeValue("1"),
		},
		{
			"pk": newStringAttributeValue("h"),
			"sk": newStringAttributeValue("2"),
		},
		{
			"pk": newStringAttributeValue("h"),
			"sk": newStringAttributeValue("3"),
		},
	}
	scanIndexForward := false
	orderQueryItems(items, "sk", &scanIndexForward)

	paged, err := applyQueryExclusiveStartKey(schema, map[string]attributeValue{
		"pk": newStringAttributeValue("h"),
		"sk": newStringAttributeValue("2"),
	}, items)
	require.NoError(t, err)
	require.Len(t, paged, 1)
	require.Equal(t, "1", paged[0]["sk"].stringValue())
}

func TestDynamoDB_TransactWriteItems_ValidationErrors(t *testing.T) {
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
	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{},
	})
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "missing transact items") ||
			strings.Contains(err.Error(), "missing required field"),
		"unexpected error: %v", err,
	)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{{}},
	})
	require.ErrorContains(t, err, "missing transact action")

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("t"),
					Item: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{Value: "k"},
					},
				},
				Delete: &types.Delete{
					TableName: aws.String("t"),
					Key: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{Value: "k"},
					},
				},
			},
		},
	})
	require.ErrorContains(t, err, "multiple transact actions are not supported")
}

func TestDynamoDB_TransactWriteItems_UpdateConditionExpression(t *testing.T) {
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
	createSimpleKeyTable(t, ctx, client)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":    &types.AttributeValueMemberS{Value: "k1"},
			"status": &types.AttributeValueMemberS{Value: "open"},
			"value":  &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName:           aws.String("t"),
					Key:                 map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}},
					UpdateExpression:    aws.String("SET #v = :next"),
					ConditionExpression: aws.String("#s = :open"),
					ExpressionAttributeNames: map[string]string{
						"#v": "value",
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":next": &types.AttributeValueMemberS{Value: "v2"},
						":open": &types.AttributeValueMemberS{Value: "open"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}},
	})
	require.NoError(t, err)
	value, ok := out.Item["value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v2", value.Value)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName:           aws.String("t"),
					Key:                 map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}},
					UpdateExpression:    aws.String("SET #v = :next"),
					ConditionExpression: aws.String("#s = :closed"),
					ExpressionAttributeNames: map[string]string{
						"#v": "value",
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":next":   &types.AttributeValueMemberS{Value: "v3"},
						":closed": &types.AttributeValueMemberS{Value: "closed"},
					},
				},
			},
		},
	})
	require.ErrorContains(t, err, "conditional check failed")

	out, err = client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}},
	})
	require.NoError(t, err)
	value, ok = out.Item["value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v2", value.Value)
}

func TestDynamoDB_TransactWriteItems_DeleteConditionExpression(t *testing.T) {
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
	createSimpleKeyTable(t, ctx, client)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":    &types.AttributeValueMemberS{Value: "del-ok"},
			"status": &types.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":    &types.AttributeValueMemberS{Value: "del-ng"},
			"status": &types.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Delete: &types.Delete{
					TableName:           aws.String("t"),
					Key:                 map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "del-ok"}},
					ConditionExpression: aws.String("#s = :open"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":open": &types.AttributeValueMemberS{Value: "open"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	okOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "del-ok"}},
	})
	require.NoError(t, err)
	require.Empty(t, okOut.Item)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Delete: &types.Delete{
					TableName:           aws.String("t"),
					Key:                 map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "del-ng"}},
					ConditionExpression: aws.String("#s = :closed"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":closed": &types.AttributeValueMemberS{Value: "closed"},
					},
				},
			},
		},
	})
	require.ErrorContains(t, err, "conditional check failed")

	ngOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "del-ng"}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, ngOut.Item)
}

func TestDynamoDB_TransactWriteItems_DeleteMissingItemNoop(t *testing.T) {
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
	createSimpleKeyTable(t, ctx, client)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Delete: &types.Delete{
					TableName: aws.String("t"),
					Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "missing"}},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestDynamoDB_TransactWriteItems_ConditionCheckOnly(t *testing.T) {
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
	createSimpleKeyTable(t, ctx, client)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":    &types.AttributeValueMemberS{Value: "guard"},
			"status": &types.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				ConditionCheck: &types.ConditionCheck{
					TableName:           aws.String("t"),
					Key:                 map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "guard"}},
					ConditionExpression: aws.String("#s = :open"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":open": &types.AttributeValueMemberS{Value: "open"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				ConditionCheck: &types.ConditionCheck{
					TableName:           aws.String("t"),
					Key:                 map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "guard"}},
					ConditionExpression: aws.String("#s = :closed"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":closed": &types.AttributeValueMemberS{Value: "closed"},
					},
				},
			},
		},
	})
	require.ErrorContains(t, err, "conditional check failed")
}

func TestDynamoDB_TransactWriteItems_RetriesOnWriteConflict(t *testing.T) {
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
	createSimpleKeyTable(t, ctx, client)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "k1"},
			"value": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)

	orig := nodes[0].dynamoServer.coordinator
	wrapped := &testCoordinatorWrapper{inner: orig}
	wrapped.failTxnDispatches.Store(1)
	nodes[0].dynamoServer.coordinator = wrapped
	defer func() {
		nodes[0].dynamoServer.coordinator = orig
	}()

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName:        aws.String("t"),
					Key:              map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}},
					UpdateExpression: aws.String("SET #v = :next"),
					ExpressionAttributeNames: map[string]string{
						"#v": "value",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":next": &types.AttributeValueMemberS{Value: "v2"},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), wrapped.injectedFailures.Load())
	require.GreaterOrEqual(t, wrapped.txnDispatches.Load(), int32(2))

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}},
	})
	require.NoError(t, err)
	value, ok := out.Item["value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v2", value.Value)
}

func TestDynamoDB_TransactWriteItems_ConditionCheckRace(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 2)
	defer shutdown(nodes)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	client0 := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[0].dynamoAddress)
	})
	client1 := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + nodes[1].dynamoAddress)
	})

	ctx := context.Background()
	_, err = client0.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("race"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("key"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("key"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = client0.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("race"),
		Item: map[string]types.AttributeValue{
			"key":    &types.AttributeValueMemberS{Value: "guard"},
			"status": &types.AttributeValueMemberS{Value: "open"},
		},
	})
	require.NoError(t, err)
	_, err = client0.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("race"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "target"},
			"value": &types.AttributeValueMemberS{Value: "old"},
		},
	})
	require.NoError(t, err)

	orig := nodes[0].dynamoServer.coordinator
	wrapped := &testCoordinatorWrapper{
		inner:        orig,
		blockEntered: make(chan struct{}),
		blockRelease: make(chan struct{}),
	}
	nodes[0].dynamoServer.coordinator = wrapped
	defer func() {
		nodes[0].dynamoServer.coordinator = orig
	}()

	txErrCh := make(chan error, 1)
	go func() {
		_, txErr := client0.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					ConditionCheck: &types.ConditionCheck{
						TableName: aws.String("race"),
						Key: map[string]types.AttributeValue{
							"key": &types.AttributeValueMemberS{Value: "guard"},
						},
						ConditionExpression: aws.String("#s = :open"),
						ExpressionAttributeNames: map[string]string{
							"#s": "status",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":open": &types.AttributeValueMemberS{Value: "open"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName:        aws.String("race"),
						Key:              map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "target"}},
						UpdateExpression: aws.String("SET #v = :next"),
						ExpressionAttributeNames: map[string]string{
							"#v": "value",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":next": &types.AttributeValueMemberS{Value: "from-tx1"},
						},
					},
				},
			},
		})
		txErrCh <- txErr
	}()

	select {
	case <-wrapped.blockEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked transact dispatch")
	}

	_, err = client1.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String("race"),
		Key:              map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "guard"}},
		UpdateExpression: aws.String("SET #s = :closed"),
		ExpressionAttributeNames: map[string]string{
			"#s": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":closed": &types.AttributeValueMemberS{Value: "closed"},
		},
	})
	require.NoError(t, err)
	close(wrapped.blockRelease)

	var txErr error
	select {
	case txErr = <-txErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for transact result")
	}
	require.ErrorContains(t, txErr, "conditional check failed")

	targetOut, err := client0.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("race"),
		Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "target"}},
	})
	require.NoError(t, err)
	targetValue, ok := targetOut.Item["value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "old", targetValue.Value)

	guardOut, err := client0.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("race"),
		Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "guard"}},
	})
	require.NoError(t, err)
	guardStatus, ok := guardOut.Item["status"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "closed", guardStatus.Value)
}

type testCoordinatorWrapper struct {
	inner kv.Coordinator

	failTxnDispatches atomic.Int32
	injectedFailures  atomic.Int32
	txnDispatches     atomic.Int32

	blockEntered chan struct{}
	blockRelease chan struct{}
	blockOnce    sync.Once
}

func (w *testCoordinatorWrapper) Dispatch(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if reqs != nil && reqs.IsTxn && len(reqs.Elems) > 0 {
		w.txnDispatches.Add(1)
		if w.blockEntered != nil && w.blockRelease != nil {
			w.blockOnce.Do(func() {
				close(w.blockEntered)
				<-w.blockRelease
			})
		}
		for {
			remaining := w.failTxnDispatches.Load()
			if remaining <= 0 {
				break
			}
			if w.failTxnDispatches.CompareAndSwap(remaining, remaining-1) {
				w.injectedFailures.Add(1)
				return nil, store.ErrWriteConflict
			}
		}
	}
	return w.inner.Dispatch(ctx, reqs)
}

func (w *testCoordinatorWrapper) IsLeader() bool {
	return w.inner.IsLeader()
}

func (w *testCoordinatorWrapper) VerifyLeader() error {
	return w.inner.VerifyLeader()
}

func (w *testCoordinatorWrapper) RaftLeader() raft.ServerAddress {
	return w.inner.RaftLeader()
}

func (w *testCoordinatorWrapper) IsLeaderForKey(key []byte) bool {
	return w.inner.IsLeaderForKey(key)
}

func (w *testCoordinatorWrapper) VerifyLeaderForKey(key []byte) error {
	return w.inner.VerifyLeaderForKey(key)
}

func (w *testCoordinatorWrapper) RaftLeaderForKey(key []byte) raft.ServerAddress {
	return w.inner.RaftLeaderForKey(key)
}

func (w *testCoordinatorWrapper) Clock() *kv.HLC {
	return w.inner.Clock()
}
