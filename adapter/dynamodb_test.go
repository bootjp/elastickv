package adapter

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestDynamoDB_PutItem_GetItem(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Endpoint:    aws.String("http://" + nodes[0].dynamoAddress),
		Credentials: credentials.NewStaticCredentials("dummy", "dummy", ""),
		DisableSSL:  aws.Bool(true),
	})
	assert.NoError(t, err)

	client := dynamodb.New(sess)

	_, err = client.PutItemWithContext(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]*dynamodb.AttributeValue{
			"key":   {S: aws.String("test")},
			"value": {S: aws.String("v")},
		},
	})
	assert.NoError(t, err)

	out, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]*dynamodb.AttributeValue{
			"key": {S: aws.String("test")},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "test", aws.StringValue(out.Item["key"].S))
	assert.Equal(t, "v", aws.StringValue(out.Item["value"].S))
}

func TestDynamoDB_TransactWriteItems(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Endpoint:    aws.String("http://" + nodes[0].dynamoAddress),
		Credentials: credentials.NewStaticCredentials("dummy", "dummy", ""),
		DisableSSL:  aws.Bool(true),
	})
	assert.NoError(t, err)

	client := dynamodb.New(sess)

	_, err = client.TransactWriteItemsWithContext(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Put: &dynamodb.Put{
					TableName: aws.String("t"),
					Item: map[string]*dynamodb.AttributeValue{
						"key":   {S: aws.String("k1")},
						"value": {S: aws.String("v1")},
					},
				},
			},
			{
				Put: &dynamodb.Put{
					TableName: aws.String("t"),
					Item: map[string]*dynamodb.AttributeValue{
						"key":   {S: aws.String("k2")},
						"value": {S: aws.String("v2")},
					},
				},
			},
		},
	})
	assert.NoError(t, err)

	out1, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]*dynamodb.AttributeValue{
			"key": {S: aws.String("k1")},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "v1", aws.StringValue(out1.Item["value"].S))

	out2, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]*dynamodb.AttributeValue{
			"key": {S: aws.String("k2")},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "v2", aws.StringValue(out2.Item["value"].S))
}

func TestDynamoDB_UpdateItem_Condition(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Endpoint:    aws.String("http://" + nodes[0].dynamoAddress),
		Credentials: credentials.NewStaticCredentials("dummy", "dummy", ""),
		DisableSSL:  aws.Bool(true),
	})
	assert.NoError(t, err)

	client := dynamodb.New(sess)

	_, err = client.PutItemWithContext(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]*dynamodb.AttributeValue{
			"key":   {S: aws.String("test")},
			"value": {S: aws.String("v1")},
		},
	})
	assert.NoError(t, err)

	_, err = client.UpdateItemWithContext(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]*dynamodb.AttributeValue{
			"key": {S: aws.String("test")},
		},
		UpdateExpression:    aws.String("SET #v = :val"),
		ConditionExpression: aws.String("attribute_exists(#k)"),
		ExpressionAttributeNames: map[string]*string{
			"#v": aws.String("value"),
			"#k": aws.String("key"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":val": {S: aws.String("v2")},
		},
	})
	assert.NoError(t, err)

	out, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("t"),
		Key: map[string]*dynamodb.AttributeValue{
			"key": {S: aws.String("test")},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "v2", aws.StringValue(out.Item["value"].S))

	_, err = client.UpdateItemWithContext(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("t"),
		Key: map[string]*dynamodb.AttributeValue{
			"key": {S: aws.String("test")},
		},
		UpdateExpression:    aws.String("SET #v = :val"),
		ConditionExpression: aws.String("attribute_not_exists(#k)"),
		ExpressionAttributeNames: map[string]*string{
			"#v": aws.String("value"),
			"#k": aws.String("key"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":val": {S: aws.String("v3")},
		},
	})
	assert.Error(t, err)
}

func TestDynamoDB_TransactWriteItems_Concurrent(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Endpoint:    aws.String("http://" + nodes[0].dynamoAddress),
		Credentials: credentials.NewStaticCredentials("dummy", "dummy", ""),
		DisableSSL:  aws.Bool(true),
	})
	assert.NoError(t, err)

	client := dynamodb.New(sess)

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
			_, err := client.TransactWriteItemsWithContext(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []*dynamodb.TransactWriteItem{
					{
						Put: &dynamodb.Put{
							TableName: aws.String("t"),
							Item: map[string]*dynamodb.AttributeValue{
								"key":   {S: aws.String(key1)},
								"value": {S: aws.String(value1)},
							},
						},
					},
					{
						Put: &dynamodb.Put{
							TableName: aws.String("t"),
							Item: map[string]*dynamodb.AttributeValue{
								"key":   {S: aws.String(key2)},
								"value": {S: aws.String(value2)},
							},
						},
					},
				},
			})
			assert.NoError(t, err, "Transaction failed for goroutine %d", i)

			// Verify both items were written correctly
			out1, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
				TableName: aws.String("t"),
				Key: map[string]*dynamodb.AttributeValue{
					"key": {S: aws.String(key1)},
				},
			})
			assert.NoError(t, err, "Get failed for key1 in goroutine %d", i)
			assert.Equal(t, value1, aws.StringValue(out1.Item["value"].S), "Value mismatch for key1 in goroutine %d", i)

			out2, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
				TableName: aws.String("t"),
				Key: map[string]*dynamodb.AttributeValue{
					"key": {S: aws.String(key2)},
				},
			})
			assert.NoError(t, err, "Get failed for key2 in goroutine %d", i)
			assert.Equal(t, value2, aws.StringValue(out2.Item["value"].S), "Value mismatch for key2 in goroutine %d", i)
		}(i)
	}

	wg.Wait()
}

func TestDynamoDB_TransactWriteItems_Concurrent_Conflicting(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Endpoint:    aws.String("http://" + nodes[0].dynamoAddress),
		Credentials: credentials.NewStaticCredentials("dummy", "dummy", ""),
		DisableSSL:  aws.Bool(true),
	})
	assert.NoError(t, err)

	client := dynamodb.New(sess)

	// Initialize some base keys that will be updated concurrently
	baseKeys := []string{"shared-key-1", "shared-key-2", "shared-key-3"}
	for _, key := range baseKeys {
		_, err := client.PutItemWithContext(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("t"),
			Item: map[string]*dynamodb.AttributeValue{
				"key":     {S: aws.String(key)},
				"value":   {S: aws.String("initial")},
				"counter": {N: aws.String("0")},
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
			
			_, err := client.TransactWriteItemsWithContext(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []*dynamodb.TransactWriteItem{
					{
						Put: &dynamodb.Put{
							TableName: aws.String("t"),
							Item: map[string]*dynamodb.AttributeValue{
								"key":     {S: aws.String(baseKeys[0])},
								"value":   {S: aws.String("updated-by-" + counterValue)},
								"counter": {N: aws.String(counterValue)},
							},
						},
					},
					{
						Put: &dynamodb.Put{
							TableName: aws.String("t"),
							Item: map[string]*dynamodb.AttributeValue{
								"key":     {S: aws.String(baseKeys[1])},
								"value":   {S: aws.String("updated-by-" + counterValue)},
								"counter": {N: aws.String(counterValue)},
							},
						},
					},
					{
						Put: &dynamodb.Put{
							TableName: aws.String("t"),
							Item: map[string]*dynamodb.AttributeValue{
								"key":     {S: aws.String(baseKeys[2])},
								"value":   {S: aws.String("updated-by-" + counterValue)},
								"counter": {N: aws.String(counterValue)},
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
		out, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
			TableName: aws.String("t"),
			Key: map[string]*dynamodb.AttributeValue{
				"key": {S: aws.String(key)},
			},
		})
		assert.NoError(t, err, "Get failed for key %s", key)
		assert.NotNil(t, out.Item, "Item should exist for key %s", key)
		
		if out.Item != nil && out.Item["value"] != nil && out.Item["counter"] != nil {
			value := aws.StringValue(out.Item["value"].S)
			counter := aws.StringValue(out.Item["counter"].N)
			
			// Verify that the value and counter are consistent (both from the same goroutine)
			assert.Contains(t, value, "updated-by-"+counter, "Value and counter should be consistent for key %s", key)
		}
	}
}
