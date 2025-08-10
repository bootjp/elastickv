package adapter

import (
	"context"
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
