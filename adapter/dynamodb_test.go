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
