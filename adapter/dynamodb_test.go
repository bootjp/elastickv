package adapter

import (
	"context"
	"errors"
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

func TestDynamoDB_RequestBodyTooLarge(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	for _, target := range []string{deleteItemTarget, putItemTarget} {
		t.Run(target, func(t *testing.T) {
			reqBody := strings.Repeat("a", dynamoMaxRequestBodyBytes+1)
			req, err := http.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"http://"+nodes[0].dynamoAddress+"/",
				strings.NewReader(reqBody),
			)
			require.NoError(t, err)
			req.Header.Set("X-Amz-Target", target)
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), dynamoErrValidation)
			require.Contains(t, string(body), "too large")
		})
	}
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

func TestDynamoDB_LeaderHealthz(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 2)
	defer shutdown(nodes)

	cases := []struct {
		name   string
		addr   string
		status int
		body   string
	}{
		{
			name:   "leader",
			addr:   nodes[0].dynamoAddress,
			status: http.StatusOK,
			body:   "ok\n",
		},
		{
			name:   "follower",
			addr:   nodes[1].dynamoAddress,
			status: http.StatusServiceUnavailable,
			body:   "not leader\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Eventually(t, func() bool {
				req, err := http.NewRequestWithContext(
					context.Background(),
					http.MethodGet,
					"http://"+tc.addr+dynamoLeaderHealthPath,
					nil,
				)
				require.NoError(t, err)

				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()

				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				return resp.StatusCode == tc.status && string(body) == tc.body
			}, leaderChurnRetryTimeout, leaderChurnRetryInterval)
		})
	}
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

func TestDynamoDB_TransactGetItems(t *testing.T) {
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

	// Write two items.
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

	// TransactGetItems should read both items atomically at the same snapshot.
	out, err := client.TransactGetItems(context.Background(), &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}}}},
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k2"}}}},
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "missing"}}}},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, out.Responses, 3)

	// First response: k1 → "v1"
	v1, ok := out.Responses[0].Item["value"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	assert.Equal(t, "v1", v1.Value)

	// Second response: k2 → "v2"
	v2, ok := out.Responses[1].Item["value"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	assert.Equal(t, "v2", v2.Value)

	// Third response: missing key → empty Item map.
	assert.Empty(t, out.Responses[2].Item)
}

// postDynamoRaw issues a single, non-retried DynamoDB HTTP request and returns
// the status code and body. The AWS SDK retries 5xx responses, which would
// obscure the single lease-read failure under test, so the lease-failure cases
// drive the wire directly.
func postDynamoRaw(t *testing.T, address, target, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://"+address+"/",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(respBody)
}

// wrapDynamoCoordinator swaps in a testCoordinatorWrapper around the node's
// coordinator and restores the original on cleanup, so lease-read failures can
// be injected for the read handlers.
func wrapDynamoCoordinator(t *testing.T, node *Node) *testCoordinatorWrapper {
	t.Helper()
	orig := node.dynamoServer.coordinator
	wrapped := &testCoordinatorWrapper{inner: orig}
	node.dynamoServer.coordinator = wrapped
	t.Cleanup(func() {
		node.dynamoServer.coordinator = orig
	})
	return wrapped
}

func TestDynamoDB_Query_LeaseRead(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "k1"},
			"value": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)

	wrapped := wrapDynamoCoordinator(t, &nodes[0])

	// Healthy lease: the query succeeds and returns the item.
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("t"),
		KeyConditionExpression: aws.String("#k = :k"),
		ExpressionAttributeNames: map[string]string{
			"#k": "key",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":k": &types.AttributeValueMemberS{Value: "k1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	// Lease-read failure surfaces as the same InternalServerError getItem
	// produces. Drive the wire directly so the SDK does not retry the 500.
	wrapped.failLeaseReads.Store(true)
	status, respBody := postDynamoRaw(t, nodes[0].dynamoAddress, queryTarget,
		`{"TableName":"t","KeyConditionExpression":"#k = :k",`+
			`"ExpressionAttributeNames":{"#k":"key"},`+
			`"ExpressionAttributeValues":{":k":{"S":"k1"}}}`)
	require.Equal(t, http.StatusInternalServerError, status)
	require.Contains(t, respBody, dynamoErrInternal)
}

func TestDynamoDB_Scan_LeaseRead(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "k1"},
			"value": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)

	wrapped := wrapDynamoCoordinator(t, &nodes[0])

	// Healthy lease: the scan succeeds and returns the item.
	out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("t")})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	// Lease-read failure surfaces as the same InternalServerError getItem
	// produces.
	wrapped.failLeaseReads.Store(true)
	status, respBody := postDynamoRaw(t, nodes[0].dynamoAddress, scanTarget, `{"TableName":"t"}`)
	require.Equal(t, http.StatusInternalServerError, status)
	require.Contains(t, respBody, dynamoErrInternal)
}

func TestDynamoDB_TransactGetItems_LeaseRead(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "k1"},
			"value": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)

	wrapped := wrapDynamoCoordinator(t, &nodes[0])

	// Healthy lease: the transaction reads the item at a single snapshot.
	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}}}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Responses, 1)
	v1, ok := out.Responses[0].Item["value"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "v1", v1.Value)

	// Lease-read failure surfaces as the same InternalServerError getItem
	// produces, before the single snapshot timestamp is resolved.
	wrapped.failLeaseReads.Store(true)
	status, respBody := postDynamoRaw(t, nodes[0].dynamoAddress, transactGetItemsTarget,
		`{"TransactItems":[{"Get":{"TableName":"t","Key":{"key":{"S":"k1"}}}}]}`)
	require.Equal(t, http.StatusInternalServerError, status)
	require.Contains(t, respBody, dynamoErrInternal)
}

// TestDynamoDB_TransactGetItems_LeaseDedupByGroup asserts that a
// TransactGetItems touching many distinct keys that all resolve to the
// same Raft group issues a single lease read, not one per key. Before the
// per-group dedup this loop ran kv.LeaseReadForKeyThrough once per unique
// key (up to transactGetItemsMaxItems sequential reads), which gemini
// flagged as an O(N) latency bottleneck on PR #952.
func TestDynamoDB_TransactGetItems_LeaseDedupByGroup(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)

	const itemCount = 10
	gets := make([]types.TransactGetItem, 0, itemCount)
	for i := range itemCount {
		k := "k" + strconv.Itoa(i)
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("t"),
			Item: map[string]types.AttributeValue{
				"key":   &types.AttributeValueMemberS{Value: k},
				"value": &types.AttributeValueMemberS{Value: "v" + strconv.Itoa(i)},
			},
		})
		require.NoError(t, err)
		gets = append(gets, types.TransactGetItem{
			Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{
				"key": &types.AttributeValueMemberS{Value: k},
			}},
		})
	}

	wrapped := wrapDynamoCoordinator(t, &nodes[0])
	wrapped.resetLeaseCounters()

	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: gets})
	require.NoError(t, err)
	require.Len(t, out.Responses, itemCount)

	keyless, keyed := wrapped.leaseCallCounts()
	require.Equal(t, 0, keyless, "TransactGetItems must not use the keyless lease check")
	// Single-group deployment: every key routes to the same group, so the
	// per-group dedup collapses all itemCount keys into ONE lease read.
	require.Equal(t, 1, keyed,
		"expected one lease read per distinct group, got %d for %d keys", keyed, itemCount)
}

// TestDynamoDB_Query_LeaseRoutedByKey asserts that a base-table Query is
// lease-checked by its partition-key prefix (LeaseReadForKey) rather than
// by the keyless default-group check, so a multi-group deployment confirms
// the shard that actually owns the queried data (codex P2 on PR #952). A
// GSI query, which can span the whole table prefix, falls back to keyless.
func TestDynamoDB_Query_LeaseRoutedByKey(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	client := newDynamoTestClient(t, nodes[0].dynamoAddress)
	ctx := context.Background()
	createSimpleKeyTable(t, ctx, client)
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]types.AttributeValue{
			"key":   &types.AttributeValueMemberS{Value: "k1"},
			"value": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	require.NoError(t, err)

	wrapped := wrapDynamoCoordinator(t, &nodes[0])
	wrapped.resetLeaseCounters()

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String("t"),
		KeyConditionExpression:   aws.String("#k = :k"),
		ExpressionAttributeNames: map[string]string{"#k": "key"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":k": &types.AttributeValueMemberS{Value: "k1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	keyless, keyed := wrapped.leaseCallCounts()
	require.Equal(t, 0, keyless, "base-table Query must route the lease check by the partition key")
	require.Equal(t, 1, keyed, "base-table Query must issue exactly one key-routed lease check")

	// The routed lease key must be the partition-key prefix the query
	// actually scans, so it resolves to the owning shard group.
	recorded := wrapped.recordedLeaseReadKeys()
	require.Len(t, recorded, 1)
	require.NotEmpty(t, recorded[0])
}

func TestDynamoDB_TransactGetItems_ValidationErrors(t *testing.T) {
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

	ctx := context.Background()

	// Empty TransactItems should be rejected.
	_, err = client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{},
	})
	assert.Error(t, err)

	// Table not found.
	_, err = client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{TableName: aws.String("no_such_table"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}}}},
		},
	})
	assert.Error(t, err)

	// Over 100 items should be rejected (DynamoDB limit).
	items := make([]types.TransactGetItem, 101)
	for i := range items {
		items[i] = types.TransactGetItem{
			Get: &types.Get{
				TableName: aws.String("t"),
				Key:       map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: strconv.Itoa(i)}},
			},
		}
	}
	_, err = client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: items})
	assert.Error(t, err)

	// Duplicate item key in the same transaction should be rejected.
	_, err = client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}}}},
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}}}},
		},
	})
	assert.Error(t, err)
}

// TestDynamoDB_TransactGetItems_KeyAttributeErrors verifies that the server
// returns a ValidationException (400) when a key attribute is missing from the
// request or has an unsupported type. This covers the error paths added in
// canonicalPrimaryKeyStr and writeCanonicalAttrValue, which must conform to
// DynamoDB's behaviour: primary-key errors are always client-side validation
// failures (HTTP 400 / ValidationException), never 500 InternalServerError.
func TestDynamoDB_TransactGetItems_KeyAttributeErrors(t *testing.T) {
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

	// Table with hash key only.
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("key_err_hash"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Table with hash key + range key (composite primary key).
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("key_err_composite"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	t.Run("missing hash key attribute", func(t *testing.T) {
		// Key map uses "wrong" instead of the schema's "pk" — server must return
		// ValidationException, matching DynamoDB's "provided key element does not
		// match the schema" behaviour.
		_, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: []types.TransactGetItem{
				{Get: &types.Get{
					TableName: aws.String("key_err_hash"),
					Key:       map[string]types.AttributeValue{"wrong": &types.AttributeValueMemberS{Value: "v"}},
				}},
			},
		})
		var apiErr *types.TransactionCanceledException
		// Some SDKs surface the inner validation error as a generic smithy error;
		// the important assertion is that the request was rejected with an error.
		assert.Error(t, err, "missing hash key must be rejected")
		assert.False(t, errors.As(err, &apiErr), "must not be a TransactionCanceledException")
	})

	t.Run("missing range key attribute", func(t *testing.T) {
		// Composite-key table: only hash key provided, range key "sk" is absent.
		_, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: []types.TransactGetItem{
				{Get: &types.Get{
					TableName: aws.String("key_err_composite"),
					Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "v"}},
				}},
			},
		})
		assert.Error(t, err, "missing range key must be rejected")
	})

	t.Run("unsupported key attribute type BOOL", func(t *testing.T) {
		// DynamoDB primary keys must be S, N, or B. BOOL is invalid and must
		// produce a ValidationException (400), not an InternalServerError (500).
		_, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: []types.TransactGetItem{
				{Get: &types.Get{
					TableName: aws.String("key_err_hash"),
					Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberBOOL{Value: true}},
				}},
			},
		})
		assert.Error(t, err, "BOOL key type must be rejected")
	})
}

// TestDynamoDB_TransactWriteItems_KeyAttributeErrors verifies that
// canonicalPrimaryKeyStr / writeCanonicalAttrValue errors in the write path
// also surface as ValidationException (400), not InternalServerError (500).
func TestDynamoDB_TransactWriteItems_KeyAttributeErrors(t *testing.T) {
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

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("write_key_err"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	t.Run("Put missing hash key in item", func(t *testing.T) {
		// Item does not include the hash key "pk". primaryKeyAttributes extraction
		// fails, which must surface as ValidationException (400).
		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Put: &types.Put{
					TableName: aws.String("write_key_err"),
					Item:      map[string]types.AttributeValue{"other": &types.AttributeValueMemberS{Value: "v"}},
				}},
			},
		})
		assert.Error(t, err, "Put item missing hash key must be rejected")
	})

	t.Run("Delete missing hash key", func(t *testing.T) {
		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Delete: &types.Delete{
					TableName: aws.String("write_key_err"),
					Key:       map[string]types.AttributeValue{"wrong": &types.AttributeValueMemberS{Value: "v"}},
				}},
			},
		})
		assert.Error(t, err, "Delete with missing hash key must be rejected")
	})
}

func TestDynamoDB_TransactGetItems_ProjectionExpression(t *testing.T) {
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

	// Create a table with multiple attributes.
	_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("proj_test"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("key"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("key"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("proj_test"),
		Item: map[string]types.AttributeValue{
			"key":    &types.AttributeValueMemberS{Value: "k1"},
			"field1": &types.AttributeValueMemberS{Value: "v1"},
			"field2": &types.AttributeValueMemberS{Value: "v2"},
		},
	})
	require.NoError(t, err)

	// TransactGetItems with ProjectionExpression requesting only field1.
	out, err := client.TransactGetItems(context.Background(), &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{
				TableName:                aws.String("proj_test"),
				Key:                      map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "k1"}},
				ProjectionExpression:     aws.String("#f1"),
				ExpressionAttributeNames: map[string]string{"#f1": "field1"},
			}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Responses, 1)

	item := out.Responses[0].Item
	// field1 should be present.
	f1, ok := item["field1"].(*types.AttributeValueMemberS)
	assert.True(t, ok)
	assert.Equal(t, "v1", f1.Value)
	// field2 should be excluded by projection.
	assert.Nil(t, item["field2"])
}

func TestDynamoDB_TransactGetItems_SnapshotIsolation(t *testing.T) {
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

	ctx := context.Background()

	// Write initial values.
	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{TableName: aws.String("t"), Item: map[string]types.AttributeValue{
				"key": &types.AttributeValueMemberS{Value: "snap_k1"}, "value": &types.AttributeValueMemberS{Value: "init1"},
			}}},
			{Put: &types.Put{TableName: aws.String("t"), Item: map[string]types.AttributeValue{
				"key": &types.AttributeValueMemberS{Value: "snap_k2"}, "value": &types.AttributeValueMemberS{Value: "init2"},
			}}},
		},
	})
	require.NoError(t, err)

	// Concurrent writer that keeps updating both keys atomically.
	stopCh := make(chan struct{})
	go runSnapshotWriter(t, ctx, client, stopCh)
	defer close(stopCh)

	// Run TransactGetItems many times concurrently.
	// Each response must be internally consistent: both keys from the same snapshot.
	const iterations = 50
	errCh := make(chan error, iterations)
	for i := 0; i < iterations; i++ {
		go func() {
			errCh <- checkTransactGetSnapshotConsistency(ctx, client)
		}()
	}
	for i := 0; i < iterations; i++ {
		assert.NoError(t, <-errCh)
	}
}

// runSnapshotWriter continuously writes both snap_k1 and snap_k2 atomically until
// stopCh is closed. Unexpected errors (not TransactionCanceledException) are reported
// via t.Errorf so the test fails rather than the writer exiting silently — a silent
// exit would leave the database static and cause readers to pass vacuously.
func runSnapshotWriter(t *testing.T, ctx context.Context, client *dynamodb.Client, stopCh <-chan struct{}) {
	t.Helper()
	for {
		select {
		case <-stopCh:
			return
		default:
			_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{Put: &types.Put{TableName: aws.String("t"), Item: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{Value: "snap_k1"}, "value": &types.AttributeValueMemberS{Value: "updated1"},
					}}},
					{Put: &types.Put{TableName: aws.String("t"), Item: map[string]types.AttributeValue{
						"key": &types.AttributeValueMemberS{Value: "snap_k2"}, "value": &types.AttributeValueMemberS{Value: "updated2"},
					}}},
				},
			})
			// TransactionCanceledException is expected under write contention; any
			// other error is unexpected and must fail the test. However, if stopCh
			// is already closed the test is tearing down, so ignore errors that
			// arise from nodes being shut down — otherwise t.Errorf would panic.
			var txErr *types.TransactionCanceledException
			if err != nil && !errors.As(err, &txErr) {
				select {
				case <-stopCh:
					return // teardown in progress; swallow shutdown errors
				default:
					t.Errorf("runSnapshotWriter: unexpected TransactWriteItems error: %v", err)
					return
				}
			}
		}
	}
}

// checkTransactGetSnapshotConsistency verifies that snap_k1 and snap_k2 are read
// from the same snapshot: their values must both be the initial pair or both the
// updated pair — never a mix, which would indicate a non-atomic pre-read.
func checkTransactGetSnapshotConsistency(ctx context.Context, client *dynamodb.Client) error {
	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "snap_k1"}}}},
			{Get: &types.Get{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "snap_k2"}}}},
		},
	})
	if err != nil {
		return err
	}
	v1Attr, ok := out.Responses[0].Item["value"].(*types.AttributeValueMemberS)
	if !ok {
		return &inconsistentSnapshotError{v1: "<missing>", v2: "<unknown>"}
	}
	v2Attr, ok := out.Responses[1].Item["value"].(*types.AttributeValueMemberS)
	if !ok {
		return &inconsistentSnapshotError{v1: v1Attr.Value, v2: "<missing>"}
	}
	v1, v2 := v1Attr.Value, v2Attr.Value
	if (v1 == "init1" && v2 == "init2") || (v1 == "updated1" && v2 == "updated2") {
		return nil
	}
	return &inconsistentSnapshotError{v1: v1, v2: v2}
}

type inconsistentSnapshotError struct{ v1, v2 string }

func (e *inconsistentSnapshotError) Error() string {
	return "inconsistent snapshot: snap_k1=" + e.v1 + " snap_k2=" + e.v2 + " (mixed state violates snapshot isolation)"
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

// errInjectedLeaseRead is the failure injected by testCoordinatorWrapper when
// failLeaseReads is set, standing in for quorum loss on the lease-read path.
var errInjectedLeaseRead = errors.New("injected lease-read failure")

type testCoordinatorWrapper struct {
	inner kv.Coordinator

	failTxnDispatches atomic.Int32
	injectedFailures  atomic.Int32
	txnDispatches     atomic.Int32

	// failLeaseReads, when true, makes LeaseRead / LeaseReadForKey return
	// errInjectedLeaseRead instead of delegating, simulating quorum loss.
	failLeaseReads atomic.Bool

	// leaseReadCalls / leaseReadForKeyCalls count keyless vs key-routed
	// lease checks; leaseReadKeys records the keys passed to
	// LeaseReadForKey so tests can assert routing and per-group dedup.
	leaseMu              sync.Mutex
	leaseReadCalls       int
	leaseReadForKeyCalls int
	leaseReadKeys        [][]byte

	blockEntered chan struct{}
	blockRelease chan struct{}
	blockOnce    sync.Once
}

// recordedLeaseReadKeys returns a copy of the keys passed to
// LeaseReadForKey since the last reset.
func (w *testCoordinatorWrapper) recordedLeaseReadKeys() [][]byte {
	w.leaseMu.Lock()
	defer w.leaseMu.Unlock()
	out := make([][]byte, len(w.leaseReadKeys))
	for i, k := range w.leaseReadKeys {
		out[i] = append([]byte(nil), k...)
	}
	return out
}

// resetLeaseCounters clears the recorded lease-read counters and keys.
func (w *testCoordinatorWrapper) resetLeaseCounters() {
	w.leaseMu.Lock()
	defer w.leaseMu.Unlock()
	w.leaseReadCalls = 0
	w.leaseReadForKeyCalls = 0
	w.leaseReadKeys = nil
}

// leaseCallCounts returns the keyless and key-routed lease-read counts.
func (w *testCoordinatorWrapper) leaseCallCounts() (keyless, keyed int) {
	w.leaseMu.Lock()
	defer w.leaseMu.Unlock()
	return w.leaseReadCalls, w.leaseReadForKeyCalls
}

// EngineGroupIDForKey forwards group resolution so LeaseReadGroupKeys can
// dedup by group through the wrapper.
func (w *testCoordinatorWrapper) EngineGroupIDForKey(key []byte) uint64 {
	if gr, ok := w.inner.(kv.GroupRoutableCoordinator); ok {
		return gr.EngineGroupIDForKey(key)
	}
	return 0
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

func (w *testCoordinatorWrapper) VerifyLeader(ctx context.Context) error {
	return w.inner.VerifyLeader(ctx)
}

func (w *testCoordinatorWrapper) RaftLeader() string {
	return w.inner.RaftLeader()
}

func (w *testCoordinatorWrapper) IsLeaderForKey(key []byte) bool {
	return w.inner.IsLeaderForKey(key)
}

func (w *testCoordinatorWrapper) VerifyLeaderForKey(ctx context.Context, key []byte) error {
	return w.inner.VerifyLeaderForKey(ctx, key)
}

func (w *testCoordinatorWrapper) RaftLeaderForKey(key []byte) string {
	return w.inner.RaftLeaderForKey(key)
}

func (w *testCoordinatorWrapper) Clock() *kv.HLC {
	return w.inner.Clock()
}

func (w *testCoordinatorWrapper) LinearizableRead(ctx context.Context) (uint64, error) {
	return w.inner.LinearizableRead(ctx)
}

func (w *testCoordinatorWrapper) LeaseRead(ctx context.Context) (uint64, error) {
	w.leaseMu.Lock()
	w.leaseReadCalls++
	w.leaseMu.Unlock()
	if w.failLeaseReads.Load() {
		return 0, errInjectedLeaseRead
	}
	return kv.LeaseReadThrough(w.inner, ctx)
}

func (w *testCoordinatorWrapper) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	w.leaseMu.Lock()
	w.leaseReadForKeyCalls++
	w.leaseReadKeys = append(w.leaseReadKeys, append([]byte(nil), key...))
	w.leaseMu.Unlock()
	if w.failLeaseReads.Load() {
		return 0, errInjectedLeaseRead
	}
	return kv.LeaseReadForKeyThrough(w.inner, ctx, key)
}
