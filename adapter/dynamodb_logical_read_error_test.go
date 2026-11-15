package adapter

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type failingDynamoItemReadStore struct {
	store.MVCCStore
	err error
	key []byte
}

func (s *failingDynamoItemReadStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if len(s.key) == 0 || bytes.Equal(s.key, key) {
		return nil, s.err
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func TestDynamoItemPlansKeepStorageReadFailureInternal(t *testing.T) {
	const privateKey = "private-physical-key"
	readErr := errors.New("read failed for " + privateKey)
	d, st := scopedDynamoFixture(t)
	schema, found, err := d.loadTableSchemaAt(t.Context(), "items", st.LastCommitTS())
	require.NoError(t, err)
	require.True(t, found)
	key := map[string]attributeValue{"id": newStringAttributeValue("one")}
	itemKey, err := schema.itemKeyFromAttributes(key)
	require.NoError(t, err)
	d.store = &failingDynamoItemReadStore{MVCCStore: st.MVCCStore, err: readErr, key: itemKey}

	tests := []struct {
		name string
		plan func() error
	}{
		{"put", func() error {
			_, err := d.preparePutItemWrite(t.Context(), putItemInput{TableName: "items", Item: key}, st.LastCommitTS())
			return err
		}},
		{"update", func() error {
			_, err := d.prepareUpdateItemWrite(t.Context(), updateItemInput{TableName: "items", Key: key}, st.LastCommitTS())
			return err
		}},
		{"delete", func() error {
			_, err := d.prepareDeleteItemWrite(t.Context(), deleteItemInput{TableName: "items", Key: key}, st.LastCommitTS())
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plan()
			require.ErrorIs(t, err, readErr)
			rec := httptest.NewRecorder()
			writeDynamoErrorFromErr(rec, err)
			require.Equal(t, http.StatusInternalServerError, rec.Code)
			require.NotContains(t, rec.Body.String(), privateKey)
			require.Equal(t, dynamoErrInternal, rec.Header().Get("x-amzn-ErrorType"))
		})
	}
}

func TestDynamoTransactPlansKeepStorageReadFailureInternal(t *testing.T) {
	const privateKey = "private-physical-key"
	readErr := errors.New("read failed for " + privateKey)
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	d := &DynamoDBServer{store: &failingDynamoItemReadStore{MVCCStore: base, err: readErr}}
	schema := dedupItemTable()
	key := map[string]attributeValue{"pk": newStringAttributeValue("one")}

	tests := []struct {
		name string
		plan func() error
	}{
		{"put", func() error {
			_, err := d.buildTransactPutPlan(t.Context(), schema, putItemInput{Item: key}, 1)
			return err
		}},
		{"update", func() error {
			_, err := d.buildTransactUpdatePlan(t.Context(), schema, transactUpdateInput{Key: key}, 1)
			return err
		}},
		{"delete", func() error {
			_, err := d.buildTransactDeletePlan(t.Context(), schema, transactDeleteInput{Key: key}, 1)
			return err
		}},
		{"condition", func() error {
			_, err := d.buildTransactConditionCheckPlan(t.Context(), schema, transactConditionInput{
				Key: key, ConditionExpression: "attribute_exists(pk)",
			}, 1)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plan()
			require.ErrorIs(t, err, readErr)
			rec := httptest.NewRecorder()
			writeDynamoErrorFromErr(rec, err)
			require.Equal(t, http.StatusInternalServerError, rec.Code)
			require.NotContains(t, rec.Body.String(), privateKey)
			require.Equal(t, dynamoErrInternal, rec.Header().Get("x-amzn-ErrorType"))
		})
	}
}
