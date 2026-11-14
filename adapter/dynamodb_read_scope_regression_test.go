package adapter

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type keyedReadScopeStore struct {
	store.MVCCStore
	keys [][][]byte
}

func (s *keyedReadScopeStore) CommittedTimestampFloorForKeys(_ context.Context, keys [][]byte) (uint64, error) {
	s.keys = append(s.keys, keys)
	return s.LastCommitTS(), nil
}
func (s *keyedReadScopeStore) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return 0, errors.New("unrelated group unavailable")
}
func scopedDynamoFixture(t *testing.T) (*DynamoDBServer, *keyedReadScopeStore) {
	t.Helper()
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	server := NewDynamoDBServer(nil, base, newLocalAdapterCoordinator(base))
	t.Cleanup(server.Stop)
	createTableForItemTests(t, server, "items")
	require.NoError(t, putItemForTest(t.Context(), server, "items", AdminItem{Attributes: map[string]AdminAttributeValue{"id": stringAttr("one"), "value": stringAttr("present")}}))
	st := &keyedReadScopeStore{MVCCStore: base}
	server.store = st
	return server, st
}
func TestAdminGetItemFencesSchemaAndDerivedItemKey(t *testing.T) {
	server, st := scopedDynamoFixture(t)
	item, found, err := server.AdminGetItem(t.Context(), fullAdminPrincipal, "items", map[string]AdminAttributeValue{"id": stringAttr("one")})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "present", *item.Attributes["value"].S)
	require.GreaterOrEqual(t, len(st.keys), 2)
	require.Len(t, st.keys[len(st.keys)-1], 2)
}
func TestDynamoStrongSchemaReadUsesTableKeyedFence(t *testing.T) {
	server, st := scopedDynamoFixture(t)
	request := kv.NewReadRequest(t.Context(), server.coordinator, server.store, nil)
	defer request.Close()
	strong := true
	schema, _, err := server.prepareReadSchema(request.Context(), "items", "", "", "", nil, &strong)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.NotEmpty(t, st.keys)
	require.Equal(t, [][]byte{dynamoTableMetaKey("items")}, st.keys[len(st.keys)-1])
}
