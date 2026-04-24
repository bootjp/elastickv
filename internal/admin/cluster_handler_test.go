package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestClusterHandler_HappyPath(t *testing.T) {
	source := ClusterInfoFunc(func(_ context.Context) (ClusterInfo, error) {
		return ClusterInfo{
			NodeID:  "node-1",
			Version: "0.42.0",
			Groups: []GroupInfo{
				{GroupID: 1, LeaderID: "node-1", IsLeader: true, Members: []string{"node-1", "node-2", "node-3"}},
				{GroupID: 2, LeaderID: "node-2", Members: []string{"node-1", "node-2", "node-3"}},
			},
		}, nil
	})
	h := NewClusterHandler(source)
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got ClusterInfo
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "node-1", got.NodeID)
	require.Equal(t, "0.42.0", got.Version)
	require.Len(t, got.Groups, 2)
	require.True(t, got.Groups[0].IsLeader)
	require.False(t, got.Timestamp.IsZero())
}

func TestClusterHandler_PreservesExplicitTimestamp(t *testing.T) {
	ts := time.Date(2026, 4, 24, 10, 0, 0, 0, time.UTC)
	source := ClusterInfoFunc(func(_ context.Context) (ClusterInfo, error) {
		return ClusterInfo{NodeID: "n", Version: "v", Timestamp: ts}, nil
	})
	h := NewClusterHandler(source)
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var got ClusterInfo
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, ts, got.Timestamp.UTC())
}

func TestClusterHandler_SourceErrorReturns500(t *testing.T) {
	source := ClusterInfoFunc(func(_ context.Context) (ClusterInfo, error) {
		return ClusterInfo{}, errors.New("describe failed")
	})
	h := NewClusterHandler(source)
	req := httptest.NewRequest(http.MethodGet, "/admin/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "cluster_describe_failed")
}

func TestClusterHandler_OnlyGET(t *testing.T) {
	h := NewClusterHandler(ClusterInfoFunc(func(_ context.Context) (ClusterInfo, error) {
		return ClusterInfo{}, nil
	}))
	req := httptest.NewRequest(http.MethodPost, "/admin/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
