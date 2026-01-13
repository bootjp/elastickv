package store

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"pgregory.net/rapid"
)

func TestMVCCStore_Property_PutGet(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Filter(func(b []byte) bool { return len(b) > 0 }).Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		ts := rapid.Uint64Range(0, ^uint64(0)-100).Draw(t, "ts")

		s := NewMVCCStore()
		ctx := context.Background()

		err := s.PutAt(ctx, key, value, ts, 0)
		if err != nil {
			t.Fatalf("PutAt failed: %v", err)
		}

		actualTS, ok, err := s.LatestCommitTS(ctx, key)
		if err != nil || !ok {
			t.Fatalf("LatestCommitTS failed: %v, ok=%v", err, ok)
		}

		got, err := s.GetAt(ctx, key, actualTS)
		if err != nil {
			t.Fatalf("GetAt(%d) failed: %v", actualTS, err)
		}
		if !bytes.Equal(got, value) {
			t.Errorf("GetAt(%d) = %q, want %q", actualTS, got, value)
		}

		laterTS := rapid.Uint64Range(actualTS, ^uint64(0)).Draw(t, "laterTS")
		gotLater, err := s.GetAt(ctx, key, laterTS)
		if err != nil {
			t.Fatalf("GetAt(%d) failed: %v", laterTS, err)
		}
		if !bytes.Equal(gotLater, value) {
			t.Errorf("GetAt(%d) = %q, want %q", laterTS, gotLater, value)
		}
	})
}

func TestMVCCStore_Property_Delete(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Filter(func(b []byte) bool { return len(b) > 0 }).Draw(t, "key")
		value := rapid.SliceOf(rapid.Byte()).Draw(t, "value")
		ts := rapid.Uint64Range(0, ^uint64(0)-100).Draw(t, "ts")

		s := NewMVCCStore()
		ctx := context.Background()

		_ = s.PutAt(ctx, key, value, ts, 0)
		actualTS, _, _ := s.LatestCommitTS(ctx, key)

		delTS := rapid.Uint64Range(actualTS+1, ^uint64(0)-1).Draw(t, "delTS")
		err := s.DeleteAt(ctx, key, delTS)
		if err != nil {
			t.Fatalf("DeleteAt failed: %v", err)
		}

		actualDelTS, ok, err := s.LatestCommitTS(ctx, key)
		if err != nil || !ok {
			t.Fatalf("LatestCommitTS failed: %v, ok=%v", err, ok)
		}

		_, err = s.GetAt(ctx, key, actualDelTS)
		if err == nil || !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("GetAt(%d) expected ErrKeyNotFound, got %v", actualDelTS, err)
		}
	})
}
