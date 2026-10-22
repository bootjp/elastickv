// Package storetest provides temporary stores with automatic cleanup.
package storetest

import (
	"testing"

	"github.com/bootjp/elastickv/store"
)

// NewPebble opens a store under tb.TempDir and closes it before removing it.
func NewPebble(tb testing.TB, opts ...store.PebbleStoreOption) store.MVCCStore {
	tb.Helper()
	st, err := store.NewPebbleStore(tb.TempDir(), opts...)
	if err != nil {
		tb.Fatalf("open Pebble: %v", err)
	}
	tb.Cleanup(func() {
		if err := st.Close(); err != nil {
			tb.Errorf("close Pebble: %v", err)
		}
	})
	return st
}
