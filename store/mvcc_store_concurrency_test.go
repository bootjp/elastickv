package store

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStore_ScanConcurrentWithWrites(t *testing.T) {
	ctx := context.Background()
	st := newTestMVCCStore(t)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)

	go func() {
		defer wg.Done()
		commitTS := uint64(1)
		for i := 1; i <= 1000; i++ {
			key := []byte("k" + strconv.Itoa(i%50))
			if err := st.PutAt(ctx, key, []byte("v"), commitTS, 0); err != nil {
				errCh <- err
				return
			}
			commitTS++
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if _, err := st.Scan(ctx, nil, nil, 100); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}
