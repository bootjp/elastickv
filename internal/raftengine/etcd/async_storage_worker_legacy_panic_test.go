package etcd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageWorkerLegacyNilPanic(t *testing.T) {
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",panicnil=1")
	for _, panics := range []bool{false, true} {
		name := "nil_success"
		if panics {
			name = "nil_panic"
		}
		t.Run(name, func(t *testing.T) {
			worker := testStorageWorker(t, 1)
			budget := newStorageWorkBudget(1, 1)
			require.NoError(t, worker.trySubmit(storageWork{run: func() (any, error) {
				if panics {
					panic(nil)
				}
				return nil, nil
			}}, budget))
			completion := <-worker.completions
			t.Cleanup(completion.Release)
			if panics {
				require.ErrorIs(t, completion.err, errStorageWorkerPanic)
			} else {
				require.NoError(t, completion.err)
			}
			require.Nil(t, completion.value)
			completion.Release()
			requireStorageWorkUsage(t, budget, 0, 0)
		})
	}
}
