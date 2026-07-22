package adapter

import (
	"testing"
	"time"
)

func TestWithRedisBlockWaitFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  RedisServerOption
		want time.Duration
	}{
		{
			name: "default",
			want: defaultRedisBlockWaitFallback,
		},
		{
			name: "positive override",
			opt:  WithRedisBlockWaitFallback(42 * time.Millisecond),
			want: 42 * time.Millisecond,
		},
		{
			name: "zero preserves default",
			opt:  WithRedisBlockWaitFallback(0),
			want: defaultRedisBlockWaitFallback,
		},
		{
			name: "negative preserves default",
			opt:  WithRedisBlockWaitFallback(-time.Millisecond),
			want: defaultRedisBlockWaitFallback,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := NewRedisServer(nil, "", nil, nil, nil, nil)
			if tc.opt != nil {
				tc.opt(server)
			}
			if server.blockWaitFallback != tc.want {
				t.Fatalf("blockWaitFallback = %v, want %v", server.blockWaitFallback, tc.want)
			}
		})
	}
}
