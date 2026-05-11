package adapter

import "testing"

func TestNormalizeRedisErrorReply(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"bare leader-not-found gets NOTLEADER prefix",
			"leader not found",
			"NOTLEADER leader not found"},
		{"wrapped leader-not-found suffix gets NOTLEADER prefix",
			"verify leader: leader not found",
			"NOTLEADER verify leader: leader not found"},
		{"not-leader suffix gets NOTLEADER prefix",
			"raft engine: not leader",
			"NOTLEADER raft engine: not leader"},
		{"leadership-lost suffix gets NOTLEADER prefix",
			"propose: leadership lost",
			"NOTLEADER propose: leadership lost"},
		{"leadership-transfer-in-progress suffix gets NOTLEADER prefix",
			"raft: leadership transfer in progress",
			"NOTLEADER raft: leadership transfer in progress"},
		{"already-prefixed NOTLEADER reply is unchanged",
			"NOTLEADER leader not found",
			"NOTLEADER leader not found"},
		{"ERR reply unchanged",
			"ERR MULTI calls can not be nested",
			"ERR MULTI calls can not be nested"},
		{"WRONGTYPE reply unchanged",
			"WRONGTYPE operation against a key holding the wrong kind of value",
			"WRONGTYPE operation against a key holding the wrong kind of value"},
		{"unrelated error unchanged",
			"some random error",
			"some random error"},
		{"empty string unchanged",
			"",
			""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeRedisErrorReply(tc.in)
			if got != tc.want {
				t.Fatalf("normalizeRedisErrorReply(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
