package main

import "testing"

func TestOptionalBoolEnv(t *testing.T) {
	t.Setenv("ELASTICKV_TEST_BOOL", "")
	if got := optionalBoolEnv("ELASTICKV_TEST_BOOL", true); !got {
		t.Fatal("empty env should use default true")
	}
	if got := optionalBoolEnv("ELASTICKV_TEST_BOOL", false); got {
		t.Fatal("empty env should use default false")
	}

	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{name: "true", raw: "true", want: true},
		{name: "one", raw: "1", want: true},
		{name: "false", raw: "false", want: false},
		{name: "zero", raw: "0", want: false},
		{name: "trimmed", raw: " false ", want: false},
		{name: "invalid uses default", raw: "disabled", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ELASTICKV_TEST_BOOL", tc.raw)
			if got := optionalBoolEnv("ELASTICKV_TEST_BOOL", true); got != tc.want {
				t.Fatalf("optionalBoolEnv()=%v, want %v", got, tc.want)
			}
		})
	}
}
