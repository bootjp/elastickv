package adapter

import "testing"

func TestExtractSigV4Signature(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "standard SigV4 header",
			input: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123def456",
			want:  "abc123def456",
		},
		{
			name:  "signature with extra whitespace",
			input: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature= abc123 ",
			want:  "abc123",
		},
		{
			name:  "different parameter order",
			input: "AWS4-HMAC-SHA256 SignedHeaders=host;x-amz-date, Credential=AKID/20240101/us-east-1/s3/aws4_request, Signature=deadbeef",
			want:  "deadbeef",
		},
		{
			name:  "missing signature",
			input: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host",
			want:  "",
		},
		{
			name:  "empty header",
			input: "",
			want:  "",
		},
		{
			name:  "no space separator",
			input: "AWS4-HMAC-SHA256Credential=foo",
			want:  "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractSigV4Signature(tc.input)
			if got != tc.want {
				t.Errorf("extractSigV4Signature(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
