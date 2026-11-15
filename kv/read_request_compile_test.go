package kv

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestReadRequestRejectsIntegerTimestampsAtCompileTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "go", "build", "./testdata/read_request_integer").CombinedOutput()
	if err == nil {
		t.Fatal("Get/Scan accepted integer timestamps")
	}
	text := string(output)
	if !strings.Contains(text, "cannot use uint64(42)") || !strings.Contains(text, "ReadSnapshot") {
		t.Fatalf("negative fixture failed for another reason: %s", text)
	}
}
