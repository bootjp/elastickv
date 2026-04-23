BUF_VERSION := 1.55.1
PROTOC_GEN_GO_VERSION := 1.36.11
PROTOC_GEN_GO_GRPC_VERSION := 1.6.1

BUF ?= buf
# BREAKING_AGAINST lets callers override the auto-detected baseline.
BREAKING_AGAINST ?=

.PHONY: all gen check-tools breaking

all: gen

check-tools:
	@command -v $(BUF) >/dev/null 2>&1 || { \
		echo "buf not found; install with:"; \
		echo "  go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)"; \
		exit 1; \
	}
	@installed=$$($(BUF) --version 2>/dev/null); \
	if [ "$$installed" != "$(BUF_VERSION)" ]; then \
		echo "buf version mismatch: expected $(BUF_VERSION), got $$installed"; \
		echo "install with: go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)"; \
		exit 1; \
	fi
	@command -v protoc-gen-go >/dev/null 2>&1 || { \
		echo "protoc-gen-go not found; install with:"; \
		echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@v$(PROTOC_GEN_GO_VERSION)"; \
		exit 1; \
	}
	@installed=$$(protoc-gen-go --version 2>/dev/null); \
	if [ "$$installed" != "protoc-gen-go v$(PROTOC_GEN_GO_VERSION)" ]; then \
		echo "protoc-gen-go version mismatch: expected v$(PROTOC_GEN_GO_VERSION), got $$installed"; \
		echo "install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@v$(PROTOC_GEN_GO_VERSION)"; \
		exit 1; \
	fi
	@command -v protoc-gen-go-grpc >/dev/null 2>&1 || { \
		echo "protoc-gen-go-grpc not found; install with:"; \
		echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v$(PROTOC_GEN_GO_GRPC_VERSION)"; \
		exit 1; \
	}
	@installed=$$(protoc-gen-go-grpc --version 2>/dev/null); \
	if [ "$$installed" != "protoc-gen-go-grpc $(PROTOC_GEN_GO_GRPC_VERSION)" ]; then \
		echo "protoc-gen-go-grpc version mismatch: expected $(PROTOC_GEN_GO_GRPC_VERSION), got $$installed"; \
		echo "install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v$(PROTOC_GEN_GO_GRPC_VERSION)"; \
		exit 1; \
	fi

breaking: check-tools
	@baseline=''; \
	if git -C .. rev-parse --verify main >/dev/null 2>&1; then \
		baseline='../.git#subdir=proto,branch=main'; \
	elif git -C .. rev-parse --verify origin/main >/dev/null 2>&1; then \
		baseline='../.git#subdir=proto,ref=refs/remotes/origin/main'; \
	else \
		echo "no breaking baseline found: neither 'main' nor 'origin/main' is reachable"; \
		echo "fetch main first (git fetch origin main) or override with BREAKING_AGAINST=..."; \
		exit 1; \
	fi; \
	if [ -n "$(BREAKING_AGAINST)" ]; then baseline='$(BREAKING_AGAINST)'; fi; \
	$(BUF) breaking --against "$$baseline"

gen: check-tools breaking
	$(BUF) generate
