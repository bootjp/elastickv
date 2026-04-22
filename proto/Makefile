BUF_VERSION := 1.55.1
PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := 1.6.1

BUF ?= buf
BREAKING_AGAINST ?= ../.git#subdir=proto,branch=main

.PHONY: all gen check-tools breaking

all: gen

check-tools:
	@command -v $(BUF) >/dev/null 2>&1 || { \
		echo "buf not found; install with:"; \
		echo "  go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)"; \
		exit 1; \
	}
	@installed_buf=$$($(BUF) --version 2>/dev/null); \
	if [ "$$installed_buf" != "$(BUF_VERSION)" ]; then \
		echo "warning: buf version mismatch (expected $(BUF_VERSION), got $$installed_buf)"; \
	fi
	@command -v protoc-gen-go >/dev/null 2>&1 || { \
		echo "protoc-gen-go not found; install with:"; \
		echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)"; \
		exit 1; \
	}
	@command -v protoc-gen-go-grpc >/dev/null 2>&1 || { \
		echo "protoc-gen-go-grpc not found; install with:"; \
		echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v$(PROTOC_GEN_GO_GRPC_VERSION)"; \
		exit 1; \
	}

breaking: check-tools
	@if git -C .. rev-parse --verify main >/dev/null 2>&1; then \
		$(BUF) breaking --against '$(BREAKING_AGAINST)'; \
	else \
		echo "skip breaking check: main branch not found"; \
	fi

gen: check-tools breaking
	$(BUF) generate
