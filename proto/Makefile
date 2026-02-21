PROTOC_VERSION := libprotoc 29.3
PROTOC_GEN_GO_VERSION := protoc-gen-go v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := protoc-gen-go-grpc 1.6.1

.PHONY: all gen check-tools

all: gen

check-tools:
	@if [ "$$(protoc --version)" != "$(PROTOC_VERSION)" ]; then \
		echo "expected $(PROTOC_VERSION), got $$(protoc --version)"; \
		exit 1; \
	fi
	@if [ "$$(protoc-gen-go --version)" != "$(PROTOC_GEN_GO_VERSION)" ]; then \
		echo "expected $(PROTOC_GEN_GO_VERSION), got $$(protoc-gen-go --version)"; \
		exit 1; \
	fi
	@if [ "$$(protoc-gen-go-grpc --version)" != "$(PROTOC_GEN_GO_GRPC_VERSION)" ]; then \
		echo "expected $(PROTOC_GEN_GO_GRPC_VERSION), got $$(protoc-gen-go-grpc --version)"; \
		exit 1; \
	fi

gen: check-tools
	protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	    service.proto
	protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	    internal.proto
	protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	    distribution.proto
