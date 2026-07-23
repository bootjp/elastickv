test:
	go test -v -race ./...

run:
	go run cmd/server/demo.go

client:
	go run cmd/client/client.go

lint:
	golangci-lint --config=.golangci.yaml run --fix

gen:
	@$(MAKE)  -C proto gen

# === TLA+ model-check support (M1 deliverable) ===
# Per docs/design/2026_05_28_implemented_tla_safety_spec.md §7.2.
# Downloads a checksum-pinned tla2tools.jar on first use, then runs TLC on
# both MCHLC.cfg (correct design — expected PASS) and MCHLC_gap.cfg
# (preconditions disabled — expected FAIL on HLC-4 with a counterexample
# that motivates the implementation gaps).
# v1.8.0 is a rolling pre-release whose asset is replaced on every upstream
# build. Pin the latest stable release so the checksum remains reproducible.
TLA_VERSION  := v1.7.4
TLA_JAR      := .cache/tla/tla2tools.jar
TLA_SHA256   := 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
TLA_URL      := https://github.com/tlaplus/tlaplus/releases/download/$(TLA_VERSION)/tla2tools.jar
TLA_LIB      := ../lib

.PHONY: tla-check tla-tools

tla-tools:
	@mkdir -p $(dir $(TLA_JAR))
	@set -eu; \
	sha256_file() { \
		if command -v sha256sum >/dev/null 2>&1; then \
			sha256sum "$$1" | awk '{print $$1}'; \
		elif command -v shasum >/dev/null 2>&1; then \
			shasum -a 256 "$$1" | awk '{print $$1}'; \
		else \
			echo "ERROR: neither sha256sum nor shasum is available." >&2; \
			return 1; \
		fi; \
	}; \
	actual=""; \
	if [ -f "$(TLA_JAR)" ]; then actual=$$(sha256_file "$(TLA_JAR)"); fi; \
	if [ "$$actual" = "$(TLA_SHA256)" ]; then \
		echo "tla2tools.jar ready at $(TLA_JAR) (SHA-256 $(TLA_SHA256))"; \
		exit 0; \
	fi; \
	echo "Downloading tla2tools.jar $(TLA_VERSION)..."; \
	trap 'rm -f "$(TLA_JAR).tmp"' EXIT HUP INT TERM; \
	curl -fsSL -o "$(TLA_JAR).tmp" "$(TLA_URL)"; \
	actual=$$(sha256_file "$(TLA_JAR).tmp"); \
	if [ "$$actual" != "$(TLA_SHA256)" ]; then \
		echo "ERROR: tla2tools.jar SHA-256 mismatch."; \
		echo "  expected: $(TLA_SHA256)"; \
		echo "  actual:   $$actual"; \
		exit 1; \
	fi; \
	mv "$(TLA_JAR).tmp" "$(TLA_JAR)"; \
	trap - EXIT HUP INT TERM; \
	echo "tla2tools.jar ready at $(TLA_JAR) (SHA-256 $(TLA_SHA256))"

tla-check: tla-tools
	@# Per-module orchestration lives in scripts/tla-check.sh so adding
	@# M3..M5 only needs an entry in that script's TLA_MODULES array
	@# and a `case` line for the gap-invariant string.  The script does
	@# the safe-config / gap-config pair per module, validates the gap
	@# failure reason via string match, and aggregates exit codes.
	@bash scripts/tla-check.sh
