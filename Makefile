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
TLA_VERSION  := v1.8.0
TLA_JAR      := .cache/tla/tla2tools.jar
TLA_SHA256   := 33de7da9ce1b7fffb9d1c184021178dbb051747be48504e65c584c423721a32e
TLA_URL      := https://github.com/tlaplus/tlaplus/releases/download/$(TLA_VERSION)/tla2tools.jar
TLA_LIB      := ../lib

.PHONY: tla-check tla-tools

tla-tools: $(TLA_JAR)

$(TLA_JAR):
	@mkdir -p $(dir $(TLA_JAR))
	@echo "Downloading tla2tools.jar $(TLA_VERSION)..."
	@curl -fsSL -o $(TLA_JAR).tmp $(TLA_URL)
	@# Prefer sha256sum (GNU coreutils, universal on Linux); fall back to
	@# shasum -a 256 (default on macOS).  Either yields the same hex
	@# digest in the first whitespace-delimited field.
	@if command -v sha256sum >/dev/null 2>&1; then \
		actual=$$(sha256sum $(TLA_JAR).tmp | awk '{print $$1}'); \
	elif command -v shasum >/dev/null 2>&1; then \
		actual=$$(shasum -a 256 $(TLA_JAR).tmp | awk '{print $$1}'); \
	else \
		echo "ERROR: neither sha256sum nor shasum is available."; \
		rm -f $(TLA_JAR).tmp; \
		exit 1; \
	fi; \
	if [ "$$actual" != "$(TLA_SHA256)" ]; then \
		echo "ERROR: tla2tools.jar SHA-256 mismatch."; \
		echo "  expected: $(TLA_SHA256)"; \
		echo "  actual:   $$actual"; \
		rm -f $(TLA_JAR).tmp; \
		exit 1; \
	fi
	@mv $(TLA_JAR).tmp $(TLA_JAR)
	@echo "tla2tools.jar ready at $(TLA_JAR) (SHA-256 $(TLA_SHA256))"

tla-check: $(TLA_JAR)
	@# Per-module orchestration lives in scripts/tla-check.sh so adding
	@# M3..M5 only needs an entry in that script's TLA_MODULES array
	@# and a `case` line for the gap-invariant string.  The script does
	@# the safe-config / gap-config pair per module, validates the gap
	@# failure reason via string match, and aggregates exit codes.
	@bash scripts/tla-check.sh
