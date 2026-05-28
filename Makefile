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
# Per docs/design/2026_05_28_proposed_tla_safety_spec.md §7.2.
# Downloads a checksum-pinned tla2tools.jar on first use, then runs TLC on
# both MCHLC.cfg (correct design — expected PASS) and MCHLC_gap.cfg
# (preconditions disabled — expected FAIL on HLC-4 with a counterexample
# that motivates the implementation gaps).
TLA_VERSION  := v1.8.0
TLA_JAR      := .cache/tla/tla2tools.jar
TLA_SHA256   := 237332bdcc79a35c7d26efa7b82c77c85c2744591c5598673a8a45085ff2a4fb
TLA_URL      := https://github.com/tlaplus/tlaplus/releases/download/$(TLA_VERSION)/tla2tools.jar
TLA_LIB      := ../lib

.PHONY: tla-check tla-tools

tla-tools: $(TLA_JAR)

$(TLA_JAR):
	@mkdir -p $(dir $(TLA_JAR))
	@echo "Downloading tla2tools.jar $(TLA_VERSION)..."
	@curl -fsSL -o $(TLA_JAR).tmp $(TLA_URL)
	@actual=$$(shasum -a 256 $(TLA_JAR).tmp | awk '{print $$1}'); \
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
	@echo "================================================================"
	@echo "  TLC: tla/hlc/MCHLC.cfg  (correct design, expected PASS)"
	@echo "================================================================"
	@cd tla/hlc && java -XX:+UseParallelGC \
		-cp ../../$(TLA_JAR) -DTLA-Library=$(TLA_LIB) \
		tlc2.TLC -nowarning -config MCHLC.cfg MCHLC.tla
	@echo
	@echo "================================================================"
	@echo "  TLC: tla/hlc/MCHLC_gap.cfg  (no preconditions, expected FAIL)"
	@echo "================================================================"
	@cd tla/hlc && if java -XX:+UseParallelGC \
		-cp ../../$(TLA_JAR) -DTLA-Library=$(TLA_LIB) \
		tlc2.TLC -nowarning -config MCHLC_gap.cfg MCHLC.tla; then \
		echo "ERROR: MCHLC_gap.cfg unexpectedly passed."; \
		echo "  The gap configuration disables HLC-4 preconditions (ii)+(iii);"; \
		echo "  TLC was supposed to surface a counterexample showing why those"; \
		echo "  preconditions are necessary. A clean pass means either the spec"; \
		echo "  no longer encodes the gap correctly or the safety guards leaked"; \
		echo "  past the EnableSafety toggle."; \
		exit 1; \
	else \
		echo; \
		echo "OK: MCHLC_gap.cfg failed as designed (HLC-4 counterexample)."; \
	fi
	@echo
	@echo "tla-check: all model-check outcomes match the design contract."
