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
# Per docs/design/2026_05_28_partial_tla_safety_spec.md §7.2.
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
	@# Capture the gap run output so we can validate not just that TLC
	@# exited non-zero but that the failure reason is exactly the HLC-4
	@# invariant violation we expect.  Without this check a parse error,
	@# deadlock, JVM crash, or different invariant violation would silently
	@# count as "expected gap counterexample" (codex P2 on PR #856 round 2).
	@cd tla/hlc && \
	  out=$$(java -XX:+UseParallelGC \
	    -cp ../../$(TLA_JAR) -DTLA-Library=$(TLA_LIB) \
	    tlc2.TLC -nowarning -config MCHLC_gap.cfg MCHLC.tla 2>&1) ; \
	  rc=$$? ; \
	  printf '%s\n' "$$out" ; \
	  if [ "$$rc" -eq 0 ]; then \
	    echo ; \
	    echo "ERROR: MCHLC_gap.cfg unexpectedly passed."; \
	    echo "  The gap configuration disables HLC-4 preconditions (ii)+(iii);"; \
	    echo "  TLC was supposed to surface a counterexample showing why those"; \
	    echo "  preconditions are necessary. A clean pass means either the spec"; \
	    echo "  no longer encodes the gap correctly or the safety guards leaked"; \
	    echo "  past the EnableSafety toggle."; \
	    exit 1; \
	  fi ; \
	  if printf '%s\n' "$$out" | grep -qF "Invariant HLC4_NoRegressionAcrossTerms is violated"; then \
	    echo ; \
	    echo "OK: MCHLC_gap.cfg failed as designed (HLC-4 counterexample)."; \
	  else \
	    echo ; \
	    echo "ERROR: MCHLC_gap.cfg failed, but the reason is NOT a HLC-4 violation."; \
	    echo "  Expected substring in TLC output:"; \
	    echo "    'Invariant HLC4_NoRegressionAcrossTerms is violated'"; \
	    echo "  The non-zero exit may indicate a parse error, deadlock, JVM"; \
	    echo "  crash, or a different invariant breaking — review the output"; \
	    echo "  above before treating this as a regression in the gap evidence."; \
	    exit 1; \
	  fi
	@echo
	@echo "tla-check: all model-check outcomes match the design contract."
