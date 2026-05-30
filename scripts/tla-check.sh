#!/usr/bin/env bash
# tla-check.sh — run TLC on every module in the elastickv TLA+ suite.
#
# Per docs/design/2026_05_28_partial_tla_safety_spec.md §7.2.  Each
# module under tla/<module>/ ships:
#   MC<MODULE>.tla       — the TLC model module
#   MC<MODULE>.cfg       — the correct-design config (expected PASS)
#   MC<MODULE>_gap.cfg   — the preconditions-disabled config (expected
#                          FAIL with a SPECIFIC invariant counterexample,
#                          checked by string match against
#                          TLA_<MODULE>_GAP_INVARIANT below)
#
# Adding a new module (M3..M5): append to TLA_MODULES, drop the per-
# module gap-invariant string into the case statement, and `make
# tla-check` picks it up automatically.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TLA_JAR="${REPO_ROOT}/.cache/tla/tla2tools.jar"

if [ ! -f "$TLA_JAR" ]; then
    echo "ERROR: ${TLA_JAR} not found — run 'make tla-tools' first." >&2
    exit 1
fi

# Modules to check, in dependency order (libs before consumers).
TLA_MODULES=( "hlc" "occ" "mvcc" "routes" "composed" )

# Per-module list of gap configurations to run.  Each entry is
# "<cfg-stem>|<expected-invariant-string>", one per line.  Modules
# may register more than one gap if multiple safety guards toggle
# independently — composed has two (Composed-1 observed-version
# guard via EnableSafety, Composed-1a current-version fence via
# EnableCurrentFence).
#
# The expected-invariant string is matched against TLC stdout via
# grep -F (literal), so a parse error / deadlock / JVM crash /
# different invariant breaking is NOT silently treated as the
# expected counterexample (codex P2 on PR #856 round 2).
gap_configs_for() {
    case "$1" in
        hlc)      printf '_gap|Invariant HLC4_NoRegressionAcrossTerms is violated\n' ;;
        occ)      printf '_gap|Invariant OCC1_CommitTsAboveStart is violated\n' ;;
        mvcc)     printf '_gap|Invariant MVCC4_NoLostCommitOnSnapshotInstall is violated\n' ;;
        routes)   printf '_gap|Invariant Routes4_NoEngineRegression is violated\n' ;;
        composed)
            printf '_gap|Invariant Composed1_CommitToOwningGroup is violated\n'
            printf '_currentfence_gap|Invariant Composed1a_CommitToCurrentOwner is violated\n'
            ;;
        *)
            echo "ERROR: no gap-config list registered for module '$1'." \
                "Add a case to scripts/tla-check.sh." >&2
            exit 64
            ;;
    esac
}

mc_basename() {
    # tla/<dir>/MC<MODULE>.tla — per-module spelling.  Acronym modules
    # (HLC, OCC, MVCC) are spelled in uppercase; word-modules (Routes,
    # Composed) use TitleCase.  Override per module rather than a
    # one-size-fits-all `tr a-z A-Z`, which would produce ugly
    # `MCROUTES` / `MCCOMPOSED` for the word cases.
    case "$1" in
        hlc)      printf 'MCHLC' ;;
        occ)      printf 'MCOCC' ;;
        mvcc)     printf 'MCMVCC' ;;
        routes)   printf 'MCRoutes' ;;
        composed) printf 'MCComposed' ;;
        *)
            echo "ERROR: no mc_basename mapping for module '$1'." \
                "Add a case to scripts/tla-check.sh." >&2
            exit 64
            ;;
    esac
}

run_tlc() {
    local module="$1"
    local cfg="$2"
    local mc
    # Same subshell-exit propagation concern as the main loop —
    # without `set -e` an `exit 64` from mc_basename would leave
    # mc="" and the subsequent java invocation would try to load
    # `.tla`, which TLC parses as a malformed path.  Fail explicitly.
    if ! mc="$(mc_basename "$module")"; then
        echo "ERROR: mc_basename failed for module '${module}'." >&2
        return 64
    fi
    ( cd "${REPO_ROOT}/tla/${module}" && \
      java -XX:+UseParallelGC \
        -cp "${TLA_JAR}" -DTLA-Library=../lib \
        tlc2.TLC -nowarning -config "${cfg}" "${mc}.tla" )
}

overall_rc=0

for module in "${TLA_MODULES[@]}"; do
    # `set -e` is not in effect (the script uses `set -uo pipefail`),
    # so `exit 64` from mc_basename / gap_invariant_for in a command
    # substitution only terminates the subshell — the parent loop
    # would otherwise continue with an empty `mc` / `gap_inv` and the
    # downstream `grep -qF` would match the empty pattern, silently
    # passing the gap check.  Check both explicitly (gemini HIGH on
    # PR #858 for gap_invariant_for; gemini MEDIUM on PR #862 for
    # mc_basename).
    if ! mc="$(mc_basename "$module")"; then
        echo "ERROR: mc_basename failed for module '${module}' — see error above." >&2
        overall_rc=1
        continue
    fi
    safe_cfg="${mc}.cfg"
    if ! gap_list="$(gap_configs_for "$module")"; then
        echo "ERROR: gap_configs_for failed for module '${module}' — see error above." >&2
        overall_rc=1
        continue
    fi

    echo "================================================================"
    echo "  TLC: tla/${module}/${safe_cfg}  (correct design, expected PASS)"
    echo "================================================================"
    if ! run_tlc "$module" "$safe_cfg"; then
        echo
        echo "ERROR: ${safe_cfg} did not pass — see TLC output above." >&2
        overall_rc=1
        continue
    fi
    echo

    # Iterate every gap config registered for this module.  Each is a
    # `<cfg-stem>|<invariant-string>` line; modules with one gap have
    # one entry, modules with multiple guards (composed) have more.
    #
    # Validate both fields are non-empty BEFORE entering the inner
    # body — a malformed entry that produces an empty gap_inv would
    # silently false-positive at the `grep -qF "$gap_inv"` step below
    # (empty pattern matches every line, so the gap would always
    # "succeed" regardless of whether TLC actually surfaced the
    # expected invariant — gemini medium on PR #878).
    while IFS='|' read -r gap_stem gap_inv; do
        [ -z "$gap_stem" ] && [ -z "$gap_inv" ] && continue
        if [ -z "$gap_stem" ] || [ -z "$gap_inv" ]; then
            echo "ERROR: malformed gap entry for module '${module}'" \
                "— expected '<stem>|<invariant>', got" \
                "stem='${gap_stem}' inv='${gap_inv}'." \
                "Fix gap_configs_for() in scripts/tla-check.sh." >&2
            overall_rc=1
            continue
        fi
        gap_cfg="${mc}${gap_stem}.cfg"
        echo "================================================================"
        echo "  TLC: tla/${module}/${gap_cfg}  (no preconditions, expected FAIL on ${gap_inv})"
        echo "================================================================"
        # Capture stdout+stderr so we can validate both the exit code AND the
        # specific invariant string.  Without the string match, a parse
        # error / deadlock / JVM crash / different invariant would silently
        # count as the expected counterexample (codex P2 on PR #856 round 2).
        gap_out=$(run_tlc "$module" "$gap_cfg" 2>&1)
        gap_rc=$?
        printf '%s\n' "$gap_out"
        if [ "$gap_rc" -eq 0 ]; then
            echo
            echo "ERROR: ${gap_cfg} unexpectedly passed." >&2
            echo "  The gap configuration disables the safety guard; TLC was" >&2
            echo "  supposed to surface a counterexample.  A clean pass means" >&2
            echo "  either the spec no longer encodes the gap correctly or the" >&2
            echo "  safety guards leaked past the EnableSafety toggle." >&2
            overall_rc=1
            continue
        fi
        if printf '%s\n' "$gap_out" | grep -qF "$gap_inv"; then
            echo
            echo "OK: ${gap_cfg} failed as designed (${gap_inv})."
        else
            echo
            echo "ERROR: ${gap_cfg} failed, but the reason is NOT \"${gap_inv}\"." >&2
            echo "  The non-zero exit may indicate a parse error, deadlock, JVM" >&2
            echo "  crash, or a different invariant breaking — review the output" >&2
            echo "  above before treating this as a regression in the gap evidence." >&2
            overall_rc=1
            continue
        fi
        echo
    done <<< "$gap_list"
done

if [ "$overall_rc" -eq 0 ]; then
    echo "tla-check: all model-check outcomes match the design contract."
else
    echo "tla-check: at least one module did not match the design contract." >&2
fi
exit "$overall_rc"
