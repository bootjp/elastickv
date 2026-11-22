#!/bin/sh
# Run from the repository root, after checking the standalone Lean files.
set -eu
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
proof="$scratch/RetryRefinement.lean"
cat lean/AdapterMutation.lean lean/adapter/Signals.lean > "$proof"
go run ./lean/adapter >> "$proof"
cat lean/adapter/Refinement.lean >> "$proof"
lean -DwarningAsError=true "$proof"
(cd lean && lean -DwarningAsError=true --run AdapterMutation.lean) > "$scratch/cases.json"
ADAPTER_MUTATION_LEAN_CASES="$scratch/cases.json" go test ./adapter -count=1 -run '^TestAdapterMutationLeanConformance$'
