# Bound filesystem Jepsen histories by write mode

Date: 2026-10-24

## Failure

Scheduled run 40068963084 completed the filesystem `overwrite` workload, but
the `rename` workload returned `:valid? :unknown`. Eight independent register
histories reported Knossos `:cause :out-of-memory`; their final reads passed.
The existing limit of 64 writes per key did not bound the number of
indeterminate writes tightly enough for the checker. Replaying one captured
64-write history reproduced the memory failure; its first 16 writes checked
successfully.

## Change

Keep the 64-write cap for `overwrite`. Limit `rename` to 16 writes per key
and increase its key count to retain the requested aggregate write budget.
Calculate each mode's budget immediately before invoking the workload.
Reject a calculated key count that exceeds the Java CLI integer limit.

The checker and its register model remain unchanged, so every completed
history is still checked for linearizability. Final reads still cover every
register on every mount.

## Verification

Test the default 16-key × 250-write request in both modes, a small request
below the cap, and the integer-limit rejection. Run the filesystem Jepsen
unit tests and the scheduled workflow with the new budget.

## Risk

The default `rename` workload initializes 250 files instead of 63 and reads
them at the end. The scheduled job timeout must cover that extra work; a full
workflow run is needed to validate runtime and checker completion.
