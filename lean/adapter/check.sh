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
go test ./kv -count=1 -run '^TestTimedMutationParticipantWatermarks$'
go test ./lean/adapter
python3 -B -m unittest discover -s lean/history -p 'test_certificate.py'
protocol="$scratch/ProtocolRefinement.lean"
cat lean/AdapterProtocols.lean > "$protocol"
go run ./lean/adapter protocols >> "$protocol"
cat lean/adapter/ProtocolRefinement.lean >> "$protocol"
lean -DwarningAsError=true "$protocol"
go test ./adapter -count=1 -run '^TestS3CreateMultipartUpload(RetriesBucketFenceConflict|StopsAfterLostCommitReply)$'
go test ./adapter -count=1 -run '^(TestRedisTTLRejectedBeforeMutation|TestLuaUsesOneExpiryInstant|TestLuaSnapshotBindsExpiryToVisibleWrites|TestLuaRetriesWriteAfterFinalValidation|TestLua_ReadSetCoversStringReads|TestLua_ReadOfUnwrittenKeyIsNotValidated|TestS3Server_AbortMultipartUpload|TestS3CommitUploadPartReadSet|TestS3CommitMultipartCompletionReadSet|TestS3CompleteMultipartUpload_RacingPartReuploadKeepsReferencedChunks|TestSQSServer_ChangeMessageVisibility)$'
cat lean/AdapterProtocols.lean lean/adapter/ProtocolCases.lean > "$scratch/ProtocolCases.lean"
lean -DwarningAsError=true --run "$scratch/ProtocolCases.lean" > "$scratch/protocol.json"
ADAPTER_PROTOCOL_CASES="$scratch/protocol.json" go test ./adapter -count=1 -run '^TestAdapterProtocolTransitions$'
