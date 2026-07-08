# SQS DLQ Redrive Policy and Admin Configuration UI

Status: Implemented
Author: bootjp
Date: 2026-06-16

---

## 1. Background and Motivation

The SQS adapter already models queues, visibility timeouts, receive counts, FIFO group locks, and the admin SQS detail page. The missing piece was a complete dead-letter queue (DLQ) control plane:

1. A source queue could reference a DLQ, but the implementation did not fully enforce the AWS `RedrivePolicy` and DLQ-side `RedriveAllowPolicy` contracts.
2. Operators could inspect and purge queue contents from the admin Web UI, but they could not configure which queue receives poison messages or how many failed receives are allowed before the move.

The failure mode is operationally severe: without an AWS-compatible DLQ redrive boundary, a poison message can be repeatedly delivered forever. The fix needs to be part of the SQS data path, not only the Web UI, because SDK/CLI clients configure DLQs through `CreateQueue` and `SetQueueAttributes` and consumers trigger redrive through `ReceiveMessage`.

This document defines:

- AWS-compatible `RedrivePolicy` parsing and validation.
- AWS-compatible `RedriveAllowPolicy` parsing and enforcement.
- The receive-time move from source queue to DLQ.
- The admin HTTP endpoint used by the Web UI to update SQS queue attributes.
- The Web UI flow for configuring source-side and DLQ-side policies.

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. **Move by failed receive count.** A message is moved to the configured DLQ when the next receive would exceed `RedrivePolicy.maxReceiveCount`.
2. **SQS-compatible policy creation and updates.** Accept the AWS `RedrivePolicy` and `RedriveAllowPolicy` JSON shapes for creating and updating DLQ configuration. The initial rollout has one explicit clearing deviation documented in §2.2 and §3.4.
3. **SQS-compatible `RedrivePolicy`.** Accept the AWS JSON shape:

   ```json
   {"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:orders-dlq","maxReceiveCount":5}
   ```

   `maxReceiveCount` defaults to 10 when omitted and is valid only in `[1, 1000]`.

4. **SQS-compatible `RedriveAllowPolicy`.** Accept the AWS JSON shape:

   ```json
   {"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:us-east-1:000000000000:orders"]}
   ```

   Supported permissions are `allowAll`, `denyAll`, and `byQueue`; `byQueue` allows at most 10 source queue ARNs.

5. **Control-plane validation.** `CreateQueue` and `SetQueueAttributes` reject invalid DLQ configuration before it is stored.
6. **Runtime validation.** The receive-time redrive path revalidates the source/DLQ relationship before moving a message, so a later DLQ policy change cannot be bypassed by an older source policy.
7. **Type safety.** FIFO sources can target only FIFO DLQs, and Standard sources can target only Standard DLQs.
8. **Admin UI configuration.** The SQS detail page lets full-access operators set:
   - Source-side `RedrivePolicy`: DLQ target + `maxReceiveCount`.
   - DLQ-side `RedriveAllowPolicy`: `allowAll` / `denyAll` / `byQueue`.
9. **Backup compatibility.** Logical backup and restore include `RedriveAllowPolicy` alongside `RedrivePolicy`.

### 2.2 Non-Goals

1. **StartMessageMoveTask / DLQ redrive back to source.** Moving messages out of a DLQ is a separate feature with partial-progress and throttling concerns. This design only covers source-to-DLQ redrive.
2. **Cross-account or cross-region DLQs.** elastickv has a single configured SQS account/region namespace. A `deadLetterTargetArn` must resolve to a queue in that same namespace.
3. **IAM-style resource policies.** `RedriveAllowPolicy` is the only DLQ access policy implemented here.
4. **Selective per-message DLQ actions in the Web UI.** Operators can peek and purge via the existing admin messages view, but this design does not add per-message delete or redrive actions.
5. **Policy clearing in the initial rollout.** AWS clients can remove a DLQ configuration by clearing attributes, but this initial rollout rejects empty `RedrivePolicy` / `RedriveAllowPolicy` values and the Web UI has no remove button. Operators can update a policy in place or delete/recreate the queue when they need a full reset. A clear/reset path is tracked as a follow-up in §10.

---

## 3. SQS Compatibility Contract

### 3.1 `RedrivePolicy`

`RedrivePolicy` is stored on the source queue meta record as the exact JSON string supplied by the client after validation.

The parser accepts:

- `deadLetterTargetArn` as a required string.
- `maxReceiveCount` as either a JSON number or a numeric JSON string, matching SDK behaviour.

Validation rules:

1. Empty or non-JSON values are rejected with `InvalidAttributeValue`.
2. `deadLetterTargetArn` is required and must have a final colon-delimited queue-name segment.
3. `maxReceiveCount` defaults to `10` when omitted.
4. `maxReceiveCount` must be an integer from `1` to `1000`, inclusive.
5. The target ARN must equal this server's ARN form for the named DLQ (`s.queueArn(dlqName)`), which enforces same account and region.
6. The source queue must not target itself.
7. The target DLQ must already exist when the policy is set.
8. Source and DLQ queue types must match:
   - Standard -> Standard
   - FIFO -> FIFO

The receive path uses the same parsed policy shape, so client-configured JSON and admin UI JSON have identical semantics.

### 3.2 `RedriveAllowPolicy`

`RedriveAllowPolicy` is stored on the DLQ queue meta record as the exact JSON string supplied by the client after validation.

The parser accepts:

```go
type rawRedriveAllowPolicy struct {
    RedrivePermission string   `json:"redrivePermission"`
    SourceQueueArns   []string `json:"sourceQueueArns"`
}
```

Validation rules:

1. Empty or non-JSON values are rejected with `InvalidAttributeValue`.
2. Missing or empty `redrivePermission` defaults to `allowAll`; an explicit `{}` is therefore equivalent to allowing all source queues.
3. `redrivePermission` must be one of:
   - `allowAll`
   - `denyAll`
   - `byQueue`
4. `allowAll` and `denyAll` must not include `sourceQueueArns`.
5. `byQueue` requires `sourceQueueArns`.
6. `byQueue.sourceQueueArns` can contain at most 10 ARNs.
7. Each source ARN must be syntactically parseable as an SQS ARN with a final queue-name segment.

The parser only validates the JSON shape and the ARN shape. Authorization is stricter: when a source queue is configured or redriven, the implementation generates the canonical source ARN with `s.queueArn(sourceQueueName)` and requires an exact string match against one of the configured `sourceQueueArns`. A cross-account, cross-region, or otherwise non-canonical ARN with the same final queue name does not authorize the source.

### 3.3 Receive Count Semantics

AWS describes `maxReceiveCount` as the number of times a message can be received before being moved to the DLQ. The implementation enforces that by checking the candidate's *next* receive:

```go
return rec.ReceiveCount + 1 > policy.MaxReceiveCount
```

Consequences:

- If `maxReceiveCount = 1`, the first receive can deliver the source message. The next receive attempt moves it to the DLQ.
- If `maxReceiveCount = 2`, the first two receives can deliver the source message. The third receive attempt moves it to the DLQ.
- A message moved to the DLQ is not returned to the source queue consumer on the receive call that triggered the move.

This preserves the intuitive "deliver up to N times, then stop delivering from the source" contract.

### 3.4 Policy Clearing Deviation

The first implementation rejects empty `RedrivePolicy` and `RedriveAllowPolicy` values. This is intentionally documented as a compatibility gap rather than hidden behind the "SQS-compatible" wording above:

- `RedrivePolicy=""` is rejected instead of removing the source queue's DLQ target.
- `RedriveAllowPolicy=""` is rejected instead of removing the explicit DLQ-side policy and returning to the implicit `allowAll` default.

The reason is API safety, not data-model limitation: clearing a DLQ target is an operator-visible behaviour change that can resume poison-message retries on the source queue. The initial Web UI only exposes positive configuration writes. A follow-up should add explicit clear/reset buttons and route those through the same audit and confirmation pattern as queue purge/delete.

---

## 4. Data Model

`sqsQueueMeta` stores both policies:

```go
type sqsQueueMeta struct {
    // ...
    RedrivePolicy      string `json:"redrive_policy,omitempty"`
    RedriveAllowPolicy string `json:"redrive_allow_policy,omitempty"`
}
```

The admin projection includes both values in the attribute map returned by `AdminDescribeQueue`:

```go
Attributes["RedrivePolicy"] = meta.RedrivePolicy
Attributes["RedriveAllowPolicy"] = meta.RedriveAllowPolicy
```

Logical backup includes both values so snapshot export/import preserves DLQ wiring:

```go
RedrivePolicy      string `json:"redrive_policy,omitempty"`
RedriveAllowPolicy string `json:"redrive_allow_policy,omitempty"`
```

No new message-key family is introduced by this design. A redrive move reuses the existing source message data key, source visibility index key, DLQ message data key, and DLQ visibility index key inside one OCC transaction.

---

## 5. Request Flows

### 5.1 CreateQueue / SetQueueAttributes

Queue attribute application remains all-or-nothing:

1. Load or construct the requested queue meta.
2. Apply each requested attribute into the in-memory meta.
3. Parse `RedrivePolicy` when present.
4. Parse `RedriveAllowPolicy` when present.
5. If `RedrivePolicy` is present, validate the target DLQ:
   - same account/region ARN,
   - not self,
   - target exists,
   - target type matches,
   - target DLQ's `RedriveAllowPolicy` allows this source.
6. Commit the meta via the existing OCC write.

If any step fails, no attributes from that request are persisted.

### 5.2 ReceiveMessage Redrive

`ReceiveMessage` parses the source queue's `RedrivePolicy` once per call. During candidate selection:

1. Load the candidate message record.
2. If no redrive policy exists, proceed with normal receive handling.
3. If `rec.ReceiveCount + 1 <= maxReceiveCount`, proceed with normal receive handling.
4. If `rec.ReceiveCount + 1 > maxReceiveCount`, move the candidate to the DLQ instead of delivering it:
   - Re-load and validate the DLQ target at the same read timestamp.
   - Re-check the DLQ-side `RedriveAllowPolicy`.
   - Delete the source message data record.
   - Delete the source visibility index entry.
   - Write the DLQ message data record.
   - Write the DLQ visibility index entry.
   - Add `DeadLetterQueueSourceArn` so DLQ consumers can identify the source queue.

The move is an OCC transaction. A write conflict means another receiver changed the same record first; the current receive call skips that candidate and continues.

### 5.3 Standard vs FIFO DLQ Record Semantics

The DLQ record preserves operational traceability while following AWS's age semantics:

- The DLQ message keeps the original `MessageID`.
- `ReceiveCount` resets on the DLQ record.
- `DeadLetterQueueSourceArn` is set to the source queue ARN.
- Standard DLQs preserve the original sent timestamp so retention age remains tied to the original enqueue time.
- FIFO DLQs reset the sent timestamp at the time of the DLQ move so retention age starts at DLQ insertion time.
- FIFO DLQ writes use the original `MessageID` as the DLQ `MessageDeduplicationId`, satisfying FIFO deduplication requirements without generating a user-visible new identifier.
- FIFO DLQ writes participate in the DLQ's FIFO sequence-number allocation so the DLQ preserves its own ordered message stream.

Visibility timestamps and retention timestamps remain distinct. A message can be visible immediately in the DLQ while its retention age is computed from the correct Standard/FIFO sent timestamp rule above.

---

## 6. Admin API

The Web UI uses a new admin endpoint:

```text
PUT /admin/api/v1/sqs/queues/{name}/attributes
```

Request body:

```json
{
  "attributes": {
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:orders-dlq\",\"maxReceiveCount\":5}"
  }
}
```

or:

```json
{
  "attributes": {
    "RedriveAllowPolicy": "{\"redrivePermission\":\"byQueue\",\"sourceQueueArns\":[\"arn:aws:sqs:us-east-1:000000000000:orders\"]}"
  }
}
```

Response:

- `204 No Content` on success.
- `400 invalid_request` for malformed JSON, empty attributes, or SQS attribute validation failure.
- `403 forbidden` for read-only principals.
- `404 queue_not_found` when the target queue does not exist.
- `503 leader_unavailable` when the local node is not the verified SQS leader.

The handler uses the existing admin write path:

1. Session auth + CSRF middleware.
2. `principalForWrite`.
3. `QueuesSource.AdminSetQueueAttributes`.
4. Bridge to `adapter.SQSServer.AdminSetQueueAttributes`.
5. Adapter calls `setQueueAttributesWithRetry`.

This intentionally avoids a parallel DLQ-specific mutation path. Admin UI writes and public SQS `SetQueueAttributes` writes share the same parser, validator, OCC retry loop, and error vocabulary.

---

## 7. Web UI

The SQS detail page adds a `DLQ settings` section below counters and above the raw configuration table.

Both forms update the **current queue** shown in the URL:

- On a source queue page, the redrive form updates that source queue's `RedrivePolicy`.
- On a DLQ queue page, the allow-policy form updates that DLQ queue's `RedriveAllowPolicy`.
- Selecting a DLQ in the source form does not automatically modify the selected DLQ. If the selected DLQ currently has `denyAll` or excludes the source, the source-form save is rejected by the server; the operator must open the DLQ's detail page and update its allow policy there.

### 7.1 Source-Side Redrive Policy Form

Inputs:

- `Dead-letter queue`: a select box populated from `ListQueues`.
- `maxReceiveCount`: a number input constrained to `[1, 1000]`.

Candidate filtering:

- The current queue is excluded.
- FIFO queues list only FIFO DLQ candidates.
- Standard queues list only Standard DLQ candidates.
- If the currently configured DLQ is no longer in the candidate list, it remains visible as the selected option so operators can see and correct stale configuration.

Save behaviour:

1. Read the current queue's `QueueArn`.
2. Derive the ARN prefix up to the final colon.
3. Convert the selected DLQ queue name into an ARN using that prefix.
4. Send:

   ```json
   {
     "attributes": {
       "RedrivePolicy": "{\"deadLetterTargetArn\":\"<derived-arn>\",\"maxReceiveCount\":<n>}"
     }
   }
   ```

5. Refresh the queue detail on success.

The UI performs client-side range validation for fast feedback, but the adapter remains authoritative.

### 7.2 DLQ-Side Allow Policy Form

Inputs:

- `Redrive permission`: `allowAll`, `denyAll`, or `byQueue`.
- `Source queue names`: shown only for `byQueue`, newline- or comma-separated.

Save behaviour:

1. For `allowAll` or `denyAll`, send only `redrivePermission`.
2. For `byQueue`, deduplicate source queue names, require at least one, require at most 10, derive source ARNs from the same account/region prefix, and send:

   ```json
   {
     "attributes": {
       "RedriveAllowPolicy": "{\"redrivePermission\":\"byQueue\",\"sourceQueueArns\":[\"<source-arn>\"]}"
     }
   }
   ```

3. Refresh the queue detail on success.

The UI does not try to predict every server-side failure. For example, an operator may type a source queue name that does not exist yet; the adapter-side `RedriveAllowPolicy` parser accepts syntactically valid source ARNs, and the source queue's later `RedrivePolicy` validation enforces whether it is actually allowed. The effective allow check is still an exact canonical source-ARN comparison, so a hand-written ARN for a different account or region cannot authorize a local source queue with the same name.

---

## 8. Failure Modes and Safety

### 8.1 DLQ Deleted After Policy Set

If a DLQ is deleted after a source queue policy is configured, receive-time redrive validation fails. The candidate is not moved. The source message remains in the source queue and can be retried after the operator fixes the policy or recreates the DLQ.

### 8.2 DLQ Allow Policy Tightened After Policy Set

If a DLQ changes from `allowAll` to `denyAll` or to a `byQueue` list that excludes the source, receive-time validation fails. This is intentional: the DLQ owner can revoke sources after they were configured.

### 8.3 Source/DLQ Type Changed by Delete/Recreate

Queue type is immutable for a single queue incarnation, but a queue name can be deleted and recreated. Receive-time validation re-checks the target queue type, so a source policy cannot move Standard messages into a newly recreated FIFO DLQ or vice versa.

### 8.4 Concurrent Receivers

Two receivers may observe the same candidate near the redrive threshold. The OCC transaction deletes the exact source record and visibility entry that were read. Only one move can commit; the other receives a conflict and skips the candidate.

### 8.5 Admin UI Stale Queue List

The UI's DLQ candidate list is advisory. Saving goes through the server validator, so a stale list cannot persist an invalid policy.

---

## 9. Testing Plan

Backend tests cover:

1. `RedrivePolicy` default `maxReceiveCount = 10`.
2. `RedrivePolicy.maxReceiveCount` range `[1, 1000]`.
3. Rejection of self-referential DLQ policies.
4. Rejection of cross-region/account DLQ ARNs.
5. Rejection of Standard -> FIFO and FIFO -> Standard DLQ pairs.
6. `RedriveAllowPolicy` `denyAll`.
7. `RedriveAllowPolicy` `byQueue` allowing only listed sources.
8. Runtime enforcement when `RedriveAllowPolicy` is changed after source policy configuration.
9. `RedriveAllowPolicy` rejects `byQueue` with missing or empty `sourceQueueArns`.
10. `RedriveAllowPolicy` rejects more than 10 source ARNs.
11. `RedriveAllowPolicy` rejects `allowAll` / `denyAll` when `sourceQueueArns` is present.
12. `RedriveAllowPolicy` with a non-canonical source ARN does not authorize a local source queue with the same final name.
13. Message move after receive count exceeds the configured threshold.
14. `DeadLetterQueueSourceArn` on DLQ messages.
15. Standard vs FIFO DLQ timestamp/deduplication behaviour.
16. Backup encode/decode preservation of `RedriveAllowPolicy`.

Admin tests cover:

1. `PUT /admin/api/v1/sqs/queues/{name}/attributes` happy path.
2. Read-only principal rejection.
3. Error mapping for missing queue and SQS validation errors.

Frontend checks cover:

1. TypeScript compile of the SQS detail page and API client.
2. Vite production build.
3. Browser smoke test that the admin SPA loads without console errors.

---

## 10. Open Follow-Ups

1. Add an explicit Web UI action to clear `RedrivePolicy`.
2. Add an explicit Web UI action to clear or reset `RedriveAllowPolicy` to the implicit default.
3. Add a DLQ redrive-back-to-source design for `StartMessageMoveTask` semantics.
4. Add richer admin audit fields for attribute updates, such as changed attribute names and target DLQ ARN, once the admin audit schema supports per-route payload metadata.
5. Consider surfacing parsed DLQ policy values in `AdminQueueSummary` as typed fields so the SPA does not have to parse AWS JSON strings client-side.
