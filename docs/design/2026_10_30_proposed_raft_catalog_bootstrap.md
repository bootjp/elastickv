# Raft catalog bootstrap

Status: Proposed v5; design accepted; local format/planner/session/storage components implemented in this branch; runtime not implemented
Author: bootjp
Date: 2026-10-30

## Context

The serializable isolation audit requires apply verdicts to depend only on
the replicated prefix. PR #1355 still compares against LastCommitTS, but
startup currently calls EnsureCatalogSnapshot before starting the gRPC
transport. An empty catalog calls CatalogStore.Save directly on the runtime
store at LastCommitTS + 1. Raft replay can run concurrently with this local
write. The two-Pebble/FSM regression at cc2a3269 proves divergent RAW and
PREPARE verdicts, including a batch that advances both applied indices while
leaving different user data.

A production caller audit found this bootstrap to be the advancing direct
writer. Route splits, job transitions, promotion completion and migration
RPCs already dispatch through a coordinator/proposer. The S3 blob cache uses
ApplyMutationsPreservingLastCommitTS and is not replicated progress.

This design proposes removing new local catalog bootstrap writes. It does
not certify or repair stores that already diverged, or close all A0/A7 gates.
The isolation audit remains partial, and #1355 remains blocked until its
replica-determinism and legacy-state acceptance obligations are satisfied.

## Decisions

### Ordered startup

1. Validate the durable format/identity intent described below before engine
   creation; classify intact restart versus missing/corrupt Raft state.
   Open stores and construct runtimes, initial configured route cache,
   coordinator, catalog handle and Distribution server without saving catalog
   data. Retain synchronous registration/epoch validation before any server
   serves. Arm the existing registration watchers in their original order.
2. Reuse startupPublicKVGate and the existing startup-gated coordinator to
   start Raft transport with public mutations/reads still unavailable. Do not
   wait for catalog initialization before the listeners needed for quorum.
   Do not add a separate public listener or bypass the authentication/TLS and
   encryption-aware proposer wiring.
3. Retain join readiness and startup rotation ordering. Start leader HLC
   lease renewal on every node independently of catalog completion, subject
   to current rotation admission, not just one completed Wait. Acquire the
   revocable bootstrap admission below before requesting timestamps.
   Bootstrap is local catalog-leader work, not an inbound public request.
   It uses the configured coordinator behind a new internal admission wrapper;
   neither the raw coordinator nor registration alone is a rotation barrier.
4. Initialize/wait for the durable catalog through Raft, then apply that
   snapshot to the local engine and seed key-visualization route IDs. Only
   then start catalog-dependent watchers, split/migration background work,
   public protocol serving and FUSE, and mark the public gate ready.
5. Context cancellation or a terminal bootstrap failure drains only startup's
   owned goroutines/listeners before stores close. Never mark ready after a
   failed or indeterminate bootstrap.

The gate must continue covering RawKV/TransactionalKV, Internal.Forward,
admin forwarding, distribution/migration mutations and S3 chunk pushes.
Raft transport, capability inspection and the explicitly authorized remote
encryption/registration control path below must still make progress. Preserve
existing rotation tests and add catalog-specific admission tests; do not
infer safety merely from a gate being present.

### Control admission before data readiness

Keep registration-ready, join-ready and catalog-locally-loaded as startup
milestones, not monotone permission to mutate. Effective control/data
admission is revocable. In particular, Wait can return on a follower before
that process later becomes leader and starts rotation asynchronously.
Registration-ready permits narrowly authorized writer registration because
rotation can depend on a follower registering on the default-group leader;
it does not authorize catalog proposals. With the current rotation and
protocol admission open, control-ready permits the following dependencies
while ordinary public data remains closed:

- Distribution.GetTimestamp, retaining existing rollout-mode activation
  authorization and request bounds;
- RawKV.RawLatestCommitTS for authoritative leader-fenced group floors only,
  never RawGet, scans, or ordinary RawKV operations;
- Internal.GetPrepareEvidenceState and ForwardAdminProposal only for fully
  decoded existing prepare-evidence activation/seal/fence/release payloads;
  unknown payloads, trailing bytes and other admin proposals remain denied;
- existing read-only capability/state inspection needed by these paths, plus
  the new prejoin probe described below (which is independent of catalog
  contents, join readiness and current membership).

Do not assume these methods are already authenticated: current GetTimestamp
and writer registration have no AdminTokenAuth protection, and server TLS
alone does not authenticate a client. Introduce a dedicated
`--startupControlTokenFile`, with a separate startup-control metadata header.
It must not infer authority from or reuse admin, migration or blob-peer
credentials. Preserve any existing underlying admin/method authorization;
the new check only authorizes the narrow startup-stage exception.

Token-bearing network calls require verified TLS and transport-security
enforcement by their per-RPC credentials. Explicit `--adminInsecureNoAuth`
is the only startup-token exception, recorded in acceptance evidence; never
infer it from loopback, absent credentials or a caller flag. Without a control
token or that explicit exception, remote control requests remain unavailable.
Local-only legacy timestamp mode can still progress without exposing remote
control RPCs, but is not exempt from the new bootstrap policy/format gates.
Configurations requiring remote Phase-D/registration progress
must fail with an actionable configuration error rather than wait forever.

Thread the per-node credentials explicitly through timestamp routing,
group-floor reads, prepare-evidence controls and registration clients. Do
not add process-global mutable credentials, replace another credential's
header, accept caller-supplied bypass booleans, log secrets, or send startup
credentials on unrelated RPCs. Membership, registration epoch, TSO rollout
and payload-specific authorization still apply. Test missing/wrong credentials,
wrong stage, wrong method/payload, TLS failure and mixed caller authority.

This token authorizes only method/stage exceptions. All peer-originated
controls, including writer registration, additionally require the mutually
authenticated connection-bound v1 session below. adminInsecureNoAuth can
waive the startup token, never peer authentication or the v1 session.
Existing admin and payload-specific controls remain conjunctive checks.

The startup dependency graph is deliberately not a single readiness flag:

| Lane | Prerequisites | Permitted progress before catalog readiness |
| --- | --- | --- |
| Identity/replay | Durable intent or explicitly provisioned cold seed roster; local journal reconciled before voting; mTLS/v1 for network traffic | Probe support and intact-state elections/catch-up; local disk replay itself needs no session, catalog or timestamp. |
| Registration | Identity lane, local epoch validation, applied member identity or exact pending learner reservation | Writer registration for that identity/epoch, including before policy activation or rotation completes. |
| Policy/reconfiguration | Identity lane, local leader, expected replicated configuration; registration if the existing wrapper requires it | Activation, reservation, guarded ConfChange and abort; no user timestamp or watermark advance. |
| Rotation/HLC | Registration dependencies; rotation uses its receiver-owned handle; HLC uses the local rotation permission defined below (leader completion or explicit follower permission when requested) | Existing rotation controls and leader lease renewal; neither waits for catalog loading. |
| Timestamp/data-group controls | Active policies in the used protection domain, receiver-local rotation/registration admission and existing TSO activation authorization | Leader-fenced floors, exact prepare-evidence controls and timestamp reservation under the handles below. |
| Catalog/data | All prior bootstrap prerequisites, then locally applied catalog plus current data admission | Guarded bootstrap; public serving only after local catalog loading. |

Policy activation and rotation may progress independently once their own
registration dependencies clear. Registration does not wait for rotation,
catalog loading or an already active policy. A marked intact group can elect
and reconcile activation intent without any catalog/TSO handle. A prejoin
candidate can handshake/probe/request catch-up authorization but cannot vote;
its transport lane opens only after the authorizing current leader has
recovered/published the corresponding applied learner addition. These lanes
do not turn startup-control authority into arbitrary admin authority.

On shutdown, unregister/stop new HLC and rotation callbacks, cancel attempts,
and join their owned child proposal goroutines before draining listeners and
closing stores. `eg.Wait()` or context cancellation alone is not proof that
the existing detached proposal/rotation work has returned. Add lifecycle
receipts/joins and tests that hold a proposal during startup cancellation.

### Scope and durable format intent

The fault model is honest supported/old binaries, crashes, partitions and
accidental downgrade. It includes ongoing add/replace, same-ID intact OLD
restart, and a single wiped OLD member without a membership RPC. It assumes
an enforcing quorum retains the activated policy and Raft history; that
quorum may be temporarily offline. No launcher, measured binary, attestation
service or external incarnation registry is introduced. Ordinary operator
PKI authenticates node identity, not executable provenance.

An entirely OLD quorum whose disks were wiped or rolled back below the
format/policy cutover is unsupported, as is re-bootstrapping a private voter
roster under an established cluster identity. With every enforcing record
and process gone, old binaries cannot distinguish that state from a legacy
cluster. This design does not claim to prevent that scenario, malicious
protocol impersonation, key theft, or stale responses from a separately
launched old endpoint. It does prevent the requested single wiped OLD
member from joining, contributing votes/log acknowledgments, installing
snapshots or registering a writer on the protected cluster.

The protection domain is the catalog group plus every default encryption,
timestamp and data group used for its floor/Phase-D dependencies. The
configured allocator/domain mapping has a generation. A mapping change
closes attempts before publication; activate newly needed groups before
publishing the mapping. Each group persists a monotone required policy P:
cluster UUID, group ID, entry/snapshot/session minimums, activation index,
generation, last configuration index, canonical ConfState and identity roster,
retired member IDs and an optional membership reservation. The roster covers
voters, outgoing voters, learners and learners-next, with stable member ID,
address and certificate-public-key digest. Boot nonces belong to sessions
and local handles, not the replicated membership digest; an intact restart
does not need an incarnation commit before elections.

Before acknowledging upgrade readiness, the startup owner publishes a
checksummed `bootstrap-intent` file containing cluster/group identity, seed
or accepted baseline configuration, minimum versions and state=required.
It then replaces `raft-engine` with exactly `etcd-bootstrap-v1\n`. For EACH
file: create a same-directory exclusive temporary file (0600), write all
bytes, fsync and close, rename over the destination, then fsync the parent
directory. Never truncate the live marker with os.WriteFile. Return success
only after both directory syncs; disk errors keep admission closed. On
restart, a partial pair means recovery-only, not legacy startup. After
activation, mirror its index/generation as a local floor in the intent file
before opening data; the replicated policy remains authoritative.

The current old reader parses the whole trimmed marker as an engine name:
only empty/`etcd` are accepted (`multiraft_runtime.go:66-72,162-174`). The
new nonempty value therefore fails old startup before factory.Create
(`:244-248`); it is not an ignored peer-local flag. Empty/malformed markers
are errors in v1. A crash before rename leaves the old valid file and cannot
produce an upgrade-ready acknowledgment; a crash after the acknowledged
rename retains the incompatible file. New startup cross-checks marker,
intent, WAL/snapshot identity and P; a missing marker with retained required
policy is repaired atomically while closed. An absent WAL or lost term/vote
state is NEVER inferred to be a fresh cluster from a missing local marker.

Initial rollout is quiesced and all-member: stop public/background writers,
drain proposals and pending ConfChanges, establish the separately required
baseline, upgrade all voters/learners, publish their durable intent/markers,
close legacy connections, and establish v1-only sessions. Only then activate
P in each group. Cold startup requires an explicit operator-provisioned
cluster UUID/seed roster and the same intent/marker before first transport;
only that initial roster can seed the genesis configuration. Existing data
or an established identity cannot use the cold-seed path. Partial activation
remains control-only and reconciles through Raft, not by clearing markers.

### Authenticated connection-bound sessions

Add a peer-session service independent of catalog state and join readiness.
Both client and server require verified mTLS, including client certificates,
with certificate identities mapped to the intended cluster/member/address
and allowed group. Server TLS alone (`internal/grpc_tls.go:40`) is not this
configuration. On the shared public/peer gRPC listener, verify client
certificates when supplied and require a verified certificate in the peer
interceptors; do not require peer certificates on unrelated public RPCs or
add a second public listener. Peer clients always supply their certificate.
A separately provisioned prospective identity can handshake for probing
without already being a member. Shared startup/admin tokens,
source address and a capability integer cannot establish node identity.

The session owner is the accepted physical TLS connection, not a reusable
GRPCConnCache address or a caller-provided context value. Its protocol is:

1. Begin carries both endpoint identities, group, fresh client boot nonce
   and challenge, installed entry/snapshot/session versions, and local state
   class (intact, prejoin or lost-state). The server validates identities
   and returns its own boot nonce/challenge, supported versions, policy
   requirement and marker-ready acknowledgment. Missing/zero versions fail.
2. Both derive a key with the TLS exporter label
   `EXPORTER-elastickv-bootstrap-v1` and SHA-256 of that canonical transcript
   as context. Finish and its acknowledgment carry opposite-role HMAC-SHA256
   proofs. The session ID is the transcript hash. No session becomes usable
   until Finish succeeds on that SAME connection. Reconnect, including a
   transparent gRPC retry, requires a new handshake; no address-wide token
   or connection-independent bearer session is issued.
3. Each peer RPC binds its full method and session ID to that connection;
   Raft messages additionally bind group and decoded From/To. On receive,
   look up the server-owned session and its current role authorization
   before passing a message to the engine. Recheck every stream message
   and snapshot chunk; revocation cancels streams and discards their spool.
   On send, require a completed reciprocal session before releasing bytes.
   Begin/Finish are the only pre-session peer service methods.

Session authorization is role-specific: an intact peer must match the
locally recovered applied identity roster; only a voter in that ConfState
can send election traffic. Lost-state peers remain probe-only. Prejoin peers
may probe and request the narrowly scoped learner authorization below; only
an already applied, nonremoved learner can obtain a catch-up grant from the
current leader, including a successor. Its applied-add receipt alone grants
no transport authority, votes or data/control proposals. Removal/key replacement
and local policy reconciliation revoke affected sessions before publishing the
new membership. A local non-RPC call uses an owner-minted typed identity
and the same stage/handle checks, never a bypass boolean in a context.

Require this on BOTH directions of Send, SendStream and SendSnapshot,
including votes/pre-votes, responses, unary fallback, snapshot metadata
before spooling, payload, install and acknowledgment. Also require it for
peer GetTimestamp, group-floor reads, prepare-evidence forwarding, probes
and writer registration; the existing method/token checks still apply.
Reconnect or process shutdown destroys connection state. A new boot of an
identity first closes its locally owned old connections; receivers retire
old boot-nonce sessions when admitting the replacement connection. Under
the honest-binary model there is one live process per identity. Session
revocation is not retroactive removal of an already-counted Raft vote.

ProbeBootstrapSupport runs inside a prejoin session, with a fresh challenge
and intended transition/configuration digest. Its response is signed by
the node identity key and binds that challenge, stable identity, address,
boot nonce, marker-ready state and installed versions. The leader verifies
it on the live session; the log carries the signed canonical response so
apply can verify identity/version/configuration binding without network
I/O or wall-clock expiry. This is an honest implementation's protocol
acknowledgment, NOT a signature attesting which executable is running.
CatalogCapabilities field 5 remains optional discovery only.

An OLD process cannot perform Begin/Finish or emit v1 peer calls. Reusing
its valid node certificate or the same member ID does not satisfy the
handshake. Surviving enforcing peers reject its votes, appends, responses,
snapshot streams and registration even if it bypassed the marker by wiping
its disks. Close all legacy channels before activation; there is no legacy
fallback for an activated group, including on an existing HTTP/2 stream.

Writer registration additionally compares FullNodeID and LocalEpoch with
the authenticated session identity and its epoch declaration, after existing
local durable epoch/collision validation. Lost-state identities cannot
register. Preserve pre-add encryption registration, but run it AFTER the
replicated learner reservation: the leader's internal pre-registration call
must name that reservation and its candidate-signed epoch declaration.
Only the target candidate may authorize that exact ID/epoch; it cannot
authorize a second caller-chosen writer. The default-group receiver verifies
the signed applied-reservation receipt, candidate declaration and source
leader session with its OWN control handle at enqueue/result. It does not
claim an instantaneous read of a different group's current reservation.
The source leader rechecks its reservation after the registration receipt
and before ConfChange submission. If that reservation was aborted, an
already-committed extra registry row grants neither membership nor transport
rights; the candidate ID is retired and never reused. This typed delegation
is not an unrestricted RegisterEncryptionWriter RPC or a shared-token bypass.

### Activation and guarded membership

For an applied configuration C, the leader collects all-member marker-ready
support responses bound to C, including joint/outgoing voters and learners.
ActivateBootstrapV1 carries C, the canonical identity roster, those responses
and expected prior policy generation. Apply first performs receipt lookup
as specified below, then checks the configuration/identity witness against
the immediately preceding replicated state. On success it atomically records
P, activation index and receipt. A mismatch records rejection without policy,
catalog or watermark effects. No local session liveness or rotation flag
participates in an FSM verdict.

A bootstrap witness is W=(cluster, group, activation index, P generation,
last configuration index, canonical ConfState/identity digest). Both the
watermark and one-phase catalog entry carry W. For a NEW entry, validate W
and absence of a reservation before any inner operation. Stale W rejects
deterministically with no inner effects. Partial activation of independent
groups grants no catalog admission; it is not atomic multi-group activation.

All membership entrypoints use one engine-owned reservation path:

1. ReserveMembership is a replicated normal entry with expected C/P, unique
   operation ID, exact transition and candidate support/epoch responses.
   Apply validates it, increments P generation and persists the reservation.
   Only one reservation exists per group; bootstrap attempts pause. A timeout
   does not free it. Run existing encryption/migration/promotion checks as
   additional prerequisites, not substitutes for the reservation.
2. AddVoter of an unknown/new identity is translated to AddLearner, catch-up,
   then a separately reserved promotion. No empty node is admitted directly
   as a voter. AddLearner, promotion, removal, address/key replacement,
   direct Engine calls and bootstrap/restore paths all use the same guard.
   Engine submission rejects missing context before ProposeConfChange.
   Persist retired IDs; removing a lost member forbids reusing its Raft ID.
   The engine membership-operation owner retains the operation ID across
   the learner/promotion steps and exposes its durable status for retries.
   Legacy AddVoter success returns only the final applied promotion index,
   never the intermediate learner index. Timeout/unknown outcome is an
   error, not a false successful voter addition; resume from durable status.
3. ConfChange and ConfChangeV2 context bind reservation ID, expected C/P,
   exact transition and identity digest. At committed apply, recognize an
   exact durable receipt BEFORE checking a new witness. Otherwise validate
   before ApplyConfChange and execute the indexed transition protocol below.
   An invalid NEW committed configuration entry halts without membership
   mutation; never skip it as though Raft agreed. No automatic unguarded
   membership path is allowed. Use explicit joint transitions/leave-joint;
   disable AutoLeave, whose generated context could bypass the reservation.
4. A new leader reconciles the reservation against committed log/configuration
   before proposing. It completes the exact operation, or commits an abort
   only after its current-term leadership fence and a committed-log barrier
   covering its inherited log tail prove no corresponding ConfChange remains
   pending. Freeze new submissions while resolving that barrier; inherited
   uncommitted ConfChanges preceding it must commit/apply before any abort.
   Abort and completion consume the reservation permanently. A delayed
   submission is rejected before enqueue; an unexpected committed stale
   ConfChange halts. Retry
   with a new reservation, never recycle an old ID.

Reservation/configuration apply closes local generations under the arbiter
BEFORE taking store locks or changing policy, and keeps them closed until
publication. Either the guarded catalog entry precedes that transition
under the old agreed configuration, or the new P generation rejects old W.
Removal invalidates W just as addition does. Peer traffic for a newly added
learner opens only under the current leader's catch-up grant below, after
that leader has published or recovered the applied transition. This permits
catch-up before the candidate installs ConfState without requiring the
original proposer to remain alive. The candidate cannot vote or originate
fresh proposals. Subsequent promotion requires leader-fenced Match through a
fixed committed barrier plus the candidate's
locally applied snapshot/policy/configuration receipt at that barrier and
completed registration. An operator skip-catchup flag cannot waive these.

### Learner catch-up authorization and leader handoff

The successful learner-add FSM batch also persists a protected applied-add
record D: origin kind=Add, cluster/group, entry identity E, operation/reservation
IDs, exact candidate Raft/writer ID, certificate-key digest, endpoint, signed epoch
declaration and post-add configuration/policy digests. D is durable evidence
of membership, not an incarnation token or a live transport grant. It remains
available after reservation consumption, independent of the original leader.
For learners already in the explicitly accepted baseline, activation atomically
seeds kind=AcceptedBaseline records bound to its receipted E and accepted C,
without inventing historical Add/reservation IDs. Cold-seed learners use that
same activation rule; their activation support responses include the signed
enrollment epoch declaration needed by D. Neither origin kind grants transport
by itself.

`AuthorizeLearnerCatchup` is a read-only peer-session RPC allowed in the
prejoin control lane without catalog readiness or rotation completion. It
retains the startup-token/mTLS/v1 checks and grants no registration or proposal
exception. It is a bidirectional control exchange on each actual transport TLS
connection, initiated by either endpoint: the candidate supplies the request
and the leader owns the fencing/signing procedure regardless of gRPC client/
server direction. Thus a candidate-to-leader control connection cannot silently
authorize a different leader-to-candidate snapshot connection. The original
leader and every successor use the SAME procedure:

1. The endpoints establish a fresh connection-bound v1 session. The candidate
   requests D for its exact identity/endpoint, supplying its current boot nonce,
   signed current epoch declaration and a fresh request challenge. It may discover a
   successor through authenticated seed peers; a redirect is discovery, never
   authority. A candidate that has not yet received D may request it by
   identity; never require an old grant. Only one authorization attempt is
   allowed per group/session; each endpoint consumes its expected nonce pair
   once. A retry/renewal, including a lost reply, starts a fresh v1 session.
2. The receiver completes local journal replay/publication and a current-term
   linearizable read applied through a fixed committed barrier B. It checks
   D against that recovered state and the CURRENT configuration: the exact
   identity/key/endpoint must still be a nonremoved learner (not a voter,
   outgoing voter or learners-next). Validate the session-bound current epoch
   declaration against the existing durable epoch/collision and registry rules;
   D's enrollment declaration is not a reusable authorization for a new boot.
   An intact learner restart may advance its epoch under those rules, never
   roll it back or silently substitute a lost-state incarnation. A pending
   transition for that identity must reconcile before granting; a retired or
   lost-state identity is denied. This lookup neither revalidates the consumed
   reservation nor creates another Reserve/Add entry.
3. Under its local arbiter A, the receiver rechecks leadership/term, applied
   configuration/policy and session generations, D and the candidate binding
   after the read. It publishes a catch-up-only grant G signed by its identity
   key, binding D's digest, B, current configuration/policy digest and index,
   leader identity/boot/term, candidate identity/endpoint/boot/epoch declaration,
   the physical connection's session ID/transcript, the request challenge and
   a fresh leader authorization nonce generated for this attempt.
   Signing may occur outside A; revalidate the exact tuple under A before
   publishing/returning it. A changed tuple requires a new fenced attempt.
4. The candidate checks the signature against the authenticated, group-allowed
   leader identity and the exact D/request/session/nonce tuple before enabling
   only catch-up messages and acknowledgments on that connection. Bind Raft
   From/To and every snapshot chunk in both directions as above. G is not a
   bearer grant and cannot authorize another boot, endpoint, session or term.
   The candidate acknowledges the exact grant on that session; the leader
   rechecks its generations after acknowledgment before releasing Raft bytes.
   The source leader checks its owned grant generations at each send/delivery;
   the candidate rejects known-obsolete grants and validates incoming lineage
   before install. Receiving a snapshot does not enable voting.

Either endpoint's reconnect/boot change, observed term/configuration/policy
change, identity removal/key/endpoint change or session revocation closes the
affected grant/streams before publishing that local change. Renewal repeats
the fenced procedure with fresh challenges/current generations; it does not
extend an old signature. A new leader may therefore reauthorize an applied
learner even if its predecessor died before sending D or any snapshot bytes.
No node claims instantaneous knowledge of a remote term/configuration change:
each honest source enforces its own current leadership and the receiver its
locally observed state. Unknown outcomes remain closed pending reconciliation.

Keep D for every active learner in protected control state, native snapshots,
row/SST exports and replay-skip coverage even when its ordinary R[E] suffix
has compacted. Restore validates D against the selected snapshot and replayed
configuration. Missing/inconsistent D for an active protected learner is a
fail-closed recovery error, not a reason to reuse its reservation. Promotion
or removal replaces its active D with terminal provenance/tombstone coverage;
persist that transition atomically before retiring D. G is volatile: restart
or snapshot install restores evidence, never an old session permission.
Only ordinary guarded promotion, locally applied voter state and the existing
catch-up/registration/readiness barriers can subsequently enable voting.

### Intact restart and lost-state replacement

An intact restart retains verifiable WAL HardState (including term/vote),
WAL-selected snapshot/log, cluster identity and required policy/intent. It
replays and reconciles the local journal before opening peer traffic, then
performs fresh v1 handshakes. It may vote/elect in a control-only lane under
its RETAINED state before catalog/data admission. This permits an entire
supported cluster with intact durable state to restart without waiting for
an incarnation-policy commit. No boot nonce changes the Raft voting identity.

Missing/corrupt WAL, lost term/vote, or an unverifiable rollback is lost-state,
even if the member ID/address, certificate or a snapshot still exists. Do
not construct a voting RawNode or seed an empty established group. Such a
process can only authenticate for probe/replacement planning; deny votes,
pre-votes, election ticks, TimeoutNow, replication acknowledgments and writer
registration under its old ID. A high-term heartbeat, downloaded snapshot
or policy receipt does not restore lost voting history.

With the surviving enforcing quorum, replacement is:

1. Quarantine the old identity's sessions and commit a guarded removal of
   its Raft ID using that quorum. Its retired-ID tombstone persists in P.
   If removal cannot commit, remain unavailable; there is no recovery-vote
   exception to manufacture quorum.
2. Provision a fresh unique Raft/writer identity for the replacement process
   (the physical host/address may be reused). Reserve its learner addition,
   perform reservation-bound pre-registration, commit the guarded learner
   addition, and establish only the catch-up transport lane described above.
3. Install authoritative log/snapshot and policy, durably apply the promotion
   barrier, then reserve/commit promotion. Only the APPLIED voter change
   enables voting. A wiped OLD process fails the v1 handshake at step 1
   independently of whether anybody invokes a membership RPC for it.

Do not substitute a stale backup for intact voting state. If no enforcing
quorum can remove the lost voter, explicit disaster recovery to a new
cluster identity is required; this design does not reconstruct forgotten
votes. The single wiped member is fenced, not silently excluded from the
fault model or readmitted under its forgotten identity.

### Indexed persistence and duplicate replay

The serial engine loop owns configuration planning/publication. The FSM
owns P and a protected receipt ledger R. Use entry identity
E=(cluster,group,index,term,SHA-256(entry type and canonical plaintext/context)).
R[E] records verdict, operation ID, pre/post configuration and policy
digests, exact target ConfState/peer inventory for a configuration entry,
and any returned watermark. A different digest at an existing index is
corruption, not a duplicate. Records use reserved non-MVCC store metadata;
RAW, transactional, admin and migration payloads cannot write/delete them.

For KV groups, add a control-metadata staging API to the existing indexed
Raft-entry batch (`store/raft_entry.go:136-216`). The batch atomically writes
P, reservation consumption, R[E], learner D creation/retirement when applicable,
durable applied index and ALL inner watermark/catalog effects, using pebble.Sync
regardless of FSM sync mode.
A deterministic rejection also writes its receipt/index, but no inner
effects. Operational failure aborts the batch and halts apply. Control-only
changes do not allocate a user commit timestamp or advance LastCommitTS.
The current scope's empty/touched behavior is not sufficient: metadata
staging must mark it touched. In-memory stores implement the same atomic
logical transaction under their store lock, without claiming disk durability.

The dedicated TSO FSM currently holds only atomic mirrors and snapshots
(`kv/tso_fsm.go:43-61`). Give it a per-group durable control store containing
its entire scalar state, P and R in ONE synced batch. Snapshot/replay selects
that state together; HLC/atomic observer mirrors publish only after commit.
Do not put policy in a separate file while independently mutating TSO floors.
The existing timestamp/Phase-D codecs and transitions remain the inner
semantics. This backing store is new, not an existing durability guarantee.

For committed ConfChange E, an engine-owned journal J[E] in that same
control-metadata store has these states (all writes synced):

| State | Durable contents and allowed action |
| --- | --- |
| PREPARED | Exact committed entry, pre/post ConfState and peer inventory, reservation and proposed post-P. No policy change, receipt or applied-index advance yet. |
| FSM_RECEIPTED | One FSM batch installs post-P/configuration projection and learner D changes, consumes reservation, writes R[E]/applied index and advances J. No networking/publication yet. |
| PUBLISHED | RawNode configuration equals the receipt's target; peer file is durably replaced and engine caches agree. Journal may be checkpointed, not independently forgotten. |

Ordering for a NEW entry is: verify committed WAL provenance and new witness;
compute the target using etcd's configuration changer on a cloned pre-state
(not by mutating RawNode); sync PREPARED; commit the FSM_RECEIPTED batch;
call RawNode.ApplyConfChange once in the owning loop and compare its result
with the recorded target; atomically replace/fsync peer metadata; publish
configuration caches and J=PUBLISHED; then permit traffic. Include the
existing migration namespace configuration projection in the staged recipe
and receipt, rather than calling its current post-ApplyConfChange hook twice.
Persistence is outside the admission mutex; configuration admission stays
closed throughout. No new entry is processed between these phases.
Journal-only phase writes never regress the durable applied index or P.

RawNode memory is disposable, not a separate durable commit. Restart first
selects the WAL-valid Raft snapshot S and its ConfState, verifies its FSM
manifest, then walks committed configuration entries after S in order before
starting network dispatch. An exact R[E] is looked up BEFORE any new W or
reservation check. Reconstruct only RawNode/configuration memory from its
recorded pre/post states; do not consume the reservation or mutate P again.
This remains true when the FSM is already at a later P generation. A missing
R with PREPARED resumes new apply from its unchanged pre-state; a missing R
at/below the claimed protected applied index invalidates replay-skip and
requires verified full restore/replay, never guessed success.

Open RawNode with its applied cursor at S, not at the newer retained FSM
index A. Drain/advance its committed Ready suffix through A while networking
is disabled: exact receipted configurations rebuild memory once; receipted
data/control entries do not mutate the FSM twice. Only then align the engine
and RawNode cursors and process entries beyond A normally. If no verified
suffix exists, restore S and replay its WAL rather than synthesizing C_A
from the newest peer file. A skipped entry at/below S is covered by the
selected verified snapshot, not by an unverified applied-index shortcut.

| Crash boundary | Restart action before networking |
| --- | --- |
| Before PREPARED sync | Replay committed entry with normal new-witness validation. |
| PREPARED, no R | Verify recipe against WAL and unchanged pre-P; commit its single FSM batch, then reconstruct/publish configuration. |
| R committed, before/after RawNode mutation | Exact receipt match; reconstruct RawNode from selected S and recorded transitions; never revalidate consumed reservation. |
| Peer file ahead of selected Raft snapshot | Treat peer file as a derived cache; rebuild from S plus journal/receipts, atomically replace it. |
| PUBLISHED, snapshot deferred or absent | Retain journal/receipts and WAL; replay the same suffix from S. PUBLISHED is not permission to truncate it. |
| New snapshot durably saved | Verify manifest/ConfState/policy coverage, then release WAL and retire only covered journal/receipt prefixes. |

Retain exact receipts/journal entries for protected controls and ConfChanges
after the oldest retained WAL-valid snapshot through the FSM applied index.
The snapshot's verified coverage boundary replaces receipts at/below S;
never re-run their witnesses. A delayed client retry at a DIFFERENT log
index is not E: reject stale W or return an operation-ID terminal receipt
without inner effects. Aborted/consumed membership operation IDs and retired
member tombstones survive receipt compaction and cannot become reusable.
Active learner D records are not discarded with their covered R[E] entries.

### Snapshots, replay-skip and imports

Capture snapshot index S, term, exact ConfState and last configuration index
in the serial engine loop together with the immutable FSM snapshot. New
SnapshotAt input carries this tuple to the serializer; background workers
must not reread a newer configuration. Both normal and configuration-only
snapshots contain the complete FSM/control state, not just peer metadata.
Do not emit one while J is between PREPARED and PUBLISHED. A pending normal
membership reservation IS snapshot state and must survive compaction.
Normalize covered journal entries to snapshot coverage, not exported local
phase flags; no partially published transition is a portable snapshot.

The v1 control manifest contains cluster/group, S/term, last configuration
index, ConfState/identity digest, full P/reservation/tombstones, active learner
D records and terminal provenance coverage, receipt coverage boundary,
maximum applied watermark, HLC ceiling, encryption
cutover/checkpoint and a digest of the control block in the inner snapshot.
KV snapshots use a new `EKVTHLC3` header, not an ignorable v2 extension;
TSO snapshots use `EKVTSO06` and include their full scalar state. Exporters,
row/SST bundles and configuration snapshots copy this manifest and the raw
control records. The decoder validates their equality before DB swap. Old
KV readers already fail on unknown EKVTHLC versions (`kv/snapshot.go:228,255`);
the incompatible engine marker is still the first intact-disk downgrade gate.

For local snapshots: finish/flush the FSM/control checkpoint, fsync the
payload and manifest, durably pin applied/control coverage at S, save the
Raft snapshot, then Release/purge old WAL and receipts. Preserve the existing
applied-index-before-SaveSnap rule (`engine.go:3853-3860`). On received
snapshots: authenticate session before spool, verify full payload/manifest
and incoming ConfState, stage replacement state while closed, durably save
the received Raft snapshot/WAL, and only then enable its applied/control
coverage for replay-skip and publish peers. A crash after DB swap but before
Raft SaveSnap must restore from the selected old Raft snapshot plus WAL;
the newer DB alone is not evidence that the snapshot was durably accepted.

Replay-skip requires matching identity, required versions, selected snapshot
coverage, verified control block and a complete receipt/configuration suffix
through the retained FSM applied index. LastAppliedIndex alone is inadequate.
Guard envelopes are NOT classified as volatile and blindly reapplied:
reconstruct HLC ceiling/Observe from durable maximum applied watermark and
accepted receipts. Do not re-run an old guarded watermark against today's
P after a configuration change. Historical pre-activation entries replay
under the explicitly recorded baseline; they are not fresh proposal rights.

Native same-lineage export/import preserves P, configuration, reservations,
receipts and identity; it cannot restore lost HardState as an intact voter.
External/rebased import is permitted only while offline into a NEW cluster
UUID, with explicit operator baseline acceptance and a destination seed
roster. Validate the source manifest first, carry forward its minimum-format
requirement and data/watermark, then write an atomic destination baseline
receipt plus policy requirement before seeding destination Raft state.
Source membership, learner D records, unfinished reservations and receipt
indices are archived as provenance, never reused as destination permissions.
Reject a source with an unresolved transition until it is reconciled; do not
silently sanitize it.
Existing RestoreExternal strips migration epochs and rebinds destination
configuration (`store/snapshot_external.go:75,93`; `engine.go:750-755`);
bootstrap policy needs this explicit import rule rather than inheriting that
stripping behavior. Import into an existing identity is rejected; same-cluster
replacement uses the surviving quorum/learner procedure instead.

### Receiver-owned operation admission

Each process has one admission arbiter A for its local engines/controllers.
A stores immutable role/term images, policy/configuration generations,
session generation, registration state and rotation state. An owned handle
H=(kind,boot nonce,operation ID,target group,term,local dependency generations)
is minted only by the receiver, never deserialized as authority from a peer.
Use these separate acquisition predicates:

| Kind/owner | Required local state |
| --- | --- |
| Catalog attempt / catalog leader | Catalog leadership, join-ready, active P, no reservation, registration and local rotation permission below, fixed allocator/domain mapping. |
| Allocator/refill / local allocator receiver | Local registration/rotation permission, active domain/mapping and their generations, plus the parent catalog handle for bootstrap; no catalog-loaded requirement or authority over a remote leader's term. |
| TSO issuance / receiving TSO leader | THAT timestamp group's leadership and active P, authenticated eligible caller, its own registration/local rotation permission and existing activation-mode authorization; no catalog-leadership requirement. |
| Prepare-evidence/floor control / target group leader | THAT group's leadership, active P, exact authorized payload/session and local registration/rotation permission; no catalog-loaded requirement. |
| Rotation / default-group controller | Default-group leadership, required registration and authorized rotation recipe; exempt only from waiting for its own rotation completion. |
| Registration, policy recovery, HLC / respective receiver | Only the lane-specific prerequisites in the startup DAG; HLC requires local rotation permission, not catalog loading. |

Local rotation permission is a tagged admission value, NOT a shared Done bit.
Check the fatal latch FIRST in every branch. An explicit not-requested startup
configuration permits work with the normal applied encryption/registration
checks; unsupported requested wiring is never treated as not-requested. When
rotation is requested, exactly these two permissions exist:

- **Leader completion:** a local default-group leader must hold its OWN
  current Rot generation's completion receipt and have joined all its owned
  children. Another node's completion, a previous term or `Wait` returning
  cannot satisfy it. Candidate/pre-candidate states grant no permission.
- **Follower permission F:** a local default-group follower may serve dependent
  catalog/TSO/HLC/control work without ever having led that group. F binds
  process boot, default-group follower term, local policy/configuration,
  applied encryption state (active DEKs/epochs and installed wrappers), verified
  local registration and responsibility generations. Those projections must
  agree with locally applied durable state; there must be no fatal latch and
  NO local owned rotation responsibility, unfinished child or unknown outcome
  from a retired run. A queued request to rotate upon future leadership is
  not present responsibility. F neither marks rotation done nor asserts that
  the remote default-group leader has completed or stopped rotating.

The default-group loop withdraws F under A BEFORE any RawNode mutation that
can acquire leadership/change term or role. If the pre/post role/term is
unchanged it may restore the same F before releasing A; otherwise it revokes
F and its dependent handles/generation before publishing the new state or
queuing callbacks. Becoming leader assigns local rotation responsibility in
that SAME critical section, keeping dependents closed until owned completion
and children join. Key/wrapper, registration, policy/configuration or term
generation changes likewise revoke F before local publication. Reacquisition
uses freshly checked applied projections, never refreshes a queued old handle.
There is no enqueue gap between leadership acquisition and an async callback.

After step-down, retiring/canceling a run alone cannot mint F. Join its
children and resolve any unknown committed outcomes against a current-leader
applied read barrier and durable DEK/registry/recipe state; install the matching
local wrappers before clearing responsibility under A. This read-only recovery
uses the policy-recovery lane, not a rotation-proposal exception for followers.
If resolution is unavailable, remain closed. A terminal failure latches closed
across step-down and follower-permission reacquisition until explicit recovery
or validated restart; neither F nor a stale completion can bypass that latch.

This preserves the current `BlockMutators` distinction: terminal error blocks
every role; otherwise it blocks a default-group leader whose task is not done
(`main_encryption_rotate_on_startup.go:160-164`), while `Wait` can return for a
follower (`:285`). V5 replaces that coarse observation with generation-bound
permissions, not a follower-generated rotation receipt. F supplies ONLY the
local rotation predicate: every receiver still acquires its own H_tso/control
handle, verifies its target-group leadership and enforces every subordinate
enqueue/result gate below. Applied local state is not a cluster-global atomic
rotation/term oracle.

The owning engine loop takes A around each RawNode Tick, Step, campaign,
transfer, configuration mutation and terminal Propose. It compares actual
pre/post role/term and invalidates dependent generations BEFORE releasing A
or publishing status/callbacks. This includes follower term changes, not
only leader-acquired/lost edges. Rotation start/failure, registration loss,
session revocation, policy/configuration change, cancellation and shutdown
invalidate under A too. Thus a different local group cannot enqueue between
a default-group term mutation and its admission invalidation.

Lock order is engine-loop ownership -> A; never acquire another engine's
loop, store/apply mutex or perform disk/RPC/quorum waits while holding A.
Store locks are released before publishing state under A. Proposer wrapper
mutexes are released before calling the engine. Callback publication only
queues work; it never calls an engine under A. Persistence transitions close
admission under A, do I/O without A, then publish verified results under A.
Unsupported requested rotation wiring is a startup error, not encryption-off.

At terminal enqueue, validate H's exact generations and role/term and call
the nonblocking RawNode.Propose in that same A critical section. Queue
items retain H through batching, retries and encryption wrapping. Bootstrap
uses dedicated single-operation queue items, not a mixed user/control batch.
Forward/retry never refreshes an old handle; the receiving engine mints its
own handle for the authenticated typed operation. After each apply/result,
revalidate before consuming the result or initiating the next child. At
the catalog owner this covers allocator call/result, guarded watermark
enqueue/receipt, exact snapshot/voucher/pin, plan construction, CommitTS
allocation/result, Dispatch and terminal catalog enqueue.

For remote issuance, carry an authenticated correlation tuple (caller boot,
operation ID, method, exact payload digest), not a portable admission token.
The TSO RPC owner acquires H_tso and retains it through ALL subordinate work:

1. Leader verification and authoritative floor reads: validate before send
   and after reply. Each receiving floor leader uses its own control handle
   and leader-fenced read; a caller's term cannot authorize that receiver.
2. CheckPhaseDProtocol, SealPhaseDCreation, PreparePhaseD and FinishPhaseD:
   fan-out children carry exact stage/payload/correlation. Each data-group
   receiver acquires H_control and validates it at every terminal protocol
   activation, seal, proof-required, fence or release enqueue and receipt/reply. The TSO
   owner revalidates H_tso before every fan-out and after every child result.
3. Cutover, pending-Phase-D, Phase-D activation and allocation-floor entries:
   carry H_tso to each terminal TSO enqueue
   and validate again after each applied verdict. These are separate calls
   today (`tso_raft.go:395-426`; `tso_pending.go:178-196,300-345`), not one
   final reservation check. Per-term initialized/finished caches are tagged
   with the full receiver generation and updated only after that validation.
4. The TSO owner checks H_tso again before publishing its reservation reply.
   The caller's allocator/refill owner retains a distinct local H_alloc,
   checks it and its parent after the RPC and before publishing a window,
   and tags locally cached windows with THAT local generation. Invalidation
   drops them and cancels/joins refills; a remote handle is never imported
   as local authority. Bootstrap requests unshared one-element reservations
   (including CommitTS minima) through the configured allocator/router, not
   unrelated prefetched slots or a different timestamp source. Revoked or
   unknown results cannot be returned/published into a shared cache; unused
   numbers are gaps. The catalog caller independently validates its H after
   receipt and before using either timestamp. A later remote term change
   does not invalidate an already successfully published unique reservation.

Durable pending/active Phase-D markers, seals and fences may have committed
before revocation. Do not undo them or start unrelated work with a new handle.
A new receiver-owned operation reconciles those exact durable stages using
the existing pending-floor protocol and fresh handles for every child. Stop
on unknown outcomes until reconciled; no timeout interprets them as absent.

Rotation has a controller-owned Rot=(boot,default-group term,generation,
recipe digest) and owned per-purpose children. Close dependent admission
before spawning them. Each child validates Rot at enqueue and completion;
its applied index, active DEK/epoch and local wrapper installation form a
completion receipt tagged with Rot. Only the controller can publish completed,
under A, after all matching children have joined and the term/generation
still matches. Old completions may record observations but cannot set a
process-wide Done flag or reopen admission. On a term change, retire/join
the old run. If locally leading the default group, leader-fenced reconciliation
of durable DEK/registry and wrapper state produces a NEW owned generation
receipt, or resumes the unfinished recipe with a new Rot. If a follower, use
the responsibility-reconciliation procedure above and acquire F, never mint
a leader completion receipt. Do not blindly repeat a committed rotation or
accept task.Done from an obsolete run (`main_encryption_rotate_on_startup.go:334-440`).
Retryable failure requires that reconciliation; terminal failure latches
closed until explicit recovery/restart.

Every owner tracks its children and callbacks. Invalidation cancels attempts,
revokes vouchers and joins children before releasing pins; shutdown first
unregisters callbacks, then joins rotation, allocator/refill and proposal
work, drains listeners, and finally closes stores. Data readiness is local
catalog-loaded AND current data admission, so later failure closes traffic.

None of these handles retracts an entry already submitted to Raft. Apply
uses only replicated witness/receipt/timestamp state, never local rotation
flags. Each remote receiver fences its own operation; its term is not an
instantaneously observable property at the catalog owner. A reservation
successfully published before a later remote term/rotation change remains
a valid unique timestamp. Partial cross-group progress is reconciled from
durable state; there is no globally atomic term/rotation oracle.

### Wire compatibility

The following are the proposed protocol's format assignments, not descriptions
of current bindings. Canonical encodings use big-endian fixed integers,
UUIDs/nonces of 16/32 bytes respectively, SHA-256 digests of 32 bytes, uint32 byte lengths,
and sorted unique member records (numeric Raft ID, role sets, identity key,
address). Reject duplicate fields/members, unknown kinds/versions, overflow,
truncation, trailing bytes and records exceeding 16 MiB before allocation.
Unknown/malformed committed protected formats halt apply without advancing
the index; a well-formed stale witness is the deterministic receipted
rejection described above. Neither falls through to legacy protobuf decode.

W encodes cluster UUID:16, group:8, activation index:8, policy generation:8,
last configuration index:8 and roster/ConfState digest:32 (80 bytes total).
Operation IDs are UUID:16; reservation IDs are their Reserve entry index:8
plus operation ID:16. Expected pre-activation state uses the same W layout
with activation index/generation zero and the accepted baseline C digest.

- Control/guard payload: `0x1f | "BC" | version=1:u8 | kind:u8 |
  bodyLen:u32 | body`. 0x1f is unassigned in the inspected opcode set and is
  not a valid protobuf wire type. Kinds 1..5 are Activate, Reserve, Abort,
  GuardedWatermark and GuardedCatalog. Bodies encode the expected W (or
  pre-activation C/P), operation ID, then length-delimited canonical evidence
  or inner bytes in that order. Inner watermark remains exact 0x17; catalog
  remains an existing single-group one-phase transaction, never arbitrary
  admin work. Duplicate receipt lookup uses engine-supplied index/term/hash.
- ConfChange context: version=0x04, operation ID, reservation ID, expected W,
  canonical exact transition and identity digest, with length-delimited peer
  ID/address. Both entry forms use this context. Existing context 0x03 is
  historical-only below the activation boundary; fresh protected changes
  cannot use it. Explicit joint enter/leave is represented in the transition.
- Begin/Finish/probe/AuthorizeLearnerCatchup are distinct peer-session RPCs,
  not additive capability flags. Session metadata uses a separate header from
  startup/admin tokens.
  Bind every generated client, server, stream and reconnect path to the
  physical TLS session owner; old protobuf unknown-field ignoring grants
  no session, configuration admission or permission to receive new entries.
- Snapshot headers are `magic:8 | manifestLen:u32 | canonical manifest |
  inner snapshot`, using the KV/TSO magics above. The manifest includes an
  explicit version=1 and bounded control-block lengths. Existing outer
  .fsm CRC/token validation covers the entire bytes, not just the data tail.

After activation, fresh unguarded policy writes and any RAW/transaction/admin
path initializing a version-zero catalog are denied at apply. Normal
post-initialization catalog updates retain their existing protocol; do not
blanket-ban catalog version-key writes. Entries retain the guard through
forwarding/wrapping. Version assignments are distinct from bootstrap semantic
version and from timestamp issuance; this design adds no timestamp opcode.
Old servers do not participate after the quiesced cutover, including ongoing
membership changes. Compatible tools may read catalog values. Post-activation
downgrade is not a marker-clear operation; export to a newly identified
cluster requires the separate baseline/recovery decision described above.

### Replicated catalog initialization

- Add a non-mutating, snapshot-scoped catalog bootstrap plan. It accepts
  only version zero with no durable routes, normalizes the configured seed
  routes, assigns nonzero route IDs, and includes version/next-route-id and
  catalog delta metadata with existing codecs. It must not call direct Save.
- The verified catalog-group leader holds the revocable admission handle
  and active witness W above. It obtains a leader-issued read timestamp
  and commit timestamp through the existing configured timestamp pipeline.
  First allocate a positive timestamp through that pipeline and commit/apply
  a W-guarded existing read-watermark-advance on the catalog group through
  its encryption-aware proposer. Use the resulting applied group watermark,
  not merely the allocated future timestamp, as the exact read snapshot.
  This also avoids treating empty-store StartTS=0 as an auto-allocation request.
  Main's existing one-phase commit fence rejects any older outstanding
  bootstrap proposal at or below that applied watermark. Thus absence-CAS
  does not incorrectly accept a write that lands below a future StartTS.
  Retain the exact read token/voucher and pin through plan creation/dispatch,
  revoking/releasing them when the attempt is invalidated.
  It rechecks empty catalog state at that exact read timestamp and dispatches
  the complete plan through the encryption-aware coordinator/proposer as a
  guarded single-group transaction, with positive CommitTS > StartTS from
  the configured allocator. Catalog version/next-id absence and all plan
  dependencies are validated atomically at apply after W. There is no new
  timestamp allocation opcode or independent wall-clock allocation, but the
  policy/guard, membership and snapshot format extensions above ARE required.
  Do not rely on ensureReadWatermark's existing opportunistic cross-group
  fast path: it can skip a future allocated timestamp on an empty store.
  Require an explicit applied receipt for this bootstrap fence and rebuild
  from a new fenced snapshot if the attempt or witness becomes invalid.
- Verify every mutation and read dependency resolves to the catalog group;
  a leader check on only the version key or default group is insufficient.
- Followers do not generate timestamps, persist seed routes, or forward
  arbitrary client traffic through a startup exception. They wait until the
  replicated catalog is locally applied, load its authoritative snapshot,
  and apply it to the route cache. The winning durable seed is authoritative;
  a follower must not overwrite it from its local engine statistics.
- A lost reply, leader change or write conflict triggers state reconciliation,
  not an assumption that a fresh write is safe. Reload the durable version;
  if populated, use it. Otherwise repeat leader verification and rebuild the
  plan from a fresh snapshot. Bounded per-attempt contexts and cancellation
  keep startup interruptible; do not return a false success on retry exhaustion.
- Populated catalogs are loaded without mutation, preserving their version,
  IDs, route histories and migration ownership data. Reject malformed catalog
  state rather than repairing it implicitly. An uninitialized catalog is not
  permission to erase user data, Raft metadata or membership.
- Production server startup must not retain a fallback to direct Save.
  Direct CatalogStore library helpers can remain for isolated tests/tools,
  but their callers must be audited and the production path pinned by tests.

### Existing stores and scope

Loading a populated catalog does not prove its historical creation was
replicated or that all replicas have equal data/watermarks. This PR cannot
retroactively establish that proof. Preserve data and document recovery as a
separate explicit quiesce/reconcile/restore operation, not an automatic wipe,
local-max initialization, volatile watermark, or unsupported safety claim.
Before enabling the new RAW/PREPARE fence on an existing group, the parent
audit must establish an agreed replicated baseline and handle old prepared
transactions and mixed-version members. Those remain merge gates for #1355.

## Implementation acceptance evidence

These are verification obligations for the specified protocol, not missing
protocol choices or evidence that any implementation has passed them.

- Runtime-red production-path regression: setup of the catalog handle must
  not issue any direct advancing store mutation. Retain the original
  local-bootstrap divergence reproduction as diagnostic evidence, not a
  passing proof after merely changing its assertions.
- Three-node cold startup must form quorum and bootstrap without deadlock;
  inspect actual catalog mutations in committed entries, durable versions,
  route IDs and local applied state, not just successful TCP connections.
- Race two leader attempts and lose a success reply. Exactly one version-1
  bootstrap wins; retries load it without another catalog version advance.
  Include the parked old proposal / future read timestamp counterexample,
  empty-store zero watermark and a separate Phase-D timestamp-group leader.
- Delay a follower's catalog apply and prove every public write/read gate
  stays closed until its local catalog is usable. Transport still progresses.
- Verify restart/join/populated catalog, shutdown/cancellation, leadership
  loss, malformed catalog, multi-group catalog routing, encryption off/on,
  writer-registration gates and rotate-on-startup.
- Park an attempt before each allocator call/result, watermark enqueue/apply,
  plan build, Dispatch, batch dequeue and terminal engine enqueue. Return
  Wait on a follower, then acquire leadership and trigger asynchronous
  rotation; inject retryable/permanent failures and term changes in the
  catalog/default/TSO groups. No closed generation may enqueue new work;
  already-enqueued outcomes must reconcile without local-state apply tests.
  Include every TSO cutover/pending/activation, data-side seal/prepare/release,
  floor read, allocator-window publication and obsolete rotation completion;
  separate catalog and TSO leaders must each acquire only their own handles.
- Keep A as default-group leader and B as its follower while B leads the
  catalog/TSO groups. B must progress under F with applied key/registration
  state and no fabricated rotation completion. Park dependent work across
  default-group Tick/Step/campaign, key/wrapper/registration changes and
  async callback publication: no revoked generation may enqueue. On becoming
  default leader, B waits for its own Rot completion AND child joins. Step
  down with an unknown child outcome or terminal failure: no F until exact
  reconciliation, and no F at all through the fatal latch. Repeat with
  not-requested rotation and reject misuse of F to bypass target TSO/control
  leadership, payload, session or subordinate enqueue/result gates.
- Exercise activation versus AddLearner/AddVoter, promotion, removal,
  replacement and joint transitions in both log orders. Inject a direct
  engine call and an old delayed ConfChange. Prove stale W has no inner
  catalog/watermark effects and invalid configuration context halts BEFORE
  ApplyConfChange. Crash at every transition-journal persistence boundary
  in the table; replay FSM_RECEIPTED entries against a later P without
  revalidating their consumed reservations. Cover both entry forms, explicit
  leave-joint, disabled AutoLeave and derived peer files ahead of snapshots.
- Commit/apply a learner add, consume its reservation, then stop that leader
  before D delivery and again mid-snapshot. With an intact surviving quorum,
  the successor must fence its current term and reauthorize from D without
  another Reserve/Add or old grant. Repeat after receipt compaction, native
  snapshot restore/replay and intact learner restart; cover baseline learners.
  Reject missing D, wrong identity/key/endpoint/epoch/boot, reused request or
  leader nonce, wrong session/term/configuration and removed/retired identities.
  Term/config changes revoke G before local publication; renewal must use a
  fresh fenced tuple. Catch-up grants/snapshots never enable votes before the
  ordinary applied promotion and readiness barriers.
- Probe a fresh non-member without catalog state or join readiness. Reject
  missing/zero capabilities, spoofed ID/address/challenge, revoked sessions
  and a shared startup/admin token used as peer identity. Exercise Begin/
  Finish on different TLS connections, reconnect/retry, both stream sides,
  unary fallback and per-chunk revocation. Neither field 5 nor server TLS
  alone may admit a candidate or authorize a different writer ID/epoch.
- Crash before/after temporary-file fsync, marker rename and directory fsync.
  The old reader must reject the exact nonempty v1 marker; no acknowledged
  upgrade may leave an empty/truncated accepted legacy marker. Test the
  marker/intent/policy cross-check and interrupted activation on all nodes.
- Persist activation, compact/restart/install native and configuration-only
  snapshots through row/SST export and exercise replay-skip with a later FSM
  policy. Reject missing/inconsistent manifest, receipt suffix or ConfState;
  reconstruct guarded-watermark HLC effects without rerunning old W. Exercise
  received DB-swap-before-SaveSnap and offline rebased import to a new UUID.
- Restart the SAME member ID with
  wiped local state and an OLD binary without calling any membership RPC:
  deny votes/appends/acks/snapshot streams and registration, including on an
  existing long-lived channel and the unary fallback. Repeat with an intact
  required marker and OLD binary. No enforcing peer may admit either case.
- Restart a whole supported cluster with intact WAL term/vote state and
  verify control-only elections precede data readiness without deadlock.
  For a supported lost-state member, verify no voting RawNode/ack/registration
  under its old ID, including after a high-term heartbeat or snapshot. Remove
  it with the surviving quorum, use a fresh learner ID, apply the promotion
  barrier and only then enable voting. Quorum loss must stay unavailable.
  All-old wiped/rolled-back quorum prevention is NOT a claimed test outcome.
- Audit every changed startup helper and direct mutation caller. Run full
  root/KV/distribution/store race suites, unfiltered lint, applicable Jepsen,
  and final-push code/security review and CI before merge.
- Record data loss, distributed concurrency, performance, consistency and
  coverage as five independent review passes. Do not check the parent
  isolation or all-design-doc goal complete from this prerequisite alone.

## Implementation checkpoint

The branch now contains `internal/raftbootstrap`: strict canonical,
checksummed intent parsing and intent-before-marker durable publication.
Each file uses an exclusive same-directory temporary file, mode 0600,
write-all, file sync/close, rename and directory sync. Inspection reconciles
the file pair with separately recovered identity/baseline/policy evidence;
unknown, corrupt, partial, unverified and lost-state outcomes do not grant
runtime readiness. Retained-policy repair restores only the format barrier,
never forgotten term/vote history. This package is not wired into startup
and does not constitute policy/session admission or a usable upgrade command.

`CatalogStore.BuildBootstrapPlanAt` now constructs the initial route, version,
next-ID and delta mutations without calling Save, selecting a local timestamp
or updating the store/cache. Reads use exactly the supplied positive fenced
snapshot. Seed route IDs are assigned in deterministic key order, dependencies
are cloned into the read set, and partial catalog state is rejected without
repair. The caller still owes the applied read fence, pin/voucher, every-key
group validation, current W/admission and a guarded one-phase dispatch.

Local checks at this checkpoint include the complete distribution race suite,
twenty repetitions of planner race tests, whole-repository lint, and ten
repetitions of the real legacy startup parser/factory boundary check. The
planner tests cover actual memory/Pebble concurrent one-winner and independent
conflicts on every mutation/read dependency. An initial test comparison of
nil versus empty start bounds was corrected to the existing codec's semantic
comparison; it was not a production failure. Format tests exercise real
filesystem errors and process termination at publication boundaries; they
are not a claim of arbitrary storage-device power-loss guarantees.

The format review found a P2 size-amplification defect in the initial encoder:
ten members sharing one 2 MiB address serialized beyond 16 MiB, and the
digest path accepted the oversized result. The retained regression failed
before repair. Encoding now preflights the total configuration, including
intent/frame overhead, before cloning/serialization, and every append obeys
the size/error latch. Digest uses the same bound. Tests accept an exact
16 MiB frame, reject one extra byte and stop all writes after an error.
All encoding/digest/publication callers were audited for error propagation.

Independent review of the planner found no validated blocker under its
explicit caller contract. It also reproduced the future-snapshot
counterexample when that contract is intentionally violated: plans built
from empty state at 10 and 100 can both commit at 50 and 150 without an
applied fence. The second overwrites version 1. Same-snapshot OCC tests do
not close this gate, nor are read keys a prefix-phantom exclusion protocol.

Production startup still reaches EnsureCatalogSnapshot and direct Save.
The retained native startup diagnostic still fails with one direct write,
local LastCommitTS 100 becoming 101, and a locally initialized version 1.
This is known-red evidence, not an approved passing test or an implemented
bootstrap. No policy/W, guarded apply/receipt journal, peer-session transport,
receiver-owned revocation, TSO durability, snapshot manifest, native baseline
or operational activation gate is marked complete by these foundations.

The exact staged-only source export passes the complete root race suite
(100.549s), distribution race suite (3.763s), format race suite (9.909s)
and whole-repository lint (zero issues). The known-red unconnected-startup
diagnostic remains outside that export and is not silently skipped.

## Review status and exclusions

### Session And Atomic Storage Components

The branch now also includes separately reviewed connection-bound peer sessions
and protected control-metadata storage. These are reusable protocol components,
not a wired production bootstrap or permission to activate policy P/W.

The session component uses the actual server-side mTLS connection, TLS exporter
and Begin/Finish proof. Subsequent proofs bind method, group, role and payload;
stream chunks are independently bound. Closed connections revoke their owned
authority. Public TLS 1.2 and existing public token behavior are preserved;
nil authorizers, forged TLS wrappers and stale transport references do not
become peer identity. Native TLS tests exercise the bridge and root fixture,
not a synthetic TLSInfo context. The production listener, all peer decoders,
membership policy and receiver-owned bootstrap arbiter remain unconnected.

Protected metadata can be staged with data/configuration projection and applied
index in one synchronous native indexed batch. Journal-only scopes do not
advance data progress. The memory implementation uses an affected-key overlay
with matching writer exclusion and abort behavior, not a whole-database copy.
Operational staging failures latch and abort the scope; ordinary replicated
request verdicts remain distinct. External snapshot/backup paths reject
protected control state rather than silently exporting it as ordinary data.

Independent native reproductions found and corrected projection-induced
watermark advancement after reopen, lost same/cross-entry OCC dependencies,
and projection/auxiliary timestamps inflating the actual SST manifest. Only the
exact configuration projection key is non-advertised; adjacent ordinary keys
remain data. All projection/blob rows remain in snapshots. Maintained tests
require the actual memory, row or SST format and verify watermark, data,
configuration, protected state and applied index after restore and reopen.
Final storage/backup race suites pass177.074s/3.323s; the original independent
review regressions plus maintained snapshot tests pass parent race3 (11.147s).
Full lint has zero issues. These component receipts do not qualify a later
composed HEAD; its own integration tests, latest-push review and CI are required.

The complete v5 receipt/manifest schema, policy activation, guarded membership,
journal replay and publication, TSO/control atomicity, agreed native baseline
and old-member recovery lanes remain acceptance gates. In particular, the
actual direct-write startup diagnostic above remains red and #1355's replica
determinism blocker is not fixed by adding these components.

V5 revises only the two new review blockers: explicit generation-bound local
follower permission distinct from owned leader rotation completion, and
successor-fenced learner catch-up authorization from retained applied-add
evidence after reservation consumption. The original honest-binary/enforcing-
quorum, strict-marker/session, lost-state nonvoting replacement and indexed
journal boundaries remain. Independent design re-review accepted these
contracts after the earlier blocking findings were addressed. This is not
implementation or merge approval. The original document-only revision ran
no runtime tests, lint, Jepsen or security review. The subsequent foundation
checkpoint above has separate evidence; production implementation and
native-baseline gates remain open.

Two safety properties are deliberately NOT established here. Historical
replica equality cannot be inferred from populated catalogs, new policy
records or matching local watermarks; the parent must establish an agreed
replicated baseline and resolve old prepared transactions separately.
Prevention of an all-old quorum after erasing/rolling back all enforcing
state is impossible with only those old processes and disks, and is outside
this honest-rollout protocol. Neither exclusion relaxes fencing a single
wiped OLD member while an enforcing quorum retains history. #1355 remains
blocked; neither the parent isolation audit nor A0/A7 is complete.
Neither new permission authorizes external/rebased restore as repair: the
existing offline new-UUID import rule remains fail-closed without its manifest
and explicit baseline prerequisites, and supplies no native-baseline proof.

## Source anchors for re-review

These are exact caller/decision lines in the sidecar source inspected for
this revision, not claims that those sites implement the proposed protocol:

| Existing source | Observed behavior / required integration |
| --- | --- |
| `main.go:4665`; `main_encryption_registration.go:137`, `:140`, `:144`, `:153`, `:156` | Distribution startup validates/arms registration watchers, then calls setupDistributionCatalog before serving. |
| `main.go:4609`; `distribution/bootstrap.go:30`; `distribution/catalog.go:777`, `:788` | Production EnsureCatalogSnapshot caller reaches direct Save/ApplyMutations with LastCommitTS + 1. Remove this production path, not just its tests. |
| `main.go:706`, `:720`, `:2813`, `:2903`, `:2904`, `:2907` | Rotation is installed after distribution startup; BlockMutators is installed on HLC/public gates and Wait precedes lease renewal, not a bootstrap-wide admission handle. |
| `main_encryption_rotate_on_startup.go:150`, `:154`, `:160-164`, `:233`, `:285` | Leader-acquired callback fires async rotation; terminal failure blocks every role, otherwise BlockMutators blocks only default leaders whose task is not done; Wait may return while follower. V5 makes follower permission explicit without claiming rotation completion. |
| `main_encryption_rotate_on_startup.go:334`, `:429`, `:440`; `internal/raftengine/etcd/engine.go:2384`, `:2391`, `:3941`, `:4002` | Current rotation completion is process-wide; RawNode Step changes term before refreshStatus callbacks. V5 binds leader completion to Rot, separates F and takes the arbiter around actual state mutation. |
| `kv/sharded_coordinator.go:1028`, `:778`, `:791`, `:819`, `:2419`, `:2423` | Dispatch/Commit registration barriers concern encrypted writes, not BlockMutators; Next/NextAfter allocate separately. |
| `kv/transaction.go:168`; `kv/raft_payload_wrapper.go:157`, `:328`; `internal/raftengine/etcd/engine.go:2051` | Transaction and encryption wrappers reach the terminal RawNode proposal. New admission must survive to this enqueue. |
| `kv/tso_raft.go:396`, `:724`; `adapter/distribution_server.go:590`, `:599` | Dedicated TSO proposes a reservation floor and remote issuance calls GetTimestamp; rollout-mode activation checks are not client authentication or rotation admission. |
| `kv/tso_pending.go:178`, `:193`, `:234`, `:240`, `:250`, `:300`, `:324`, `:330`, `:337`; `kv/tso.go:924`, `:936` | Timestamp issuance has subordinate cutover/seal/prepare/release/pending/floor operations and cached windows. Each receiver owns its generation through enqueue and result. |
| `kv/tso_fsm.go:43`, `:127`, `:287`, `:312`; `store/raft_entry.go:136`, `:185`, `:209` | TSO mirrors/snapshots are not an atomic policy store; KV entry batching needs explicit control-metadata staging. The proposed protocol defines their common receipt/policy transaction contract. |
| `kv/read_watermark_advance.go:88`, `:114`, `:133`, `:136`; `kv/fsm.go:2157`; `store/lsm_store.go:2572` | Applied watermark persistence, existing forwarding/skip conditions and fenced one-phase apply. A future allocation alone is not an applied read fence. |
| `adapter/admin_grpc.go:763`; `adapter/encryption_admin.go:1807`; `internal/grpc_tls.go:40`; `main.go:3647` | AdminTokenAuth's predicate excludes GetTimestamp and RegisterEncryptionWriter; TLS server config has no client-certificate requirement. Peer identity/session checks are separate from method/stage tokens. |
| `multiraft_runtime.go:66`, `:134`, `:162`, `:210`, `:244` | The old marker reader rejects unknown nonempty engine names before factory creation, accepts empty as legacy etcd, and currently writes by truncation. The proposed protocol requires incompatible content and atomic synced replacement. |
| `adapter/distribution_server.go:1143`, `:1146`, `:1150`; `proto/distribution.proto:220`, `:222` | Existing capability RPC reads the catalog and returns watch capabilities, not a membership-independent signed bootstrap-support receipt. |
| `internal/raftadmin/server.go:128`, `:132`, `:161`, `:194`; `main.go:4314` | PreAdd hook precedes membership submission, promotion/removal have separate paths, and current wiring composes migration/encryption gates. An RPC mutex does not serialize activation apply. |
| `main_encryption_confchange.go:114`, `:129`, `:139` | Existing pre-add registration precedes the membership proposal. The proposed protocol inserts the reservation first and binds the preserved registration step to the candidate declaration. |
| `main_migration_namespace_membership.go:112`, `:157`, `:172`, `:184` | Existing prospective probe and before/after configuration-index reads are useful patterns, not a connection-bound v1 session or an apply-time bootstrap witness. |
| `internal/raftengine/etcd/engine.go:2184`, `:2199`, `:3024`, `:3029`, `:3048`, `:3053` | ConfChange context is encoded/proposed; both committed entry forms currently call ApplyConfChange before recording FSM configuration. Add pre-apply validation and crash recovery, not only a prehook. |
| `internal/raftengine/etcd/engine.go:856`, `:915`, `:927`, `:3422`, `:3441`, `:3451`, `:3853`; `internal/raftengine/etcd/wal_store.go:41`, `:101` | Cold replay, configuration snapshots, derived peers and SaveSnap have distinct persistence boundaries. Missing WAL currently reaches join/bootstrap; it is not permission to restore a lost voting identity. |
| `kv/snapshot.go:27`, `:228`, `:255`; `kv/fsm.go:1509`, `:1541`; `store/snapshot_external.go:75`, `:93`; `internal/raftengine/etcd/engine.go:750` | Current KV v2 header extensions can be ignored; external restore strips/rebinds configuration lineage. The proposed protocol uses a required manifest and explicit new-UUID import rules; V5 retains active learner D independently of receipt suffix compaction. |
| `internal/raftengine/etcd/engine.go:2133`, `:2157`, `:2163`, `:3019`, `:3043` | Promotion inspects leader-local Progress.Match; applied configurations are recorded on both entry forms. V5 adds successor-fenced catch-up reauthorization from durable D, not a promotion bypass or a receive-progress change. |
| `proto/etcd_raft.proto:6`, `:7`, `:8`; `internal/raftengine/etcd/grpc_transport.go:913`, `:931`, `:986`, `:998`, `:1001`, `:1315` | Snapshot/stream/unary handlers forward decoded messages to the engine; all paths need authenticated identity/incarnation admission, including before snapshot spooling. |
