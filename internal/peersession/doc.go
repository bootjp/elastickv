// Package peersession implements the volatile, connection-bound v1 session
// component of the accepted catalog bootstrap design. It is not runtime policy
// activation, a catch-up grant issuer, catalog startup, or binary attestation.
//
// Security boundary:
//
//   - NewOwner clones provisioned certificate-SPKI -> cluster/member/address/
//     allowed-group mappings. TLS chain/hostname verification is mandatory.
//     The existing TLS 1.2 public contract is preserved. Peer exporter requires
//     TLS 1.3 or TLS 1.2 with EMS; standard crypto/tls rejects unavailable/unsafe
//     exporters, and renegotiation is disabled. There is no unsafe fallback.
//     Server TLS may VerifyClientCertIfGiven for unrelated public RPCs; peer RPCs
//     additionally require a verified and provisioned certificate. Shared tokens,
//     source addresses, discovery capabilities and context booleans are not identity.
//     Credentials accept static certificate configs only; dynamic certificate/
//     config callbacks are deliberately rejected rather than bypassing local-key
//     validation. PKI reload/rotation must close/recreate owners/credentials under
//     the runtime's lifecycle owner; no hot-reload integration is supplied here.
//   - TransportCredentials wraps native grpc-go TLS credentials. Its returned
//     net.Conn.Close destroys all state/keys on that actual TLS connection.
//     ClientAuthInfo/ServerAuthInfo contain private owner-minted references, not
//     cache/address-wide capabilities. A fabricated zero AuthInfo cannot authorize.
//   - Begin/Finish bind both identities, group, boot nonces, fresh challenges,
//     installed/minimum versions, state classes and owner policy/configuration
//     generation/floor/marker acknowledgments. The canonical transcript hash is
//     both the session ID and context for TLS exporter label
//     EXPORTER-elastickv-bootstrap-v1. Client-finish/server-ack use distinct
//     HMAC-SHA256 role labels. Until Finish succeeds on that SAME connection,
//     the session stays explicitly pending. There are no legacy/bearer fallbacks.
//   - Handle credentials inspect native RequestInfo.AuthInfo on EVERY gRPC
//     attempt. Reconnect/transparent retry onto another connection rejects the
//     old credential/proof. Handshake must run again. Only one Begin per group
//     per physical connection is accepted, including after a failed Finish;
//     renewal uses a fresh TLS connection. At most 64 groups can be pending/
//     complete on a connection. A retired boot nonce is not readmitted; boot
//     history is bounded to 1024 per provisioned identity, then fails closed.
//     A replacement boot revokes that identity's sessions across ALL groups.
//     Invoke/NewStream explicitly compose supplied per-call credentials with
//     the session credential; existing token headers are not replaced. Header
//     collisions deny, including case variants. Payload/role checks repeat on
//     native retry attempts as well as send/delivery of each stream message.
//   - Every restricted RPC proves full method/session/group binding. Trusted
//     adapters fully decode request/reply From/To, group and traffic class; checks
//     repeat on both stream directions and every snapshot chunk. Only locally
//     applied intact voters can vote. Lost-state stays probe-only. Prejoin and
//     learners need a CURRENT exact, reciprocally acknowledged G projection for
//     catch-up. Nil authorizer, omitted PendingClear, pending transition, unknown
//     grant phase or mismatched binding denies. G never authorizes votes/control.
//     Intact elections/PolicyControl may progress before P activation; timestamp/
//     data Control/Data additionally require a nonzero required P floor/digest.
//     Method.Stream can mint fresh per-stream decoders from the actual Binding,
//     binding metadata-free chunks to validated first metadata without global
//     state. Receipts seal each decoded message, NOT replicated apply evidence.
//
// Parent integration gates (not implemented here):
//
//   - Register this service and install ServerOptions on the EXISTING listener;
//     explicitly enumerate public methods and provide audited, complete business
//     decoders. Keep startup-token/admin/payload/epoch authorization conjunctive.
//     Registration of an unapplied candidate still needs the separate exact
//     reservation/signed delegation path, not CatchupStatus or ordinary registration.
//   - Publish only independently recovered/applied Group projections. Pending and
//     Stage have closed zero values. Call UpdateGroup BEFORE publishing every
//     role/policy/configuration/term/stage change; increment Generation. RevokeGroup
//     also covers external G/epoch revocations. Revocation cancels handles and
//     closes physical transports so blocked native receives terminate. This may
//     also terminate other RPCs multiplexed on that same peer TLS connection.
//     Group.Pending is local replay/publication reconciliation; a replicated
//     reservation does not stop the voter lane. CatchupStatus.Pending checks the
//     candidate's exact transition separately. Required floors cannot regress.
//   - A trusted CatchupAuthorizer must verify complete v5 D/G signatures, current
//     epoch, learner identity, fixed linearizable barrier, leader/term, policy,
//     configuration and reciprocal acknowledgment. Recheck runs OUTSIDE the
//     session mutex, with session/role/generation validation before and after;
//     it must honor session cancellation. This avoids a session -> external
//     arbiter -> session lock cycle. Invalidate owner
//     sessions BEFORE exposing revoked grants/terms, not just on the next chunk.
//   - Use Handle.Invoke/NewStream on send, Inbound/CheckIncoming at runtime
//     enqueue/install/result boundaries under the runtime's OWN admission arbiter.
//     For stateful streams capture Received immediately after each RecvMsg and
//     use Receipt.Check at later boundaries, without consuming a decoder twice.
//     The interceptor's earlier check is not an atomic engine-enqueue permission.
//     After validated metadata/first chunk, own snapshot spools by handler context
//     and discard on cancellation/failure. Close old local transports before a
//     new boot starts, and Close owners/drain handlers on shutdown. Remote changes
//     are enforced when locally observed, never claimed instantaneously known.
//   - adapter/split_migration_auth.go recognizes legacy native TLSInfo and the
//     owner's current server connection through TransportTLSInfo. An unminted,
//     client-side or closed wrapper cannot provide this transport proof. Public
//     TLS 1.2 without a client certificate retains migration token authorization.
//     This bridge proves confidentiality only, not peer/session/role admission;
//     actual listener and decoder integration above remains required.
//
// Wire/allocation boundary:
//
// Dedicated generated protobuf Frame has exactly one nonempty body field.
// StrictCodec rejects unknown/duplicate fields, overlong lengths, truncation,
// trailing bytes and encoded new Frames over 16 MiB BEFORE Body clone/nested
// record decoding. Binary records begin EKVPS001:8 | kind:u8 (Begin=1,
// answer=2, Finish=3, ack=4); integers are big-endian, UUIDs/digests/nonces are
// fixed 16/32/32 bytes, strings have u32 byte lengths. Begin encodes client then
// server Node(cluster,member:u64,address,key), group:u64, client boot/challenge,
// then view(installed/minimum:6*u32, floor:2*u64, policy/configuration:2*32,
// generation:u64, state:u8, marker-ready:u8). Answer encodes request SHA-256,
// server boot/challenge and server view. Finish/ack encode session:32, proof:32.
// Transcript SHA-256 covers u32-length Begin and u32-length answer in that order.
// Fixed layouts plus canonical re-encoding reject unknown kinds/versions/states,
// noncanonical booleans, missing values, overflow, truncation and trailing bytes.
// SHA/HMAC use standard crypto; no secrets or exporter keys are logged.
//
// Shared native gRPC framing remains internal.GRPCMaxMessageBytes (64 MiB), as
// do all existing public/business protobuf codecs and Handle business call caps.
// grpc-go v1.84 has no per-method server allocation cap: its encoded wire buffer
// (up to 64 MiB) may ALREADY exist when StrictCodec runs. We do not claim zero
// wire allocation or lower the shared cap to simulate it. Begin/Finish CLIENT
// receive caps are 16 MiB; oversized new Frame rejection precedes body allocation.
// Native public >16 MiB, real EtcdRaft MsgApp forwarding >16 MiB, old 64 MiB cap,
// and zero additional allocations on oversized Frame rejection are tested.
//
// Pinned-source audit: grpc-go credentials/tls.go returns TLSInfo containing the
// verified ConnectionState/exporter. internal/transport/http2_client.go supplies
// that transport's AuthInfo and full method to PerRPCCredentials on each attempt;
// http2_server.go supplies it to peer.Peer. Both close the returned net.Conn on
// transport teardown. rpc_util.go parser.recvMsg checks the shared receive cap
// before reading the wire body; codec execution follows that read. No custom
// HTTP/2 parser, process-global credentials or new public listener is introduced.
package peersession
