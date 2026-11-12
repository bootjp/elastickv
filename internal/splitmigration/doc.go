// Package splitmigration wires split migration into a server process: the
// capability gate and its peer probe, the promotion / migration / apply-member
// client factories with their dedicated bearer token, the membership and
// namespace admission gates, the ownership-refresh runner, and the readiness
// gate with the gRPC coverage interceptor that fail closed until migration is
// enabled. The binary adapts its Raft runtimes and protected-bootstrap dial
// options to the plain inputs these take.
package splitmigration
