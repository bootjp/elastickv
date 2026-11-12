// Package tsowiring configures a node's timestamp oracle for the operator's
// rollout stage: it owns the TSO flags and mode-file validation, wires the
// legacy local bridge or the dedicated group-0 allocator with leader routing
// and the runtime mode controller, authorizes one-way cutover / Phase-D
// activation against the local mode, and starts the mode reloader and the HLC
// lease renewal loop. The binary passes the dedicated group and any extra
// leader-routing options (such as protected-bootstrap dial options).
package tsowiring
