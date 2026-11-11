package main

import (
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/keyviz"
)

// keyVizFanoutConfig bundles the operator-supplied fan-out flags.
// Empty Nodes leaves the keyviz handler in single-node mode.
type keyVizFanoutConfig struct {
	Nodes   []string
	Timeout time.Duration
}

// keyvizSourceFromSampler boxes a *keyviz.MemSampler into the
// admin.KeyVizSource interface understood by ServerDeps. Returning a
// nil interface (not a typed-nil) when the sampler is disabled is
// load-bearing: the admin handler's "keyviz disabled → 503" branch
// only fires on an interface-nil; a typed-nil *MemSampler stored as
// a non-nil interface would silently return an empty matrix instead
// of the explicit "feature off" signal the SPA expects.
func keyvizSourceFromSampler(s *keyviz.MemSampler) admin.KeyVizSource {
	if s == nil {
		return nil
	}
	return s
}

// keyvizHotKeysSourceFromSampler is the keyvizSourceFromSampler twin
// for the per-cell hot-key drill-down handler. Returning an interface-nil
// (not a typed-nil) when the sampler is disabled is again load-bearing:
// the hot-keys handler's "keyviz disabled → 503" branch checks for
// interface-nil, and a typed-nil *MemSampler stored as a non-nil
// interface would route the request into MemSampler methods that
// already nil-check internally — correct but masking the explicit
// "feature off" signal we want the SPA to render.
func keyvizHotKeysSourceFromSampler(s *keyviz.MemSampler) admin.KeyVizHotKeysSource {
	if s == nil {
		return nil
	}
	return s
}

// buildKeyVizFanout assembles the Phase 2-C fan-out aggregator from
// the operator-supplied flag values. selfListen is the local admin
// listener address (used to filter the local node out of the peer
// list so symmetric `--keyvizFanoutNodes=node1,node2,node3` configs
// stamped onto every host do not loop back over HTTP). Returns nil
// when no peers remain after filtering, leaving the keyviz handler
// in single-node mode.
//
// The matching rule is conservative: a peer is treated as "self"
// when its host:port equals selfListen literally OR when it equals
// selfListen with the host normalised to 127.0.0.1 (operators
// commonly bind admin to 0.0.0.0 but list 127.0.0.1 as the
// per-host fan-out entry). Anything else is treated as a peer.
func buildKeyVizFanout(selfListen string, cfg keyVizFanoutConfig) *admin.KeyVizFanout {
	if len(cfg.Nodes) == 0 {
		return nil
	}
	peers := make([]string, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		if isSelfFanoutNode(selfListen, n) {
			continue
		}
		peers = append(peers, n)
	}
	if len(peers) == 0 {
		return nil
	}
	f := admin.NewKeyVizFanout(selfListen, peers)
	if cfg.Timeout > 0 {
		f = f.WithTimeout(cfg.Timeout)
	}
	return f.WithLogger(slog.Default().With(slog.String("component", "admin.keyviz.fanout")))
}

// buildKeyVizHotKeysFanout is the hot-keys companion of
// buildKeyVizFanout: same peer-list reuse, same self-filter rule.
// Returns nil when no peers remain after filtering (single-node
// hot-keys behaviour preserved). Operators get the cluster-wide
// drill-down for free by leaving --keyvizFanoutNodes set — no
// separate flag, matching design §6.
func buildKeyVizHotKeysFanout(selfListen string, cfg keyVizFanoutConfig) *admin.KeyVizHotKeysFanout {
	if len(cfg.Nodes) == 0 {
		return nil
	}
	peers := make([]string, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		if isSelfFanoutNode(selfListen, n) {
			continue
		}
		peers = append(peers, n)
	}
	if len(peers) == 0 {
		return nil
	}
	f := admin.NewKeyVizHotKeysFanout(selfListen, peers)
	if cfg.Timeout > 0 {
		f = f.WithTimeout(cfg.Timeout)
	}
	return f.WithLogger(slog.Default().With(slog.String("component", "admin.keyviz.hotkeys.fanout")))
}

// isSelfFanoutNode returns true when n names this node's own admin
// listener. A relaxed match handles the common bind-vs-advertise
// asymmetry: bind on 0.0.0.0:8080 but advertise (and list) as
// 127.0.0.1:8080.
func isSelfFanoutNode(selfListen, n string) bool {
	n = strings.TrimSpace(n)
	if n == "" {
		return true
	}
	stripped := stripScheme(n)
	if stripped == selfListen {
		return true
	}
	host, port, err := net.SplitHostPort(stripped)
	if err != nil {
		return false
	}
	selfHost, selfPort, err := net.SplitHostPort(selfListen)
	if err != nil || port != selfPort {
		return false
	}
	if isWildcardHost(selfHost) {
		return isLoopbackHost(host)
	}
	return host == selfHost
}

func isWildcardHost(h string) bool { return h == "0.0.0.0" || h == "::" || h == "" }

func isLoopbackHost(h string) bool {
	return h == "127.0.0.1" || h == "localhost" || h == "::1"
}

func stripScheme(raw string) string {
	if i := strings.Index(raw, "://"); i >= 0 {
		return raw[i+3:]
	}
	return raw
}

// parseCSV splits a flag value like "a,b,c" into a slice with empty and
// whitespace-only entries dropped. It is not in shard_config.go because
// admin's comma-separated list format is simpler than raft groups.
func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trim := strings.TrimSpace(p); trim != "" {
			out = append(out, trim)
		}
	}
	return out
}
