package monitoring

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// clusterWideFilesystemGauges are refreshed by every mounted node from a scan
// of the whole store, so each node exports the same cluster-wide value. A
// dashboard or alert that sums them across nodes multiplies the real count by
// the number of mounted nodes; they have to be aggregated with max.
var clusterWideFilesystemGauges = []string{
	"elastickv_fs_open_handle_lease_active",
	"elastickv_fs_orphan_inode_gc_pending",
	"elastickv_fs_files_by_group",
	"elastickv_fs_file_multi_shard_current",
	"elastickv_fs_file_move_inflight",
}

// nodeLocalFilesystemGauges describe only the exporting node, so they are
// summed across nodes; a max would hide every node but one.
var nodeLocalFilesystemGauges = []string{
	"elastickv_fs_file_move_active_local",
}

func filesystemDashboardExprs(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("grafana", "dashboards", "elastickv-filesystem.json"))
	require.NoError(t, err)
	var doc any
	require.NoError(t, json.Unmarshal(raw, &doc))
	return collectExprs(doc)
}

func collectExprs(node any) []string {
	switch v := node.(type) {
	case map[string]any:
		var out []string
		for key, child := range v {
			if key == "expr" {
				if expr, ok := child.(string); ok {
					out = append(out, expr)
				}
				continue
			}
			out = append(out, collectExprs(child)...)
		}
		return out
	case []any:
		var out []string
		for _, child := range v {
			out = append(out, collectExprs(child)...)
		}
		return out
	default:
		return nil
	}
}

func filesystemAlertExprs(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("prometheus", "rules", "filesystem.yml"))
	require.NoError(t, err)
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if expr, ok := strings.CutPrefix(trimmed, "expr:"); ok {
			out = append(out, strings.TrimSpace(expr))
		}
	}
	require.NotEmpty(t, out)
	return out
}

func aggregationOver(aggregator, metric string) *regexp.Regexp {
	return regexp.MustCompile(aggregator + `(\s+by\s*\([^)]*\))?\s*\(\s*` + regexp.QuoteMeta(metric) + `\b`)
}

func TestFilesystemClusterWideGaugesAreNotSummedAcrossNodes(t *testing.T) {
	t.Parallel()

	sources := map[string][]string{
		"dashboard": filesystemDashboardExprs(t),
		"alerts":    filesystemAlertExprs(t),
	}
	for name, exprs := range sources {
		for _, metric := range clusterWideFilesystemGauges {
			summed := aggregationOver("sum", metric)
			maxed := aggregationOver("max", metric)
			seen := false
			for _, expr := range exprs {
				if !strings.Contains(expr, metric) {
					continue
				}
				seen = true
				require.Falsef(t, summed.MatchString(expr), "%s sums cluster-wide gauge %s across nodes: %s", name, metric, expr)
				require.Truef(t, maxed.MatchString(expr), "%s must aggregate cluster-wide gauge %s with max: %s", name, metric, expr)
			}
			if name == "dashboard" {
				require.Truef(t, seen, "dashboard has no panel for %s", metric)
			}
		}
		for _, metric := range nodeLocalFilesystemGauges {
			summed := aggregationOver("sum", metric)
			maxed := aggregationOver("max", metric)
			seen := false
			for _, expr := range exprs {
				if !strings.Contains(expr, metric) {
					continue
				}
				seen = true
				require.Falsef(t, maxed.MatchString(expr), "%s takes max of node-local gauge %s: %s", name, metric, expr)
				require.Truef(t, summed.MatchString(expr), "%s must aggregate node-local gauge %s with sum: %s", name, metric, expr)
			}
			if name == "dashboard" {
				require.Truef(t, seen, "dashboard has no panel for %s", metric)
			}
		}
	}
}
