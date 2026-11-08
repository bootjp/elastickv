"""Exercise the actual dashboard queries against colliding deployment labels.

Run with a compatible promtool on PATH: python3 -m unittest discover -s monitoring/tests
"""
import json
import pathlib
import re
import subprocess
import tempfile
import unittest


class DashboardMatchingTest(unittest.TestCase):
    def test_raft_index_differences_with_all_deployments(self):
        dashboards = pathlib.Path(__file__).resolve().parents[1] / 'grafana' / 'dashboards'
        inputs = []
        # Reused node IDs and addresses across jobs, clusters, and an unlabeled cluster.
        for labels, commit, applied, last_log in [
            ('job="kv",cluster="a"', 25, 20, 28),
            ('job="kv",cluster="b"', 45, 30, 50),
            ('job="other",cluster="a"', 90, 50, 99),
            ('job="kv"', 100, 40, 110),
        ]:
            for metric, value in [('commit', commit), ('applied', applied), ('last_log', last_log)]:
                inputs.append({
                    'series': f'elastickv_raft_{metric}_index{{{labels},group="1",node_id="same",node_address="peer:50051"}}',
                    'values': f'{value}+0x5',
                })

        def expressions(node):
            if isinstance(node, dict):
                if 'expr' in node:
                    yield node['expr']
                for value in node.values():
                    yield from expressions(value)
            elif isinstance(node, list):
                for value in node:
                    yield from expressions(value)

        cases = []
        for filename in ['elastickv-cluster-overview.json', 'elastickv-raft-status.json']:
            for expr in expressions(json.loads((dashboards / filename).read_text())):
                if ' - on(' not in expr or 'elastickv_raft_applied_index' not in expr:
                    continue
                # Expand the All selections exactly as regex matchers; do not rewrite matching labels.
                query = re.sub(r'\$\w+', '.*', expr)
                labels = '{}'
                if expr.startswith('max by (group, node_id, node_address)'):
                    labels = '{group="1",node_id="same",node_address="peer:50051"}'
                elif expr.startswith('max by (group)'):
                    labels = '{group="1"}'
                cases.append({'expr': query, 'eval_time': '1m', 'exp_samples': [
                    {'labels': labels, 'value': 70 if 'last_log_index' in expr else 60}
                ]})
        self.assertEqual(len(cases), 9, 'Exercise every index subtraction panel')
        fixture = {'evaluation_interval': '1m', 'tests': [{
            'interval': '1m', 'input_series': inputs, 'promql_expr_test': cases,
        }]}
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / 'dashboard-matching.json'
            path.write_text(json.dumps(fixture))
            result = subprocess.run(['promtool', 'test', 'rules', str(path)],
                                    capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
