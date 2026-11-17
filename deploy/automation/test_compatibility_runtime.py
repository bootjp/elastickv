"""Bounded synthetic-protocol and observation finalization checks."""

import hashlib
import io
import unittest
from unittest.mock import Mock, patch

import agent
import compatibility_runtime as runtime


class RuntimeEvidenceTests(unittest.TestCase):
    def verify_messages(self, batches):
        clients = {name: Mock() for name in ("dynamodb", "s3", "sqs")}
        clients["dynamodb"].get_item.return_value = {
            "Item": {"checksum": {"S": hashlib.sha256(runtime.PAYLOAD).hexdigest()}}
        }
        clients["s3"].get_object.return_value = {"Body": io.BytesIO(runtime.PAYLOAD)}
        clients["sqs"].get_queue_url.return_value = {"QueueUrl": "synthetic"}
        clients["sqs"].receive_message.side_effect = [
            {"Messages": [{"Body": body} for body in batch]} for batch in batches
        ]
        with (
            patch.object(runtime, "clients", return_value=clients),
            patch.object(
                runtime,
                "redis",
                side_effect=lambda _, name, cmd, key: runtime.VALUES[key].encode(),
            ),
        ):
            return runtime.verify_seed(Mock())

    def test_partial_and_repeated_sqs_delivery_retains_exact_body_check(self):
        a, b = sorted(runtime.MESSAGES)
        self.assertEqual(
            2, self.verify_messages([[a], [a], [b]])["sqs_message_bodies_preserved"]
        )
        with self.assertRaises(agent.ReleaseError):
            self.verify_messages([[a], ["unexpected"]])
        with self.assertRaises(agent.ReleaseError):
            self.verify_messages([[a]] * 10)

    def test_evidence_cannot_finalize_while_observer_is_running(self):
        observer = runtime.Observer(Mock())
        observer.thread = Mock()
        observer.thread.is_alive.return_value = True
        with self.assertRaises(agent.ReleaseError):
            observer.__exit__(None, None, None)
        self.assertTrue(observer.done.is_set())
        observer.thread.is_alive.return_value = False
        observer.__exit__(None, None, None)
