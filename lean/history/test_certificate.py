import unittest

from certificate import fifo_batch, make, projection


def kills_and_recovery():
    events = []
    for node in ("n1", "n2", "n3"):
        for f, value in (("kill", [node]), ("start", dict.fromkeys(("n1", "n2", "n3")))):
            for t in ("invoke", "info"):
                events.append(dict(process="nemesis", type=t, f=f, target=node, value=value))
    for t in ("invoke", "info"):
        events.append(dict(process="nemesis", type=t, f="start", recovery=True,
                           value=dict.fromkeys(("n1", "n2", "n3"))))
    for n in ("n1", "n2", "n3"):
        for t in ("invoke", "ok"):
            events.append(dict(process=n, node=n, type=t, f="recv", value=[], final=True))
    for i, e in enumerate(events):
        e["index"] = i
    return events


class QueueEvidenceTest(unittest.TestCase):
    def test_delete_only_preserves_deletion_without_counting_as_receive(self):
        events = kills_and_recovery()
        self.assertIsNotNone(make("sqs-standard", events))
        reply = events[-1]
        reply.update({"receive-performed?": False, "deleted": ["m"]})
        self.assertEqual(["delete"], [a["kind"] for a in projection("sqs-standard", events[-2], reply)])
        with self.assertRaisesRegex(ValueError, "missing recovery observation"):
            make("sqs-standard", events)

    def test_observed_batch_must_be_explicit(self):
        response = {"type": "info", "value": []}
        self.assertEqual([], fifo_batch({"receive-start": 1}, response))
        observed = {"receive-start": 1, "received-at": 2}
        with self.assertRaisesRegex(ValueError, "missing FIFO raw batch"):
            fifo_batch(observed, response)
        self.assertEqual([], fifo_batch(dict(observed, deliveries=[]), response))
