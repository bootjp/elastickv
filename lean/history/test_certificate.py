import unittest

from certificate import fifo_batch, fifo_observations, make, projection


def kills_and_recovery(with_message=True):
    events = []
    if with_message:
        for t in ("invoke", "ok"):
            events.append(dict(process="n1", node="n1", type=t, f="send", value="m"))
    for node in ("n1", "n2", "n3"):
        for f, value in (("kill", [node]), ("start", dict.fromkeys(("n1", "n2", "n3")))):
            for t in ("invoke", "info"):
                events.append(dict(process="nemesis", type=t, f=f, target=node, value=value))
    for t in ("invoke", "info"):
        events.append(dict(process="nemesis", type=t, f="start", recovery=True,
                           value=dict.fromkeys(("n1", "n2", "n3"))))
    for n in ("n1", "n2", "n3"):
        for t in ("invoke", "ok"):
            values = ["m"] if with_message and n == "n1" and t == "ok" else []
            events.append(dict(process=n, node=n, type=t, f="recv", value=values, deleted=values, final=True))
    for i, e in enumerate(events):
        e["index"] = i
    return events


class QueueEvidenceTest(unittest.TestCase):
    def test_send_required(self):
        with self.assertRaisesRegex(ValueError, "no acknowledged send"):
            make("sqs-standard", kills_and_recovery(False))

    def test_fault_after_recovery(self):
        events = kills_and_recovery()
        start = next(i for i, event in enumerate(events) if event.get("target") == "n3")
        faults = events[start:start+4]
        del events[start:start+4]
        events.extend(faults)
        for i, event in enumerate(events):
            event["index"] = i
        with self.assertRaisesRegex(ValueError, "fault after recovery"):
            make("sqs-standard", events)

    def test_fifo_redelivery_identity(self):
        def send(identity):
            return ({"f": "send"}, {"type": "ok", "value": identity, "sqs-observation": {
                "clock-id": "c", "send-start": 1, "send-end": 2}})

        def recv(at, identity, message, receipt):
            return ({"f": "recv"}, {"type": "ok", "sqs-observation": {
                "clock-id": "c", "receive-start": at, "received-at": at + 1, "deliveries": [
                    {"tuple": identity, "delivery-index": 0, "message-hash": message * 64,
                     "receipt-hash": receipt * 64}]}})

        one, two = (0, 1), (0, 2)
        first = [send(one), send(two), recv(3, one, "a", "1")]
        self.assertEqual(2, len(fifo_observations(first + [recv(6, one, "a", "2")])))
        for delivery, error in ((recv(6, two, "b", "1"), "reused FIFO receipt"),
                                (recv(6, one, "b", "2"), "changed FIFO message ID"),
                                (recv(6, two, "a", "2"), "reused FIFO message ID")):
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                fifo_observations(first + [delivery])

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
