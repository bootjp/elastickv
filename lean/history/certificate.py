"""Construct a candidate execution for a hosted kill history. Lean checks the order."""
import argparse
from collections import defaultdict
from graphlib import TopologicalSorter
import json
from pathlib import Path


def require(ok, message):
    if not ok:
        raise ValueError(message)


def paired(workload, events):
    pending = {}
    stops = []
    clients = []
    for i, e in enumerate(events):
        require(e["index"] == i, f"missing event at {i}")
        p = e["process"]
        if p == "nemesis":
            stops.append(e)
        elif e["type"] == "invoke":
            require(p not in pending, f"overlapping calls by {p}")
            pending[p] = e
        else:
            start = pending.pop(p)
            require(start["f"] == e["f"], f"unmatched response at {i}")
            require(e["type"] in ("ok", "fail", "info"), f"unknown response at {i}")
            clients.append((start, e))
    require(not pending and len(stops) % 2 == 0, "incomplete history")
    kills, starts = set(), set()
    healed = None
    down = set()
    nodes = {"n1", "n2", "n3"}
    for a, b in zip(stops[::2], stops[1::2]):
        require(a["f"] == b["f"] and a.get("target") == b.get("target")
                and b["type"] == "info" and not b.get("error"), "incomplete fault")
        if a["f"] == "kill":
            target = a["target"]
            require(target in nodes and not down and target in b["value"], "unconfirmed kill")
            kills.add(target)
            down.add(target)
        elif a["f"] == "start":
            require(isinstance(b["value"], dict) and set(b["value"]) == nodes, "unconfirmed start")
            if a.get("recovery") and b.get("recovery"):
                healed = b["index"]
            else:
                require(down == {a["target"]}, "out-of-order restart")
                starts.add(a["target"])
            down.clear()
        else:
            raise ValueError("expected a process kill/start history")
    require(kills == starts == nodes and healed is not None, "missing restart")
    finals = [(a, b) for a, b in clients if a.get("final")]
    require(all(a["index"] > healed for a, _ in finals), "early recovery read")
    observed = {b.get("node") for _, b in finals if b["type"] == "ok"
                and (not workload.startswith("sqs-") or
                     (b["f"] == "recv" and b.get("receive-performed?") is not False))}
    require(observed == nodes,
            "missing recovery observation")
    return clients


def action(kind, key, value=()):
    return dict(kind=kind, key=str(key), value=list(value))


def projection(workload, a, b):
    f, iv, rv = a["f"], a["value"], b["value"]
    ok = b["type"] == "ok"
    if workload in ("redis", "dynamodb"):
        require(f == "txn", f)
        if not ok:
            return [action("append", k, [v]) for tag, k, v in iv if tag == "append"]
        require(len(iv) == len(rv), "transaction response length")
        result = []
        for (tag, k, arg), (out, key, val) in zip(iv, rv):
            require((tag, k) == (out, key), "transaction response shape")
            if tag == "append":
                require(arg == val, "append identity")
                result.append(action(tag, k, [arg]))
            else:
                require(tag == "r" and arg is None, "unexpected micro-operation")
                result.append(action("read", k, [] if val is None else val))
        return result
    if workload.startswith("sqs-"):
        fifo = workload == "sqs-htfifo"
        def ident(v):
            return json.dumps(v, separators=(",", ":"))
        if f == "send":
            require(rv is not None or not ok, "missing send identity")
            return [] if rv is None else [action("send", ident(rv))]
        require(f == "recv", f)
        if fifo:
            obs = b.get("sqs-observation", {})
            batch = fifo_batch(obs, b)
            raw = [d["tuple"] for d in batch]
            deleted = [d["tuple"] for d in batch if d.get("delete", {}).get("type") == "ok"]
            require(deleted == (rv or []), "FIFO delete acknowledgment mismatch")
        else:
            raw = rv or []
            deleted = b.get("deleted", [])
        return ([action("deliver", ident(v)) for v in raw]
                + [action("delete", ident(v)) for v in deleted])
    require(f in ("read", "write") and len(iv) == 2, f)
    if f == "read" and not ok:
        return []
    require(rv[0] == iv[0] and (f == "read" or rv == iv), "register identity")
    return [action(f, iv[0], [] if rv[1] is None else [rv[1]])]


class Witness:
    def __init__(self, calls):
        self.calls = calls
        self.chosen = {i for i, c in enumerate(calls) if c["status"] == "ok"}
        self.order = TopologicalSorter()

    def before(self, a, b):
        if a != b:
            self.order.add(b, a)

    def serialize(self):
        # A chain of event nodes represents real time without quadratic edges.
        times = set()
        for i in self.chosen:
            c = self.calls[i]
            times.add(c["start"])
            self.before(("time", c["start"]), i)
            if c["status"] != "info":
                times.add(c["stop"])
                self.before(i, ("time", c["stop"]))
        ordered = sorted(times)
        for a, b in zip(ordered, ordered[1:]):
            self.before(("time", a), ("time", b))
        for rank, i in enumerate(self.order.static_order()):
            if isinstance(i, int):
                require(i in self.chosen, "dependency on excluded call")
                self.calls[i]["rank"] = rank


def registers(w, initial):
    by_key = defaultdict(lambda: defaultdict(list))
    producers = {}
    observed = set()
    for i, c in enumerate(w.calls):
        for a in c["actions"]:
            kv = a["key"], tuple(a["value"])
            if a["kind"] == "write" and c["status"] != "fail":
                require(kv not in producers, f"non-unique register writer {kv}")
                producers[kv] = i
            elif a["kind"] == "read" and i in w.chosen:
                observed.add(kv)
    for kv in observed:
        if kv[1] != initial:
            w.chosen.add(producers[kv])
    for i in sorted(w.chosen):
        a, = w.calls[i]["actions"]
        by_key[a["key"]][tuple(a["value"])].append(i)
    for key, groups in by_key.items():
        order = TopologicalSorter()
        bounds = {}
        for v, group in groups.items():
            order.add(v)
            if v != initial:
                for i in group:
                    w.before(producers[key, v], i)
            bounds[v] = (min((w.calls[i]["stop"] for i in group
                              if w.calls[i]["status"] != "info"), default=float("inf")),
                         max(w.calls[i]["start"] for i in group))
        for a, (end, _) in bounds.items():
            for b, (_, start) in bounds.items():
                if a != b and (a == initial or end < start):
                    order.add(b, a)
        values = list(order.static_order())
        for a, b in zip(values, values[1:]):
            for i in groups[a]:
                w.before(i, producers[key, b])
    return [(k, list(initial)) for k in by_key]


def appends(w, finals):
    final = {}
    for i in finals:
        for a in w.calls[i]["actions"]:
            k, values = a["key"], a["value"]
            require(a["kind"] == "read" and final.setdefault(k, values) == values,
                    f"divergent final list for {k}")
    writer = {}
    for i, c in enumerate(w.calls):
        for a in c["actions"]:
            if a["kind"] == "append" and c["status"] != "fail":
                k, v = a["key"], a["value"][0]
                require((k, v) not in writer, "non-unique append identity")
                writer[k, v] = i
    for k, vs in final.items():
        owners = [writer[k, v] for v in vs]
        w.chosen.update(owners)
        for a, b in zip(owners, owners[1:]):
            w.before(a, b)
    for i in w.chosen:
        for a in w.calls[i]["actions"]:
            if a["kind"] != "read":
                continue
            k, prefix = a["key"], a["value"]
            values = final[k]
            require(values[:len(prefix)] == prefix, f"non-prefix read at {i}")
            if prefix:
                w.before(writer[k, prefix[-1]], i)
            if len(prefix) < len(values):
                w.before(i, writer[k, values[len(prefix)]])
    return []


def queue(w, fifo):
    sends, deliveries, deletes = {}, defaultdict(list), {}
    for i, c in enumerate(w.calls):
        for a in c["actions"]:
            k = a["key"]
            if a["kind"] == "send" and c["status"] != "fail":
                require(k not in sends, "duplicate send identity")
                sends[k] = i
            elif a["kind"] == "deliver":
                deliveries[k].append(i)
            elif a["kind"] == "delete":
                require(k not in deletes, "two acknowledged deletions")
                deletes[k] = i
    for k, calls in deliveries.items():
        w.chosen.add(sends[k])
        for i in calls:
            w.before(sends[k], i)
            if k in deletes:
                w.before(i, deletes[k])
    for k, i in sends.items():
        if w.calls[i]["status"] == "ok":
            require(k in deliveries, "acknowledged send missing from drain")
            require(fifo or k in deletes, "acknowledged send missing confirmed delete")
    return []


def fifo_batch(obs, response):
    batch = obs.get("deliveries")
    if obs.get("received-at") is None:
        require(response["type"] != "ok" and batch is None and not response["value"],
                "missing FIFO receive response")
        return []
    require(isinstance(batch, list), "missing FIFO raw batch")
    require(0 <= obs["receive-start"] <= obs["received-at"], "invalid FIFO receive interval")
    return batch


def fifo_observations(pairs):
    sends = {}
    observed = []
    clocks = set()
    for a, b in pairs:
        obs = b.get("sqs-observation", {})
        if obs:
            clocks.add(obs["clock-id"])
        if a["f"] == "send" and "send-start" in obs and b["type"] != "fail":
            sends[tuple(b["value"])] = (obs["send-start"], obs.get("send-end"))
        if a["f"] == "recv":
            for i, raw in enumerate(fifo_batch(obs, b)):
                require(raw["delivery-index"] == i, "invalid FIFO delivery index")
                deletion = raw.get("delete", {})
                end = None
                if deletion.get("type") == "ok":
                    require(deletion["delivery-index"] == i and deletion["receipt-hash"] == raw["receipt-hash"]
                            and obs["received-at"] <= deletion["start"] <= deletion["end"],
                            "invalid FIFO confirmed deletion")
                    end = deletion["end"]
                observed.append((obs["received-at"], i, tuple(raw["tuple"]), obs["receive-start"], end))
    require(len(clocks) == 1 and observed, "missing or inconsistent FIFO observation clock")
    result = []
    for received, idx, identity, receive_start, delete_end in sorted(observed, key=lambda row: row[:2]):
        start, end = sends[identity]
        require(start <= received, "delivery predates send")
        result.append(dict(group=identity[0], sequence=identity[1], receiveStart=receive_start,
                           deleteEnd=delete_end, sendStart=start, sendEnd=end,
                           receivedAt=received, batchIndex=idx))
    return result


def make(workload, events):
    pairs = paired(workload, events)
    calls, finals = [], []
    touched = set()
    recovered = defaultdict(set)
    for a, b in pairs:
        acts = projection(workload, a, b)
        status = b["type"]
        if workload.startswith("sqs-") and a["f"] == "recv" and acts:
            # The component receipt/delete responses observed before an outer
            # error are mandatory effects, bounded by that outer response.
            status = "ok"
        if not workload.startswith("sqs-"):
            keys = {op[1] for op in a["value"]} if a["f"] == "txn" else {a["value"][0]}
            if a.get("final"):
                recovered[b["node"]].update(keys)
            else:
                touched.update(keys)
        if a.get("final") and not workload.startswith("sqs-"):
            require(status == "ok", "unsuccessful final read")
            finals.append(len(calls))
        calls.append(dict(start=a["index"], stop=b["index"], status=status,
                          actions=acts, rank=None))
    if not workload.startswith("sqs-"):
        require(touched and all(touched <= recovered[n] for n in ("n1", "n2", "n3")),
                "missing final read for a touched key")
    w = Witness(calls)
    if workload in ("dynamodb", "redis"):
        initial = appends(w, finals)
    elif workload.startswith("sqs-"):
        initial = queue(w, workload == "sqs-htfifo")
    else:
        initial = registers(w, (-1,) if workload.startswith("filesystem-") else ())
    w.serialize()
    deliveries = fifo_observations(pairs) if workload == "sqs-htfifo" else []
    return dict(initial=initial, calls=calls, deliveries=deliveries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", choices=["dynamodb", "redis", "grpc-raw", "grpc-transactional",
                                            "s3", "s3-offload", "filesystem-overwrite", "filesystem-rename",
                                            "sqs-standard", "sqs-htfifo"])
    parser.add_argument("history", type=Path)
    parser.add_argument("certificate", type=Path)
    args = parser.parse_args()
    with args.history.open() as src:
        result = make(args.workload, [json.loads(line) for line in src])
    args.certificate.write_text(json.dumps(result, separators=(",", ":")))
