"""Synthetic protocol checks confined to the dedicated compatibility namespace."""

import base64
import hashlib
import json
import os
import re
import socket
import threading
import time
import urllib.parse

import agent
from settings import CONFIG as SITE

NS = os.environ.get("ELASTICKV_COMPAT_NAMESPACE", SITE["validation_namespace"])
if NS != SITE["validation_namespace"] or NS == SITE["production_namespace"]:
    raise agent.UnsafeState("Invalid dedicated compatibility namespace")
MEMBERS = ["elastickv-" + str(i) for i in range(3)]
SEED = "compatibility-" + os.environ.get("POD_UID", "local")[:12]
PAYLOAD = bytes(range(256)) * 1024
VALUES = {
    f"compatibility-seed:{i:04d}": hashlib.sha256(f"{NS}:{i}".encode()).hexdigest()
    for i in range(128)
}
MESSAGES = {"compatibility-preserve-A", "compatibility-preserve-B"}
if not __debug__:
    raise agent.UnsafeState("Python optimization must remain disabled")


class ScopedKubernetes(agent.Kubernetes):
    def request(self, path, method="GET", data=None):
        expected = getattr(self, "expected_volumes", {})
        if path.startswith("/api/v1/persistentvolumes/"):
            if method != "GET" or path.rsplit("/", 1)[-1] not in {
                r["pv"] for r in expected.values()
            }:
                raise agent.UnsafeState("Unregistered volume access forbidden")
            return super().request(path, method, data)
        match = re.search(r"/namespaces/([^/]+)/", path)
        if not match or match.group(1) != NS:
            raise agent.UnsafeState(
                "Production or cluster-scoped Kubernetes access forbidden"
            )
        result = super().request(path, method, data)
        if method == "GET" and "/persistentvolumeclaims/" in path:
            from compatibility_profile import verify_dedicated_volume

            name = path.rsplit("/", 1)[-1]
            if name not in expected:
                raise agent.UnsafeState("Unregistered claim access forbidden")
            pv = self.request("/api/v1/persistentvolumes/" + expected[name]["pv"])
            try:
                verify_dedicated_volume(result, pv, expected[name], NS)
            except ValueError as error:
                raise agent.UnsafeState(str(error)) from None
        return result


def redis(runtime, name, *parts):
    data = b"*" + str(len(parts)).encode() + b"\r\n"
    for part in parts:
        raw = str(part).encode()
        data += b"$" + str(len(raw)).encode() + b"\r\n" + raw + b"\r\n"
    host = name + ".elastickv-peer." + NS + ".svc"
    with socket.create_connection((host, 6379), timeout=5) as connection:
        connection.settimeout(15)
        connection.sendall(data)
        reader = connection.makefile("rb")
        result = reader.readline()
        if result.startswith(b"-"):
            raise agent.ReleaseError("Validation Redis command failed")
        if result.startswith(b"$"):
            length = int(result[1:-2])
            return reader.read(length) if length >= 0 else None
        return result


def clients(runtime):
    import boto3
    from botocore.config import Config

    secret = runtime.kube.request(
        runtime.namespace + "/secrets/elastickv-api-credentials"
    )
    credential = json.loads(base64.b64decode(secret["data"]["credentials.json"]))[
        "credentials"
    ][0]
    config = Config(
        connect_timeout=3,
        read_timeout=10,
        retries={"max_attempts": 1},
        s3={"addressing_style": "path"},
    )
    return {
        service: boto3.client(
            service,
            endpoint_url=runtime.policy["endpoints"][service],
            region_name="us-east-1",
            aws_access_key_id=credential["access_key_id"],
            aws_secret_access_key=credential["secret_access_key"],
            config=config,
        )
        for service in ("dynamodb", "s3", "sqs")
    }


def seed(runtime):
    leader = runtime.quorum(runtime.pods())
    for key, value in VALUES.items():
        redis(runtime, leader, "SET", key, value)
    c = clients(runtime)
    c["dynamodb"].create_table(
        TableName=SEED,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    c["dynamodb"].put_item(
        TableName=SEED,
        Item={
            "id": {"S": "persistent"},
            "checksum": {"S": hashlib.sha256(PAYLOAD).hexdigest()},
        },
    )
    c["s3"].create_bucket(Bucket=SEED)
    c["s3"].put_object(Bucket=SEED, Key="persistent.bin", Body=PAYLOAD)
    queue = c["sqs"].create_queue(QueueName=SEED)["QueueUrl"]
    for message in sorted(MESSAGES):
        c["sqs"].send_message(QueueUrl=queue, MessageBody=message)
    agent.log(
        "synthetic-data-seeded",
        redis_keys=len(VALUES),
        s3_bytes=len(PAYLOAD),
        sqs_messages=len(MESSAGES),
    )


def verify_seed(runtime):
    c = clients(runtime)
    for name in MEMBERS:
        for key, expected in VALUES.items():
            if redis(runtime, name, "GET", key) != expected.encode():
                raise agent.ReleaseError("Persistent validation Redis data changed")
    item = c["dynamodb"].get_item(
        TableName=SEED, Key={"id": {"S": "persistent"}}, ConsistentRead=True
    )["Item"]
    checksum = hashlib.sha256(PAYLOAD).hexdigest()
    if (
        item["checksum"]["S"] != checksum
        or c["s3"].get_object(Bucket=SEED, Key="persistent.bin")["Body"].read()
        != PAYLOAD
    ):
        raise agent.ReleaseError("Persistent validation object data changed")
    queue = c["sqs"].get_queue_url(QueueName=SEED)["QueueUrl"]
    messages = (
        c["sqs"]
        .receive_message(
            QueueUrl=queue,
            MaxNumberOfMessages=10,
            VisibilityTimeout=0,
            WaitTimeSeconds=1,
        )
        .get("Messages", [])
    )
    if {m["Body"] for m in messages} != MESSAGES:
        raise agent.ReleaseError("Persistent validation SQS messages changed")
    return {
        "redis_keys_verified_on_each_of_three_members": len(VALUES),
        "dynamodb_consistent_read": True,
        "s3_bytes": len(PAYLOAD),
        "payload_sha256": checksum,
        "sqs_message_bodies_preserved": len(MESSAGES),
    }


def inventory(runtime, allow_missing=False):
    if allow_missing:
        selector = urllib.parse.urlencode(
            {"labelSelector": "app.kubernetes.io/name=elastickv"}
        )
        rows = runtime.kube.request(runtime.namespace + "/pods?" + selector)["items"]
        pods = {p["metadata"]["name"]: p for p in rows}
        if not set(pods) <= set(MEMBERS):
            raise agent.UnsafeState("Unexpected validation member")
    else:
        pods = runtime.pods()
    result = {}
    for name in MEMBERS:
        p = pods.get(name)
        if p is None:
            result[name] = {
                "uid": None,
                "ready": False,
                "image": None,
                "state": "missing",
            }
            continue
        container = p["spec"]["containers"][0]
        status = next(
            (
                c
                for c in p.get("status", {}).get("containerStatuses", [])
                if c["name"] == "elastickv"
            ),
            {},
        )
        state = status.get("state", {})
        state_name = "pending"
        if "running" in state:
            state_name = "running"
        elif "waiting" in state:
            state_name = "waiting:" + state["waiting"].get("reason", "")
        elif "terminated" in state:
            state_name = "terminated:" + str(state["terminated"].get("exitCode"))
        probe = container["readinessProbe"]
        result[name] = {
            "uid": p["metadata"]["uid"],
            "ready": agent.pod_ready(p),
            "image": container["image"],
            "revision": p["metadata"]["labels"]["controller-revision-hash"],
            "readiness_probe": probe.get("httpGet", {}).get(
                "path", "tcp:" + str(probe.get("tcpSocket", {}).get("port"))
            ),
            "started": status.get("started", False),
            "restarts": status.get("restartCount", 0),
            "state": state_name,
            "last_exit_code": status.get("lastState", {})
            .get("terminated", {})
            .get("exitCode"),
        }
    return result


class Observer:
    def __init__(self, runtime):
        self.runtime = runtime
        self.samples = []
        self.errors = 0
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.done.is_set():
            try:
                pods = inventory(self.runtime, allow_missing=True)
                self.samples.append(
                    {
                        "time": time.time(),
                        "ready": sum(p["ready"] for p in pods.values()),
                        "pods": pods,
                    }
                )
            except Exception:
                self.errors += 1
            self.done.wait(1)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.done.set()
        self.thread.join(timeout=10)
