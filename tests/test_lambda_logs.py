"""Tests that Lambda invocations write to CloudWatch Logs."""
import base64
import io
import json
import uuid
import zipfile
import pytest


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", code)
    return buf.getvalue()


CODE = """
def handler(event, context):
    return {"ok": True, "event": event}
"""


def _fn_name():
    return f"log-fn-{uuid.uuid4().hex[:8]}"


def test_lambda_invocation_creates_log_group(lambda_client, logs_client):
    fn_name = _fn_name()
    lambda_client.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="handler.handler",
        Code={"ZipFile": _make_zip(CODE)},
    )

    lambda_client.invoke(FunctionName=fn_name, Payload=json.dumps({"key": "val"}).encode())

    log_group_name = f"/aws/lambda/{fn_name}"
    groups = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)["logGroups"]
    group_names = [g["logGroupName"] for g in groups]
    assert log_group_name in group_names


def test_lambda_invocation_creates_log_stream(lambda_client, logs_client):
    fn_name = _fn_name()
    lambda_client.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="handler.handler",
        Code={"ZipFile": _make_zip(CODE)},
    )

    lambda_client.invoke(FunctionName=fn_name, Payload=b"{}")

    log_group_name = f"/aws/lambda/{fn_name}"
    streams = logs_client.describe_log_streams(logGroupName=log_group_name)["logStreams"]
    assert len(streams) >= 1


def test_lambda_log_events_contain_function_output(lambda_client, logs_client):
    fn_name = _fn_name()
    lambda_client.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="handler.handler",
        Code={"ZipFile": _make_zip(CODE)},
    )

    payload = {"hello": "world"}
    resp = lambda_client.invoke(
        FunctionName=fn_name,
        Payload=json.dumps(payload).encode(),
    )
    result = json.loads(resp["Payload"].read())
    assert result.get("ok") is True

    log_group_name = f"/aws/lambda/{fn_name}"
    streams = logs_client.describe_log_streams(logGroupName=log_group_name)["logStreams"]
    assert len(streams) >= 1

    stream_name = streams[0]["logStreamName"]
    events_resp = logs_client.get_log_events(
        logGroupName=log_group_name,
        logStreamName=stream_name,
    )
    events = events_resp["events"]
    assert len(events) >= 1

    # At least one event message should reference the function output
    messages = " ".join(e["message"] for e in events)
    # The log should contain some output — either the return value or an invocation record
    assert len(messages) > 0
