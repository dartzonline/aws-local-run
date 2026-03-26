"""Advanced SQS feature tests: FIFO, DLQ, batch ops, tags, attributes."""
import json
import uuid
import pytest


def _qname(prefix="adv"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def test_receive_on_empty_queue_returns_empty(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("empty"))["QueueUrl"]
    resp = sqs_client.receive_message(QueueUrl=url, WaitTimeSeconds=1)
    # Should not raise; Messages key missing or empty list
    assert resp.get("Messages", []) == []


def test_fifo_queue_send_and_receive_in_order(sqs_client):
    name = _qname("fifo") + ".fifo"
    url = sqs_client.create_queue(
        QueueName=name,
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]

    bodies = ["first", "second", "third"]
    for body in bodies:
        sqs_client.send_message(
            QueueUrl=url,
            MessageBody=body,
            MessageGroupId="grp1",
        )

    received = []
    for _ in range(len(bodies)):
        msgs = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=1).get("Messages", [])
        if msgs:
            received.append(msgs[0]["Body"])
            sqs_client.delete_message(QueueUrl=url, ReceiptHandle=msgs[0]["ReceiptHandle"])

    assert received == bodies


def test_dlq_redrive_policy_set_and_read(sqs_client):
    dlq_name = _qname("dlq")
    dlq_url = sqs_client.create_queue(QueueName=dlq_name)["QueueUrl"]
    dlq_arn = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    src_name = _qname("src-dlq")
    src_url = sqs_client.create_queue(QueueName=src_name)["QueueUrl"]

    redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"})
    sqs_client.set_queue_attributes(
        QueueUrl=src_url, Attributes={"RedrivePolicy": redrive}
    )

    attrs = sqs_client.get_queue_attributes(
        QueueUrl=src_url, AttributeNames=["RedrivePolicy"]
    )["Attributes"]
    assert "RedrivePolicy" in attrs
    policy = json.loads(attrs["RedrivePolicy"])
    assert policy["deadLetterTargetArn"] == dlq_arn


def test_send_message_batch_three_messages_all_arrive(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("batch-send"))["QueueUrl"]
    entries = [
        {"Id": f"msg{i}", "MessageBody": f"body{i}"} for i in range(3)
    ]
    resp = sqs_client.send_message_batch(QueueUrl=url, Entries=entries)
    assert len(resp.get("Successful", [])) == 3
    assert len(resp.get("Failed", [])) == 0

    # Receive all three
    received = []
    for _ in range(3):
        msgs = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=3).get("Messages", [])
        received.extend(msgs)
        if len(received) >= 3:
            break

    assert len(received) == 3
    bodies = {m["Body"] for m in received}
    assert bodies == {"body0", "body1", "body2"}


def test_delete_message_batch(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("batch-del"))["QueueUrl"]
    for i in range(3):
        sqs_client.send_message(QueueUrl=url, MessageBody=f"del{i}")

    msgs = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=3).get("Messages", [])
    assert len(msgs) == 3

    entries = [{"Id": f"d{i}", "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)]
    resp = sqs_client.delete_message_batch(QueueUrl=url, Entries=entries)
    assert len(resp.get("Successful", [])) == 3
    assert len(resp.get("Failed", [])) == 0

    # Queue should be empty now
    remaining = sqs_client.receive_message(QueueUrl=url).get("Messages", [])
    assert remaining == []


def test_change_message_visibility_batch(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("chgvis-batch"))["QueueUrl"]
    for i in range(3):
        sqs_client.send_message(QueueUrl=url, MessageBody=f"vis{i}")

    msgs = sqs_client.receive_message(
        QueueUrl=url, MaxNumberOfMessages=3, VisibilityTimeout=60
    ).get("Messages", [])
    assert len(msgs) == 3

    entries = [
        {"Id": f"v{i}", "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
        for i, m in enumerate(msgs)
    ]
    resp = sqs_client.change_message_visibility_batch(QueueUrl=url, Entries=entries)
    assert len(resp.get("Successful", [])) == 3

    # Messages should be visible again
    visible = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=3).get("Messages", [])
    assert len(visible) == 3


def test_get_queue_attributes_returns_all(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("attrs"))["QueueUrl"]
    attrs = sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    # Core attributes must be present
    assert "QueueArn" in attrs
    assert "ApproximateNumberOfMessages" in attrs
    assert "VisibilityTimeout" in attrs


def test_tag_queue(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("tag"))["QueueUrl"]
    sqs_client.tag_queue(QueueUrl=url, Tags={"env": "staging", "team": "core"})
    resp = sqs_client.list_queue_tags(QueueUrl=url)
    assert resp["Tags"]["env"] == "staging"
    assert resp["Tags"]["team"] == "core"


def test_list_queue_tags(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("listtag"))["QueueUrl"]
    sqs_client.tag_queue(QueueUrl=url, Tags={"x": "1", "y": "2"})
    tags = sqs_client.list_queue_tags(QueueUrl=url)["Tags"]
    assert tags.get("x") == "1"
    assert tags.get("y") == "2"


def test_untag_queue(sqs_client):
    url = sqs_client.create_queue(QueueName=_qname("untag"))["QueueUrl"]
    sqs_client.tag_queue(QueueUrl=url, Tags={"keep": "yes", "remove": "no"})
    sqs_client.untag_queue(QueueUrl=url, TagKeys=["remove"])
    tags = sqs_client.list_queue_tags(QueueUrl=url)["Tags"]
    assert "remove" not in tags
    assert tags.get("keep") == "yes"
