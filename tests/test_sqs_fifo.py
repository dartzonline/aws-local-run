"""SQS FIFO tests."""
import pytest
import time


FIFO_NAME = "test-fifo-queue.fifo"
ENDPOINT = "http://127.0.0.1:14566"


def _sqs():
    import boto3
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def _create_fifo(client, name=None, cbd=False):
    n = name or FIFO_NAME
    attrs = {"FifoQueue": "true"}
    if cbd:
        attrs["ContentBasedDeduplication"] = "true"
    r = client.create_queue(QueueName=n, Attributes=attrs)
    return r["QueueUrl"]


class TestSQSFifo:
    def test_create_fifo_queue(self, sqs_client):
        url = _create_fifo(sqs_client, "create-test.fifo")
        attrs = sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["FifoQueue"])
        assert attrs["Attributes"].get("FifoQueue") == "true"

    def test_messages_ordered_within_group(self, sqs_client):
        import uuid as _uuid
        run_id = _uuid.uuid4().hex[:8]
        url = _create_fifo(sqs_client, "order-test.fifo", cbd=True)
        sqs_client.purge_queue(QueueUrl=url)
        for i in range(3):
            sqs_client.send_message(
                QueueUrl=url,
                MessageBody=f"msg-{run_id}-{i}",
                MessageGroupId="grp1",
            )
        bodies = []
        for _ in range(3):
            r = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=1, VisibilityTimeout=0)
            msgs = r.get("Messages", [])
            if msgs:
                bodies.append(msgs[0]["Body"])
                sqs_client.delete_message(QueueUrl=url, ReceiptHandle=msgs[0]["ReceiptHandle"])
        assert bodies == [f"msg-{run_id}-0", f"msg-{run_id}-1", f"msg-{run_id}-2"]

    def test_deduplication_rejects_duplicate(self, sqs_client):
        import uuid as _uuid
        url = _create_fifo(sqs_client, "dedup-test.fifo")
        sqs_client.purge_queue(QueueUrl=url)
        unique_dedup = f"dedup-{_uuid.uuid4().hex[:8]}"
        r1 = sqs_client.send_message(
            QueueUrl=url,
            MessageBody="hello",
            MessageGroupId="g1",
            MessageDeduplicationId=unique_dedup,
        )
        r2 = sqs_client.send_message(
            QueueUrl=url,
            MessageBody="hello",
            MessageGroupId="g1",
            MessageDeduplicationId=unique_dedup,
        )
        # Both return the same MessageId (silent dedup)
        assert r1["MessageId"] == r2["MessageId"]
        # Only one message in queue
        msgs = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=10, VisibilityTimeout=0)
        assert len(msgs.get("Messages", [])) == 1

    def test_content_based_dedup(self, sqs_client):
        import uuid as _uuid
        unique_body = f"unique-body-{_uuid.uuid4().hex}"
        url = _create_fifo(sqs_client, "cbd-test.fifo", cbd=True)
        sqs_client.purge_queue(QueueUrl=url)
        r1 = sqs_client.send_message(QueueUrl=url, MessageBody=unique_body, MessageGroupId="g1")
        r2 = sqs_client.send_message(QueueUrl=url, MessageBody=unique_body, MessageGroupId="g1")
        assert r1["MessageId"] == r2["MessageId"]

    def test_one_inflight_per_group(self, sqs_client):
        import uuid as _uuid
        run_id = _uuid.uuid4().hex[:8]
        url = _create_fifo(sqs_client, "inflight-test.fifo", cbd=True)
        sqs_client.purge_queue(QueueUrl=url)
        sqs_client.send_message(QueueUrl=url, MessageBody=f"msg-1-{run_id}", MessageGroupId="grpA")
        sqs_client.send_message(QueueUrl=url, MessageBody=f"msg-2-{run_id}", MessageGroupId="grpA")
        # Receive first message — grpA is now in-flight
        r1 = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=5, VisibilityTimeout=30)
        msgs = r1.get("Messages", [])
        assert len(msgs) == 1, "Only one message from grpA while it's in-flight"

    def test_delete_clears_group_inflight(self, sqs_client):
        import uuid as _uuid
        run_id = _uuid.uuid4().hex[:8]
        url = _create_fifo(sqs_client, "delete-inflight.fifo", cbd=True)
        sqs_client.purge_queue(QueueUrl=url)
        sqs_client.send_message(QueueUrl=url, MessageBody=f"m1-{run_id}", MessageGroupId="grpB")
        sqs_client.send_message(QueueUrl=url, MessageBody=f"m2-{run_id}", MessageGroupId="grpB")
        r1 = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=1, VisibilityTimeout=30)
        msgs = r1.get("Messages", [])
        assert len(msgs) == 1
        rh = msgs[0]["ReceiptHandle"]
        sqs_client.delete_message(QueueUrl=url, ReceiptHandle=rh)
        # Now grpB should be free — second message receivable
        r2 = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=1, VisibilityTimeout=30)
        msgs2 = r2.get("Messages", [])
        assert len(msgs2) == 1
        assert msgs2[0]["Body"] == f"m2-{run_id}"
