"""SQS extended tests — multi-message, visibility, tags, attributes."""
import time
import pytest


class TestSQSExtended:
    def test_send_multiple_messages(self, sqs_client):
        url = sqs_client.create_queue(QueueName="multi-msg-q")["QueueUrl"]
        for body in ["msg1", "msg2", "msg3"]:
            sqs_client.send_message(QueueUrl=url, MessageBody=body)
        resp = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=3)
        assert len(resp["Messages"]) >= 1

    def test_message_md5(self, sqs_client):
        import hashlib
        url = sqs_client.create_queue(QueueName="md5-q")["QueueUrl"]
        sqs_client.send_message(QueueUrl=url, MessageBody="checksum")
        msg = sqs_client.receive_message(QueueUrl=url)["Messages"][0]
        expected = hashlib.md5(b"checksum").hexdigest()
        assert msg["MD5OfBody"] == expected

    def test_visibility_timeout(self, sqs_client):
        url = sqs_client.create_queue(QueueName="vis-q")["QueueUrl"]
        sqs_client.send_message(QueueUrl=url, MessageBody="hidden")
        # receive with 30s visibility — message is now invisible
        msgs = sqs_client.receive_message(QueueUrl=url, VisibilityTimeout=30)
        assert len(msgs["Messages"]) == 1
        # second receive should get nothing
        msgs2 = sqs_client.receive_message(QueueUrl=url)
        assert "Messages" not in msgs2 or len(msgs2["Messages"]) == 0

    def test_change_message_visibility(self, sqs_client):
        url = sqs_client.create_queue(QueueName="chgvis-q")["QueueUrl"]
        sqs_client.send_message(QueueUrl=url, MessageBody="x")
        msg = sqs_client.receive_message(QueueUrl=url, VisibilityTimeout=30)["Messages"][0]
        # Make it visible immediately
        sqs_client.change_message_visibility(
            QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=0
        )
        msgs2 = sqs_client.receive_message(QueueUrl=url)
        assert len(msgs2["Messages"]) == 1

    def test_set_queue_attributes(self, sqs_client):
        url = sqs_client.create_queue(QueueName="setattr-q")["QueueUrl"]
        sqs_client.set_queue_attributes(
            QueueUrl=url, Attributes={"VisibilityTimeout": "60"}
        )
        attrs = sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])
        assert attrs["Attributes"]["VisibilityTimeout"] == "60"

    def test_queue_tags(self, sqs_client):
        url = sqs_client.create_queue(QueueName="tag-q")["QueueUrl"]
        sqs_client.tag_queue(QueueUrl=url, Tags={"env": "prod", "team": "infra"})
        resp = sqs_client.list_queue_tags(QueueUrl=url)
        assert resp["Tags"]["env"] == "prod"

    def test_untag_queue(self, sqs_client):
        url = sqs_client.create_queue(QueueName="untag-q")["QueueUrl"]
        sqs_client.tag_queue(QueueUrl=url, Tags={"k1": "v1", "k2": "v2"})
        sqs_client.untag_queue(QueueUrl=url, TagKeys=["k1"])
        resp = sqs_client.list_queue_tags(QueueUrl=url)
        assert "k1" not in resp["Tags"]
        assert resp["Tags"]["k2"] == "v2"

    def test_list_queues_prefix(self, sqs_client):
        sqs_client.create_queue(QueueName="pfx-alpha")
        sqs_client.create_queue(QueueName="pfx-beta")
        sqs_client.create_queue(QueueName="other-q")
        resp = sqs_client.list_queues(QueueNamePrefix="pfx-")
        urls = resp.get("QueueUrls", [])
        assert any("pfx-alpha" in u for u in urls)
        assert any("pfx-beta" in u for u in urls)
        assert not any("other-q" in u for u in urls)

    def test_approximate_message_count(self, sqs_client):
        url = sqs_client.create_queue(QueueName="count-q")["QueueUrl"]
        sqs_client.send_message(QueueUrl=url, MessageBody="a")
        sqs_client.send_message(QueueUrl=url, MessageBody="b")
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        assert int(attrs["Attributes"]["ApproximateNumberOfMessages"]) == 2
