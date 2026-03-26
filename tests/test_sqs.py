"""SQS integration tests."""

class TestSQS:
    def test_create_and_list_queues(self, sqs_client):
        sqs_client.create_queue(QueueName="test-q-1")
        resp = sqs_client.list_queues()
        assert any("test-q-1" in u for u in resp.get("QueueUrls", []))

    def test_send_and_receive_message(self, sqs_client):
        resp = sqs_client.create_queue(QueueName="msg-queue")
        url = resp["QueueUrl"]
        sqs_client.send_message(QueueUrl=url, MessageBody="hello sqs")
        msgs = sqs_client.receive_message(QueueUrl=url)
        assert msgs["Messages"][0]["Body"] == "hello sqs"
        sqs_client.delete_message(QueueUrl=url, ReceiptHandle=msgs["Messages"][0]["ReceiptHandle"])

    def test_purge_queue(self, sqs_client):
        resp = sqs_client.create_queue(QueueName="purge-q")
        url = resp["QueueUrl"]
        sqs_client.send_message(QueueUrl=url, MessageBody="gone")
        sqs_client.purge_queue(QueueUrl=url)
        msgs = sqs_client.receive_message(QueueUrl=url)
        assert "Messages" not in msgs or len(msgs["Messages"]) == 0

    def test_get_queue_url(self, sqs_client):
        sqs_client.create_queue(QueueName="url-q")
        resp = sqs_client.get_queue_url(QueueName="url-q")
        assert "url-q" in resp["QueueUrl"]

    def test_get_queue_attributes(self, sqs_client):
        resp = sqs_client.create_queue(QueueName="attr-q")
        url = resp["QueueUrl"]
        attrs = sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])
        assert "VisibilityTimeout" in attrs["Attributes"]

    def test_delete_queue(self, sqs_client):
        resp = sqs_client.create_queue(QueueName="del-q")
        url = resp["QueueUrl"]
        sqs_client.delete_queue(QueueUrl=url)
