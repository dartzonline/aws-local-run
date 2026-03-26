"""SNS integration tests."""

class TestSNS:
    def test_create_and_list_topics(self, sns_client):
        sns_client.create_topic(Name="test-topic")
        resp = sns_client.list_topics()
        arns = [t["TopicArn"] for t in resp["Topics"]]
        assert any("test-topic" in a for a in arns)

    def test_subscribe_and_list(self, sns_client):
        topic = sns_client.create_topic(Name="sub-topic")
        arn = topic["TopicArn"]
        sns_client.subscribe(TopicArn=arn, Protocol="email", Endpoint="test@example.com")
        subs = sns_client.list_subscriptions_by_topic(TopicArn=arn)
        assert len(subs["Subscriptions"]) >= 1
        assert subs["Subscriptions"][0]["Protocol"] == "email"

    def test_publish(self, sns_client):
        topic = sns_client.create_topic(Name="pub-topic")
        resp = sns_client.publish(TopicArn=topic["TopicArn"], Message="hello sns")
        assert "MessageId" in resp

    def test_delete_topic(self, sns_client):
        topic = sns_client.create_topic(Name="del-topic")
        sns_client.delete_topic(TopicArn=topic["TopicArn"])
        resp = sns_client.list_topics()
        arns = [t["TopicArn"] for t in resp.get("Topics", [])]
        assert topic["TopicArn"] not in arns

    def test_unsubscribe(self, sns_client):
        topic = sns_client.create_topic(Name="unsub-topic")
        sub = sns_client.subscribe(TopicArn=topic["TopicArn"], Protocol="sqs", Endpoint="arn:aws:sqs:us-east-1:000000000000:q")
        sns_client.unsubscribe(SubscriptionArn=sub["SubscriptionArn"])
