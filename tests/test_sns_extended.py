"""SNS extended tests — attributes, subscriptions, publish details."""
import pytest


class TestSNSExtended:
    def test_get_topic_attributes(self, sns_client):
        arn = sns_client.create_topic(Name="attr-topic")["TopicArn"]
        resp = sns_client.get_topic_attributes(TopicArn=arn)
        assert resp["Attributes"]["TopicArn"] == arn

    def test_set_topic_attributes(self, sns_client):
        arn = sns_client.create_topic(Name="set-attr-topic")["TopicArn"]
        sns_client.set_topic_attributes(
            TopicArn=arn,
            AttributeName="DisplayName",
            AttributeValue="MyDisplay",
        )
        attrs = sns_client.get_topic_attributes(TopicArn=arn)["Attributes"]
        assert attrs.get("DisplayName") == "MyDisplay"

    def test_list_subscriptions(self, sns_client, sqs_client):
        topic_arn = sns_client.create_topic(Name="list-sub-topic")["TopicArn"]
        q = sqs_client.create_queue(QueueName="list-sub-q")["QueueUrl"]
        q_arn = sqs_client.get_queue_attributes(
            QueueUrl=q, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
        resp = sns_client.list_subscriptions()
        arns = [s["TopicArn"] for s in resp["Subscriptions"]]
        assert topic_arn in arns

    def test_multiple_subscribers(self, sns_client, sqs_client):
        topic_arn = sns_client.create_topic(Name="multi-sub-topic")["TopicArn"]
        for name in ("q-a", "q-b"):
            url = sqs_client.create_queue(QueueName=name)["QueueUrl"]
            q_arn = sqs_client.get_queue_attributes(
                QueueUrl=url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
        subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        assert len(subs) >= 2

    def test_publish_with_subject(self, sns_client):
        arn = sns_client.create_topic(Name="subj-topic")["TopicArn"]
        resp = sns_client.publish(TopicArn=arn, Message="hello", Subject="greeting")
        assert "MessageId" in resp

    def test_unsubscribe(self, sns_client, sqs_client):
        topic_arn = sns_client.create_topic(Name="unsub-topic")["TopicArn"]
        q = sqs_client.create_queue(QueueName="unsub-q")["QueueUrl"]
        q_arn = sqs_client.get_queue_attributes(
            QueueUrl=q, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sub_arn = sns_client.subscribe(
            TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn
        )["SubscriptionArn"]
        sns_client.unsubscribe(SubscriptionArn=sub_arn)
        subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        sub_arns = [s["SubscriptionArn"] for s in subs]
        assert sub_arn not in sub_arns

    def test_delete_and_recreate_topic(self, sns_client):
        arn1 = sns_client.create_topic(Name="recycle-topic")["TopicArn"]
        sns_client.delete_topic(TopicArn=arn1)
        arn2 = sns_client.create_topic(Name="recycle-topic")["TopicArn"]
        topics = sns_client.list_topics()["Topics"]
        topic_arns = [t["TopicArn"] for t in topics]
        assert arn2 in topic_arns
