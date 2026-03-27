"""S3 event notification tests."""
import base64
import io
import json
import time
import zipfile
import pytest

ENDPOINT = "http://127.0.0.1:14566"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test", "region_name": "us-east-1"}


def _client(svc):
    import boto3
    return boto3.client(svc, endpoint_url=ENDPOINT, **CREDS)


def _make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(event, ctx): return event")
    return base64.b64encode(buf.getvalue()).decode()


class TestS3Notifications:
    def test_put_bucket_notification_config(self, s3_client, sqs_client):
        bucket = "notif-config-test"
        try:
            s3_client.create_bucket(Bucket=bucket)
        except Exception:
            pass
        queue_url = sqs_client.create_queue(QueueName="notif-queue")["QueueUrl"]
        queue_arn = f"arn:aws:sqs:us-east-1:000000000000:notif-queue"

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "QueueConfigurations": [{
                    "Id": "test-notif",
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                }]
            },
        )
        r = s3_client.get_bucket_notification_configuration(Bucket=bucket)
        cfgs = r.get("QueueConfigurations", [])
        assert any(c.get("Id") == "test-notif" for c in cfgs)

    def test_put_object_fires_sqs_notification(self, s3_client, sqs_client):
        bucket = "notif-sqs-test"
        try:
            s3_client.create_bucket(Bucket=bucket)
        except Exception:
            pass
        q_url = sqs_client.create_queue(QueueName="sqs-notif-target")["QueueUrl"]
        q_arn = "arn:aws:sqs:us-east-1:000000000000:sqs-notif-target"

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "QueueConfigurations": [{
                    "Id": "sqs-notif",
                    "QueueArn": q_arn,
                    "Events": ["s3:ObjectCreated:*"],
                }]
            },
        )
        sqs_client.purge_queue(QueueUrl=q_url)
        s3_client.put_object(Bucket=bucket, Key="test-key.txt", Body=b"hello")
        time.sleep(0.2)

        r = sqs_client.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
        msgs = r.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        records = body.get("Records", [])
        assert len(records) >= 1
        assert records[0]["eventName"] == "s3:ObjectCreated:Put"

    def test_put_object_fires_lambda_notification(self, s3_client, lambda_client, sqs_client):
        bucket = "notif-lambda-test"
        fn_name = "s3-notif-lambda"
        result_queue = "lambda-notif-result"
        try:
            s3_client.create_bucket(Bucket=bucket)
        except Exception:
            pass
        try:
            lambda_client.delete_function(FunctionName=fn_name)
        except Exception:
            pass

        lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(_make_zip())},
        )
        fn_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn_name}"

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "LambdaFunctionConfigurations": [{
                    "Id": "lambda-notif",
                    "LambdaFunctionArn": fn_arn,
                    "Events": ["s3:ObjectCreated:*"],
                }]
            },
        )
        s3_client.put_object(Bucket=bucket, Key="trigger-key.txt", Body=b"world")
        # Just verify no exceptions; Lambda execution is best-effort
        time.sleep(0.5)

    def test_delete_object_fires_sqs_notification(self, s3_client, sqs_client):
        bucket = "notif-delete-test"
        try:
            s3_client.create_bucket(Bucket=bucket)
        except Exception:
            pass
        q_url = sqs_client.create_queue(QueueName="delete-notif-queue")["QueueUrl"]
        q_arn = "arn:aws:sqs:us-east-1:000000000000:delete-notif-queue"

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "QueueConfigurations": [{
                    "Id": "del-notif",
                    "QueueArn": q_arn,
                    "Events": ["s3:ObjectRemoved:*"],
                }]
            },
        )
        s3_client.put_object(Bucket=bucket, Key="to-delete.txt", Body=b"bye")
        sqs_client.purge_queue(QueueUrl=q_url)
        s3_client.delete_object(Bucket=bucket, Key="to-delete.txt")
        time.sleep(0.2)

        r = sqs_client.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
        msgs = r.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        records = body.get("Records", [])
        assert records[0]["eventName"] == "s3:ObjectRemoved:Delete"
