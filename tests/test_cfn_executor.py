"""CloudFormation execution tests — verify that resources actually get created."""
import json
import pytest
import time

ENDPOINT = "http://127.0.0.1:14566"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test", "region_name": "us-east-1"}


def _cfn():
    import boto3
    return boto3.client("cloudformation", endpoint_url=ENDPOINT, **CREDS)


def _s3():
    import boto3
    return boto3.client("s3", endpoint_url=ENDPOINT, **CREDS)


def _sqs():
    import boto3
    return boto3.client("sqs", endpoint_url=ENDPOINT, **CREDS)


def _ddb():
    import boto3
    return boto3.client("dynamodb", endpoint_url=ENDPOINT, **CREDS)


def _sns():
    import boto3
    return boto3.client("sns", endpoint_url=ENDPOINT, **CREDS)


def _iam():
    import boto3
    return boto3.client("iam", endpoint_url=ENDPOINT, **CREDS)


def _make_stack_name(suffix):
    return f"cfn-exec-{suffix}-{int(time.time()) % 100000}"


class TestCFNExecutor:
    def test_stack_creates_s3_bucket(self):
        cfn = _cfn()
        s3 = _s3()
        stack_name = _make_stack_name("s3")
        bucket_name = f"cfn-bucket-{int(time.time()) % 100000}"
        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "MyBucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": bucket_name},
                }
            },
        })
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        # Verify bucket exists
        buckets = s3.list_buckets()["Buckets"]
        names = [b["Name"] for b in buckets]
        assert bucket_name in names

    def test_stack_creates_sqs_queue(self):
        cfn = _cfn()
        sqs = _sqs()
        stack_name = _make_stack_name("sqs")
        queue_name = f"cfn-queue-{int(time.time()) % 100000}"
        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": queue_name},
                }
            },
        })
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        r = sqs.list_queues(QueueNamePrefix=queue_name)
        assert any(queue_name in url for url in r.get("QueueUrls", []))

    def test_stack_creates_dynamodb_table(self):
        cfn = _cfn()
        ddb = _ddb()
        stack_name = _make_stack_name("ddb")
        table_name = f"cfn-table-{int(time.time()) % 100000}"
        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "MyTable": {
                    "Type": "AWS::DynamoDB::Table",
                    "Properties": {
                        "TableName": table_name,
                        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                        "BillingMode": "PAY_PER_REQUEST",
                    },
                }
            },
        })
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        tables = ddb.list_tables()["TableNames"]
        assert table_name in tables

    def test_stack_creates_sns_topic(self):
        cfn = _cfn()
        sns = _sns()
        stack_name = _make_stack_name("sns")
        topic_name = f"cfn-topic-{int(time.time()) % 100000}"
        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "MyTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {"TopicName": topic_name},
                }
            },
        })
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        r = sns.list_topics()
        topic_arns = [t["TopicArn"] for t in r.get("Topics", [])]
        assert any(topic_name in arn for arn in topic_arns)

    def test_stack_creates_iam_role(self):
        cfn = _cfn()
        iam = _iam()
        stack_name = _make_stack_name("iam")
        role_name = f"cfn-role-{int(time.time()) % 100000}"
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        })
        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "MyRole": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "RoleName": role_name,
                        "AssumeRolePolicyDocument": json.loads(trust),
                    },
                }
            },
        })
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        r = iam.list_roles()
        role_names = [r2["RoleName"] for r2 in r.get("Roles", [])]
        assert role_name in role_names

    def test_delete_stack_removes_resources(self):
        cfn = _cfn()
        s3 = _s3()
        stack_name = _make_stack_name("del")
        bucket_name = f"cfn-del-bucket-{int(time.time()) % 100000}"
        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "DelBucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": bucket_name},
                }
            },
        })
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        buckets_before = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        assert bucket_name in buckets_before

        cfn.delete_stack(StackName=stack_name)
        r = cfn.list_stacks()
        stack_names = [s["StackName"] for s in r.get("StackSummaries", [])]
        assert stack_name not in stack_names
