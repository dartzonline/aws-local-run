"""CloudFormation stack operation tests."""
import json
import uuid
import pytest


def _stack_name():
    return f"cf-test-{uuid.uuid4().hex[:8]}"


def _simple_template(bucket_name, queue_name, table_name):
    return json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": bucket_name},
            },
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": queue_name},
            },
            "MyTable": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": table_name,
                    "AttributeDefinitions": [
                        {"AttributeName": "id", "AttributeType": "S"}
                    ],
                    "KeySchema": [
                        {"AttributeName": "id", "KeyType": "HASH"}
                    ],
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                },
            },
        },
    })


def test_create_stack_returns_stack_id(s3_client, sqs_client, dynamodb_client):
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    stack_name = _stack_name()
    template = _simple_template(
        f"cf-bucket-{suffix}", f"cf-queue-{suffix}", f"cf-table-{suffix}"
    )
    resp = cf.create_stack(StackName=stack_name, TemplateBody=template)
    assert "StackId" in resp
    assert stack_name in resp["StackId"]


def test_describe_stack_returns_create_complete(s3_client, sqs_client, dynamodb_client):
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    stack_name = _stack_name()
    template = _simple_template(
        f"cf-bucket-{suffix}", f"cf-queue-{suffix}", f"cf-table-{suffix}"
    )
    cf.create_stack(StackName=stack_name, TemplateBody=template)
    stacks = cf.describe_stacks(StackName=stack_name)["Stacks"]
    assert len(stacks) == 1
    assert stacks[0]["StackStatus"] == "CREATE_COMPLETE"


def test_list_stacks_includes_new_stack():
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    stack_name = _stack_name()
    template = _simple_template(
        f"cf-bucket-{suffix}", f"cf-queue-{suffix}", f"cf-table-{suffix}"
    )
    cf.create_stack(StackName=stack_name, TemplateBody=template)
    summaries = cf.list_stacks()["StackSummaries"]
    names = [s["StackName"] for s in summaries]
    assert stack_name in names


def test_s3_bucket_created_by_stack(s3_client):
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    bucket_name = f"cf-bucket-{suffix}"
    stack_name = _stack_name()
    template = _simple_template(bucket_name, f"cf-queue-{suffix}", f"cf-table-{suffix}")
    cf.create_stack(StackName=stack_name, TemplateBody=template)

    # Should not raise — bucket exists
    s3_client.head_bucket(Bucket=bucket_name)


def test_sqs_queue_created_by_stack(sqs_client):
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    queue_name = f"cf-queue-{suffix}"
    stack_name = _stack_name()
    template = _simple_template(f"cf-bucket-{suffix}", queue_name, f"cf-table-{suffix}")
    cf.create_stack(StackName=stack_name, TemplateBody=template)

    queues = sqs_client.list_queues(QueueNamePrefix=queue_name).get("QueueUrls", [])
    assert any(queue_name in q for q in queues)


def test_dynamodb_table_created_by_stack(dynamodb_client):
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    table_name = f"cf-table-{suffix}"
    stack_name = _stack_name()
    template = _simple_template(f"cf-bucket-{suffix}", f"cf-queue-{suffix}", table_name)
    cf.create_stack(StackName=stack_name, TemplateBody=template)

    tables = dynamodb_client.list_tables()["TableNames"]
    assert table_name in tables


def test_get_template_body_returns_original():
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    stack_name = _stack_name()
    template = _simple_template(
        f"cf-bucket-{suffix}", f"cf-queue-{suffix}", f"cf-table-{suffix}"
    )
    cf.create_stack(StackName=stack_name, TemplateBody=template)
    resp = cf.get_template(StackName=stack_name)
    body = resp["TemplateBody"]
    # Body should contain the original template (may be the original string or re-serialized)
    if isinstance(body, dict):
        assert "Resources" in body
    else:
        assert "Resources" in body


def test_delete_stack():
    import boto3
    cf = boto3.client(
        "cloudformation",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    suffix = uuid.uuid4().hex[:8]
    stack_name = _stack_name()
    template = _simple_template(
        f"cf-bucket-{suffix}", f"cf-queue-{suffix}", f"cf-table-{suffix}"
    )
    cf.create_stack(StackName=stack_name, TemplateBody=template)

    cf.delete_stack(StackName=stack_name)

    summaries = cf.list_stacks()["StackSummaries"]
    names = [s["StackName"] for s in summaries]
    assert stack_name not in names
