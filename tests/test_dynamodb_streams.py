"""DynamoDB Streams tests."""
import pytest
import time

ENDPOINT = "http://127.0.0.1:14566"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test", "region_name": "us-east-1"}


def _ddb():
    import boto3
    return boto3.client("dynamodb", endpoint_url=ENDPOINT, **CREDS)


def _streams():
    import boto3
    return boto3.client("dynamodbstreams", endpoint_url=ENDPOINT, **CREDS)


def _ensure_table(client, name):
    try:
        client.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
    except Exception:
        pass


class TestDynamoDBStreams:
    def test_enable_stream_via_describe(self, dynamodb_client):
        tname = "streams-enable-test"
        _ensure_table(dynamodb_client, tname)
        # Put an item so a stream record is created
        dynamodb_client.put_item(
            TableName=tname,
            Item={"id": {"S": "s1"}, "val": {"S": "hello"}},
        )
        streams = _streams()
        r = streams.list_streams(TableName=tname)
        # After put, streams should be available
        assert "Streams" in r

    def test_put_item_creates_insert_record(self, dynamodb_client):
        tname = "streams-insert-test"
        _ensure_table(dynamodb_client, tname)
        dynamodb_client.put_item(
            TableName=tname,
            Item={"id": {"S": "ins1"}, "data": {"S": "value"}},
        )
        streams = _streams()
        r = streams.list_streams(TableName=tname)
        stream_list = r.get("Streams", [])
        assert len(stream_list) >= 1
        stream_arn = stream_list[0]["StreamArn"]
        desc = streams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]
        assert len(shards) >= 1
        shard_id = shards[0]["ShardId"]
        it_r = streams.get_shard_iterator(
            StreamArn=stream_arn,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        iterator = it_r["ShardIterator"]
        rec_r = streams.get_records(ShardIterator=iterator)
        records = rec_r.get("Records", [])
        event_names = [r["eventName"] for r in records]
        assert "INSERT" in event_names or "MODIFY" in event_names

    def test_delete_item_creates_remove_record(self, dynamodb_client):
        tname = "streams-delete-test"
        _ensure_table(dynamodb_client, tname)
        dynamodb_client.put_item(
            TableName=tname,
            Item={"id": {"S": "del1"}},
        )
        dynamodb_client.delete_item(
            TableName=tname,
            Key={"id": {"S": "del1"}},
        )
        streams = _streams()
        r = streams.list_streams(TableName=tname)
        stream_list = r.get("Streams", [])
        assert len(stream_list) >= 1
        stream_arn = stream_list[0]["StreamArn"]
        desc = streams.describe_stream(StreamArn=stream_arn)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
        it_r = streams.get_shard_iterator(
            StreamArn=stream_arn,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        rec_r = streams.get_records(ShardIterator=it_r["ShardIterator"])
        records = rec_r.get("Records", [])
        event_names = [r["eventName"] for r in records]
        assert "REMOVE" in event_names

    def test_get_shard_iterator_and_records(self, dynamodb_client):
        tname = "streams-iterator-test"
        _ensure_table(dynamodb_client, tname)
        # Put 3 items
        for i in range(3):
            dynamodb_client.put_item(
                TableName=tname,
                Item={"id": {"S": f"it-{i}"}},
            )
        streams = _streams()
        r = streams.list_streams(TableName=tname)
        stream_list = r.get("Streams", [])
        assert len(stream_list) >= 1
        stream_arn = stream_list[0]["StreamArn"]
        desc = streams.describe_stream(StreamArn=stream_arn)
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
        it_r = streams.get_shard_iterator(
            StreamArn=stream_arn,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        assert "ShardIterator" in it_r
        rec_r = streams.get_records(ShardIterator=it_r["ShardIterator"])
        assert "Records" in rec_r
        assert "NextShardIterator" in rec_r
        assert len(rec_r["Records"]) >= 3
