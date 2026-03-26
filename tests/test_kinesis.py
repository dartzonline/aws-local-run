"""Tests for the Kinesis service emulator."""
import base64
import pytest


def test_create_and_list_stream(kinesis_client):
    kinesis_client.create_stream(StreamName="test-stream", ShardCount=1)
    resp = kinesis_client.list_streams()
    assert "test-stream" in resp["StreamNames"]


def test_create_duplicate_stream_fails(kinesis_client):
    kinesis_client.create_stream(StreamName="dup-stream", ShardCount=1)
    with pytest.raises(Exception):
        kinesis_client.create_stream(StreamName="dup-stream", ShardCount=1)


def test_describe_stream(kinesis_client):
    kinesis_client.create_stream(StreamName="desc-stream", ShardCount=2)
    resp = kinesis_client.describe_stream(StreamName="desc-stream")
    desc = resp["StreamDescription"]
    assert desc["StreamName"] == "desc-stream"
    assert desc["StreamStatus"] == "ACTIVE"
    assert len(desc["Shards"]) == 2


def test_delete_stream(kinesis_client):
    kinesis_client.create_stream(StreamName="del-stream", ShardCount=1)
    kinesis_client.delete_stream(StreamName="del-stream")
    resp = kinesis_client.list_streams()
    assert "del-stream" not in resp["StreamNames"]


def test_put_and_get_record(kinesis_client):
    kinesis_client.create_stream(StreamName="put-stream", ShardCount=1)
    data = base64.b64encode(b"hello world").decode()
    put_resp = kinesis_client.put_record(
        StreamName="put-stream",
        Data=base64.b64decode(data),
        PartitionKey="key1",
    )
    assert "SequenceNumber" in put_resp
    assert "ShardId" in put_resp

    shard_id = put_resp["ShardId"]
    iter_resp = kinesis_client.get_shard_iterator(
        StreamName="put-stream",
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    it = iter_resp["ShardIterator"]
    records_resp = kinesis_client.get_records(ShardIterator=it)
    assert len(records_resp["Records"]) == 1
    assert records_resp["Records"][0]["PartitionKey"] == "key1"


def test_put_records_batch(kinesis_client):
    kinesis_client.create_stream(StreamName="batch-stream", ShardCount=2)
    records = [
        {"Data": b"record1", "PartitionKey": "pk1"},
        {"Data": b"record2", "PartitionKey": "pk2"},
        {"Data": b"record3", "PartitionKey": "pk3"},
    ]
    resp = kinesis_client.put_records(StreamName="batch-stream", Records=records)
    assert resp["FailedRecordCount"] == 0
    assert len(resp["Records"]) == 3


def test_shard_iterator_latest(kinesis_client):
    kinesis_client.create_stream(StreamName="latest-stream", ShardCount=1)
    # Put a record first
    kinesis_client.put_record(StreamName="latest-stream", Data=b"before", PartitionKey="k")
    # LATEST iterator should return nothing since we set it before putting
    shards = kinesis_client.describe_stream(StreamName="latest-stream")["StreamDescription"]["Shards"]
    shard_id = shards[0]["ShardId"]
    it_resp = kinesis_client.get_shard_iterator(
        StreamName="latest-stream",
        ShardId=shard_id,
        ShardIteratorType="LATEST",
    )
    resp = kinesis_client.get_records(ShardIterator=it_resp["ShardIterator"])
    # LATEST points past the last record, so nothing comes back
    assert len(resp["Records"]) == 0


def test_list_shards(kinesis_client):
    kinesis_client.create_stream(StreamName="shards-stream", ShardCount=3)
    resp = kinesis_client.list_shards(StreamName="shards-stream")
    assert len(resp["Shards"]) == 3
