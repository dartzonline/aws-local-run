"""Tests for DynamoDB TTL support."""
import time
import pytest


@pytest.fixture
def ttl_table(dynamodb_client):
    name = "ttl-test-table"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
    except Exception:
        pass


def test_update_and_describe_ttl(dynamodb_client, ttl_table):
    dynamodb_client.update_time_to_live(
        TableName=ttl_table,
        TimeToLiveSpecification={"AttributeName": "expiry", "Enabled": True},
    )
    r = dynamodb_client.describe_time_to_live(TableName=ttl_table)
    desc = r["TimeToLiveDescription"]
    assert desc["TimeToLiveStatus"] == "ENABLED"
    assert desc["AttributeName"] == "expiry"


def test_disable_ttl(dynamodb_client, ttl_table):
    dynamodb_client.update_time_to_live(
        TableName=ttl_table,
        TimeToLiveSpecification={"AttributeName": "expiry", "Enabled": True},
    )
    dynamodb_client.update_time_to_live(
        TableName=ttl_table,
        TimeToLiveSpecification={"AttributeName": "expiry", "Enabled": False},
    )
    r = dynamodb_client.describe_time_to_live(TableName=ttl_table)
    assert r["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"


def test_expired_items_excluded_from_scan(dynamodb_client, ttl_table):
    dynamodb_client.update_time_to_live(
        TableName=ttl_table,
        TimeToLiveSpecification={"AttributeName": "expiry", "Enabled": True},
    )
    # Put an expired item
    expired_ts = int(time.time()) - 100
    dynamodb_client.put_item(
        TableName=ttl_table,
        Item={"id": {"S": "expired"}, "expiry": {"N": str(expired_ts)}},
    )
    # Put a live item
    future_ts = int(time.time()) + 3600
    dynamodb_client.put_item(
        TableName=ttl_table,
        Item={"id": {"S": "live"}, "expiry": {"N": str(future_ts)}},
    )
    # Scan should only return live item
    r = dynamodb_client.scan(TableName=ttl_table)
    ids = [item["id"]["S"] for item in r["Items"]]
    assert "live" in ids
    assert "expired" not in ids
