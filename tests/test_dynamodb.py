"""DynamoDB integration tests."""
import pytest

class TestDynamoDB:
    def _ensure_table(self, client, name="ddb-table"):
        try:
            client.create_table(
                TableName=name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
        except Exception:
            pass

    def test_create_and_list_tables(self, dynamodb_client):
        self._ensure_table(dynamodb_client)
        resp = dynamodb_client.list_tables()
        assert "ddb-table" in resp["TableNames"]

    def test_describe_table(self, dynamodb_client):
        self._ensure_table(dynamodb_client)
        resp = dynamodb_client.describe_table(TableName="ddb-table")
        assert resp["Table"]["TableName"] == "ddb-table"
        assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_put_and_get_item(self, dynamodb_client):
        self._ensure_table(dynamodb_client, "items-table")
        dynamodb_client.put_item(TableName="items-table", Item={"id": {"S": "1"}, "name": {"S": "alice"}})
        resp = dynamodb_client.get_item(TableName="items-table", Key={"id": {"S": "1"}})
        assert resp["Item"]["name"]["S"] == "alice"

    def test_delete_item(self, dynamodb_client):
        self._ensure_table(dynamodb_client, "del-table")
        dynamodb_client.put_item(TableName="del-table", Item={"id": {"S": "x"}})
        dynamodb_client.delete_item(TableName="del-table", Key={"id": {"S": "x"}})
        resp = dynamodb_client.get_item(TableName="del-table", Key={"id": {"S": "x"}})
        assert "Item" not in resp

    def test_scan(self, dynamodb_client):
        self._ensure_table(dynamodb_client, "scan-table")
        dynamodb_client.put_item(TableName="scan-table", Item={"id": {"S": "a"}})
        dynamodb_client.put_item(TableName="scan-table", Item={"id": {"S": "b"}})
        resp = dynamodb_client.scan(TableName="scan-table")
        assert resp["Count"] >= 2

    def test_update_item(self, dynamodb_client):
        self._ensure_table(dynamodb_client, "upd-table")
        dynamodb_client.put_item(TableName="upd-table", Item={"id": {"S": "u1"}, "val": {"S": "old"}})
        dynamodb_client.update_item(
            TableName="upd-table", Key={"id": {"S": "u1"}},
            UpdateExpression="SET #v = :newval",
            ExpressionAttributeNames={"#v": "val"},
            ExpressionAttributeValues={":newval": {"S": "new"}},
        )
        resp = dynamodb_client.get_item(TableName="upd-table", Key={"id": {"S": "u1"}})
        assert resp["Item"]["val"]["S"] == "new"

    def test_batch_write_and_get(self, dynamodb_client):
        self._ensure_table(dynamodb_client, "batch-table")
        dynamodb_client.batch_write_item(RequestItems={
            "batch-table": [
                {"PutRequest": {"Item": {"id": {"S": "b1"}}}},
                {"PutRequest": {"Item": {"id": {"S": "b2"}}}},
            ]
        })
        resp = dynamodb_client.batch_get_item(RequestItems={
            "batch-table": {"Keys": [{"id": {"S": "b1"}}, {"id": {"S": "b2"}}]}
        })
        assert len(resp["Responses"]["batch-table"]) == 2

    def test_delete_table(self, dynamodb_client):
        self._ensure_table(dynamodb_client, "killme-table")
        dynamodb_client.delete_table(TableName="killme-table")
        resp = dynamodb_client.list_tables()
        assert "killme-table" not in resp["TableNames"]
