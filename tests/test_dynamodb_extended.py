"""DynamoDB extended tests — projections, query, remove expression, return values."""
import pytest


class TestDynamoDBExtended:
    def _table(self, client, name):
        try:
            client.create_table(
                TableName=name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
        except Exception:
            pass

    def test_get_item_projection(self, dynamodb_client):
        self._table(dynamodb_client, "proj-table")
        dynamodb_client.put_item(
            TableName="proj-table",
            Item={"pk": {"S": "1"}, "name": {"S": "alice"}, "age": {"N": "30"}},
        )
        resp = dynamodb_client.get_item(
            TableName="proj-table",
            Key={"pk": {"S": "1"}},
            ProjectionExpression="name",
        )
        assert "name" in resp["Item"]
        assert "age" not in resp["Item"]

    def test_put_item_return_values(self, dynamodb_client):
        self._table(dynamodb_client, "rv-table")
        dynamodb_client.put_item(TableName="rv-table", Item={"pk": {"S": "x"}, "v": {"S": "old"}})
        resp = dynamodb_client.put_item(
            TableName="rv-table",
            Item={"pk": {"S": "x"}, "v": {"S": "new"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["v"]["S"] == "old"

    def test_update_item_remove_expression(self, dynamodb_client):
        self._table(dynamodb_client, "rm-table")
        dynamodb_client.put_item(
            TableName="rm-table",
            Item={"pk": {"S": "r1"}, "keep": {"S": "yes"}, "drop": {"S": "bye"}},
        )
        dynamodb_client.update_item(
            TableName="rm-table",
            Key={"pk": {"S": "r1"}},
            UpdateExpression="REMOVE drop",
        )
        resp = dynamodb_client.get_item(TableName="rm-table", Key={"pk": {"S": "r1"}})
        assert "drop" not in resp["Item"]
        assert resp["Item"]["keep"]["S"] == "yes"

    def test_update_item_upsert(self, dynamodb_client):
        self._table(dynamodb_client, "upsert-table")
        dynamodb_client.update_item(
            TableName="upsert-table",
            Key={"pk": {"S": "new"}},
            UpdateExpression="SET val = :v",
            ExpressionAttributeValues={":v": {"S": "created"}},
        )
        resp = dynamodb_client.get_item(TableName="upsert-table", Key={"pk": {"S": "new"}})
        assert resp["Item"]["val"]["S"] == "created"

    def test_scan_returns_all_items(self, dynamodb_client):
        self._table(dynamodb_client, "scan2-table")
        for i in range(5):
            dynamodb_client.put_item(
                TableName="scan2-table", Item={"pk": {"S": str(i)}}
            )
        resp = dynamodb_client.scan(TableName="scan2-table")
        assert resp["Count"] >= 5

    def test_batch_write_delete(self, dynamodb_client):
        self._table(dynamodb_client, "bwdel-table")
        dynamodb_client.put_item(TableName="bwdel-table", Item={"pk": {"S": "del1"}})
        dynamodb_client.batch_write_item(RequestItems={
            "bwdel-table": [{"DeleteRequest": {"Key": {"pk": {"S": "del1"}}}}]
        })
        resp = dynamodb_client.get_item(TableName="bwdel-table", Key={"pk": {"S": "del1"}})
        assert "Item" not in resp

    def test_list_tables_limit(self, dynamodb_client):
        for name in ["lt1", "lt2", "lt3"]:
            try:
                dynamodb_client.create_table(
                    TableName=name,
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
                )
            except Exception:
                pass
        resp = dynamodb_client.list_tables(Limit=2)
        assert len(resp["TableNames"]) <= 2

    def test_get_nonexistent_item(self, dynamodb_client):
        self._table(dynamodb_client, "miss-item-table")
        resp = dynamodb_client.get_item(
            TableName="miss-item-table", Key={"pk": {"S": "ghost"}}
        )
        assert "Item" not in resp

    def test_delete_item_return_values(self, dynamodb_client):
        self._table(dynamodb_client, "del-rv-table")
        dynamodb_client.put_item(
            TableName="del-rv-table", Item={"pk": {"S": "d1"}, "data": {"S": "val"}}
        )
        resp = dynamodb_client.delete_item(
            TableName="del-rv-table",
            Key={"pk": {"S": "d1"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["data"]["S"] == "val"
