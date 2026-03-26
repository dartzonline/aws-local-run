"""EventBridge integration tests."""

class TestEventBridge:
    def test_put_and_list_rules(self, events_client):
        events_client.put_rule(Name="test-rule", ScheduleExpression="rate(5 minutes)")
        resp = events_client.list_rules()
        names = [r["Name"] for r in resp["Rules"]]
        assert "test-rule" in names

    def test_describe_rule(self, events_client):
        events_client.put_rule(Name="desc-rule", ScheduleExpression="rate(1 hour)")
        resp = events_client.describe_rule(Name="desc-rule")
        assert resp["Name"] == "desc-rule"

    def test_put_and_list_targets(self, events_client):
        events_client.put_rule(Name="target-rule", ScheduleExpression="rate(1 minute)")
        events_client.put_targets(Rule="target-rule", Targets=[
            {"Id": "t1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:my-fn"}
        ])
        resp = events_client.list_targets_by_rule(Rule="target-rule")
        assert len(resp["Targets"]) == 1
        assert resp["Targets"][0]["Id"] == "t1"

    def test_remove_targets(self, events_client):
        events_client.put_rule(Name="rm-rule", ScheduleExpression="rate(1 minute)")
        events_client.put_targets(Rule="rm-rule", Targets=[{"Id": "t1", "Arn": "arn:aws:sqs:x"}])
        events_client.remove_targets(Rule="rm-rule", Ids=["t1"])
        resp = events_client.list_targets_by_rule(Rule="rm-rule")
        assert len(resp["Targets"]) == 0

    def test_put_events(self, events_client):
        resp = events_client.put_events(Entries=[
            {"Source": "my.app", "DetailType": "test", "Detail": '{"key":"val"}'}
        ])
        assert resp["FailedEntryCount"] == 0
        assert len(resp["Entries"]) == 1

    def test_delete_rule(self, events_client):
        events_client.put_rule(Name="del-rule", ScheduleExpression="rate(1 minute)")
        events_client.delete_rule(Name="del-rule")
        resp = events_client.list_rules()
        names = [r["Name"] for r in resp["Rules"]]
        assert "del-rule" not in names
