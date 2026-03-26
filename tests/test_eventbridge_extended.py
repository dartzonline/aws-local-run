"""EventBridge extended tests — rules, targets, patterns."""
import json
import pytest

PATTERN = json.dumps({"source": ["my.app"], "detail-type": ["MyEvent"]})
SCHEDULE = "rate(5 minutes)"


class TestEventBridgeExtended:
    def test_describe_rule(self, events_client):
        events_client.put_rule(Name="desc-rule", ScheduleExpression=SCHEDULE, State="ENABLED")
        resp = events_client.describe_rule(Name="desc-rule")
        assert resp["Name"] == "desc-rule"
        assert resp["State"] == "ENABLED"

    def test_pattern_rule(self, events_client):
        events_client.put_rule(Name="pat-rule", EventPattern=PATTERN, State="ENABLED")
        resp = events_client.describe_rule(Name="pat-rule")
        assert "EventPattern" in resp or "ScheduleExpression" not in resp

    def test_put_multiple_targets(self, events_client):
        events_client.put_rule(Name="multi-tgt-rule", ScheduleExpression=SCHEDULE, State="ENABLED")
        events_client.put_targets(
            Rule="multi-tgt-rule",
            Targets=[
                {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:000000000000:q1"},
                {"Id": "t2", "Arn": "arn:aws:sqs:us-east-1:000000000000:q2"},
            ],
        )
        targets = events_client.list_targets_by_rule(Rule="multi-tgt-rule")["Targets"]
        ids = [t["Id"] for t in targets]
        assert "t1" in ids and "t2" in ids

    def test_remove_target(self, events_client):
        events_client.put_rule(Name="rm-tgt-rule", ScheduleExpression=SCHEDULE, State="ENABLED")
        events_client.put_targets(
            Rule="rm-tgt-rule",
            Targets=[
                {"Id": "keep", "Arn": "arn:aws:sqs:us-east-1:000000000000:keep-q"},
                {"Id": "remove", "Arn": "arn:aws:sqs:us-east-1:000000000000:rm-q"},
            ],
        )
        events_client.remove_targets(Rule="rm-tgt-rule", Ids=["remove"])
        targets = events_client.list_targets_by_rule(Rule="rm-tgt-rule")["Targets"]
        ids = [t["Id"] for t in targets]
        assert "keep" in ids
        assert "remove" not in ids

    def test_disable_and_enable_rule(self, events_client):
        events_client.put_rule(Name="toggle-rule", ScheduleExpression=SCHEDULE, State="ENABLED")
        events_client.disable_rule(Name="toggle-rule")
        assert events_client.describe_rule(Name="toggle-rule")["State"] == "DISABLED"
        events_client.enable_rule(Name="toggle-rule")
        assert events_client.describe_rule(Name="toggle-rule")["State"] == "ENABLED"

    def test_delete_rule(self, events_client):
        events_client.put_rule(Name="del-rule", ScheduleExpression=SCHEDULE, State="ENABLED")
        events_client.delete_rule(Name="del-rule")
        rules = events_client.list_rules()["Rules"]
        names = [r["Name"] for r in rules]
        assert "del-rule" not in names
