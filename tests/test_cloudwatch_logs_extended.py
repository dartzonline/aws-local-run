"""CloudWatch Logs extended tests — retention, filter, delete stream."""
import time
import pytest


class TestCloudWatchLogsExtended:
    def test_multiple_streams(self, logs_client):
        logs_client.create_log_group(logGroupName="/multi/streams")
        logs_client.create_log_stream(logGroupName="/multi/streams", logStreamName="s1")
        logs_client.create_log_stream(logGroupName="/multi/streams", logStreamName="s2")
        resp = logs_client.describe_log_streams(logGroupName="/multi/streams")
        names = [s["logStreamName"] for s in resp["logStreams"]]
        assert "s1" in names
        assert "s2" in names

    def test_put_multiple_events(self, logs_client):
        logs_client.create_log_group(logGroupName="/multi/events")
        logs_client.create_log_stream(logGroupName="/multi/events", logStreamName="stream")
        logs_client.put_log_events(
            logGroupName="/multi/events",
            logStreamName="stream",
            logEvents=[
                {"timestamp": 1000, "message": "first"},
                {"timestamp": 2000, "message": "second"},
                {"timestamp": 3000, "message": "third"},
            ],
        )
        resp = logs_client.get_log_events(
            logGroupName="/multi/events", logStreamName="stream"
        )
        messages = [e["message"] for e in resp["events"]]
        assert "first" in messages
        assert "third" in messages

    def test_put_retention_policy(self, logs_client):
        logs_client.create_log_group(logGroupName="/retention/test")
        logs_client.put_retention_policy(logGroupName="/retention/test", retentionInDays=7)
        resp = logs_client.describe_log_groups(logGroupNamePrefix="/retention/test")
        group = next(g for g in resp["logGroups"] if g["logGroupName"] == "/retention/test")
        assert group.get("retentionInDays") == 7

    def test_delete_log_stream(self, logs_client):
        logs_client.create_log_group(logGroupName="/del/stream")
        logs_client.create_log_stream(logGroupName="/del/stream", logStreamName="gone")
        logs_client.delete_log_stream(logGroupName="/del/stream", logStreamName="gone")
        resp = logs_client.describe_log_streams(logGroupName="/del/stream")
        names = [s["logStreamName"] for s in resp["logStreams"]]
        assert "gone" not in names

    def test_describe_log_groups_prefix(self, logs_client):
        logs_client.create_log_group(logGroupName="/app/service1")
        logs_client.create_log_group(logGroupName="/app/service2")
        logs_client.create_log_group(logGroupName="/other/service")
        resp = logs_client.describe_log_groups(logGroupNamePrefix="/app/")
        names = [g["logGroupName"] for g in resp["logGroups"]]
        assert "/app/service1" in names
        assert "/other/service" not in names

    def test_get_log_events_empty_stream(self, logs_client):
        logs_client.create_log_group(logGroupName="/empty/stream")
        logs_client.create_log_stream(logGroupName="/empty/stream", logStreamName="empty")
        resp = logs_client.get_log_events(
            logGroupName="/empty/stream", logStreamName="empty"
        )
        assert resp["events"] == []
