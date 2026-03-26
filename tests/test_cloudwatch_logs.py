"""CloudWatch Logs integration tests."""

class TestCloudWatchLogs:
    def test_create_and_describe_log_groups(self, logs_client):
        logs_client.create_log_group(logGroupName="/test/logs")
        resp = logs_client.describe_log_groups()
        names = [g["logGroupName"] for g in resp["logGroups"]]
        assert "/test/logs" in names

    def test_create_log_stream(self, logs_client):
        logs_client.create_log_group(logGroupName="/stream/test")
        logs_client.create_log_stream(logGroupName="/stream/test", logStreamName="s1")
        resp = logs_client.describe_log_streams(logGroupName="/stream/test")
        names = [s["logStreamName"] for s in resp["logStreams"]]
        assert "s1" in names

    def test_put_and_get_log_events(self, logs_client):
        logs_client.create_log_group(logGroupName="/events/test")
        logs_client.create_log_stream(logGroupName="/events/test", logStreamName="stream1")
        logs_client.put_log_events(
            logGroupName="/events/test", logStreamName="stream1",
            logEvents=[{"timestamp": 1000000, "message": "hello log"}],
        )
        resp = logs_client.get_log_events(logGroupName="/events/test", logStreamName="stream1")
        assert any("hello log" in e["message"] for e in resp["events"])

    def test_delete_log_group(self, logs_client):
        logs_client.create_log_group(logGroupName="/del/logs")
        logs_client.delete_log_group(logGroupName="/del/logs")
        resp = logs_client.describe_log_groups()
        names = [g["logGroupName"] for g in resp["logGroups"]]
        assert "/del/logs" not in names
