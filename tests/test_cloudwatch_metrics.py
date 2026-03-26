"""Tests for the CloudWatch Metrics service emulator."""
import pytest
from datetime import datetime, timezone


def test_put_metric_data(cloudwatch_client):
    cloudwatch_client.put_metric_data(
        Namespace="TestApp",
        MetricData=[{"MetricName": "Requests", "Value": 10.0, "Unit": "Count"}],
    )
    # If no exception, it passed


def test_list_metrics(cloudwatch_client):
    cloudwatch_client.put_metric_data(
        Namespace="ListApp",
        MetricData=[{"MetricName": "Errors", "Value": 5.0, "Unit": "Count"}],
    )
    resp = cloudwatch_client.list_metrics(Namespace="ListApp")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "Errors" in names


def test_list_metrics_with_name_filter(cloudwatch_client):
    cloudwatch_client.put_metric_data(
        Namespace="FilterApp",
        MetricData=[
            {"MetricName": "Hits", "Value": 1.0, "Unit": "Count"},
            {"MetricName": "Misses", "Value": 2.0, "Unit": "Count"},
        ],
    )
    resp = cloudwatch_client.list_metrics(Namespace="FilterApp", MetricName="Hits")
    assert len(resp["Metrics"]) == 1
    assert resp["Metrics"][0]["MetricName"] == "Hits"


def test_get_metric_statistics(cloudwatch_client):
    cloudwatch_client.put_metric_data(
        Namespace="StatsApp",
        MetricData=[{"MetricName": "Latency", "Value": 100.0, "Unit": "Milliseconds"}],
    )
    resp = cloudwatch_client.get_metric_statistics(
        Namespace="StatsApp",
        MetricName="Latency",
        StartTime=datetime(2000, 1, 1, tzinfo=timezone.utc),
        EndTime=datetime(2099, 1, 1, tzinfo=timezone.utc),
        Period=60,
        Statistics=["Average", "Sum", "Maximum"],
    )
    # Should have at least one datapoint
    assert len(resp["Datapoints"]) >= 1


def test_put_and_describe_alarm(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="test-alarm",
        MetricName="Requests",
        Namespace="TestApp",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=100.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    resp = cloudwatch_client.describe_alarms(AlarmNames=["test-alarm"])
    alarms = resp["MetricAlarms"]
    assert len(alarms) >= 1
    found = next((a for a in alarms if a["AlarmName"] == "test-alarm"), None)
    assert found is not None
    assert found["Threshold"] == 100.0


def test_set_alarm_state(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="state-alarm",
        MetricName="X",
        Namespace="NS",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=10.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cloudwatch_client.set_alarm_state(
        AlarmName="state-alarm",
        StateValue="ALARM",
        StateReason="test",
    )
    resp = cloudwatch_client.describe_alarms(AlarmNames=["state-alarm"])
    alarm = resp["MetricAlarms"][0]
    assert alarm["StateValue"] == "ALARM"


def test_delete_alarms(cloudwatch_client):
    cloudwatch_client.put_metric_alarm(
        AlarmName="del-alarm",
        MetricName="X",
        Namespace="NS",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=5.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cloudwatch_client.delete_alarms(AlarmNames=["del-alarm"])
    resp = cloudwatch_client.describe_alarms(AlarmNames=["del-alarm"])
    assert len(resp["MetricAlarms"]) == 0
