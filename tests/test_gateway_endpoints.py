"""Tests for _localrun meta-endpoints and the /health route."""
import uuid
import requests
import pytest

ENDPOINT = "http://127.0.0.1:14566"


def test_health_returns_running():
    resp = requests.get(f"{ENDPOINT}/health", timeout=5)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("status") == "running"


def test_resources_returns_resources_and_count():
    resp = requests.get(f"{ENDPOINT}/_localrun/resources", timeout=5)
    assert resp.status_code == 200
    data = resp.json()
    assert "resources" in data
    assert "count" in data
    assert isinstance(data["resources"], list)
    assert data["count"] == len(data["resources"])


def test_requests_returns_request_list():
    # Make at least one AWS-like call first so the log has entries
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    s3.list_buckets()

    resp = requests.get(f"{ENDPOINT}/_localrun/requests", timeout=5)
    assert resp.status_code == 200
    data = resp.json()
    assert "requests" in data
    assert isinstance(data["requests"], list)


def test_reset_returns_true():
    resp = requests.post(f"{ENDPOINT}/_localrun/reset", timeout=5)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("reset") is True


def test_regions_returns_list():
    resp = requests.get(f"{ENDPOINT}/_localrun/regions", timeout=5)
    assert resp.status_code == 200
    data = resp.json()
    assert "regions" in data
    assert isinstance(data["regions"], list)
    assert len(data["regions"]) >= 1


def test_add_fault_fires_and_delete_removes_it():
    import boto3
    import botocore.exceptions

    # Clear any stale faults first
    requests.delete(f"{ENDPOINT}/_localrun/faults", timeout=5)

    fault_payload = {
        "service": "sqs",
        "action": "ListQueues",
        "type": "error",
        "error_type": "ServiceUnavailable",
        "error_message": "Injected test fault",
        "error_code": 503,
        "probability": 1.0,
    }
    add_resp = requests.post(f"{ENDPOINT}/_localrun/faults", json=fault_payload, timeout=5)
    assert add_resp.status_code == 200
    fault_id = add_resp.json()["id"]

    # Verify the fault fires
    sqs = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    with pytest.raises(Exception):
        sqs.list_queues()

    # Remove the fault
    del_resp = requests.delete(f"{ENDPOINT}/_localrun/faults?id={fault_id}", timeout=5)
    assert del_resp.status_code == 200

    # Should work normally now
    sqs.list_queues()


def test_get_faults_shows_added_fault():
    # Clear first
    requests.delete(f"{ENDPOINT}/_localrun/faults", timeout=5)

    fault_payload = {
        "service": "s3",
        "type": "latency",
        "latency_ms": 1,
        "probability": 0.0,  # won't actually fire
    }
    requests.post(f"{ENDPOINT}/_localrun/faults", json=fault_payload, timeout=5)

    resp = requests.get(f"{ENDPOINT}/_localrun/faults", timeout=5)
    assert resp.status_code == 200
    faults = resp.json()["faults"]
    assert len(faults) >= 1
    assert any(f.get("service") == "s3" for f in faults)

    # Clean up
    requests.delete(f"{ENDPOINT}/_localrun/faults", timeout=5)


def test_state_save_and_snapshots():
    snap_name = f"snap-{uuid.uuid4().hex[:8]}"

    # Save a named snapshot
    save_resp = requests.post(
        f"{ENDPOINT}/_localrun/state/save/{snap_name}", timeout=10
    )
    # The endpoint may return 200 or 500 if data_dir is not set; either way check the JSON shape
    data = save_resp.json()
    if save_resp.status_code == 200:
        assert "message" in data or "path" in data

        # List snapshots — our new snapshot should appear
        snap_resp = requests.get(f"{ENDPOINT}/_localrun/state/snapshots", timeout=5)
        assert snap_resp.status_code == 200
        snapshots = snap_resp.json().get("snapshots", [])
        assert snap_name in snapshots
    else:
        # data_dir not configured — that's acceptable for a default test env
        assert "error" in data
