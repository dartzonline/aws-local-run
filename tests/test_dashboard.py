import requests

def test_dashboard_ui(localrun_server):
    resp = requests.get("http://127.0.0.1:14566/_localrun/ui")
    assert resp.status_code == 200
    assert "<title>LocalRun Dashboard</title>" in resp.text

def test_dashboard_api(localrun_server):
    import boto3
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:14566",
                      aws_access_key_id="test", aws_secret_access_key="test",
                      region_name="us-east-1")
    s3.create_bucket(Bucket="test-dashboard-bucket")
    
    resp = requests.get("http://127.0.0.1:14566/_localrun/api/state")
    assert resp.status_code == 200
    data = resp.json()
    
    assert "s3" in data
    # Check that test-dashboard-bucket is in the state
    found = any(b["name"] == "test-dashboard-bucket" for b in data["s3"])
    assert found
