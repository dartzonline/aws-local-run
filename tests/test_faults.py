import requests
import time

def test_faults_api(localrun_server):
    # Ensure empty initially
    resp = requests.get("http://127.0.0.1:14566/_localrun/faults").json()
    assert len(resp["faults"]) == 0

    # Add a latency fault for S3 ListBuckets
    fault_req = {
        "service": "s3",
        "action": "ListBuckets",
        "type": "latency",
        "latency_ms": 200,
        "probability": 1.0
    }
    resp = requests.post("http://127.0.0.1:14566/_localrun/faults", json=fault_req)
    assert resp.status_code == 200
    fid = resp.json()["id"]

    # Test it (S3 list buckets is GET /)
    import boto3
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:14566",
                      aws_access_key_id="test", aws_secret_access_key="test",
                      region_name="us-east-1")
    
    start = time.time()
    s3.list_buckets()
    duration = time.time() - start
    assert duration >= 0.2  # Should have 200ms latency

    # Clear faults
    requests.delete("http://127.0.0.1:14566/_localrun/faults")
    
    # Test error fault on DynamoDB
    fault_req = {
        "service": "dynamodb",
        "action": "ListTables",
        "type": "error",
        "error_type": "ProvisionedThroughputExceededException",
        "error_message": "Rate exceeded",
        "error_code": 400,
        "probability": 1.0
    }
    requests.post("http://127.0.0.1:14566/_localrun/faults", json=fault_req)

    dynamodb = boto3.client("dynamodb", endpoint_url="http://127.0.0.1:14566",
                            aws_access_key_id="test", aws_secret_access_key="test",
                            region_name="us-east-1")
    
    import botocore.exceptions
    try:
        dynamodb.list_tables()
        assert False, "Should have raised exception"
    except botocore.exceptions.ClientError as e:
        assert e.response["Error"]["Code"] == "ProvisionedThroughputExceededException"
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    # Clear again
    requests.delete("http://127.0.0.1:14566/_localrun/faults")
