import os
import requests
import boto3

def test_state_persistence(tmp_path):
    # We test this manually by using the StateManager directly on a fake engines dict,
    # and also testing the API if possible.
    
    from localrun.state import StateManager
    from localrun.services.s3 import S3Service
    
    data_dir = str(tmp_path)
    sm = StateManager(data_dir=data_dir)
    
    # Fake state
    s3 = S3Service()
    s3.buckets["test-persist-bucket"] = {}
    
    engines = {"s3": s3}
    
    # Save state
    assert sm.save_state(engines) == True
    assert os.path.exists(os.path.join(data_dir, "localrun_state.json"))
    
    # Modify state
    engines["s3"].buckets.clear()
    assert len(engines["s3"].buckets) == 0
    
    # Load state
    assert sm.load_state(engines) == True
    
    # Verify restored
    assert "test-persist-bucket" in engines["s3"].buckets

def test_state_api(localrun_server, tmp_path):
    # To test the API, we need the server to have a data_dir set.
    # We can inject it into the global config for this test.
    from localrun.config import get_config
    config = get_config()
    old_data_dir = config.data_dir
    config.data_dir = str(tmp_path)
    
    try:
        # Create a bucket
        s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:14566",
                          aws_access_key_id="test", aws_secret_access_key="test",
                          region_name="us-east-1")
        s3.create_bucket(Bucket="api-persist-bucket")
        
        # Save state via API
        resp = requests.post("http://127.0.0.1:14566/_localrun/state/save")
        assert resp.status_code == 200
        
        # Delete the bucket
        s3.delete_bucket(Bucket="api-persist-bucket")
        resp = s3.list_buckets()
        assert len([b for b in resp.get("Buckets", []) if b["Name"] == "api-persist-bucket"]) == 0
        
        # Load state via API
        resp = requests.post("http://127.0.0.1:14566/_localrun/state/load")
        assert resp.status_code == 200
        
        # Verify bucket is back
        resp = s3.list_buckets()
        assert len([b for b in resp.get("Buckets", []) if b["Name"] == "api-persist-bucket"]) == 1
        
    finally:
        config.data_dir = old_data_dir

