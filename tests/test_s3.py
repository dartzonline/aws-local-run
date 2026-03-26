"""S3 integration tests."""
import pytest

class TestS3:
    def test_create_and_list_buckets(self, s3_client):
        s3_client.create_bucket(Bucket="test-s3-1")
        buckets = s3_client.list_buckets()["Buckets"]
        names = [b["Name"] for b in buckets]
        assert "test-s3-1" in names

    def test_put_and_get_object(self, s3_client):
        s3_client.create_bucket(Bucket="obj-bucket")
        s3_client.put_object(Bucket="obj-bucket", Key="file.txt", Body=b"hello world")
        resp = s3_client.get_object(Bucket="obj-bucket", Key="file.txt")
        assert resp["Body"].read() == b"hello world"

    def test_delete_object(self, s3_client):
        s3_client.create_bucket(Bucket="del-bucket")
        s3_client.put_object(Bucket="del-bucket", Key="gone.txt", Body=b"bye")
        s3_client.delete_object(Bucket="del-bucket", Key="gone.txt")
        with pytest.raises(Exception):
            s3_client.get_object(Bucket="del-bucket", Key="gone.txt")

    def test_head_object(self, s3_client):
        s3_client.create_bucket(Bucket="head-bucket")
        s3_client.put_object(Bucket="head-bucket", Key="meta.txt", Body=b"data123")
        resp = s3_client.head_object(Bucket="head-bucket", Key="meta.txt")
        assert resp["ContentLength"] == 7

    def test_copy_object(self, s3_client):
        s3_client.create_bucket(Bucket="copy-src")
        s3_client.put_object(Bucket="copy-src", Key="orig.txt", Body=b"original")
        s3_client.copy_object(Bucket="copy-src", Key="copy.txt", CopySource="copy-src/orig.txt")
        resp = s3_client.get_object(Bucket="copy-src", Key="copy.txt")
        assert resp["Body"].read() == b"original"

    def test_list_objects_with_prefix(self, s3_client):
        s3_client.create_bucket(Bucket="prefix-bucket")
        s3_client.put_object(Bucket="prefix-bucket", Key="logs/a.txt", Body=b"a")
        s3_client.put_object(Bucket="prefix-bucket", Key="logs/b.txt", Body=b"b")
        s3_client.put_object(Bucket="prefix-bucket", Key="data/c.txt", Body=b"c")
        resp = s3_client.list_objects_v2(Bucket="prefix-bucket", Prefix="logs/")
        keys = [o["Key"] for o in resp.get("Contents", [])]
        assert "logs/a.txt" in keys
        assert "data/c.txt" not in keys

    def test_delete_bucket(self, s3_client):
        s3_client.create_bucket(Bucket="killme-bucket")
        s3_client.delete_bucket(Bucket="killme-bucket")
        buckets = s3_client.list_buckets()["Buckets"]
        assert "killme-bucket" not in [b["Name"] for b in buckets]

    def test_head_bucket(self, s3_client):
        s3_client.create_bucket(Bucket="headb-bucket")
        resp = s3_client.head_bucket(Bucket="headb-bucket")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_multi_delete(self, s3_client):
        s3_client.create_bucket(Bucket="multi-del")
        s3_client.put_object(Bucket="multi-del", Key="a.txt", Body=b"a")
        s3_client.put_object(Bucket="multi-del", Key="b.txt", Body=b"b")
        s3_client.delete_objects(Bucket="multi-del", Delete={"Objects": [{"Key": "a.txt"}, {"Key": "b.txt"}]})
        resp = s3_client.list_objects_v2(Bucket="multi-del")
        assert resp.get("KeyCount", 0) == 0
