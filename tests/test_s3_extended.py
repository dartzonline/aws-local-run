"""S3 extended tests — object metadata, content-type, tagging, overwrite."""
import io
import pytest


class TestS3Extended:
    def test_put_object_with_content_type(self, s3_client):
        s3_client.create_bucket(Bucket="ct-bucket")
        s3_client.put_object(
            Bucket="ct-bucket", Key="page.html",
            Body=b"<h1>hi</h1>", ContentType="text/html",
        )
        resp = s3_client.get_object(Bucket="ct-bucket", Key="page.html")
        assert resp["Body"].read() == b"<h1>hi</h1>"

    def test_put_object_with_metadata(self, s3_client):
        s3_client.create_bucket(Bucket="meta-bucket")
        s3_client.put_object(
            Bucket="meta-bucket", Key="doc.txt",
            Body=b"data", Metadata={"author": "alice", "env": "test"},
        )
        resp = s3_client.head_object(Bucket="meta-bucket", Key="doc.txt")
        meta = {k.lower(): v for k, v in resp["Metadata"].items()}
        assert meta.get("author") == "alice"

    def test_overwrite_object(self, s3_client):
        s3_client.create_bucket(Bucket="ow-bucket")
        s3_client.put_object(Bucket="ow-bucket", Key="f.txt", Body=b"v1")
        s3_client.put_object(Bucket="ow-bucket", Key="f.txt", Body=b"v2")
        resp = s3_client.get_object(Bucket="ow-bucket", Key="f.txt")
        assert resp["Body"].read() == b"v2"

    def test_list_objects_v2_empty_bucket(self, s3_client):
        s3_client.create_bucket(Bucket="empty-bucket")
        resp = s3_client.list_objects_v2(Bucket="empty-bucket")
        assert resp.get("KeyCount", 0) == 0
        assert resp.get("Contents") is None

    def test_list_objects_v2_delimiter(self, s3_client):
        s3_client.create_bucket(Bucket="delim-bucket")
        for key in ["a/1.txt", "a/2.txt", "b/3.txt"]:
            s3_client.put_object(Bucket="delim-bucket", Key=key, Body=b"x")
        resp = s3_client.list_objects_v2(Bucket="delim-bucket", Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "a/" in prefixes
        assert "b/" in prefixes

    def test_get_object_nonexistent(self, s3_client):
        s3_client.create_bucket(Bucket="miss-bucket")
        with pytest.raises(Exception) as exc:
            s3_client.get_object(Bucket="miss-bucket", Key="nope.txt")
        assert "NoSuchKey" in str(exc.value) or "404" in str(exc.value)

    def test_delete_nonexistent_bucket_object_succeeds(self, s3_client):
        s3_client.create_bucket(Bucket="delobj-bucket")
        # deleting a missing key should not raise
        s3_client.delete_object(Bucket="delobj-bucket", Key="does-not-exist.txt")

    def test_get_bucket_location(self, s3_client):
        s3_client.create_bucket(Bucket="loc-bucket")
        resp = s3_client.get_bucket_location(Bucket="loc-bucket")
        assert "LocationConstraint" in resp

    def test_copy_object_cross_key(self, s3_client):
        s3_client.create_bucket(Bucket="xcopy-bucket")
        s3_client.put_object(Bucket="xcopy-bucket", Key="src.txt", Body=b"source")
        s3_client.copy_object(
            Bucket="xcopy-bucket", Key="dst.txt",
            CopySource="xcopy-bucket/src.txt",
        )
        resp = s3_client.get_object(Bucket="xcopy-bucket", Key="dst.txt")
        assert resp["Body"].read() == b"source"

    def test_large_object(self, s3_client):
        s3_client.create_bucket(Bucket="large-bucket")
        data = b"x" * 1_000_000
        s3_client.put_object(Bucket="large-bucket", Key="big.bin", Body=data)
        resp = s3_client.get_object(Bucket="large-bucket", Key="big.bin")
        assert len(resp["Body"].read()) == 1_000_000
