"""STS integration tests."""

class TestSTS:
    def test_get_caller_identity(self, sts_client):
        resp = sts_client.get_caller_identity()
        assert resp["Account"] == "000000000000"
        assert "arn:aws:iam::" in resp["Arn"]

    def test_assume_role(self, sts_client):
        resp = sts_client.assume_role(
            RoleArn="arn:aws:iam::000000000000:role/test-role",
            RoleSessionName="test-session",
        )
        creds = resp["Credentials"]
        assert "AccessKeyId" in creds
        assert "SecretAccessKey" in creds
        assert "SessionToken" in creds
