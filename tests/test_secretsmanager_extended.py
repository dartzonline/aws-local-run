"""Secrets Manager extended tests — tags, restore, version stages."""
import pytest


class TestSecretsManagerExtended:
    def test_tag_resource(self, secretsmanager_client):
        secretsmanager_client.create_secret(Name="tag/secret", SecretString="val")
        secretsmanager_client.tag_resource(
            SecretId="tag/secret",
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        resp = secretsmanager_client.describe_secret(SecretId="tag/secret")
        tags = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
        assert tags.get("env") == "prod"

    def test_restore_secret(self, secretsmanager_client):
        secretsmanager_client.create_secret(Name="restore/secret", SecretString="alive")
        secretsmanager_client.delete_secret(
            SecretId="restore/secret", ForceDeleteWithoutRecovery=False
        )
        secretsmanager_client.restore_secret(SecretId="restore/secret")
        resp = secretsmanager_client.get_secret_value(SecretId="restore/secret")
        assert resp["SecretString"] == "alive"

    def test_secret_versioning(self, secretsmanager_client):
        secretsmanager_client.create_secret(Name="ver/secret", SecretString="v1")
        resp1 = secretsmanager_client.get_secret_value(SecretId="ver/secret")
        v1_id = resp1["VersionId"]

        secretsmanager_client.put_secret_value(SecretId="ver/secret", SecretString="v2")
        resp2 = secretsmanager_client.get_secret_value(SecretId="ver/secret")
        assert resp2["SecretString"] == "v2"
        assert resp2["VersionId"] != v1_id

    def test_get_secret_by_arn(self, secretsmanager_client):
        resp = secretsmanager_client.create_secret(Name="arn/secret", SecretString="by-arn")
        arn = resp["ARN"]
        val = secretsmanager_client.get_secret_value(SecretId=arn)
        assert val["SecretString"] == "by-arn"

    def test_create_secret_with_description(self, secretsmanager_client):
        secretsmanager_client.create_secret(
            Name="desc/secret2",
            SecretString="x",
            Description="my description",
        )
        resp = secretsmanager_client.describe_secret(SecretId="desc/secret2")
        assert resp["Description"] == "my description"

    def test_list_excludes_deleted(self, secretsmanager_client):
        secretsmanager_client.create_secret(Name="listed/secret", SecretString="v")
        secretsmanager_client.create_secret(Name="deleted/secret", SecretString="v")
        secretsmanager_client.delete_secret(
            SecretId="deleted/secret", ForceDeleteWithoutRecovery=True
        )
        resp = secretsmanager_client.list_secrets()
        names = [s["Name"] for s in resp["SecretList"]]
        assert "listed/secret" in names
        assert "deleted/secret" not in names
