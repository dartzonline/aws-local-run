"""Tests for KMS service."""
import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_key(kms_client):
    r = kms_client.create_key(Description="test key", KeyUsage="ENCRYPT_DECRYPT")
    meta = r["KeyMetadata"]
    assert meta["KeyId"]
    assert meta["Arn"]
    assert meta["Enabled"] is True

    desc = kms_client.describe_key(KeyId=meta["KeyId"])
    assert desc["KeyMetadata"]["KeyId"] == meta["KeyId"]


def test_list_keys(kms_client):
    kms_client.create_key(Description="list test")
    r = kms_client.list_keys()
    assert len(r["Keys"]) >= 1


def test_create_and_resolve_alias(kms_client):
    key = kms_client.create_key(Description="alias test")["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/mykey", TargetKeyId=key)
    aliases = kms_client.list_aliases()["Aliases"]
    names = [a["AliasName"] for a in aliases]
    assert "alias/mykey" in names

    # resolve by alias
    desc = kms_client.describe_key(KeyId="alias/mykey")
    assert desc["KeyMetadata"]["KeyId"] == key

    kms_client.delete_alias(AliasName="alias/mykey")
    aliases2 = kms_client.list_aliases()["Aliases"]
    assert "alias/mykey" not in [a["AliasName"] for a in aliases2]


def test_encrypt_decrypt(kms_client):
    key = kms_client.create_key(Description="enc test")["KeyMetadata"]["KeyId"]
    plaintext = b"hello world"
    enc = kms_client.encrypt(KeyId=key, Plaintext=plaintext)
    blob = enc["CiphertextBlob"]
    assert blob != plaintext

    dec = kms_client.decrypt(CiphertextBlob=blob)
    assert dec["Plaintext"] == plaintext


def test_generate_data_key(kms_client):
    key = kms_client.create_key(Description="dek test")["KeyMetadata"]["KeyId"]
    r = kms_client.generate_data_key(KeyId=key, KeySpec="AES_256")
    assert "Plaintext" in r
    assert "CiphertextBlob" in r
    assert len(r["Plaintext"]) == 32


def test_disable_enable_key(kms_client):
    key = kms_client.create_key(Description="toggle")["KeyMetadata"]["KeyId"]
    kms_client.disable_key(KeyId=key)
    desc = kms_client.describe_key(KeyId=key)
    assert desc["KeyMetadata"]["Enabled"] is False

    kms_client.enable_key(KeyId=key)
    desc2 = kms_client.describe_key(KeyId=key)
    assert desc2["KeyMetadata"]["Enabled"] is True


def test_schedule_and_cancel_deletion(kms_client):
    key = kms_client.create_key(Description="deletion test")["KeyMetadata"]["KeyId"]
    kms_client.schedule_key_deletion(KeyId=key, PendingWindowInDays=7)
    desc = kms_client.describe_key(KeyId=key)
    assert desc["KeyMetadata"]["KeyState"] == "PendingDeletion"

    kms_client.cancel_key_deletion(KeyId=key)
    desc2 = kms_client.describe_key(KeyId=key)
    assert desc2["KeyMetadata"]["Enabled"] is True


def test_key_policy(kms_client):
    key = kms_client.create_key()["KeyMetadata"]["KeyId"]
    policy = '{"Version":"2012-10-17","Statement":[]}'
    kms_client.put_key_policy(KeyId=key, PolicyName="default", Policy=policy)
    r = kms_client.get_key_policy(KeyId=key, PolicyName="default")
    assert "Version" in r["Policy"]


def test_tag_key(kms_client):
    key = kms_client.create_key()["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key, Tags=[{"TagKey": "env", "TagValue": "test"}])
    r = kms_client.list_resource_tags(KeyId=key)
    tags = {t["TagKey"]: t["TagValue"] for t in r["Tags"]}
    assert tags["env"] == "test"

    kms_client.untag_resource(KeyId=key, TagKeys=["env"])
    r2 = kms_client.list_resource_tags(KeyId=key)
    assert not r2["Tags"]
