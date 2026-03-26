"""IAM extended tests — policies, roles, users."""
import json
import pytest

POLICY_DOC = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
    }
)

ASSUME_DOC = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)


class TestIAMExtended:
    def test_create_and_get_policy(self, iam_client):
        resp = iam_client.create_policy(
            PolicyName="test-policy",
            PolicyDocument=POLICY_DOC,
        )
        arn = resp["Policy"]["Arn"]
        fetched = iam_client.get_policy(PolicyArn=arn)
        assert fetched["Policy"]["PolicyName"] == "test-policy"

    def test_list_policies_includes_created(self, iam_client):
        arn = iam_client.create_policy(
            PolicyName="list-pol", PolicyDocument=POLICY_DOC
        )["Policy"]["Arn"]
        listed = iam_client.list_policies()["Policies"]
        assert any(p["Arn"] == arn for p in listed)

    def test_delete_policy(self, iam_client):
        arn = iam_client.create_policy(
            PolicyName="del-pol", PolicyDocument=POLICY_DOC
        )["Policy"]["Arn"]
        iam_client.delete_policy(PolicyArn=arn)
        listed = iam_client.list_policies()["Policies"]
        assert not any(p["Arn"] == arn for p in listed)

    def test_get_role(self, iam_client):
        iam_client.create_role(
            RoleName="get-role", AssumeRolePolicyDocument=ASSUME_DOC
        )
        resp = iam_client.get_role(RoleName="get-role")
        assert resp["Role"]["RoleName"] == "get-role"

    def test_attach_detach_role_policy(self, iam_client):
        iam_client.create_role(
            RoleName="attach-role", AssumeRolePolicyDocument=ASSUME_DOC
        )
        pol_arn = iam_client.create_policy(
            PolicyName="attach-pol", PolicyDocument=POLICY_DOC
        )["Policy"]["Arn"]
        iam_client.attach_role_policy(RoleName="attach-role", PolicyArn=pol_arn)
        attached = iam_client.list_attached_role_policies(RoleName="attach-role")[
            "AttachedPolicies"
        ]
        assert any(p["PolicyArn"] == pol_arn for p in attached)
        iam_client.detach_role_policy(RoleName="attach-role", PolicyArn=pol_arn)
        attached2 = iam_client.list_attached_role_policies(RoleName="attach-role")[
            "AttachedPolicies"
        ]
        assert not any(p["PolicyArn"] == pol_arn for p in attached2)

    def test_create_user_and_get(self, iam_client):
        iam_client.create_user(UserName="testuser")
        resp = iam_client.get_user(UserName="testuser")
        assert resp["User"]["UserName"] == "testuser"

    def test_list_users(self, iam_client):
        iam_client.create_user(UserName="user-list-one")
        users = iam_client.list_users()["Users"]
        names = [u["UserName"] for u in users]
        assert "user-list-one" in names

    def test_inline_role_policy(self, iam_client):
        iam_client.create_role(RoleName="inline-role", AssumeRolePolicyDocument=ASSUME_DOC)
        iam_client.put_role_policy(RoleName="inline-role", PolicyName="inline-pol", PolicyDocument=POLICY_DOC)
        r = iam_client.list_role_policies(RoleName="inline-role")
        assert "inline-pol" in r["PolicyNames"]
        r2 = iam_client.get_role_policy(RoleName="inline-role", PolicyName="inline-pol")
        assert r2["PolicyName"] == "inline-pol"
        iam_client.delete_role_policy(RoleName="inline-role", PolicyName="inline-pol")
        r3 = iam_client.list_role_policies(RoleName="inline-role")
        assert "inline-pol" not in r3["PolicyNames"]

    def test_instance_profile(self, iam_client):
        iam_client.create_instance_profile(InstanceProfileName="test-ip")
        r = iam_client.get_instance_profile(InstanceProfileName="test-ip")
        assert r["InstanceProfile"]["InstanceProfileName"] == "test-ip"
        profiles = iam_client.list_instance_profiles()["InstanceProfiles"]
        assert any(p["InstanceProfileName"] == "test-ip" for p in profiles)
        iam_client.delete_instance_profile(InstanceProfileName="test-ip")

    def test_access_keys(self, iam_client):
        iam_client.create_user(UserName="ak-user")
        r = iam_client.create_access_key(UserName="ak-user")
        key = r["AccessKey"]
        assert key["AccessKeyId"].startswith("AKIA")
        keys = iam_client.list_access_keys(UserName="ak-user")["AccessKeyMetadata"]
        assert any(k["AccessKeyId"] == key["AccessKeyId"] for k in keys)
        iam_client.delete_access_key(UserName="ak-user", AccessKeyId=key["AccessKeyId"])
        keys2 = iam_client.list_access_keys(UserName="ak-user")["AccessKeyMetadata"]
        assert not any(k["AccessKeyId"] == key["AccessKeyId"] for k in keys2)
