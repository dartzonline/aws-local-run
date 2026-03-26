"""Cognito user pool tests.

Covers pool lifecycle, user pool clients, user management, sign-up, and auth flows.
Tokens issued are fake JWTs (unsigned) — sufficient for testing the wiring.
"""
import uuid
import pytest


@pytest.fixture
def cognito():
    import boto3
    return boto3.client(
        "cognito-idp",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def _pool_name():
    return f"pool-{uuid.uuid4().hex[:8]}"


def test_create_user_pool_returns_id(cognito):
    resp = cognito.create_user_pool(PoolName=_pool_name())
    pool = resp["UserPool"]
    assert "Id" in pool
    assert pool["Id"].startswith("us-east-1_")


def test_describe_user_pool(cognito):
    name = _pool_name()
    pool_id = cognito.create_user_pool(PoolName=name)["UserPool"]["Id"]
    resp = cognito.describe_user_pool(UserPoolId=pool_id)
    assert resp["UserPool"]["Id"] == pool_id
    assert resp["UserPool"]["Name"] == name


def test_list_user_pools_includes_created(cognito):
    name = _pool_name()
    pool_id = cognito.create_user_pool(PoolName=name)["UserPool"]["Id"]
    resp = cognito.list_user_pools(MaxResults=60)
    ids = [p["Id"] for p in resp["UserPools"]]
    assert pool_id in ids


def test_create_user_pool_client(cognito):
    pool_id = cognito.create_user_pool(PoolName=_pool_name())["UserPool"]["Id"]
    resp = cognito.create_user_pool_client(
        UserPoolId=pool_id,
        ClientName="my-app-client",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )
    client = resp["UserPoolClient"]
    assert "ClientId" in client
    assert client["ClientName"] == "my-app-client"
    assert client["UserPoolId"] == pool_id


def test_admin_create_user(cognito):
    pool_id = cognito.create_user_pool(PoolName=_pool_name())["UserPool"]["Id"]
    username = f"user-{uuid.uuid4().hex[:8]}"
    resp = cognito.admin_create_user(
        UserPoolId=pool_id,
        Username=username,
        TemporaryPassword="Temp@1234",
        UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
    )
    user = resp["User"]
    assert user["Username"] == username
    assert user["UserStatus"] == "FORCE_CHANGE_PASSWORD"
    assert user["Enabled"] is True


def test_sign_up_user(cognito):
    pool_id = cognito.create_user_pool(PoolName=_pool_name())["UserPool"]["Id"]
    client_id = cognito.create_user_pool_client(
        UserPoolId=pool_id, ClientName="signup-client"
    )["UserPoolClient"]["ClientId"]

    username = f"signup-{uuid.uuid4().hex[:8]}"
    resp = cognito.sign_up(
        ClientId=client_id,
        Username=username,
        Password="MyPassword@123",
        UserAttributes=[{"Name": "email", "Value": f"{username}@example.com"}],
    )
    assert resp["UserConfirmed"] is False
    assert "UserSub" in resp


def test_initiate_auth_user_password(cognito):
    pool_id = cognito.create_user_pool(PoolName=_pool_name())["UserPool"]["Id"]
    client_id = cognito.create_user_pool_client(
        UserPoolId=pool_id,
        ClientName="auth-client",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
    )["UserPoolClient"]["ClientId"]

    username = f"auth-{uuid.uuid4().hex[:8]}"
    password = "Auth@Pass123"

    # Create user via AdminCreateUser then set a permanent password
    cognito.admin_create_user(
        UserPoolId=pool_id,
        Username=username,
        TemporaryPassword=password,
    )
    cognito.admin_set_user_password(
        UserPoolId=pool_id,
        Username=username,
        Password=password,
        Permanent=True,
    )

    resp = cognito.initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH",
        ClientId=client_id,
        AuthParameters={"USERNAME": username, "PASSWORD": password},
    )
    auth_result = resp["AuthenticationResult"]
    assert "AccessToken" in auth_result
    assert "IdToken" in auth_result
    assert "RefreshToken" in auth_result
    assert auth_result["TokenType"] == "Bearer"


def test_admin_get_user(cognito):
    pool_id = cognito.create_user_pool(PoolName=_pool_name())["UserPool"]["Id"]
    username = f"admin-get-{uuid.uuid4().hex[:8]}"
    cognito.admin_create_user(
        UserPoolId=pool_id,
        Username=username,
        TemporaryPassword="Temp@1234",
    )
    resp = cognito.admin_get_user(UserPoolId=pool_id, Username=username)
    assert resp["Username"] == username


def test_admin_delete_user(cognito):
    pool_id = cognito.create_user_pool(PoolName=_pool_name())["UserPool"]["Id"]
    username = f"del-user-{uuid.uuid4().hex[:8]}"
    cognito.admin_create_user(
        UserPoolId=pool_id,
        Username=username,
        TemporaryPassword="Temp@1234",
    )
    # Confirm user exists
    cognito.admin_get_user(UserPoolId=pool_id, Username=username)

    cognito.admin_delete_user(UserPoolId=pool_id, Username=username)

    with pytest.raises(Exception):
        cognito.admin_get_user(UserPoolId=pool_id, Username=username)


def test_delete_user_pool(cognito):
    pool_id = cognito.create_user_pool(PoolName=_pool_name())["UserPool"]["Id"]

    # Verify pool exists in listing
    pools_before = cognito.list_user_pools(MaxResults=60)["UserPools"]
    assert any(p["Id"] == pool_id for p in pools_before)

    cognito.delete_user_pool(UserPoolId=pool_id)

    pools_after = cognito.list_user_pools(MaxResults=60)["UserPools"]
    assert not any(p["Id"] == pool_id for p in pools_after)
