"""Cognito user pool emulator.

Supports the basic user pool lifecycle, user management, and auth flows.
Tokens are fake JWTs (unsigned) — enough for testing wiring.
"""
import base64
import json
import logging
import time
import uuid

from flask import Request, Response

from localrun.config import get_config
from localrun.utils import iso_timestamp

logger = logging.getLogger("localrun.cognito")


def _json(req):
    try:
        return json.loads(req.get_data(as_text=True) or "{}")
    except Exception:
        return {}


def _resp(data, status=200):
    return Response(
        json.dumps(data, default=str),
        status,
        content_type="application/x-amz-json-1.1",
    )


def _err(code, msg, status=400):
    return Response(
        json.dumps({"__type": code, "message": msg}),
        status,
        content_type="application/x-amz-json-1.1",
    )


def _make_token(username, pool_id):
    now = int(time.time())
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "RS256", "typ": "JWT"}).encode()
    ).rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        json.dumps({
            "sub": username,
            "token_use": "access",
            "cognito:username": username,
            "iss": f"https://cognito-idp.us-east-1.amazonaws.com/{pool_id}",
            "exp": now + 3600,
            "iat": now,
        }).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}.fakesig"


class CognitoService:
    def __init__(self):
        self.pools = {}         # pool_id -> pool dict
        self.pool_clients = {}  # client_id -> client dict
        self.users = {}         # pool_id -> {username: user_dict}
        self.tokens = {}        # access_token -> {username, pool_id, expires_at}

    def handle(self, req, path):
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        body = _json(req)

        actions = {
            "CreateUserPool": self._create_pool,
            "DeleteUserPool": self._delete_pool,
            "DescribeUserPool": self._describe_pool,
            "ListUserPools": self._list_pools,
            "CreateUserPoolClient": self._create_client,
            "DescribeUserPoolClient": self._describe_client,
            "ListUserPoolClients": self._list_clients,
            "DeleteUserPoolClient": self._delete_client,
            "AdminCreateUser": self._admin_create_user,
            "AdminDeleteUser": self._admin_delete_user,
            "AdminGetUser": self._admin_get_user,
            "AdminSetUserPassword": self._admin_set_password,
            "ListUsers": self._list_users,
            "SignUp": self._sign_up,
            "ConfirmSignUp": self._confirm_sign_up,
            "InitiateAuth": self._initiate_auth,
            "GetUser": self._get_user,
            "ForgotPassword": self._forgot_password,
            "ConfirmForgotPassword": self._confirm_forgot_password,
            "GlobalSignOut": self._global_sign_out,
        }
        handler = actions.get(action)
        if not handler:
            return _err("UnknownOperationException", f"Unknown operation: {action}")
        return handler(body)

    # --- pool ARN helper ---

    def _pool_arn(self, pool_id):
        c = get_config()
        return f"arn:aws:cognito-idp:{c.region}:{c.account_id}:userpool/{pool_id}"

    # --- pool operations ---

    def _create_pool(self, body):
        name = body.get("PoolName", "")
        if not name:
            return _err("InvalidParameterException", "PoolName required")
        pool_id = "us-east-1_" + uuid.uuid4().hex[:8]
        now = iso_timestamp()
        pool = {
            "Id": pool_id,
            "Name": name,
            "Arn": self._pool_arn(pool_id),
            "Status": "Active",
            "CreationDate": now,
            "LastModifiedDate": now,
            "Policies": body.get("Policies", {}),
            "Schema": body.get("Schema", []),
            "AutoVerifiedAttributes": body.get("AutoVerifiedAttributes", []),
        }
        self.pools[pool_id] = pool
        self.users[pool_id] = {}
        logger.info("Created user pool: %s (%s)", name, pool_id)
        return _resp({"UserPool": pool})

    def _delete_pool(self, body):
        pool_id = body.get("UserPoolId", "")
        if pool_id not in self.pools:
            return _err("ResourceNotFoundException", f"Pool not found: {pool_id}", 404)
        del self.pools[pool_id]
        self.users.pop(pool_id, None)
        # remove clients for this pool
        remove = [cid for cid, c in self.pool_clients.items() if c["UserPoolId"] == pool_id]
        for cid in remove:
            del self.pool_clients[cid]
        logger.info("Deleted user pool: %s", pool_id)
        return _resp({})

    def _describe_pool(self, body):
        pool_id = body.get("UserPoolId", "")
        pool = self.pools.get(pool_id)
        if not pool:
            return _err("ResourceNotFoundException", f"Pool not found: {pool_id}", 404)
        return _resp({"UserPool": pool})

    def _list_pools(self, body):
        summaries = []
        for pool in self.pools.values():
            summaries.append({
                "Id": pool["Id"],
                "Name": pool["Name"],
                "Status": pool["Status"],
                "CreationDate": pool["CreationDate"],
                "LastModifiedDate": pool["LastModifiedDate"],
            })
        return _resp({"UserPools": summaries})

    # --- client operations ---

    def _create_client(self, body):
        pool_id = body.get("UserPoolId", "")
        if pool_id not in self.pools:
            return _err("ResourceNotFoundException", f"Pool not found: {pool_id}", 404)
        client_name = body.get("ClientName", "")
        client_id = uuid.uuid4().hex[:26]
        now = iso_timestamp()
        client = {
            "UserPoolId": pool_id,
            "ClientId": client_id,
            "ClientName": client_name,
            "ExplicitAuthFlows": body.get("ExplicitAuthFlows", []),
            "CreationDate": now,
            "LastModifiedDate": now,
        }
        if body.get("GenerateSecret"):
            client["ClientSecret"] = uuid.uuid4().hex + uuid.uuid4().hex
        self.pool_clients[client_id] = client
        logger.info("Created user pool client: %s (%s)", client_name, client_id)
        return _resp({"UserPoolClient": client})

    def _describe_client(self, body):
        client_id = body.get("ClientId", "")
        client = self.pool_clients.get(client_id)
        if not client:
            return _err("ResourceNotFoundException", f"Client not found: {client_id}", 404)
        return _resp({"UserPoolClient": client})

    def _list_clients(self, body):
        pool_id = body.get("UserPoolId", "")
        clients = [c for c in self.pool_clients.values() if c["UserPoolId"] == pool_id]
        return _resp({"UserPoolClients": clients})

    def _delete_client(self, body):
        client_id = body.get("ClientId", "")
        self.pool_clients.pop(client_id, None)
        return _resp({})

    # --- user operations ---

    def _admin_create_user(self, body):
        pool_id = body.get("UserPoolId", "")
        username = body.get("Username", "")
        if pool_id not in self.pools:
            return _err("ResourceNotFoundException", f"Pool not found: {pool_id}", 404)
        if not username:
            return _err("InvalidParameterException", "Username required")
        now = iso_timestamp()
        user = {
            "Username": username,
            "UserStatus": "FORCE_CHANGE_PASSWORD",
            "Enabled": True,
            "UserCreateDate": now,
            "UserLastModifiedDate": now,
            "Attributes": body.get("UserAttributes", []),
            "_password": body.get("TemporaryPassword", ""),
        }
        self.users[pool_id][username] = user
        logger.info("Admin created user: %s in pool %s", username, pool_id)
        return _resp({"User": _public_user(user)})

    def _admin_delete_user(self, body):
        pool_id = body.get("UserPoolId", "")
        username = body.get("Username", "")
        if pool_id in self.users:
            self.users[pool_id].pop(username, None)
        return _resp({})

    def _admin_get_user(self, body):
        pool_id = body.get("UserPoolId", "")
        username = body.get("Username", "")
        pool_users = self.users.get(pool_id, {})
        user = pool_users.get(username)
        if not user:
            return _err("UserNotFoundException", f"User not found: {username}", 404)
        return _resp(_public_user(user))

    def _admin_set_password(self, body):
        pool_id = body.get("UserPoolId", "")
        username = body.get("Username", "")
        password = body.get("Password", "")
        permanent = body.get("Permanent", False)
        pool_users = self.users.get(pool_id, {})
        user = pool_users.get(username)
        if not user:
            return _err("UserNotFoundException", f"User not found: {username}", 404)
        user["_password"] = password
        if permanent:
            user["UserStatus"] = "CONFIRMED"
        user["UserLastModifiedDate"] = iso_timestamp()
        return _resp({})

    def _list_users(self, body):
        pool_id = body.get("UserPoolId", "")
        pool_users = self.users.get(pool_id, {})
        limit = body.get("Limit", 0)
        result = []
        for user in pool_users.values():
            result.append(_public_user(user))
            if limit and len(result) >= limit:
                break
        return _resp({"Users": result})

    # --- sign-up / auth ---

    def _sign_up(self, body):
        client_id = body.get("ClientId", "")
        username = body.get("Username", "")
        password = body.get("Password", "")
        client = self.pool_clients.get(client_id)
        if not client:
            return _err("ResourceNotFoundException", f"Client not found: {client_id}", 404)
        pool_id = client["UserPoolId"]
        if pool_id not in self.users:
            self.users[pool_id] = {}
        user_sub = uuid.uuid4().hex
        now = iso_timestamp()
        attrs = body.get("UserAttributes", [])
        # ensure sub attribute is present
        attrs.append({"Name": "sub", "Value": user_sub})
        user = {
            "Username": username,
            "UserStatus": "UNCONFIRMED",
            "Enabled": True,
            "UserCreateDate": now,
            "UserLastModifiedDate": now,
            "Attributes": attrs,
            "_password": password,
        }
        self.users[pool_id][username] = user
        logger.info("SignUp: %s in pool %s", username, pool_id)
        return _resp({"UserConfirmed": False, "UserSub": user_sub})

    def _confirm_sign_up(self, body):
        client_id = body.get("ClientId", "")
        username = body.get("Username", "")
        client = self.pool_clients.get(client_id)
        if not client:
            return _err("ResourceNotFoundException", f"Client not found: {client_id}", 404)
        pool_id = client["UserPoolId"]
        pool_users = self.users.get(pool_id, {})
        user = pool_users.get(username)
        if not user:
            return _err("UserNotFoundException", f"User not found: {username}", 404)
        user["UserStatus"] = "CONFIRMED"
        user["UserLastModifiedDate"] = iso_timestamp()
        return _resp({})

    def _initiate_auth(self, body):
        auth_flow = body.get("AuthFlow", "")
        client_id = body.get("ClientId", "")
        params = body.get("AuthParameters", {})

        client = self.pool_clients.get(client_id)
        if not client:
            return _err("ResourceNotFoundException", f"Client not found: {client_id}", 404)
        pool_id = client["UserPoolId"]

        if auth_flow in ("USER_PASSWORD_AUTH", "USER_SRP_AUTH"):
            username = params.get("USERNAME", "")
            password = params.get("PASSWORD", "")
            pool_users = self.users.get(pool_id, {})
            user = pool_users.get(username)
            if not user:
                return _err("UserNotFoundException", f"User not found: {username}", 401)
            # For SRP we skip real crypto and just check username existence
            if auth_flow == "USER_PASSWORD_AUTH" and user.get("_password") != password:
                return _err("NotAuthorizedException", "Incorrect username or password", 400)
            token = _make_token(username, pool_id)
            refresh = uuid.uuid4().hex
            self.tokens[token] = {
                "username": username,
                "pool_id": pool_id,
                "expires_at": int(time.time()) + 3600,
            }
            logger.info("InitiateAuth success: %s", username)
            return _resp({
                "AuthenticationResult": {
                    "AccessToken": token,
                    "IdToken": token,
                    "RefreshToken": refresh,
                    "ExpiresIn": 3600,
                    "TokenType": "Bearer",
                }
            })

        return _err("InvalidParameterException", f"Unsupported AuthFlow: {auth_flow}")

    def _get_user(self, body):
        token = body.get("AccessToken", "")
        entry = self.tokens.get(token)
        if not entry:
            return _err("NotAuthorizedException", "Invalid access token", 401)
        pool_users = self.users.get(entry["pool_id"], {})
        user = pool_users.get(entry["username"])
        if not user:
            return _err("UserNotFoundException", "User not found", 404)
        return _resp({
            "Username": user["Username"],
            "UserAttributes": user.get("Attributes", []),
        })

    def _forgot_password(self, body):
        client_id = body.get("ClientId", "")
        username = body.get("Username", "")
        client = self.pool_clients.get(client_id)
        if not client:
            return _err("ResourceNotFoundException", f"Client not found: {client_id}", 404)
        pool_id = client["UserPoolId"]
        pool_users = self.users.get(pool_id, {})
        user = pool_users.get(username)
        if user:
            user["UserStatus"] = "RESET_REQUIRED"
            user["UserLastModifiedDate"] = iso_timestamp()
        return _resp({
            "CodeDeliveryDetails": {
                "Destination": "email@example.com",
                "DeliveryMedium": "EMAIL",
            }
        })

    def _confirm_forgot_password(self, body):
        client_id = body.get("ClientId", "")
        username = body.get("Username", "")
        password = body.get("Password", "")
        client = self.pool_clients.get(client_id)
        if not client:
            return _err("ResourceNotFoundException", f"Client not found: {client_id}", 404)
        pool_id = client["UserPoolId"]
        pool_users = self.users.get(pool_id, {})
        user = pool_users.get(username)
        if not user:
            return _err("UserNotFoundException", f"User not found: {username}", 404)
        user["_password"] = password
        user["UserStatus"] = "CONFIRMED"
        user["UserLastModifiedDate"] = iso_timestamp()
        return _resp({})

    def _global_sign_out(self, body):
        token = body.get("AccessToken", "")
        self.tokens.pop(token, None)
        return _resp({})

    def reset(self):
        self.pools = {}
        self.pool_clients = {}
        self.users = {}
        self.tokens = {}


def _public_user(user):
    """Return a user dict without the internal _password field."""
    return {k: v for k, v in user.items() if not k.startswith("_")}
