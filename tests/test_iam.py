"""IAM integration tests."""

class TestIAM:
    def test_create_and_list_roles(self, iam_client):
        iam_client.create_role(RoleName="test-role", AssumeRolePolicyDocument="{}")
        resp = iam_client.list_roles()
        names = [r["RoleName"] for r in resp["Roles"]]
        assert "test-role" in names

    def test_get_role(self, iam_client):
        iam_client.create_role(RoleName="get-role", AssumeRolePolicyDocument="{}")
        resp = iam_client.get_role(RoleName="get-role")
        assert resp["Role"]["RoleName"] == "get-role"

    def test_delete_role(self, iam_client):
        iam_client.create_role(RoleName="del-role", AssumeRolePolicyDocument="{}")
        iam_client.delete_role(RoleName="del-role")

    def test_create_and_list_users(self, iam_client):
        iam_client.create_user(UserName="test-user")
        resp = iam_client.list_users()
        names = [u["UserName"] for u in resp["Users"]]
        assert "test-user" in names

    def test_delete_user(self, iam_client):
        iam_client.create_user(UserName="del-user")
        iam_client.delete_user(UserName="del-user")
