"""SSM Parameter Store integration tests."""

class TestSSM:
    def test_put_and_get_parameter(self, ssm_client):
        ssm_client.put_parameter(Name="/app/db_host", Value="localhost", Type="String")
        resp = ssm_client.get_parameter(Name="/app/db_host")
        assert resp["Parameter"]["Value"] == "localhost"

    def test_get_parameters(self, ssm_client):
        ssm_client.put_parameter(Name="/app/key1", Value="v1", Type="String")
        ssm_client.put_parameter(Name="/app/key2", Value="v2", Type="String")
        resp = ssm_client.get_parameters(Names=["/app/key1", "/app/key2", "/app/missing"])
        assert len(resp["Parameters"]) == 2
        assert "/app/missing" in resp["InvalidParameters"]

    def test_delete_parameter(self, ssm_client):
        ssm_client.put_parameter(Name="/del/param", Value="gone", Type="String")
        ssm_client.delete_parameter(Name="/del/param")

    def test_describe_parameters(self, ssm_client):
        ssm_client.put_parameter(Name="/desc/param", Value="x", Type="String")
        resp = ssm_client.describe_parameters()
        names = [p["Name"] for p in resp["Parameters"]]
        assert "/desc/param" in names

    def test_get_parameters_by_path(self, ssm_client):
        ssm_client.put_parameter(Name="/path/a", Value="1", Type="String")
        ssm_client.put_parameter(Name="/path/b", Value="2", Type="String")
        ssm_client.put_parameter(Name="/other/c", Value="3", Type="String")
        resp = ssm_client.get_parameters_by_path(Path="/path/")
        names = [p["Name"] for p in resp["Parameters"]]
        assert "/path/a" in names
        assert "/other/c" not in names

    def test_overwrite_parameter(self, ssm_client):
        ssm_client.put_parameter(Name="/ow/param", Value="old", Type="String")
        ssm_client.put_parameter(Name="/ow/param", Value="new", Type="String", Overwrite=True)
        resp = ssm_client.get_parameter(Name="/ow/param")
        assert resp["Parameter"]["Value"] == "new"
        assert resp["Parameter"]["Version"] == 2
