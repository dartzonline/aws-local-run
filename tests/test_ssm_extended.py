"""SSM extended tests — SecureString, delete, path recursion, versions."""
import pytest


class TestSSMExtended:
    def test_put_secure_string(self, ssm_client):
        ssm_client.put_parameter(Name="/sec/param", Value="topsecret", Type="SecureString")
        resp = ssm_client.get_parameter(Name="/sec/param", WithDecryption=True)
        assert resp["Parameter"]["Value"] == "topsecret"
        assert resp["Parameter"]["Type"] == "SecureString"

    def test_put_string_list(self, ssm_client):
        ssm_client.put_parameter(Name="/list/param", Value="a,b,c", Type="StringList")
        resp = ssm_client.get_parameter(Name="/list/param")
        assert resp["Parameter"]["Value"] == "a,b,c"

    def test_version_increments(self, ssm_client):
        ssm_client.put_parameter(Name="/ver/param", Value="v1", Type="String")
        ssm_client.put_parameter(Name="/ver/param", Value="v2", Type="String", Overwrite=True)
        ssm_client.put_parameter(Name="/ver/param", Value="v3", Type="String", Overwrite=True)
        resp = ssm_client.get_parameter(Name="/ver/param")
        assert resp["Parameter"]["Version"] == 3

    def test_get_parameters_missing(self, ssm_client):
        ssm_client.put_parameter(Name="/exists/p", Value="yes", Type="String")
        resp = ssm_client.get_parameters(Names=["/exists/p", "/missing/p"])
        found = [p["Name"] for p in resp["Parameters"]]
        assert "/exists/p" in found
        assert "/missing/p" in resp["InvalidParameters"]

    def test_delete_parameter(self, ssm_client):
        ssm_client.put_parameter(Name="/del2/param", Value="bye", Type="String")
        ssm_client.delete_parameter(Name="/del2/param")
        resp = ssm_client.get_parameters(Names=["/del2/param"])
        assert "/del2/param" in resp["InvalidParameters"]

    def test_get_parameters_by_path_recursive(self, ssm_client):
        ssm_client.put_parameter(Name="/deep/a/b", Value="1", Type="String")
        ssm_client.put_parameter(Name="/deep/a/c", Value="2", Type="String")
        ssm_client.put_parameter(Name="/deep/x", Value="3", Type="String")
        resp = ssm_client.get_parameters_by_path(Path="/deep/", Recursive=True)
        names = [p["Name"] for p in resp["Parameters"]]
        assert "/deep/a/b" in names
        assert "/deep/x" in names

    def test_describe_parameters_filter(self, ssm_client):
        ssm_client.put_parameter(Name="/filter/p1", Value="v", Type="String")
        resp = ssm_client.describe_parameters()
        names = [p["Name"] for p in resp["Parameters"]]
        assert "/filter/p1" in names
