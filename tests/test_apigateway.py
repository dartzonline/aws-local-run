"""API Gateway REST API tests."""
import uuid
import pytest


@pytest.fixture
def apigw():
    import boto3
    return boto3.client(
        "apigateway",
        endpoint_url="http://127.0.0.1:14566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def _api_name():
    return f"test-api-{uuid.uuid4().hex[:8]}"


def test_create_rest_api_returns_id(apigw):
    resp = apigw.create_rest_api(name=_api_name(), description="test api")
    assert "id" in resp
    assert len(resp["id"]) > 0


def test_get_rest_api(apigw):
    name = _api_name()
    api_id = apigw.create_rest_api(name=name)["id"]
    resp = apigw.get_rest_api(restApiId=api_id)
    assert resp["id"] == api_id
    assert resp["name"] == name


def test_list_rest_apis_includes_created(apigw):
    name = _api_name()
    api_id = apigw.create_rest_api(name=name)["id"]
    apis = apigw.get_rest_apis()["items"]
    ids = [a["id"] for a in apis]
    assert api_id in ids


def test_create_resource_under_root(apigw):
    api_id = apigw.create_rest_api(name=_api_name())["id"]
    resources = apigw.get_resources(restApiId=api_id)["items"]
    root = next(r for r in resources if r["path"] == "/")

    resp = apigw.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="items",
    )
    assert resp["pathPart"] == "items"
    assert resp["path"] == "/items"
    assert "id" in resp


def test_put_method_get_no_auth(apigw):
    api_id = apigw.create_rest_api(name=_api_name())["id"]
    resources = apigw.get_resources(restApiId=api_id)["items"]
    root = next(r for r in resources if r["path"] == "/")
    resource_id = apigw.create_resource(
        restApiId=api_id, parentId=root["id"], pathPart="hello"
    )["id"]

    resp = apigw.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    assert resp["httpMethod"] == "GET"
    assert resp["authorizationType"] == "NONE"


def test_put_integration_mock(apigw):
    api_id = apigw.create_rest_api(name=_api_name())["id"]
    resources = apigw.get_resources(restApiId=api_id)["items"]
    root = next(r for r in resources if r["path"] == "/")
    resource_id = apigw.create_resource(
        restApiId=api_id, parentId=root["id"], pathPart="mock"
    )["id"]
    apigw.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )

    resp = apigw.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    assert resp["type"] == "MOCK"


def test_create_deployment(apigw):
    api_id = apigw.create_rest_api(name=_api_name())["id"]
    resp = apigw.create_deployment(restApiId=api_id)
    assert "id" in resp


def test_create_stage(apigw):
    api_id = apigw.create_rest_api(name=_api_name())["id"]
    deployment_id = apigw.create_deployment(restApiId=api_id)["id"]

    resp = apigw.create_stage(
        restApiId=api_id,
        stageName="prod",
        deploymentId=deployment_id,
    )
    assert resp["stageName"] == "prod"
    assert resp["deploymentId"] == deployment_id


def test_tag_and_list_tags(apigw):
    api_id = apigw.create_rest_api(name=_api_name())["id"]
    arn = f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"
    apigw.tag_resource(resourceArn=arn, tags={"env": "test", "team": "api"})
    resp = apigw.get_tags(resourceArn=arn)
    assert resp["tags"].get("env") == "test"
    assert resp["tags"].get("team") == "api"


def test_delete_rest_api(apigw):
    api_id = apigw.create_rest_api(name=_api_name())["id"]

    # Verify it exists
    apis_before = apigw.get_rest_apis()["items"]
    assert any(a["id"] == api_id for a in apis_before)

    apigw.delete_rest_api(restApiId=api_id)

    apis_after = apigw.get_rest_apis()["items"]
    assert not any(a["id"] == api_id for a in apis_after)
