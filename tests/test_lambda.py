"""Lambda service tests."""
import base64, io, json, zipfile
import pytest


def _zip(handler_src: str, filename: str = "index.py") -> str:
    """Return base64-encoded zip containing a single Python file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, handler_src)
    return base64.b64encode(buf.getvalue()).decode()


ECHO_HANDLER = _zip("def handler(event, context):\n    return event\n")

ADD_HANDLER = _zip(
    "def handler(event, context):\n"
    "    return {'sum': event['a'] + event['b']}\n"
)

ENV_HANDLER = _zip(
    "import os\n"
    "def handler(event, context):\n"
    "    return {'value': os.environ.get('MY_VAR', 'missing')}\n"
)

ERR_HANDLER = _zip(
    "def handler(event, context):\n"
    "    raise ValueError('something went wrong')\n"
)

TIMEOUT_HANDLER = _zip(
    "import time\n"
    "def handler(event, context):\n"
    "    time.sleep(10)\n"
    "    return {}\n"
)

CTX_HANDLER = _zip(
    "def handler(event, context):\n"
    "    return {'name': context.function_name, 'arn': context.invoked_function_arn}\n"
)


class TestLambdaCRUD:
    def test_create_function(self, lambda_client):
        resp = lambda_client.create_function(
            FunctionName="test-create",
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        assert resp["FunctionName"] == "test-create"
        assert resp["Runtime"] == "python3.12"
        assert resp["State"] == "Active"

    def test_create_duplicate_raises(self, lambda_client):
        lambda_client.create_function(
            FunctionName="dup-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        with pytest.raises(Exception):
            lambda_client.create_function(
                FunctionName="dup-fn", Runtime="python3.12",
                Role="arn:aws:iam::000000000000:role/r",
                Handler="index.handler",
                Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
            )

    def test_get_function(self, lambda_client):
        lambda_client.create_function(
            FunctionName="test-get", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        resp = lambda_client.get_function(FunctionName="test-get")
        assert resp["Configuration"]["FunctionName"] == "test-get"

    def test_get_nonexistent(self, lambda_client):
        with pytest.raises(Exception):
            lambda_client.get_function(FunctionName="does-not-exist")

    def test_list_functions(self, lambda_client):
        for name in ("list-fn-a", "list-fn-b"):
            lambda_client.create_function(
                FunctionName=name, Runtime="python3.12",
                Role="arn:aws:iam::000000000000:role/r",
                Handler="index.handler",
                Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
            )
        fns = lambda_client.list_functions()["Functions"]
        names = [f["FunctionName"] for f in fns]
        assert "list-fn-a" in names and "list-fn-b" in names

    def test_delete_function(self, lambda_client):
        lambda_client.create_function(
            FunctionName="del-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        lambda_client.delete_function(FunctionName="del-fn")
        fns = lambda_client.list_functions()["Functions"]
        assert not any(f["FunctionName"] == "del-fn" for f in fns)

    def test_update_function_configuration(self, lambda_client):
        lambda_client.create_function(
            FunctionName="cfg-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        resp = lambda_client.update_function_configuration(
            FunctionName="cfg-fn", Timeout=60, MemorySize=256,
            Description="updated",
        )
        assert resp["Timeout"] == 60
        assert resp["MemorySize"] == 256
        assert resp["Description"] == "updated"

    def test_update_function_code(self, lambda_client):
        lambda_client.create_function(
            FunctionName="code-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        old_sha = lambda_client.get_function(FunctionName="code-fn")["Configuration"]["CodeSha256"]
        lambda_client.update_function_code(
            FunctionName="code-fn",
            ZipFile=base64.b64decode(ADD_HANDLER),
        )
        new_sha = lambda_client.get_function(FunctionName="code-fn")["Configuration"]["CodeSha256"]
        assert old_sha != new_sha

    def test_function_arn_format(self, lambda_client):
        resp = lambda_client.create_function(
            FunctionName="arn-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        assert resp["FunctionArn"].startswith("arn:aws:lambda:")
        assert "arn-fn" in resp["FunctionArn"]


class TestLambdaInvoke:
    def test_invoke_echo(self, lambda_client):
        lambda_client.create_function(
            FunctionName="echo-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        payload = {"hello": "world", "num": 42}
        resp = lambda_client.invoke(
            FunctionName="echo-fn",
            Payload=json.dumps(payload).encode(),
        )
        result = json.loads(resp["Payload"].read())
        assert result == payload

    def test_invoke_compute(self, lambda_client):
        lambda_client.create_function(
            FunctionName="add-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ADD_HANDLER)},
        )
        resp = lambda_client.invoke(
            FunctionName="add-fn",
            Payload=json.dumps({"a": 3, "b": 7}).encode(),
        )
        result = json.loads(resp["Payload"].read())
        assert result["sum"] == 10

    def test_invoke_with_environment(self, lambda_client):
        lambda_client.create_function(
            FunctionName="env-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ENV_HANDLER)},
            Environment={"Variables": {"MY_VAR": "hello-from-env"}},
        )
        resp = lambda_client.invoke(FunctionName="env-fn", Payload=b"{}")
        result = json.loads(resp["Payload"].read())
        assert result["value"] == "hello-from-env"

    def test_invoke_error_sets_header(self, lambda_client):
        lambda_client.create_function(
            FunctionName="err-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ERR_HANDLER)},
        )
        resp = lambda_client.invoke(
            FunctionName="err-fn", Payload=b"{}",
        )
        assert resp.get("FunctionError") == "Unhandled"
        body = json.loads(resp["Payload"].read())
        assert "errorMessage" in body

    def test_invoke_context(self, lambda_client):
        lambda_client.create_function(
            FunctionName="ctx-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(CTX_HANDLER)},
        )
        resp = lambda_client.invoke(FunctionName="ctx-fn", Payload=b"{}")
        result = json.loads(resp["Payload"].read())
        assert result["name"] == "ctx-fn"
        assert "arn:aws:lambda:" in result["arn"]

    def test_invoke_timeout(self, lambda_client):
        lambda_client.create_function(
            FunctionName="timeout-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Timeout=1,
            Code={"ZipFile": base64.b64decode(TIMEOUT_HANDLER)},
        )
        resp = lambda_client.invoke(FunctionName="timeout-fn", Payload=b"{}")
        assert resp.get("FunctionError") == "Unhandled"

    def test_invoke_async(self, lambda_client):
        lambda_client.create_function(
            FunctionName="async-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        resp = lambda_client.invoke(
            FunctionName="async-fn",
            InvocationType="Event",
            Payload=b"{}",
        )
        assert resp["StatusCode"] == 202

    def test_invoke_no_code(self, lambda_client):
        lambda_client.create_function(
            FunctionName="nocode-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(_zip(""))},
        )


class TestLambdaAliases:
    def test_create_alias(self, lambda_client):
        lambda_client.create_function(
            FunctionName="alias-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        resp = lambda_client.create_alias(
            FunctionName="alias-fn", Name="prod", FunctionVersion="$LATEST",
        )
        assert resp["Name"] == "prod"
        assert "alias-fn" in resp["AliasArn"]

    def test_get_alias(self, lambda_client):
        lambda_client.create_function(
            FunctionName="alias-get-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        lambda_client.create_alias(FunctionName="alias-get-fn", Name="staging", FunctionVersion="$LATEST")
        alias = lambda_client.get_alias(FunctionName="alias-get-fn", Name="staging")
        assert alias["Name"] == "staging"

    def test_update_alias(self, lambda_client):
        lambda_client.create_function(
            FunctionName="alias-upd-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        lambda_client.create_alias(FunctionName="alias-upd-fn", Name="v1", FunctionVersion="$LATEST")
        resp = lambda_client.update_alias(
            FunctionName="alias-upd-fn", Name="v1", Description="updated alias",
        )
        assert resp["Description"] == "updated alias"

    def test_list_aliases(self, lambda_client):
        lambda_client.create_function(
            FunctionName="alias-list-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        lambda_client.create_alias(FunctionName="alias-list-fn", Name="a1", FunctionVersion="$LATEST")
        lambda_client.create_alias(FunctionName="alias-list-fn", Name="a2", FunctionVersion="$LATEST")
        aliases = lambda_client.list_aliases(FunctionName="alias-list-fn")["Aliases"]
        names = [a["Name"] for a in aliases]
        assert "a1" in names and "a2" in names

    def test_delete_alias(self, lambda_client):
        lambda_client.create_function(
            FunctionName="alias-del-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        lambda_client.create_alias(FunctionName="alias-del-fn", Name="old", FunctionVersion="$LATEST")
        lambda_client.delete_alias(FunctionName="alias-del-fn", Name="old")
        aliases = lambda_client.list_aliases(FunctionName="alias-del-fn")["Aliases"]
        assert not any(a["Name"] == "old" for a in aliases)


class TestLambdaPermissions:
    def test_add_permission(self, lambda_client):
        lambda_client.create_function(
            FunctionName="perm-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        resp = lambda_client.add_permission(
            FunctionName="perm-fn",
            StatementId="allow-s3",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn="arn:aws:s3:::my-bucket",
        )
        assert "Statement" in resp

    def test_get_policy(self, lambda_client):
        lambda_client.create_function(
            FunctionName="pol-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        lambda_client.add_permission(
            FunctionName="pol-fn", StatementId="stmt-1",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        )
        resp = lambda_client.get_policy(FunctionName="pol-fn")
        policy = json.loads(resp["Policy"])
        sids = [s["StatementId"] for s in policy["Statement"]]
        assert "stmt-1" in sids

    def test_remove_permission(self, lambda_client):
        lambda_client.create_function(
            FunctionName="rm-perm-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        lambda_client.add_permission(
            FunctionName="rm-perm-fn", StatementId="to-remove",
            Action="lambda:InvokeFunction", Principal="s3.amazonaws.com",
        )
        lambda_client.remove_permission(FunctionName="rm-perm-fn", StatementId="to-remove")
        with pytest.raises(Exception):
            lambda_client.get_policy(FunctionName="rm-perm-fn")


class TestLambdaEventSourceMappings:
    def test_create_esm(self, lambda_client, sqs_client):
        lambda_client.create_function(
            FunctionName="esm-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        q = sqs_client.create_queue(QueueName="esm-q")["QueueUrl"]
        q_arn = sqs_client.get_queue_attributes(QueueUrl=q, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        resp = lambda_client.create_event_source_mapping(
            FunctionName="esm-fn",
            EventSourceArn=q_arn,
            BatchSize=5,
        )
        assert resp["UUID"]
        assert resp["BatchSize"] == 5
        assert resp["State"] == "Enabled"

    def test_list_esm(self, lambda_client, sqs_client):
        lambda_client.create_function(
            FunctionName="esm-list-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        q = sqs_client.create_queue(QueueName="esm-list-q")["QueueUrl"]
        q_arn = sqs_client.get_queue_attributes(QueueUrl=q, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        lambda_client.create_event_source_mapping(FunctionName="esm-list-fn", EventSourceArn=q_arn)
        mappings = lambda_client.list_event_source_mappings(FunctionName="esm-list-fn")["EventSourceMappings"]
        assert len(mappings) >= 1

    def test_get_esm(self, lambda_client, sqs_client):
        lambda_client.create_function(
            FunctionName="esm-get-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        q = sqs_client.create_queue(QueueName="esm-get-q")["QueueUrl"]
        q_arn = sqs_client.get_queue_attributes(QueueUrl=q, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        esm_id = lambda_client.create_event_source_mapping(FunctionName="esm-get-fn", EventSourceArn=q_arn)["UUID"]
        mapping = lambda_client.get_event_source_mapping(UUID=esm_id)
        assert mapping["UUID"] == esm_id

    def test_delete_esm(self, lambda_client, sqs_client):
        lambda_client.create_function(
            FunctionName="esm-del-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        q = sqs_client.create_queue(QueueName="esm-del-q")["QueueUrl"]
        q_arn = sqs_client.get_queue_attributes(QueueUrl=q, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        esm_id = lambda_client.create_event_source_mapping(FunctionName="esm-del-fn", EventSourceArn=q_arn)["UUID"]
        lambda_client.delete_event_source_mapping(UUID=esm_id)
        mappings = lambda_client.list_event_source_mappings(FunctionName="esm-del-fn")["EventSourceMappings"]
        assert not any(m["UUID"] == esm_id for m in mappings)


class TestLambdaTags:
    def test_tag_function(self, lambda_client):
        lambda_client.create_function(
            FunctionName="tag-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
            Tags={"env": "dev", "team": "platform"},
        )
        resp = lambda_client.list_tags(Resource="tag-fn")
        assert resp["Tags"].get("env") == "dev"

    def test_tag_after_create(self, lambda_client):
        lambda_client.create_function(
            FunctionName="tag2-fn", Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": base64.b64decode(ECHO_HANDLER)},
        )
        fn_arn = lambda_client.get_function(FunctionName="tag2-fn")["Configuration"]["FunctionArn"]
        lambda_client.tag_resource(Resource=fn_arn, Tags={"owner": "alice"})
        tags = lambda_client.list_tags(Resource=fn_arn)["Tags"]
        assert tags.get("owner") == "alice"
