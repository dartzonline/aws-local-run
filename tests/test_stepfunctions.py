"""Tests for the Step Functions service emulator."""
import json
import pytest


ROLE_ARN = "arn:aws:iam::000000000000:role/step-fn-role"
DEFINITION = json.dumps({"Comment": "Empty pass-through"})


def test_create_state_machine(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="test-sm",
        definition=DEFINITION,
        roleArn=ROLE_ARN,
    )
    assert "stateMachineArn" in resp
    assert "test-sm" in resp["stateMachineArn"]


def test_create_duplicate_fails(stepfunctions_client):
    stepfunctions_client.create_state_machine(
        name="dup-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    with pytest.raises(Exception):
        stepfunctions_client.create_state_machine(
            name="dup-sm", definition=DEFINITION, roleArn=ROLE_ARN
        )


def test_describe_state_machine(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="desc-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    arn = resp["stateMachineArn"]
    desc = stepfunctions_client.describe_state_machine(stateMachineArn=arn)
    assert desc["name"] == "desc-sm"
    assert desc["status"] == "ACTIVE"


def test_list_state_machines(stepfunctions_client):
    stepfunctions_client.create_state_machine(
        name="list-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    resp = stepfunctions_client.list_state_machines()
    names = [sm["name"] for sm in resp["stateMachines"]]
    assert "list-sm" in names


def test_start_execution(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="exec-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    arn = resp["stateMachineArn"]
    exec_resp = stepfunctions_client.start_execution(
        stateMachineArn=arn,
        name="my-exec",
        input=json.dumps({"key": "value"}),
    )
    assert "executionArn" in exec_resp


def test_describe_execution(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="desc-exec-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    arn = resp["stateMachineArn"]
    exec_resp = stepfunctions_client.start_execution(
        stateMachineArn=arn,
        name="desc-exec",
        input="{}",
    )
    exec_arn = exec_resp["executionArn"]
    desc = stepfunctions_client.describe_execution(executionArn=exec_arn)
    # LocalRun auto-succeeds all executions
    assert desc["status"] == "SUCCEEDED"


def test_list_executions(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="list-exec-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    arn = resp["stateMachineArn"]
    stepfunctions_client.start_execution(stateMachineArn=arn, input="{}")
    stepfunctions_client.start_execution(stateMachineArn=arn, input="{}")
    list_resp = stepfunctions_client.list_executions(stateMachineArn=arn)
    assert len(list_resp["executions"]) >= 2


def test_get_execution_history(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="hist-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    arn = resp["stateMachineArn"]
    exec_resp = stepfunctions_client.start_execution(stateMachineArn=arn, input="{}")
    hist = stepfunctions_client.get_execution_history(
        executionArn=exec_resp["executionArn"]
    )
    types = [e["type"] for e in hist["events"]]
    assert "ExecutionStarted" in types
    assert "ExecutionSucceeded" in types


def test_delete_state_machine(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="del-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    arn = resp["stateMachineArn"]
    stepfunctions_client.delete_state_machine(stateMachineArn=arn)
    with pytest.raises(Exception):
        stepfunctions_client.describe_state_machine(stateMachineArn=arn)


def test_tag_and_list_tags(stepfunctions_client):
    resp = stepfunctions_client.create_state_machine(
        name="tag-sm", definition=DEFINITION, roleArn=ROLE_ARN
    )
    arn = resp["stateMachineArn"]
    stepfunctions_client.tag_resource(
        resourceArn=arn,
        tags=[{"key": "env", "value": "test"}],
    )
    tags_resp = stepfunctions_client.list_tags_for_resource(resourceArn=arn)
    assert any(t["key"] == "env" for t in tags_resp["tags"])
