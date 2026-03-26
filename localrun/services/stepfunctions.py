"""Step Functions service emulator.

Stores state machine definitions but does not execute ASL.
Executions auto-succeed immediately — enough for testing wiring.
"""
import json
import logging
import time
import uuid

from flask import Request, Response

from localrun.config import get_config
from localrun.utils import iso_timestamp

logger = logging.getLogger("localrun.stepfunctions")


def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.0")


def _err(code, msg, status=400):
    return Response(json.dumps({"__type": code, "message": msg}), status, content_type="application/x-amz-json-1.0")


class StepFunctionsService:
    def __init__(self):
        self.state_machines = {}  # arn -> sm dict
        self.executions = {}      # arn -> execution dict

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        try:
            body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception:
            body = {}

        actions = {
            "CreateStateMachine": self._create_sm,
            "DeleteStateMachine": self._delete_sm,
            "DescribeStateMachine": self._describe_sm,
            "ListStateMachines": self._list_sms,
            "UpdateStateMachine": self._update_sm,
            "StartExecution": self._start_execution,
            "StopExecution": self._stop_execution,
            "DescribeExecution": self._describe_execution,
            "ListExecutions": self._list_executions,
            "GetExecutionHistory": self._get_execution_history,
            "TagResource": self._tag_resource,
            "UntagResource": self._untag_resource,
            "ListTagsForResource": self._list_tags,
        }
        handler = actions.get(action)
        if not handler:
            return _err("UnknownOperationException", f"Unknown operation: {action}")
        return handler(body)

    def _make_sm_arn(self, name):
        c = get_config()
        return f"arn:aws:states:{c.region}:{c.account_id}:stateMachine:{name}"

    def _make_exec_arn(self, sm_name, exec_name):
        c = get_config()
        return f"arn:aws:states:{c.region}:{c.account_id}:execution:{sm_name}:{exec_name}"

    def _create_sm(self, body):
        name = body.get("name", "")
        if not name:
            return _err("InvalidParameterException", "name required")
        arn = self._make_sm_arn(name)
        if arn in self.state_machines:
            return _err("StateMachineAlreadyExists", f"State machine {name} already exists")
        sm = {
            "stateMachineArn": arn,
            "name": name,
            "definition": body.get("definition", "{}"),
            "roleArn": body.get("roleArn", ""),
            "type": body.get("type", "STANDARD"),
            "status": "ACTIVE",
            "creationDate": time.time(),
            "tags": {},
        }
        self.state_machines[arn] = sm
        logger.info("Created state machine: %s", name)
        return _resp({"stateMachineArn": arn, "creationDate": sm["creationDate"]})

    def _delete_sm(self, body):
        arn = body.get("stateMachineArn", "")
        if arn not in self.state_machines:
            return _err("StateMachineDoesNotExist", f"State machine not found: {arn}")
        del self.state_machines[arn]
        logger.info("Deleted state machine: %s", arn)
        return _resp({})

    def _describe_sm(self, body):
        arn = body.get("stateMachineArn", "")
        sm = self.state_machines.get(arn)
        if not sm:
            return _err("StateMachineDoesNotExist", f"State machine not found: {arn}")
        return _resp({
            "stateMachineArn": sm["stateMachineArn"],
            "name": sm["name"],
            "status": sm["status"],
            "definition": sm["definition"],
            "roleArn": sm["roleArn"],
            "type": sm["type"],
            "creationDate": sm["creationDate"],
        })

    def _list_sms(self, body):
        items = []
        for sm in self.state_machines.values():
            items.append({"stateMachineArn": sm["stateMachineArn"], "name": sm["name"], "type": sm["type"], "creationDate": sm["creationDate"]})
        return _resp({"stateMachines": items})

    def _update_sm(self, body):
        arn = body.get("stateMachineArn", "")
        sm = self.state_machines.get(arn)
        if not sm:
            return _err("StateMachineDoesNotExist", f"State machine not found: {arn}")
        if "definition" in body:
            sm["definition"] = body["definition"]
        if "roleArn" in body:
            sm["roleArn"] = body["roleArn"]
        return _resp({"stateMachineArn": arn, "updateDate": time.time()})

    def _start_execution(self, body):
        sm_arn = body.get("stateMachineArn", "")
        sm = self.state_machines.get(sm_arn)
        if not sm:
            return _err("StateMachineDoesNotExist", f"State machine not found: {sm_arn}")
        exec_name = body.get("name", uuid.uuid4().hex)
        exec_arn = self._make_exec_arn(sm["name"], exec_name)
        input_str = body.get("input", "{}")
        now = time.time()
        execution = {
            "executionArn": exec_arn,
            "stateMachineArn": sm_arn,
            "name": exec_name,
            # Auto-succeed: LocalRun doesn't run ASL, so we mark it done right away
            "status": "SUCCEEDED",
            "startDate": now,
            "stopDate": now,
            "input": input_str,
            "output": input_str,  # pass-through
        }
        self.executions[exec_arn] = execution
        logger.info("Started execution: %s -> %s", sm["name"], exec_name)
        return _resp({"executionArn": exec_arn, "startDate": now})

    def _stop_execution(self, body):
        exec_arn = body.get("executionArn", "")
        ex = self.executions.get(exec_arn)
        if not ex:
            return _err("ExecutionDoesNotExist", f"Execution not found: {exec_arn}")
        if ex["status"] == "RUNNING":
            ex["status"] = "ABORTED"
            ex["stopDate"] = time.time()
        return _resp({"stopDate": ex["stopDate"]})

    def _describe_execution(self, body):
        exec_arn = body.get("executionArn", "")
        ex = self.executions.get(exec_arn)
        if not ex:
            return _err("ExecutionDoesNotExist", f"Execution not found: {exec_arn}")
        return _resp(ex)

    def _list_executions(self, body):
        sm_arn = body.get("stateMachineArn", "")
        status_filter = body.get("statusFilter", "")
        items = []
        for ex in self.executions.values():
            if sm_arn and ex["stateMachineArn"] != sm_arn:
                continue
            if status_filter and ex["status"] != status_filter:
                continue
            items.append({
                "executionArn": ex["executionArn"],
                "stateMachineArn": ex["stateMachineArn"],
                "name": ex["name"],
                "status": ex["status"],
                "startDate": ex["startDate"],
            })
        return _resp({"executions": items})

    def _get_execution_history(self, body):
        exec_arn = body.get("executionArn", "")
        ex = self.executions.get(exec_arn)
        if not ex:
            return _err("ExecutionDoesNotExist", f"Execution not found: {exec_arn}")
        events = [
            {"id": 1, "timestamp": ex["startDate"], "type": "ExecutionStarted",
             "executionStartedEventDetails": {"input": ex["input"]}},
        ]
        if ex["status"] == "SUCCEEDED":
            events.append({"id": 2, "timestamp": ex.get("stopDate", ex["startDate"]), "type": "ExecutionSucceeded",
                           "executionSucceededEventDetails": {"output": ex.get("output", "{}")}})
        elif ex["status"] in ("FAILED", "ABORTED"):
            events.append({"id": 2, "timestamp": ex.get("stopDate", ex["startDate"]), "type": "ExecutionAborted"})
        return _resp({"events": events})

    def _tag_resource(self, body):
        arn = body.get("resourceArn", "")
        sm = self.state_machines.get(arn)
        if not sm:
            return _err("ResourceNotFound", f"Resource not found: {arn}")
        sm["tags"].update({t["key"]: t["value"] for t in body.get("tags", [])})
        return _resp({})

    def _untag_resource(self, body):
        arn = body.get("resourceArn", "")
        sm = self.state_machines.get(arn)
        if not sm:
            return _err("ResourceNotFound", f"Resource not found: {arn}")
        for k in body.get("tagKeys", []):
            sm["tags"].pop(k, None)
        return _resp({})

    def _list_tags(self, body):
        arn = body.get("resourceArn", "")
        sm = self.state_machines.get(arn)
        if not sm:
            return _err("ResourceNotFound", f"Resource not found: {arn}")
        tags = [{"key": k, "value": v} for k, v in sm["tags"].items()]
        return _resp({"tags": tags})

    def reset(self):
        self.state_machines = {}
        self.executions = {}
