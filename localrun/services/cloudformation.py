"""CloudFormation service emulator (stub)."""
import json, logging, time, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, new_request_id

logger = logging.getLogger("localrun.cloudformation")

class CloudFormationService:
    def __init__(self):
        self.stacks = {}  # name -> stack dict

    def handle(self, req: Request, path: str) -> Response:
        action = req.args.get("Action") or req.form.get("Action", "")
        handlers = {
            "CreateStack": self._create, "DeleteStack": self._delete,
            "ListStacks": self._list, "DescribeStacks": self._describe,
            "DescribeStackResources": self._describe_resources,
            "DescribeStackEvents": self._describe_events,
            "GetTemplate": self._get_template,
        }
        h = handlers.get(action)
        if not h:
            return error_response("InvalidAction", f"Action {action} not valid", 400)
        return h(req)

    def _p(self, req):
        from urllib.parse import parse_qs
        params = dict(req.args)
        if req.content_type and "form" in req.content_type:
            params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items():
                params[k] = v[0] if len(v) == 1 else v
        return params

    def _xml(self, action, content):
        body = f'<?xml version="1.0"?>\n<{action}Response xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">\n  <{action}Result>\n{content}\n  </{action}Result>\n  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>\n</{action}Response>'
        return Response(body, 200, content_type="application/xml")

    def _arn(self, name, sid):
        c = get_config()
        return f"arn:aws:cloudformation:{c.region}:{c.account_id}:stack/{name}/{sid}"

    def _create(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        if not name: return error_response("ValidationError", "StackName required", 400)
        if name in self.stacks: return error_response("AlreadyExistsException", f"Stack {name} exists", 400)
        sid = str(uuid.uuid4())
        self.stacks[name] = {
            "StackName": name, "StackId": self._arn(name, sid),
            "StackStatus": "CREATE_COMPLETE", "CreationTime": time.time(),
            "TemplateBody": p.get("TemplateBody", ""),
            "Parameters": [], "Resources": [], "Events": [],
        }
        logger.info("Created stack: %s", name)
        return self._xml("CreateStack", f"    <StackId>{self.stacks[name]['StackId']}</StackId>")

    def _delete(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        self.stacks.pop(name, None)
        return self._xml("DeleteStack", "")

    def _list(self, req):
        xml = ""
        for s in self.stacks.values():
            xml += f"""    <member>
      <StackName>{s['StackName']}</StackName>
      <StackId>{s['StackId']}</StackId>
      <StackStatus>{s['StackStatus']}</StackStatus>
    </member>\n"""
        return self._xml("ListStacks", f"  <StackSummaries>\n{xml}  </StackSummaries>")

    def _describe(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        s = self.stacks.get(name)
        if not s: return error_response("ValidationError", f"Stack {name} not found", 400)
        return self._xml("DescribeStacks", f"""  <Stacks>
    <member>
      <StackName>{s['StackName']}</StackName>
      <StackId>{s['StackId']}</StackId>
      <StackStatus>{s['StackStatus']}</StackStatus>
    </member>
  </Stacks>""")

    def _describe_resources(self, req):
        return self._xml("DescribeStackResources", "  <StackResources></StackResources>")

    def _describe_events(self, req):
        return self._xml("DescribeStackEvents", "  <StackEvents></StackEvents>")

    def _get_template(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        s = self.stacks.get(name)
        tmpl = s["TemplateBody"] if s else ""
        return self._xml("GetTemplate", f"    <TemplateBody>{tmpl}</TemplateBody>")

    def reset(self):
        self.stacks = {}
