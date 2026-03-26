"""IAM service emulator (stub — no actual permission enforcement)."""
import json, logging, time, uuid
from dataclasses import dataclass, field
from urllib.parse import parse_qs
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, iso_timestamp, new_request_id

logger = logging.getLogger("localrun.iam")

class IAMService:
    def __init__(self):
        self.roles = {}
        self.policies = {}
        self.users = {}
        self.attached = {}  # role_name -> [policy_arns]

    def handle(self, req: Request, path: str) -> Response:
        action = req.args.get("Action") or req.form.get("Action", "")
        if not action:
            params = parse_qs(req.get_data(as_text=True))
            action = params.get("Action", [""])[0]
        actions = {
            "CreateRole": self._create_role, "DeleteRole": self._delete_role,
            "GetRole": self._get_role, "ListRoles": self._list_roles,
            "CreatePolicy": self._create_policy, "AttachRolePolicy": self._attach_policy,
            "DetachRolePolicy": self._detach_policy, "ListAttachedRolePolicies": self._list_attached,
            "GetPolicy": self._get_policy, "ListPolicies": self._list_policies, "DeletePolicy": self._delete_policy,
            "CreateUser": self._create_user, "DeleteUser": self._delete_user,
            "GetUser": self._get_user, "ListUsers": self._list_users,
        }
        handler = actions.get(action)
        if not handler: return error_response("InvalidAction", f"Invalid: {action}", 400)
        return handler(req)

    def _p(self, req):
        params = dict(req.args)
        if req.content_type and "form" in req.content_type: params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items(): params[k] = v[0] if len(v)==1 else v
        return params

    def _xml(self, action, content):
        body = f'<?xml version="1.0"?>\n<{action}Response xmlns="https://iam.amazonaws.com/doc/2010-05-08/">\n  <{action}Result>\n{content}\n  </{action}Result>\n  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>\n</{action}Response>'
        return Response(body, 200, content_type="application/xml")

    def _create_role(self, req):
        p = self._p(req); name = p.get("RoleName", "")
        if not name: return error_response("ValidationException", "RoleName required", 400)
        c = get_config(); arn = f"arn:aws:iam::{c.account_id}:role/{name}"
        self.roles[name] = {"RoleName": name, "Arn": arn, "CreateDate": iso_timestamp(),
                            "AssumeRolePolicyDocument": p.get("AssumeRolePolicyDocument", ""),
                            "Path": p.get("Path", "/"), "RoleId": uuid.uuid4().hex[:20].upper()}
        self.attached.setdefault(name, [])
        logger.info("Created role: %s", name)
        return self._xml("CreateRole", f"    <Role><RoleName>{name}</RoleName><Arn>{arn}</Arn><CreateDate>{self.roles[name]['CreateDate']}</CreateDate><Path>/</Path><RoleId>{self.roles[name]['RoleId']}</RoleId></Role>")

    def _delete_role(self, req):
        p = self._p(req); name = p.get("RoleName", "")
        self.roles.pop(name, None); self.attached.pop(name, None)
        return self._xml("DeleteRole", "")

    def _get_role(self, req):
        p = self._p(req); name = p.get("RoleName", "")
        role = self.roles.get(name)
        if not role: return error_response("NoSuchEntity", "Role not found", 404)
        return self._xml("GetRole", f"    <Role><RoleName>{role['RoleName']}</RoleName><Arn>{role['Arn']}</Arn><CreateDate>{role['CreateDate']}</CreateDate></Role>")

    def _list_roles(self, req):
        xml = "    <Roles>\n"
        for r in self.roles.values():
            xml += f"      <member><RoleName>{r['RoleName']}</RoleName><Arn>{r['Arn']}</Arn></member>\n"
        xml += "    </Roles>"
        return self._xml("ListRoles", xml)

    def _create_policy(self, req):
        p = self._p(req); name = p.get("PolicyName", "")
        c = get_config(); arn = f"arn:aws:iam::{c.account_id}:policy/{name}"
        self.policies[arn] = {"PolicyName": name, "Arn": arn, "CreateDate": iso_timestamp()}
        return self._xml("CreatePolicy", f"    <Policy><PolicyName>{name}</PolicyName><Arn>{arn}</Arn></Policy>")

    def _get_policy(self, req):
        p = self._p(req); arn = p.get("PolicyArn", "")
        pol = self.policies.get(arn)
        if not pol: return error_response("NoSuchEntity", "Policy not found", 404)
        return self._xml("GetPolicy", f"    <Policy><PolicyName>{pol['PolicyName']}</PolicyName><Arn>{pol['Arn']}</Arn></Policy>")

    def _list_policies(self, req):
        xml = "    <Policies>\n"
        for pol in self.policies.values():
            xml += f"      <member><PolicyName>{pol['PolicyName']}</PolicyName><Arn>{pol['Arn']}</Arn></member>\n"
        xml += "    </Policies>"
        return self._xml("ListPolicies", xml)

    def _delete_policy(self, req):
        p = self._p(req); arn = p.get("PolicyArn", "")
        self.policies.pop(arn, None)
        return self._xml("DeletePolicy", "")

    def _attach_policy(self, req):
        p = self._p(req); role = p.get("RoleName", ""); policy_arn = p.get("PolicyArn", "")
        self.attached.setdefault(role, []).append(policy_arn)
        return self._xml("AttachRolePolicy", "")

    def _detach_policy(self, req):
        p = self._p(req); role = p.get("RoleName", ""); policy_arn = p.get("PolicyArn", "")
        if role in self.attached: self.attached[role] = [a for a in self.attached[role] if a != policy_arn]
        return self._xml("DetachRolePolicy", "")

    def _list_attached(self, req):
        p = self._p(req); role = p.get("RoleName", "")
        arns = self.attached.get(role, [])
        xml = "    <AttachedPolicies>\n"
        for arn in arns:
            pol = self.policies.get(arn, {})
            xml += f"      <member><PolicyName>{pol.get('PolicyName', '')}</PolicyName><PolicyArn>{arn}</PolicyArn></member>\n"
        xml += "    </AttachedPolicies>"
        return self._xml("ListAttachedRolePolicies", xml)

    def _create_user(self, req):
        p = self._p(req); name = p.get("UserName", "")
        c = get_config(); arn = f"arn:aws:iam::{c.account_id}:user/{name}"
        self.users[name] = {"UserName": name, "Arn": arn, "CreateDate": iso_timestamp(), "UserId": uuid.uuid4().hex[:20].upper()}
        return self._xml("CreateUser", f"    <User><UserName>{name}</UserName><Arn>{arn}</Arn></User>")

    def _delete_user(self, req):
        p = self._p(req); self.users.pop(p.get("UserName", ""), None)
        return self._xml("DeleteUser", "")

    def _get_user(self, req):
        p = self._p(req); u = self.users.get(p.get("UserName", ""))
        if not u: return error_response("NoSuchEntity", "User not found", 404)
        return self._xml("GetUser", f"    <User><UserName>{u['UserName']}</UserName><Arn>{u['Arn']}</Arn></User>")

    def _list_users(self, req):
        xml = "    <Users>\n"
        for u in self.users.values():
            xml += f"      <member><UserName>{u['UserName']}</UserName><Arn>{u['Arn']}</Arn></member>\n"
        xml += "    </Users>"
        return self._xml("ListUsers", xml)

    def reset(self):
        self.roles = {}
        self.policies = {}
        self.users = {}
        self.attached = {}
