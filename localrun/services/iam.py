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
        self.inline_policies = {}   # role_name -> {policy_name: policy_doc}
        self.instance_profiles = {}  # profile_name -> {arn, roles: []}
        self.groups = {}
        self.access_keys = {}  # user_name -> [{AccessKeyId, SecretAccessKey, Status}]

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
            "PutRolePolicy": self._put_role_policy, "GetRolePolicy": self._get_role_policy,
            "ListRolePolicies": self._list_role_policies, "DeleteRolePolicy": self._delete_role_policy,
            "CreateInstanceProfile": self._create_instance_profile,
            "DeleteInstanceProfile": self._delete_instance_profile,
            "GetInstanceProfile": self._get_instance_profile,
            "ListInstanceProfiles": self._list_instance_profiles,
            "AddRoleToInstanceProfile": self._add_role_to_profile,
            "RemoveRoleFromInstanceProfile": self._remove_role_from_profile,
            "CreateGroup": self._create_group, "DeleteGroup": self._delete_group,
            "GetGroup": self._get_group, "ListGroups": self._list_groups,
            "AddUserToGroup": self._add_user_to_group,
            "CreateAccessKey": self._create_access_key,
            "DeleteAccessKey": self._delete_access_key,
            "ListAccessKeys": self._list_access_keys,
            "UpdateAccessKey": self._update_access_key,
            "PassRole": lambda req: self._xml("PassRole", ""),
            "TagRole": lambda req: self._xml("TagRole", ""),
            "UntagRole": lambda req: self._xml("UntagRole", ""),
            "ListRoleTags": lambda req: self._xml("ListRoleTags", "    <Tags></Tags>"),
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

    def _put_role_policy(self, req):
        p = self._p(req)
        role = p.get("RoleName", ""); policy_name = p.get("PolicyName", "")
        doc = p.get("PolicyDocument", "")
        self.inline_policies.setdefault(role, {})[policy_name] = doc
        return self._xml("PutRolePolicy", "")

    def _get_role_policy(self, req):
        p = self._p(req)
        role = p.get("RoleName", ""); policy_name = p.get("PolicyName", "")
        doc = self.inline_policies.get(role, {}).get(policy_name)
        if doc is None:
            return error_response("NoSuchEntity", "Policy not found", 404)
        return self._xml("GetRolePolicy", f"    <RoleName>{role}</RoleName>\n    <PolicyName>{policy_name}</PolicyName>\n    <PolicyDocument>{doc}</PolicyDocument>")

    def _list_role_policies(self, req):
        p = self._p(req); role = p.get("RoleName", "")
        names = list(self.inline_policies.get(role, {}).keys())
        xml = "    <PolicyNames>\n"
        for n in names:
            xml += f"      <member>{n}</member>\n"
        xml += "    </PolicyNames>"
        return self._xml("ListRolePolicies", xml)

    def _delete_role_policy(self, req):
        p = self._p(req)
        role = p.get("RoleName", ""); policy_name = p.get("PolicyName", "")
        self.inline_policies.get(role, {}).pop(policy_name, None)
        return self._xml("DeleteRolePolicy", "")

    def _create_instance_profile(self, req):
        p = self._p(req); name = p.get("InstanceProfileName", "")
        c = get_config(); arn = f"arn:aws:iam::{c.account_id}:instance-profile/{name}"
        self.instance_profiles[name] = {"InstanceProfileName": name, "Arn": arn, "Path": p.get("Path", "/"), "Roles": [], "InstanceProfileId": uuid.uuid4().hex[:20].upper(), "CreateDate": iso_timestamp()}
        return self._xml("CreateInstanceProfile", f"    <InstanceProfile><InstanceProfileName>{name}</InstanceProfileName><Arn>{arn}</Arn></InstanceProfile>")

    def _delete_instance_profile(self, req):
        p = self._p(req); self.instance_profiles.pop(p.get("InstanceProfileName", ""), None)
        return self._xml("DeleteInstanceProfile", "")

    def _get_instance_profile(self, req):
        p = self._p(req); name = p.get("InstanceProfileName", "")
        prof = self.instance_profiles.get(name)
        if not prof: return error_response("NoSuchEntity", "Instance profile not found", 404)
        return self._xml("GetInstanceProfile", f"    <InstanceProfile><InstanceProfileName>{name}</InstanceProfileName><Arn>{prof['Arn']}</Arn></InstanceProfile>")

    def _list_instance_profiles(self, req):
        xml = "    <InstanceProfiles>\n"
        for p in self.instance_profiles.values():
            xml += f"      <member><InstanceProfileName>{p['InstanceProfileName']}</InstanceProfileName><Arn>{p['Arn']}</Arn></member>\n"
        xml += "    </InstanceProfiles>"
        return self._xml("ListInstanceProfiles", xml)

    def _add_role_to_profile(self, req):
        p = self._p(req); profile = p.get("InstanceProfileName", ""); role = p.get("RoleName", "")
        if profile in self.instance_profiles:
            self.instance_profiles[profile]["Roles"].append(role)
        return self._xml("AddRoleToInstanceProfile", "")

    def _remove_role_from_profile(self, req):
        p = self._p(req); profile = p.get("InstanceProfileName", ""); role = p.get("RoleName", "")
        if profile in self.instance_profiles:
            self.instance_profiles[profile]["Roles"] = [r for r in self.instance_profiles[profile]["Roles"] if r != role]
        return self._xml("RemoveRoleFromInstanceProfile", "")

    def _create_group(self, req):
        p = self._p(req); name = p.get("GroupName", "")
        c = get_config(); arn = f"arn:aws:iam::{c.account_id}:group/{name}"
        self.groups[name] = {"GroupName": name, "Arn": arn, "GroupId": uuid.uuid4().hex[:20].upper(), "Path": p.get("Path", "/"), "members": []}
        return self._xml("CreateGroup", f"    <Group><GroupName>{name}</GroupName><Arn>{arn}</Arn></Group>")

    def _delete_group(self, req):
        p = self._p(req); self.groups.pop(p.get("GroupName", ""), None)
        return self._xml("DeleteGroup", "")

    def _get_group(self, req):
        p = self._p(req); name = p.get("GroupName", "")
        g = self.groups.get(name)
        if not g: return error_response("NoSuchEntity", "Group not found", 404)
        return self._xml("GetGroup", f"    <Group><GroupName>{name}</GroupName><Arn>{g['Arn']}</Arn></Group>    <Users></Users>")

    def _list_groups(self, req):
        xml = "    <Groups>\n"
        for g in self.groups.values():
            xml += f"      <member><GroupName>{g['GroupName']}</GroupName><Arn>{g['Arn']}</Arn></member>\n"
        xml += "    </Groups>"
        return self._xml("ListGroups", xml)

    def _add_user_to_group(self, req):
        p = self._p(req); group = p.get("GroupName", ""); user = p.get("UserName", "")
        if group in self.groups: self.groups[group]["members"].append(user)
        return self._xml("AddUserToGroup", "")

    def _create_access_key(self, req):
        p = self._p(req); user = p.get("UserName", "test")
        key_id = "AKIA" + uuid.uuid4().hex[:16].upper()
        secret = uuid.uuid4().hex + uuid.uuid4().hex
        entry = {"AccessKeyId": key_id, "SecretAccessKey": secret, "Status": "Active", "UserName": user, "CreateDate": iso_timestamp()}
        self.access_keys.setdefault(user, []).append(entry)
        return self._xml("CreateAccessKey", f"    <AccessKey><AccessKeyId>{key_id}</AccessKeyId><SecretAccessKey>{secret}</SecretAccessKey><Status>Active</Status><UserName>{user}</UserName></AccessKey>")

    def _delete_access_key(self, req):
        p = self._p(req); user = p.get("UserName", ""); key_id = p.get("AccessKeyId", "")
        keys = self.access_keys.get(user, [])
        self.access_keys[user] = [k for k in keys if k["AccessKeyId"] != key_id]
        return self._xml("DeleteAccessKey", "")

    def _list_access_keys(self, req):
        p = self._p(req); user = p.get("UserName", "")
        keys = self.access_keys.get(user, [])
        xml = "    <AccessKeyMetadata>\n"
        for k in keys:
            xml += f"      <member><AccessKeyId>{k['AccessKeyId']}</AccessKeyId><Status>{k['Status']}</Status><UserName>{k['UserName']}</UserName></member>\n"
        xml += "    </AccessKeyMetadata>"
        return self._xml("ListAccessKeys", xml)

    def _update_access_key(self, req):
        p = self._p(req); user = p.get("UserName", ""); key_id = p.get("AccessKeyId", ""); status = p.get("Status", "Active")
        for k in self.access_keys.get(user, []):
            if k["AccessKeyId"] == key_id: k["Status"] = status
        return self._xml("UpdateAccessKey", "")

    def reset(self):
        self.roles = {}
        self.policies = {}
        self.users = {}
        self.attached = {}
        self.inline_policies = {}
        self.instance_profiles = {}
        self.groups = {}
        self.access_keys = {}
