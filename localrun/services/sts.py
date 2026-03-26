"""STS service emulator."""
import json, logging, time, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, new_request_id

logger = logging.getLogger("localrun.sts")

class STSService:
    def handle(self, req: Request, path: str) -> Response:
        action = req.args.get("Action") or req.form.get("Action", "")
        handlers = {
            "GetCallerIdentity": self._get_caller_identity,
            "AssumeRole": self._assume_role,
            "GetSessionToken": self._get_session_token,
        }
        handler = handlers.get(action)
        if not handler:
            return error_response("InvalidAction", f"Action {action} not valid", 400)
        return handler(req)

    def _get_caller_identity(self, req):
        c = get_config()
        xml = f"""<?xml version="1.0"?>
<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>arn:aws:iam::{c.account_id}:root</Arn>
    <UserId>{c.account_id}</UserId>
    <Account>{c.account_id}</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>
</GetCallerIdentityResponse>"""
        return Response(xml, 200, content_type="application/xml")

    def _assume_role(self, req):
        c = get_config()
        role_arn = req.form.get("RoleArn", req.args.get("RoleArn", ""))
        session = req.form.get("RoleSessionName", req.args.get("RoleSessionName", "session"))
        now = int(time.time())
        xml = f"""<?xml version="1.0"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <AssumedRoleUser>
      <Arn>{role_arn}/{session}</Arn>
      <AssumedRoleId>AROA{uuid.uuid4().hex[:16].upper()}:{session}</AssumedRoleId>
    </AssumedRoleUser>
    <Credentials>
      <AccessKeyId>ASIA{uuid.uuid4().hex[:16].upper()}</AccessKeyId>
      <SecretAccessKey>{uuid.uuid4().hex}</SecretAccessKey>
      <SessionToken>{uuid.uuid4().hex}{uuid.uuid4().hex}</SessionToken>
      <Expiration>{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 3600))}</Expiration>
    </Credentials>
  </AssumeRoleResult>
  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>
</AssumeRoleResponse>"""
        return Response(xml, 200, content_type="application/xml")

    def _get_session_token(self, req):
        c = get_config()
        now = int(time.time())
        xml = f"""<?xml version="1.0"?>
<GetSessionTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetSessionTokenResult>
    <Credentials>
      <AccessKeyId>ASIA{uuid.uuid4().hex[:16].upper()}</AccessKeyId>
      <SecretAccessKey>{uuid.uuid4().hex}</SecretAccessKey>
      <SessionToken>{uuid.uuid4().hex}{uuid.uuid4().hex}</SessionToken>
      <Expiration>{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 3600))}</Expiration>
    </Credentials>
  </GetSessionTokenResult>
  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>
</GetSessionTokenResponse>"""
        return Response(xml, 200, content_type="application/xml")

    def reset(self):
        # STS is stateless — nothing to clear
        pass
