"""SES (Simple Email Service) emulator."""
import json
import logging
import time
import uuid
from urllib.parse import parse_qs
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import new_request_id

logger = logging.getLogger("localrun.ses")


def _xml(action, content):
    body = (
        '<?xml version="1.0"?>\n'
        '<' + action + 'Response xmlns="http://ses.amazonaws.com/doc/2010-12-01/">\n'
        '  <' + action + 'Result>\n' + content + '\n'
        '  </' + action + 'Result>\n'
        '  <ResponseMetadata><RequestId>' + new_request_id() + '</RequestId></ResponseMetadata>\n'
        '</' + action + 'Response>'
    )
    return Response(body, 200, content_type="application/xml")


class SESService:
    def __init__(self):
        self.verified_emails = []
        self.verified_domains = []
        # inbox stores all sent emails for inspection
        self.inbox = []
        self.send_quota = {"Max24HourSend": 200, "SentLast24Hours": 0, "MaxSendRate": 10}

    def handle(self, req, path):
        action = req.args.get("Action") or req.form.get("Action", "")
        if not action:
            params = parse_qs(req.get_data(as_text=True))
            action = params.get("Action", [""])[0]
        actions = {
            "SendEmail": self._send_email,
            "SendRawEmail": self._send_raw_email,
            "VerifyEmailIdentity": self._verify_email,
            "VerifyDomainIdentity": self._verify_domain,
            "ListIdentities": self._list_identities,
            "GetSendQuota": self._get_send_quota,
            "GetSendStatistics": self._get_send_stats,
            "DeleteIdentity": self._delete_identity,
        }
        handler = actions.get(action)
        if not handler:
            return Response(
                json.dumps({"error": "InvalidAction", "message": action}),
                400, content_type="application/json",
            )
        return handler(req)

    def _p(self, req):
        params = dict(req.args)
        if req.content_type and "form" in req.content_type:
            params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items():
                params[k] = v[0] if len(v) == 1 else v
        return params

    def _send_email(self, req):
        p = self._p(req)
        source = p.get("Source", "")
        to_addr = p.get("Destination.ToAddresses.member.1", "")
        subject = p.get("Message.Subject.Data", "")
        body_text = p.get("Message.Body.Text.Data", "")
        body_html = p.get("Message.Body.Html.Data", "")
        msg_id = str(uuid.uuid4())
        entry = {
            "MessageId": msg_id, "Source": source, "To": to_addr,
            "Subject": subject, "BodyText": body_text, "BodyHtml": body_html,
            "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.inbox.append(entry)
        self.send_quota["SentLast24Hours"] += 1
        logger.info("SES SendEmail from=%s to=%s subj=%s", source, to_addr, subject)
        return _xml("SendEmail", "    <MessageId>" + msg_id + "</MessageId>")

    def _send_raw_email(self, req):
        p = self._p(req)
        source = p.get("Source", "")
        raw_data = p.get("RawMessage.Data", "")
        msg_id = str(uuid.uuid4())
        entry = {
            "MessageId": msg_id, "Source": source, "RawData": raw_data,
            "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.inbox.append(entry)
        self.send_quota["SentLast24Hours"] += 1
        logger.info("SES SendRawEmail from=%s", source)
        return _xml("SendRawEmail", "    <MessageId>" + msg_id + "</MessageId>")

    def _verify_email(self, req):
        p = self._p(req)
        email = p.get("EmailAddress", "")
        if email and email not in self.verified_emails:
            self.verified_emails.append(email)
        logger.info("SES verified email: %s", email)
        return _xml("VerifyEmailIdentity", "")

    def _verify_domain(self, req):
        p = self._p(req)
        domain = p.get("Domain", "")
        if domain and domain not in self.verified_domains:
            self.verified_domains.append(domain)
        token = uuid.uuid4().hex[:16]
        logger.info("SES verified domain: %s", domain)
        return _xml("VerifyDomainIdentity", "    <VerificationToken>" + token + "</VerificationToken>")

    def _list_identities(self, req):
        p = self._p(req)
        id_type = p.get("IdentityType", "")
        identities = []
        if id_type != "Domain":
            identities += self.verified_emails
        if id_type != "EmailAddress":
            identities += self.verified_domains
        xml = "    <Identities>\n"
        for ident in identities:
            xml += "      <member>" + ident + "</member>\n"
        xml += "    </Identities>"
        return _xml("ListIdentities", xml)

    def _delete_identity(self, req):
        p = self._p(req)
        identity = p.get("Identity", "")
        if identity in self.verified_emails:
            self.verified_emails.remove(identity)
        if identity in self.verified_domains:
            self.verified_domains.remove(identity)
        return _xml("DeleteIdentity", "")

    def _get_send_quota(self, req):
        xml = (
            "    <Max24HourSend>" + str(self.send_quota["Max24HourSend"]) + "</Max24HourSend>\n"
            "    <SentLast24Hours>" + str(self.send_quota["SentLast24Hours"]) + "</SentLast24Hours>\n"
            "    <MaxSendRate>" + str(self.send_quota["MaxSendRate"]) + "</MaxSendRate>"
        )
        return _xml("GetSendQuota", xml)

    def _get_send_stats(self, req):
        xml = "    <SendDataPoints>\n"
        if self.inbox:
            xml += (
                "      <member>\n"
                "        <DeliveryAttempts>" + str(len(self.inbox)) + "</DeliveryAttempts>\n"
                "        <Bounces>0</Bounces>\n"
                "        <Complaints>0</Complaints>\n"
                "        <Rejects>0</Rejects>\n"
                "      </member>\n"
            )
        xml += "    </SendDataPoints>"
        return _xml("GetSendStatistics", xml)

    def reset(self):
        self.verified_emails = []
        self.verified_domains = []
        self.inbox = []
        self.send_quota = {"Max24HourSend": 200, "SentLast24Hours": 0, "MaxSendRate": 10}
