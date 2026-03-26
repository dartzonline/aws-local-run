"""ACM (Certificate Manager) service emulator."""
import json, logging, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import json_error, iso_timestamp, parse_json_body

logger = logging.getLogger("localrun.acm")

_FAKE_CERT = (
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAlocalrunFakeCert==\n"
    "-----END CERTIFICATE-----"
)
_FAKE_CHAIN = (
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAg8AMIIBCgKCAQEAlocalrunFakeChain==\n"
    "-----END CERTIFICATE-----"
)


def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.1")


class ACMService:
    def __init__(self):
        self.certs = {}  # arn -> cert dict

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        body = parse_json_body(req)
        handlers = {
            "RequestCertificate": self._request_cert,
            "DescribeCertificate": self._describe_cert,
            "ListCertificates": self._list_certs,
            "DeleteCertificate": self._delete_cert,
            "GetCertificate": self._get_cert,
            "AddTagsToCertificate": self._add_tags,
            "RemoveTagsFromCertificate": self._remove_tags,
            "ListTagsForCertificate": self._list_tags,
        }
        h = handlers.get(action)
        if not h:
            return json_error("InvalidAction", f"Unknown action: {action}")
        return h(body)

    def _arn(self, domain):
        c = get_config()
        uid = uuid.uuid4().hex[:8]
        return f"arn:aws:acm:{c.region}:{c.account_id}:certificate/{uid}"

    def _request_cert(self, body):
        domain = body.get("DomainName", "")
        if not domain:
            return json_error("InvalidParameterException", "DomainName required")
        extra = body.get("SubjectAlternativeNames", [])
        arn = self._arn(domain)
        ts = iso_timestamp()
        self.certs[arn] = {
            "CertificateArn": arn,
            "DomainName": domain,
            "SubjectAlternativeNames": [domain] + extra,
            "Status": "ISSUED",
            "Type": "AMAZON_ISSUED",
            "CreatedAt": ts,
            "IssuedAt": ts,
            "Tags": [],
        }
        logger.info("ACM RequestCertificate domain=%s arn=%s", domain, arn)
        return _resp({"CertificateArn": arn})

    def _describe_cert(self, body):
        arn = body.get("CertificateArn", "")
        cert = self.certs.get(arn)
        if not cert:
            return json_error("ResourceNotFoundException", "Certificate not found", 404)
        return _resp({"Certificate": cert})

    def _list_certs(self, body):
        out = []
        for cert in self.certs.values():
            out.append({"CertificateArn": cert["CertificateArn"], "DomainName": cert["DomainName"]})
        return _resp({"CertificateSummaryList": out})

    def _delete_cert(self, body):
        arn = body.get("CertificateArn", "")
        if arn not in self.certs:
            return json_error("ResourceNotFoundException", "Certificate not found", 404)
        self.certs.pop(arn, None)
        logger.info("ACM DeleteCertificate arn=%s", arn)
        return _resp({})

    def _get_cert(self, body):
        arn = body.get("CertificateArn", "")
        if arn not in self.certs:
            return json_error("ResourceNotFoundException", "Certificate not found", 404)
        return _resp({"Certificate": _FAKE_CERT, "CertificateChain": _FAKE_CHAIN})

    def _add_tags(self, body):
        arn = body.get("CertificateArn", "")
        cert = self.certs.get(arn)
        if not cert:
            return json_error("ResourceNotFoundException", "Certificate not found", 404)
        for tag in body.get("Tags", []):
            cert["Tags"].append(tag)
        return _resp({})

    def _remove_tags(self, body):
        arn = body.get("CertificateArn", "")
        cert = self.certs.get(arn)
        if not cert:
            return json_error("ResourceNotFoundException", "Certificate not found", 404)
        remove = {t["Key"] for t in body.get("Tags", [])}
        kept = []
        for t in cert["Tags"]:
            if t.get("Key") not in remove:
                kept.append(t)
        cert["Tags"] = kept
        return _resp({})

    def _list_tags(self, body):
        arn = body.get("CertificateArn", "")
        cert = self.certs.get(arn)
        if not cert:
            return json_error("ResourceNotFoundException", "Certificate not found", 404)
        return _resp({"Tags": cert["Tags"]})

    def reset(self):
        self.certs = {}
