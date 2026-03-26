"""Shared utilities."""
import hashlib, json, time, uuid
from flask import Response
import xmltodict

def generate_arn(service, region, account_id, resource_type, resource_name):
    return f"arn:aws:{service}:{region}:{account_id}:{resource_type}/{resource_name}"

def xml_response(root_tag, content_dict, status=200):
    body = xmltodict.unparse({root_tag: content_dict}, pretty=True)
    return Response(body, status, content_type="application/xml")

def json_response(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.1")

def error_response(code, message, status=400, xmlns=""):
    ns = f' xmlns="{xmlns}"' if xmlns else ""
    body = f'<?xml version="1.0" encoding="UTF-8"?>\n<ErrorResponse{ns}>\n  <Error>\n    <Code>{code}</Code>\n    <Message>{message}</Message>\n  </Error>\n  <RequestId>{new_request_id()}</RequestId>\n</ErrorResponse>'
    return Response(body, status, content_type="application/xml")

def json_error(code, message, status=400):
    return Response(json.dumps({"__type": code, "message": message}), status, content_type="application/x-amz-json-1.1")

def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def etag(data: bytes) -> str:
    return f'"{md5_hex(data)}"'

def iso_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

def epoch_ms() -> int:
    return int(time.time() * 1000)

def new_request_id() -> str:
    return str(uuid.uuid4())

def new_message_id() -> str:
    return str(uuid.uuid4())

def parse_json_body(req):
    try:
        return json.loads(req.get_data(as_text=True) or "{}")
    except Exception:
        return {}
