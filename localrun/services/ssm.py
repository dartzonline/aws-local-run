"""SSM Parameter Store service emulator."""
import base64, json, logging, time
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import parse_json_body, json_error, new_request_id

logger = logging.getLogger("localrun.ssm")

def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.1")

class SSMService:
    def __init__(self):
        self.parameters = {}  # name -> {Name, Type, Value, Version, LastModifiedDate, ARN, ...}

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        body = parse_json_body(req)
        handlers = {
            "PutParameter": self._put, "GetParameter": self._get,
            "GetParameters": self._get_many, "DeleteParameter": self._delete,
            "DescribeParameters": self._describe, "GetParametersByPath": self._get_by_path,
            "AddTagsToResource": self._add_tags, "RemoveTagsFromResource": self._remove_tags,
            "ListTagsForResource": self._list_tags,
        }
        h = handlers.get(action)
        if not h:
            return json_error("InvalidAction", f"Unknown action: {action}")
        return h(body)

    def _arn(self, name):
        c = get_config()
        return f"arn:aws:ssm:{c.region}:{c.account_id}:parameter{name}"

    def _put(self, body):
        name = body.get("Name", "")
        if not name: return json_error("ValidationException", "Name required")
        overwrite = body.get("Overwrite", False)
        exists = name in self.parameters
        if exists and not overwrite:
            return json_error("ParameterAlreadyExists", f"Parameter {name} already exists")
        version = self.parameters[name]["Version"] + 1 if exists else 1
        param_type = body.get("Type", "String")
        value = body.get("Value", "")
        # SecureString: encode the value
        if param_type == "SecureString":
            value = base64.b64encode(value.encode()).decode()
        self.parameters[name] = {
            "Name": name, "Type": param_type,
            "Value": value, "Version": version,
            "LastModifiedDate": time.time(), "ARN": self._arn(name),
            "Description": body.get("Description", ""),
            "Tags": body.get("Tags", []),
        }
        logger.info("Put parameter: %s (v%d, type=%s)", name, version, param_type)
        return _resp({"Version": version})

    def _get(self, body):
        name = body.get("Name", "")
        p = self.parameters.get(name)
        if not p: return json_error("ParameterNotFound", f"Parameter {name} not found", 400)
        value = p["Value"]
        # Decrypt SecureString if requested
        with_decrypt = body.get("WithDecryption", False)
        if p["Type"] == "SecureString" and with_decrypt:
            try:
                value = base64.b64decode(value).decode()
            except Exception:
                pass
        return _resp({"Parameter": {"Name": p["Name"], "Type": p["Type"], "Value": value,
                                     "Version": p["Version"], "LastModifiedDate": p["LastModifiedDate"],
                                     "ARN": p["ARN"]}})

    def _get_many(self, body):
        names = body.get("Names", [])
        found, missing = [], []
        for n in names:
            p = self.parameters.get(n)
            if p:
                found.append({"Name": p["Name"], "Type": p["Type"], "Value": p["Value"],
                               "Version": p["Version"], "ARN": p["ARN"]})
            else:
                missing.append(n)
        return _resp({"Parameters": found, "InvalidParameters": missing})

    def _delete(self, body):
        name = body.get("Name", "")
        if name not in self.parameters:
            return json_error("ParameterNotFound", f"Parameter {name} not found", 400)
        del self.parameters[name]
        return _resp({})

    def _describe(self, body):
        params = []
        for p in self.parameters.values():
            params.append({"Name": p["Name"], "Type": p["Type"], "Version": p["Version"],
                           "LastModifiedDate": p["LastModifiedDate"], "Description": p.get("Description","")})
        return _resp({"Parameters": params})

    def _get_by_path(self, body):
        prefix = body.get("Path", "/")
        recursive = body.get("Recursive", False)
        results = []
        for p in self.parameters.values():
            name = p["Name"]
            if not name.startswith(prefix): continue
            rest = name[len(prefix):]
            if not recursive and "/" in rest.strip("/"): continue
            results.append({"Name": p["Name"], "Type": p["Type"], "Value": p["Value"],
                             "Version": p["Version"], "ARN": p["ARN"]})
        return _resp({"Parameters": results})

    def _add_tags(self, body):
        name = body.get("ResourceId", "")
        p = self.parameters.get(name)
        if not p:
            return json_error("InvalidResourceId", f"Parameter {name} not found", 400)
        existing = {t["Key"]: t["Value"] for t in p.get("Tags", [])}
        for tag in body.get("Tags", []):
            existing[tag["Key"]] = tag["Value"]
        p["Tags"] = [{"Key": k, "Value": v} for k, v in existing.items()]
        return _resp({})

    def _remove_tags(self, body):
        name = body.get("ResourceId", "")
        p = self.parameters.get(name)
        if not p:
            return json_error("InvalidResourceId", f"Parameter {name} not found", 400)
        keys_to_remove = set(body.get("TagKeys", []))
        p["Tags"] = [t for t in p.get("Tags", []) if t["Key"] not in keys_to_remove]
        return _resp({})

    def _list_tags(self, body):
        name = body.get("ResourceId", "")
        p = self.parameters.get(name)
        if not p:
            return json_error("InvalidResourceId", f"Parameter {name} not found", 400)
        return _resp({"TagList": p.get("Tags", [])})

    def reset(self):
        self.parameters = {}
