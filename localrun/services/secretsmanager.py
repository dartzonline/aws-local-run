"""Secrets Manager service emulator."""
import json, logging, time, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import parse_json_body, json_error, new_request_id

logger = logging.getLogger("localrun.secretsmanager")

def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.1")

class SecretsManagerService:
    def __init__(self):
        self.secrets = {}  # name -> {arn, name, versions: {id: {value, stages}}, description, tags, created, updated}

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        body = parse_json_body(req)
        handlers = {
            "CreateSecret": self._create, "GetSecretValue": self._get_value,
            "PutSecretValue": self._put_value, "DeleteSecret": self._delete,
            "ListSecrets": self._list, "DescribeSecret": self._describe,
            "UpdateSecret": self._update, "TagResource": self._tag,
            "RestoreSecret": self._restore,
            "UntagResource": self._untag, "ListSecretVersionIds": self._list_versions,
        }
        h = handlers.get(action)
        if not h:
            return json_error("InvalidAction", f"Unknown action: {action}")
        return h(body)

    def _arn(self, name):
        c = get_config()
        return f"arn:aws:secretsmanager:{c.region}:{c.account_id}:secret:{name}-{uuid.uuid4().hex[:6]}"

    def _create(self, body):
        name = body.get("Name", "")
        if not name: return json_error("InvalidParameterException", "Name required")
        if name in self.secrets: return json_error("ResourceExistsException", f"Secret {name} already exists")
        arn = self._arn(name)
        vid = str(uuid.uuid4())
        secret_val = body.get("SecretString", "")
        self.secrets[name] = {
            "ARN": arn, "Name": name, "Description": body.get("Description", ""),
            "versions": {vid: {"value": secret_val, "stages": ["AWSCURRENT"]}},
            "Tags": body.get("Tags", []),
            "CreatedDate": time.time(), "LastChangedDate": time.time(),
            "deleted": False,
        }
        logger.info("Created secret: %s", name)
        return _resp({"ARN": arn, "Name": name, "VersionId": vid})

    def _get_value(self, body):
        name = body.get("SecretId", "")
        s = self._find(name)
        if not s or s.get("deleted"): return json_error("ResourceNotFoundException", "Secret not found", 404)
        vid = body.get("VersionId")
        stage = body.get("VersionStage", "AWSCURRENT")
        for v, info in s["versions"].items():
            if vid and v != vid: continue
            if stage in info["stages"]:
                return _resp({"ARN": s["ARN"], "Name": s["Name"], "VersionId": v,
                              "SecretString": info["value"], "VersionStages": info["stages"],
                              "CreatedDate": s["CreatedDate"]})
        return json_error("ResourceNotFoundException", "Version not found", 404)

    def _put_value(self, body):
        name = body.get("SecretId", "")
        s = self._find(name)
        if not s: return json_error("ResourceNotFoundException", "Secret not found", 404)
        vid = str(uuid.uuid4())
        for v in s["versions"].values():
            if "AWSCURRENT" in v["stages"]: v["stages"].remove("AWSCURRENT"); v["stages"].append("AWSPREVIOUS")
        s["versions"][vid] = {"value": body.get("SecretString", ""), "stages": ["AWSCURRENT"]}
        s["LastChangedDate"] = time.time()
        return _resp({"ARN": s["ARN"], "Name": s["Name"], "VersionId": vid})

    def _delete(self, body):
        name = body.get("SecretId", "")
        s = self._find(name)
        if not s: return json_error("ResourceNotFoundException", "Secret not found", 404)
        s["deleted"] = True
        return _resp({"ARN": s["ARN"], "Name": s["Name"], "DeletionDate": time.time()})

    def _restore(self, body):
        name = body.get("SecretId", "")
        s = self._find(name)
        if not s: return json_error("ResourceNotFoundException", "Secret not found", 404)
        s["deleted"] = False
        return _resp({"ARN": s["ARN"], "Name": s["Name"]})

    def _list(self, body):
        out = []
        for s in self.secrets.values():
            if s.get("deleted"): continue
            out.append({"ARN": s["ARN"], "Name": s["Name"], "Description": s.get("Description",""),
                         "LastChangedDate": s["LastChangedDate"]})
        return _resp({"SecretList": out})

    def _describe(self, body):
        name = body.get("SecretId", "")
        s = self._find(name)
        if not s: return json_error("ResourceNotFoundException", "Secret not found", 404)
        return _resp({"ARN": s["ARN"], "Name": s["Name"], "Description": s.get("Description",""),
                       "Tags": s.get("Tags",[]), "CreatedDate": s["CreatedDate"],
                       "LastChangedDate": s["LastChangedDate"]})

    def _update(self, body):
        name = body.get("SecretId", "")
        s = self._find(name)
        if not s: return json_error("ResourceNotFoundException", "Secret not found", 404)
        if "Description" in body: s["Description"] = body["Description"]
        if "SecretString" in body:
            vid = str(uuid.uuid4())
            for v in s["versions"].values():
                if "AWSCURRENT" in v["stages"]: v["stages"].remove("AWSCURRENT")
            s["versions"][vid] = {"value": body["SecretString"], "stages": ["AWSCURRENT"]}
        s["LastChangedDate"] = time.time()
        return _resp({"ARN": s["ARN"], "Name": s["Name"]})

    def _tag(self, body):
        sid = body.get("SecretId", "")
        s = self._find(sid)
        if not s: return json_error("ResourceNotFoundException", "Secret not found", 404)
        s.setdefault("Tags", []).extend(body.get("Tags", []))
        return _resp({})

    def _untag(self, body):
        sid = body.get("SecretId", "")
        s = self._find(sid)
        if not s: return json_error("ResourceNotFoundException", "Secret not found", 404)
        keys_to_remove = set(body.get("TagKeys", []))
        s["Tags"] = [t for t in s.get("Tags", []) if t.get("Key") not in keys_to_remove]
        return _resp({})

    def _list_versions(self, body):
        sid = body.get("SecretId", "")
        s = self._find(sid)
        if not s or s.get("deleted"): return json_error("ResourceNotFoundException", "Secret not found", 404)
        versions = []
        for vid, info in s["versions"].items():
            versions.append({"VersionId": vid, "VersionStages": info["stages"]})
        return _resp({"ARN": s["ARN"], "Name": s["Name"], "Versions": versions})

    def _find(self, name_or_arn):
        if name_or_arn in self.secrets: return self.secrets[name_or_arn]
        for s in self.secrets.values():
            if s["ARN"] == name_or_arn: return s
        return None

    def reset(self):
        self.secrets = {}
