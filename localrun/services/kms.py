"""KMS service emulator (TrentService JSON protocol)."""
import base64, json, logging, os, time, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import parse_json_body, json_error, new_request_id

logger = logging.getLogger("localrun.kms")

_DEFAULT_POLICY = '{"Version":"2012-10-17","Statement":[]}'

def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.1")

def _b64enc(data):
    return base64.b64encode(data).decode()

def _b64dec(s):
    return base64.b64decode(s)

def _xor(data, key_bytes):
    if not key_bytes:
        return data
    out = []
    n = len(key_bytes)
    for i, b in enumerate(data):
        out.append(b ^ key_bytes[i % n])
    return bytes(out)

def _pack_ct(key_id, ct_bytes):
    """Pack key_id + ciphertext so decrypt can find the right key."""
    kid_bytes = key_id.encode("utf-8")
    prefix = len(kid_bytes).to_bytes(2, "big") + kid_bytes
    return prefix + ct_bytes

def _unpack_ct(blob):
    """Returns (key_id, ciphertext) from a packed blob."""
    kid_len = int.from_bytes(blob[:2], "big")
    key_id = blob[2:2 + kid_len].decode("utf-8")
    ct = blob[2 + kid_len:]
    return key_id, ct

class KMSService:
    def __init__(self):
        self.keys = {}     # key_id -> key dict
        self.aliases = {}  # alias_name -> key_id

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        body = parse_json_body(req)
        handlers = {
            "CreateKey": self._create_key,
            "DescribeKey": self._describe_key,
            "ListKeys": self._list_keys,
            "ListAliases": self._list_aliases,
            "CreateAlias": self._create_alias,
            "DeleteAlias": self._delete_alias,
            "EnableKey": self._enable_key,
            "DisableKey": self._disable_key,
            "ScheduleKeyDeletion": self._schedule_deletion,
            "CancelKeyDeletion": self._cancel_deletion,
            "GenerateDataKey": self._gen_data_key,
            "GenerateDataKeyWithoutPlaintext": self._gen_data_key_no_pt,
            "Encrypt": self._encrypt,
            "Decrypt": self._decrypt,
            "GenerateRandom": self._gen_random,
            "GetKeyPolicy": self._get_policy,
            "PutKeyPolicy": self._put_policy,
            "TagResource": self._tag_resource,
            "UntagResource": self._untag_resource,
            "ListResourceTags": self._list_tags,
        }
        h = handlers.get(action)
        if not h:
            return json_error("InvalidAction", "Unknown action: " + action)
        return h(body)

    def _arn(self, key_id):
        c = get_config()
        return "arn:aws:kms:" + c.region + ":" + c.account_id + ":key/" + key_id

    def _alias_arn(self, alias_name):
        c = get_config()
        return "arn:aws:kms:" + c.region + ":" + c.account_id + ":" + alias_name

    def _resolve(self, key_id):
        if key_id.startswith("alias/"):
            key_id = self.aliases.get(key_id, "")
        if key_id.startswith("arn:"):
            key_id = key_id.split("/")[-1]
        return self.keys.get(key_id)

    def _create_key(self, body):
        kid = str(uuid.uuid4())
        arn = self._arn(kid)
        k = {
            "KeyId": kid, "Arn": arn,
            "Description": body.get("Description", ""),
            "KeyUsage": body.get("KeyUsage", "ENCRYPT_DECRYPT"),
            "KeySpec": body.get("KeySpec", "SYMMETRIC_DEFAULT"),
            "Enabled": True, "KeyState": "Enabled",
            "CreationDate": time.time(),
            "Policy": _DEFAULT_POLICY, "Tags": [],
        }
        self.keys[kid] = k
        logger.info("Created KMS key: %s", kid)
        return _resp({"KeyMetadata": _meta(k)})

    def _describe_key(self, body):
        kid = body.get("KeyId", "")
        k = self._resolve(kid)
        if not k:
            return json_error("NotFoundException", "Key not found: " + kid, 404)
        return _resp({"KeyMetadata": _meta(k)})

    def _list_keys(self, body):
        items = []
        for k in self.keys.values():
            items.append({"KeyId": k["KeyId"], "KeyArn": k["Arn"]})
        return _resp({"Keys": items, "Truncated": False})

    def _list_aliases(self, body):
        items = []
        for name, kid in self.aliases.items():
            k = self.keys.get(kid, {})
            items.append({"AliasName": name, "TargetKeyId": kid,
                          "AliasArn": self._alias_arn(name)})
        return _resp({"Aliases": items, "Truncated": False})

    def _create_alias(self, body):
        name = body.get("AliasName", "")
        kid = body.get("TargetKeyId", "")
        if not name.startswith("alias/"):
            return json_error("InvalidAliasNameException", "Alias must start with alias/")
        self.aliases[name] = kid
        return _resp({})

    def _delete_alias(self, body):
        name = body.get("AliasName", "")
        self.aliases.pop(name, None)
        return _resp({})

    def _enable_key(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        k["Enabled"] = True
        k["KeyState"] = "Enabled"
        return _resp({})

    def _disable_key(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        k["Enabled"] = False
        k["KeyState"] = "Disabled"
        return _resp({})

    def _schedule_deletion(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        days = int(body.get("PendingWindowInDays", 30))
        del_date = time.time() + days * 86400
        k["KeyState"] = "PendingDeletion"
        k["Enabled"] = False
        k["DeletionDate"] = del_date
        return _resp({"KeyId": k["KeyId"], "DeletionDate": del_date, "PendingWindowInDays": days})

    def _cancel_deletion(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        # AWS leaves the key Disabled after cancellation; we re-enable for simplicity
        k["KeyState"] = "Enabled"
        k["Enabled"] = True
        k.pop("DeletionDate", None)
        return _resp({"KeyId": k["KeyId"], "KeyState": k["KeyState"]})

    def _gen_data_key(self, body):
        kid = body.get("KeyId", "")
        k = self._resolve(kid)
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        pt = os.urandom(32)
        key_mat = k["KeyId"].encode()
        ct = _pack_ct(k["KeyId"], _xor(pt, key_mat))
        return _resp({"KeyId": k["KeyId"], "Plaintext": _b64enc(pt), "CiphertextBlob": _b64enc(ct)})

    def _gen_data_key_no_pt(self, body):
        kid = body.get("KeyId", "")
        k = self._resolve(kid)
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        pt = os.urandom(32)
        key_mat = k["KeyId"].encode()
        ct = _pack_ct(k["KeyId"], _xor(pt, key_mat))
        return _resp({"KeyId": k["KeyId"], "CiphertextBlob": _b64enc(ct)})

    def _encrypt(self, body):
        kid = body.get("KeyId", "")
        k = self._resolve(kid)
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        pt = _b64dec(body.get("Plaintext", ""))
        key_mat = k["KeyId"].encode()
        ct = _pack_ct(k["KeyId"], _xor(pt, key_mat))
        return _resp({"KeyId": k["KeyId"], "CiphertextBlob": _b64enc(ct),
                      "EncryptionAlgorithm": "SYMMETRIC_DEFAULT"})

    def _decrypt(self, body):
        blob = _b64dec(body.get("CiphertextBlob", ""))
        try:
            key_id, ct = _unpack_ct(blob)
        except Exception:
            # Fallback: no embedded key_id, use provided KeyId or first key
            key_id = body.get("KeyId", "")
            ct = blob
        k = self._resolve(key_id) if key_id else None
        if not k and self.keys:
            k = next(iter(self.keys.values()))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        key_mat = k["KeyId"].encode()
        pt = _xor(ct, key_mat)
        return _resp({"KeyId": k["KeyId"], "Plaintext": _b64enc(pt),
                      "EncryptionAlgorithm": "SYMMETRIC_DEFAULT"})

    def _gen_random(self, body):
        n = int(body.get("NumberOfBytes", 32))
        return _resp({"Plaintext": _b64enc(os.urandom(n))})

    def _get_policy(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        return _resp({"Policy": k.get("Policy", _DEFAULT_POLICY), "PolicyName": "default"})

    def _put_policy(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        k["Policy"] = body.get("Policy", _DEFAULT_POLICY)
        return _resp({})

    def _tag_resource(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        existing = {t["TagKey"]: t["TagValue"] for t in k.get("Tags", [])}
        for tag in body.get("Tags", []):
            existing[tag["TagKey"]] = tag["TagValue"]
        k["Tags"] = [{"TagKey": key, "TagValue": val} for key, val in existing.items()]
        return _resp({})

    def _untag_resource(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        keys_to_rm = set(body.get("TagKeys", []))
        k["Tags"] = [t for t in k.get("Tags", []) if t["TagKey"] not in keys_to_rm]
        return _resp({})

    def _list_tags(self, body):
        k = self._resolve(body.get("KeyId", ""))
        if not k:
            return json_error("NotFoundException", "Key not found", 404)
        return _resp({"Tags": k.get("Tags", []), "Truncated": False})

    def reset(self):
        self.keys = {}
        self.aliases = {}


def _meta(k):
    m = {
        "KeyId": k["KeyId"], "Arn": k["Arn"],
        "Description": k.get("Description", ""),
        "KeyUsage": k.get("KeyUsage", "ENCRYPT_DECRYPT"),
        "KeySpec": k.get("KeySpec", "SYMMETRIC_DEFAULT"),
        "Enabled": k.get("Enabled", True),
        "KeyState": k.get("KeyState", "Enabled"),
        "CreationDate": k.get("CreationDate", 0),
    }
    if "DeletionDate" in k:
        m["DeletionDate"] = k["DeletionDate"]
    return m
