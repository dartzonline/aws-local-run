"""Lambda service emulator."""
import base64, hashlib, json, logging, os, subprocess, sys, tempfile, threading, uuid, zipfile
from dataclasses import dataclass, field
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, iso_timestamp, new_request_id

logger = logging.getLogger("localrun.lambda")

def _json_resp(data, status=200):
    return Response(json.dumps(data, default=str), status=status, content_type="application/json")

@dataclass
class LambdaFunction:
    name: str; arn: str; runtime: str; handler: str; role: str
    code_zip: bytes = b""; description: str = ""; timeout: int = 30
    memory_size: int = 128; environment: dict = field(default_factory=dict)
    last_modified: str = ""; code_sha256: str = ""; code_size: int = 0
    tags: dict = field(default_factory=dict)
    layers: list = field(default_factory=list)
    def __post_init__(self):
        if not self.last_modified: self.last_modified = iso_timestamp()
        self.code_sha256 = base64.b64encode(hashlib.sha256(self.code_zip).digest()).decode()
        self.code_size = len(self.code_zip)

    def refresh_code_meta(self):
        self.code_sha256 = base64.b64encode(hashlib.sha256(self.code_zip).digest()).decode()
        self.code_size = len(self.code_zip)
        self.last_modified = iso_timestamp()


class LambdaService:
    def __init__(self):
        self.functions = {}       # name -> LambdaFunction
        self.aliases = {}         # "name:alias" -> alias dict
        self.event_source_mappings = {}  # uuid -> mapping dict
        self.permissions = {}     # func_name -> [statement]
        self.layers_store = {}    # layer_name -> list of layer version dicts
        self.async_errors = {}    # func_name -> list of recent errors (last 10)

    def handle(self, req: Request, path: str) -> Response:
        method = req.method
        parts = path.strip("/").split("/")
        # Strip version prefix (2015-03-31, 2014-*, 2017-03-31, etc.)
        if parts and (parts[0][:4].isdigit()):
            parts = parts[1:]
        if not parts:
            return _json_resp({"error": "Invalid path"}, 400)
        resource = parts[0]

        if resource == "functions":
            if len(parts) == 1:
                if method == "GET": return self._list_functions(req)
                if method == "POST": return self._create_function(req)
            elif len(parts) >= 2:
                func_name = parts[1]
                if len(parts) == 2:
                    if method == "GET": return self._get_function(func_name)
                    if method == "DELETE": return self._delete_function(func_name)
                    if method == "PUT": return self._update_config(req, func_name)
                elif len(parts) >= 3:
                    sub = parts[2]
                    if sub == "invocations" and method == "POST":
                        return self._invoke(req, func_name)
                    if sub == "code" and method == "PUT":
                        return self._update_code(req, func_name)
                    if sub == "configuration":
                        if method == "GET": return self._get_function(func_name)
                        if method == "PUT": return self._update_config(req, func_name)
                    if sub == "aliases":
                        if len(parts) == 3:
                            if method == "GET": return self._list_aliases(func_name)
                            if method == "POST": return self._create_alias(req, func_name)
                        elif len(parts) >= 4:
                            alias_name = parts[3]
                            if method == "GET": return self._get_alias(func_name, alias_name)
                            if method == "PUT": return self._update_alias(req, func_name, alias_name)
                            if method == "DELETE": return self._delete_alias(func_name, alias_name)
                    if sub == "policy":
                        if len(parts) == 3:
                            if method == "GET": return self._get_policy(func_name)
                            if method == "POST": return self._add_permission(req, func_name)
                        elif len(parts) >= 4:
                            sid = parts[3]
                            if method == "DELETE": return self._remove_permission(func_name, sid)
                    if sub == "event-source-mappings":
                        if method == "GET": return self._list_esm(func_name)
                    if sub == "tags":
                        if method == "GET": return self._list_tags(func_name)
                        if method == "POST": return self._tag_resource(req, func_name)
                    if sub == "async-invoke-errors":
                        if method == "GET": return self._get_async_errors(func_name)

        if resource == "event-source-mappings":
            if len(parts) == 1:
                if method == "GET": return self._list_all_esm(req)
                if method == "POST": return self._create_esm(req)
            elif len(parts) == 2:
                esm_uuid = parts[1]
                if method == "GET": return self._get_esm(esm_uuid)
                if method == "DELETE": return self._delete_esm(esm_uuid)
                if method == "PUT": return self._update_esm(req, esm_uuid)

        if resource == "tags":
            # /tags/<arn-or-name>  — arn may contain slashes, rejoin
            arn = "/".join(parts[1:])
            if method == "GET": return self._list_tags_by_arn(arn)
            if method == "POST": return self._tag_by_arn(req, arn)
            if method == "DELETE": return self._untag_by_arn(req, arn)

        if resource == "layers":
            if len(parts) == 1:
                if method == "GET": return self._list_layers()
            elif len(parts) == 2:
                layer_name = parts[1]
                if method == "GET": return self._list_layer_versions(layer_name)
            elif len(parts) == 3 and parts[2] == "versions":
                layer_name = parts[1]
                if method == "POST": return self._publish_layer_version(req, layer_name)
                if method == "GET": return self._list_layer_versions(layer_name)
            elif len(parts) == 4 and parts[2] == "versions":
                layer_name = parts[1]
                version = parts[3]
                if method == "GET": return self._get_layer_version(layer_name, version)
                if method == "DELETE": return self._delete_layer_version(layer_name, version)

        return _json_resp({"message": "Not Found"}, 404)

    def _func_config(self, f: LambdaFunction) -> dict:
        return {
            "FunctionName": f.name, "FunctionArn": f.arn, "Runtime": f.runtime,
            "Handler": f.handler, "Role": f.role, "Description": f.description,
            "Timeout": f.timeout, "MemorySize": f.memory_size,
            "CodeSha256": f.code_sha256, "CodeSize": f.code_size,
            "LastModified": f.last_modified, "State": "Active",
            "Environment": {"Variables": f.environment},
        }

    def _create_function(self, req):
        try:
            body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception:
            body = {}
        name = body.get("FunctionName", "")
        if not name: return _json_resp({"message": "FunctionName required"}, 400)
        if name in self.functions: return _json_resp({"message": f"Function {name} already exists"}, 409)
        c = get_config()
        code = body.get("Code", {}); zip_bytes = b""
        if "ZipFile" in code: zip_bytes = base64.b64decode(code["ZipFile"])
        fn = LambdaFunction(
            name=name, arn=f"arn:aws:lambda:{c.region}:{c.account_id}:function:{name}",
            runtime=body.get("Runtime", "python3.12"), handler=body.get("Handler", "index.handler"),
            role=body.get("Role", ""), code_zip=zip_bytes, description=body.get("Description", ""),
            timeout=body.get("Timeout", 30), memory_size=body.get("MemorySize", 128),
            environment=body.get("Environment", {}).get("Variables", {}),
            tags=body.get("Tags", {}), layers=body.get("Layers", []),
        )
        self.functions[name] = fn
        self.permissions[name] = []
        logger.info("Created function: %s (%s)", name, fn.runtime)
        return _json_resp(self._func_config(fn), 201)

    def _list_functions(self, req):
        runtime_filter = req.args.get("FunctionVersion")
        funcs = [self._func_config(f) for f in self.functions.values()]
        return _json_resp({"Functions": funcs, "NextMarker": None})

    def _get_function(self, name):
        fn = self.functions.get(name)
        if not fn: return _json_resp({"message": f"Function {name} not found"}, 404)
        return _json_resp({"Configuration": self._func_config(fn), "Code": {"RepositoryType": "S3"}})

    def _delete_function(self, name):
        if name not in self.functions: return _json_resp({"message": f"Function {name} not found"}, 404)
        del self.functions[name]; logger.info("Deleted function: %s", name)
        return Response("", 204)

    def _update_code(self, req, name):
        fn = self.functions.get(name)
        if not fn: return _json_resp({"message": f"Function {name} not found"}, 404)
        try:
            body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception:
            body = {}
        if "ZipFile" in body: fn.code_zip = base64.b64decode(body["ZipFile"])
        fn.refresh_code_meta()
        return _json_resp(self._func_config(fn))

    def _update_config(self, req, name):
        fn = self.functions.get(name)
        if not fn: return _json_resp({"message": f"Function {name} not found"}, 404)
        try:
            body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception:
            body = {}
        if "Handler" in body: fn.handler = body["Handler"]
        if "Runtime" in body: fn.runtime = body["Runtime"]
        if "Timeout" in body: fn.timeout = body["Timeout"]
        if "MemorySize" in body: fn.memory_size = body["MemorySize"]
        if "Description" in body: fn.description = body["Description"]
        if "Role" in body: fn.role = body["Role"]
        if "Environment" in body: fn.environment = body["Environment"].get("Variables", {})
        fn.last_modified = iso_timestamp()
        return _json_resp(self._func_config(fn))

    # --- Aliases ---

    def _alias_key(self, func_name, alias_name): return f"{func_name}:{alias_name}"

    def _create_alias(self, req, func_name):
        fn = self.functions.get(func_name)
        if not fn: return _json_resp({"message": f"Function {func_name} not found"}, 404)
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        alias_name = body.get("Name", "")
        if not alias_name: return _json_resp({"message": "Name required"}, 400)
        key = self._alias_key(func_name, alias_name)
        if key in self.aliases: return _json_resp({"message": "Alias already exists"}, 409)
        alias = {
            "AliasArn": f"{fn.arn}:{alias_name}",
            "Name": alias_name,
            "FunctionVersion": body.get("FunctionVersion", "$LATEST"),
            "Description": body.get("Description", ""),
        }
        self.aliases[key] = alias
        logger.info("Created alias %s for %s", alias_name, func_name)
        return _json_resp(alias, 201)

    def _get_alias(self, func_name, alias_name):
        alias = self.aliases.get(self._alias_key(func_name, alias_name))
        if not alias: return _json_resp({"message": "Alias not found"}, 404)
        return _json_resp(alias)

    def _update_alias(self, req, func_name, alias_name):
        key = self._alias_key(func_name, alias_name)
        alias = self.aliases.get(key)
        if not alias: return _json_resp({"message": "Alias not found"}, 404)
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        if "FunctionVersion" in body: alias["FunctionVersion"] = body["FunctionVersion"]
        if "Description" in body: alias["Description"] = body["Description"]
        return _json_resp(alias)

    def _delete_alias(self, func_name, alias_name):
        self.aliases.pop(self._alias_key(func_name, alias_name), None)
        return Response("", 204)

    def _list_aliases(self, func_name):
        prefix = f"{func_name}:"
        aliases = [v for k, v in self.aliases.items() if k.startswith(prefix)]
        return _json_resp({"Aliases": aliases})

    # --- Permissions / Resource Policy ---

    def _add_permission(self, req, func_name):
        fn = self.functions.get(func_name)
        if not fn: return _json_resp({"message": f"Function {func_name} not found"}, 404)
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        sid = body.get("StatementId", str(uuid.uuid4()))
        statement = {"StatementId": sid, "Action": body.get("Action", ""), "Principal": body.get("Principal", ""), "SourceArn": body.get("SourceArn", "")}
        self.permissions.setdefault(func_name, []).append(statement)
        return _json_resp({"Statement": json.dumps(statement)}, 201)

    def _get_policy(self, func_name):
        if func_name not in self.functions: return _json_resp({"message": f"Function {func_name} not found"}, 404)
        stmts = self.permissions.get(func_name, [])
        if not stmts: return _json_resp({"message": "No policy found"}, 404)
        policy = {"Version": "2012-10-17", "Statement": stmts}
        return _json_resp({"Policy": json.dumps(policy), "RevisionId": "1"})

    def _remove_permission(self, func_name, sid):
        stmts = self.permissions.get(func_name, [])
        self.permissions[func_name] = [s for s in stmts if s["StatementId"] != sid]
        return Response("", 204)

    # --- Event Source Mappings ---

    def _create_esm(self, req):
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        func_name = body.get("FunctionName", "")
        if func_name not in self.functions: return _json_resp({"message": f"Function {func_name} not found"}, 404)
        esm_id = str(uuid.uuid4())
        mapping = {
            "UUID": esm_id, "FunctionArn": self.functions[func_name].arn,
            "EventSourceArn": body.get("EventSourceArn", ""),
            "StartingPosition": body.get("StartingPosition", "LATEST"),
            "BatchSize": body.get("BatchSize", 10),
            "State": "Enabled", "LastModified": iso_timestamp(),
        }
        self.event_source_mappings[esm_id] = mapping
        logger.info("Created ESM %s -> %s", body.get("EventSourceArn", ""), func_name)
        return _json_resp(mapping, 201)

    def _get_esm(self, esm_uuid):
        m = self.event_source_mappings.get(esm_uuid)
        if not m: return _json_resp({"message": "ESM not found"}, 404)
        return _json_resp(m)

    def _update_esm(self, req, esm_uuid):
        m = self.event_source_mappings.get(esm_uuid)
        if not m: return _json_resp({"message": "ESM not found"}, 404)
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        if "BatchSize" in body: m["BatchSize"] = body["BatchSize"]
        if "Enabled" in body: m["State"] = "Enabled" if body["Enabled"] else "Disabled"
        m["LastModified"] = iso_timestamp()
        return _json_resp(m)

    def _delete_esm(self, esm_uuid):
        self.event_source_mappings.pop(esm_uuid, None)
        return Response("", 204)

    def _list_all_esm(self, req):
        func_name = req.args.get("FunctionName")
        mappings = list(self.event_source_mappings.values())
        if func_name and func_name in self.functions:
            fn_arn = self.functions[func_name].arn
            mappings = [m for m in mappings if m["FunctionArn"] == fn_arn]
        return _json_resp({"EventSourceMappings": mappings})

    def _list_esm(self, func_name):
        fn = self.functions.get(func_name)
        if not fn: return _json_resp({"message": f"Function {func_name} not found"}, 404)
        mappings = [m for m in self.event_source_mappings.values() if m["FunctionArn"] == fn.arn]
        return _json_resp({"EventSourceMappings": mappings})

    # --- Tags ---

    def _list_tags(self, func_name):
        fn = self.functions.get(func_name)
        if not fn: return _json_resp({"message": f"Function {func_name} not found"}, 404)
        return _json_resp({"Tags": fn.tags})

    def _tag_resource(self, req, func_name):
        fn = self.functions.get(func_name)
        if not fn: return _json_resp({"message": f"Function {func_name} not found"}, 404)
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        fn.tags.update(body.get("Tags", {}))
        return Response("", 204)

    def _list_tags_by_arn(self, arn):
        for fn in self.functions.values():
            if fn.arn in arn or fn.name in arn:
                return _json_resp({"Tags": fn.tags})
        return _json_resp({"Tags": {}})

    def _tag_by_arn(self, req, arn):
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        for fn in self.functions.values():
            if fn.arn in arn or fn.name in arn:
                fn.tags.update(body.get("Tags", {}))
                break
        return Response("", 204)

    def _untag_by_arn(self, req, arn):
        tag_keys = req.args.getlist("tagKey")
        for fn in self.functions.values():
            if fn.arn in arn or fn.name in arn:
                for k in tag_keys: fn.tags.pop(k, None)
                break
        return Response("", 204)

    # --- Layers ---

    def _list_layers(self):
        result = []
        for name, versions in self.layers_store.items():
            if versions:
                result.append({"LayerName": name, "LatestMatchingVersion": versions[-1]})
        return _json_resp({"Layers": result})

    def _publish_layer_version(self, req, layer_name):
        try: body = json.loads(req.get_data(as_text=True) or "{}")
        except Exception: body = {}
        c = get_config()
        versions = self.layers_store.setdefault(layer_name, [])
        version_num = len(versions) + 1
        layer_arn = f"arn:aws:lambda:{c.region}:{c.account_id}:layer:{layer_name}"
        version_dict = {
            "LayerName": layer_name,
            "LayerArn": layer_arn,
            "LayerVersionArn": f"{layer_arn}:{version_num}",
            "Version": version_num,
            "Description": body.get("Description", ""),
            "CreatedDate": iso_timestamp(),
            "CompatibleRuntimes": body.get("CompatibleRuntimes", []),
        }
        versions.append(version_dict)
        logger.info("Published layer %s version %d", layer_name, version_num)
        return _json_resp(version_dict, 201)

    def _list_layer_versions(self, layer_name):
        versions = self.layers_store.get(layer_name, [])
        return _json_resp({"LayerVersions": versions})

    def _get_layer_version(self, layer_name, version):
        versions = self.layers_store.get(layer_name, [])
        try:
            v = int(version)
        except ValueError:
            return _json_resp({"message": "Invalid version number"}, 400)
        for ver in versions:
            if ver["Version"] == v:
                return _json_resp(ver)
        return _json_resp({"message": f"Layer version {version} not found"}, 404)

    def _delete_layer_version(self, layer_name, version):
        versions = self.layers_store.get(layer_name, [])
        try:
            v = int(version)
        except ValueError:
            return _json_resp({"message": "Invalid version number"}, 400)
        self.layers_store[layer_name] = [ver for ver in versions if ver["Version"] != v]
        return Response("", 204)

    # --- Async error tracking ---

    def _record_async_error(self, func_name, error_msg):
        errors = self.async_errors.setdefault(func_name, [])
        errors.append({"timestamp": iso_timestamp(), "errorMessage": error_msg})
        # Keep only the last 10
        if len(errors) > 10:
            self.async_errors[func_name] = errors[-10:]

    def _get_async_errors(self, func_name):
        if func_name not in self.functions:
            return _json_resp({"message": f"Function {func_name} not found"}, 404)
        errors = self.async_errors.get(func_name, [])
        return _json_resp({"FunctionName": func_name, "Errors": errors})

    # --- Invocation ---

    def _invoke(self, req, name):
        fn = self.functions.get(name)
        if not fn: return _json_resp({"message": f"Function {name} not found"}, 404)
        payload = req.get_data(as_text=True) or "{}"
        invocation_type = req.headers.get("X-Amz-Invocation-Type", "RequestResponse")
        if invocation_type == "Event":
            # Fire and forget — run in background, return immediately
            t = threading.Thread(target=self._async_execute, args=(fn, payload), daemon=True)
            t.start()
            return Response("", 202)
        result = self._execute_function(fn, payload)
        resp_body = result.get("body", {})
        resp = Response(
            json.dumps(resp_body, default=str) if not isinstance(resp_body, str) else resp_body,
            200, content_type="application/json",
        )
        if result.get("error"):
            resp.headers["X-Amz-Function-Error"] = "Unhandled"
        resp.headers["X-Amz-Log-Result"] = ""
        return resp

    def _async_execute(self, fn, payload):
        result = self._execute_function(fn, payload)
        if result.get("error"):
            err_msg = result.get("body", {})
            if isinstance(err_msg, dict):
                err_msg = err_msg.get("errorMessage", str(err_msg))
            self._record_async_error(fn.name, str(err_msg))

    def _get_effective_timeout(self, fn):
        t = fn.timeout
        if t <= 0:
            t = 3
        return t

    def _build_subprocess_env(self, fn):
        env = os.environ.copy()
        env.update(fn.environment)
        return env

    def _execute_python(self, fn, tmpdir, payload_file, env_file):
        module_name, func_name = fn.handler.rsplit(".", 1)
        runner = (
            "import sys, json, os\n"
            f"sys.path.insert(0, {repr(tmpdir)})\n"
            f"os.environ.update(json.load(open({repr(env_file)})))\n"
            f"import {module_name}\n"
            f"event = json.load(open({repr(payload_file)}))\n"
            f"ctx = type('Ctx',(),{{'function_name':{repr(fn.name)},'memory_limit_in_mb':{fn.memory_size},"
            f"'invoked_function_arn':{repr(fn.arn)},'aws_request_id':{repr(str(uuid.uuid4()))}}})()\n"
            f"result = {module_name}.{func_name}(event, ctx)\n"
            "print(json.dumps(result, default=str))\n"
        )
        timeout = self._get_effective_timeout(fn)
        proc = subprocess.run(
            [sys.executable, "-c", runner],
            capture_output=True, text=True, timeout=timeout, cwd=tmpdir,
        )
        return proc

    def _execute_nodejs(self, fn, tmpdir, payload_file):
        module_name, func_name = fn.handler.rsplit(".", 1)
        request_id = str(uuid.uuid4())
        runner_content = (
            "const handler = require('./{module_name}');\n"
            "const fs = require('fs');\n"
            "const event = JSON.parse(fs.readFileSync('{payload_file}', 'utf8'));\n"
            "const ctx = {{functionName: '{name}', memoryLimitInMB: {mem}, "
            "invokedFunctionArn: '{arn}', awsRequestId: '{rid}'}};\n"
            "Promise.resolve(handler.{func_name}(event, ctx))"
            ".then(r => {{ console.log(JSON.stringify(r)); process.exit(0); }})"
            ".catch(e => {{ console.error(e.message); process.exit(1); }});\n"
        ).format(
            module_name=module_name,
            payload_file=payload_file,
            name=fn.name,
            mem=fn.memory_size,
            arn=fn.arn,
            rid=request_id,
            func_name=func_name,
        )
        runner_file = os.path.join(tmpdir, "_runner.js")
        with open(runner_file, "w") as rf:
            rf.write(runner_content)
        env = self._build_subprocess_env(fn)
        timeout = self._get_effective_timeout(fn)
        try:
            proc = subprocess.run(
                ["node", runner_file],
                capture_output=True, text=True, timeout=timeout, cwd=tmpdir, env=env,
            )
        except FileNotFoundError:
            return None, "node is not installed or not on PATH — cannot execute nodejs runtime"
        return proc, None

    def _execute_go(self, fn, tmpdir, payload):
        # Look for bootstrap binary first, then fall back to module name
        bootstrap = os.path.join(tmpdir, "bootstrap")
        if not os.path.isfile(bootstrap):
            module_name = fn.handler.rsplit(".", 1)[0]
            alt = os.path.join(tmpdir, module_name)
            if os.path.isfile(alt):
                bootstrap = alt
            else:
                return None, "No bootstrap binary found in deployment package"
        # Make executable in case zip didn't preserve permissions
        os.chmod(bootstrap, 0o755)
        env = self._build_subprocess_env(fn)
        timeout = self._get_effective_timeout(fn)
        proc = subprocess.run(
            [bootstrap],
            input=payload, capture_output=True, text=True, timeout=timeout, cwd=tmpdir, env=env,
        )
        return proc, None

    def _parse_proc_output(self, proc, fn):
        if proc.returncode != 0:
            logger.warning("Lambda %s error: %s", fn.name, proc.stderr.strip())
            return {"error": True, "body": {"errorMessage": proc.stderr.strip()}}
        try:
            return {"body": json.loads(proc.stdout.strip())}
        except json.JSONDecodeError:
            return {"body": proc.stdout.strip()}

    def _execute_function(self, fn, payload):
        if not fn.code_zip:
            return {"error": True, "body": {"errorMessage": "No function code"}}

        # Container image: stub only
        if fn.runtime == "image":
            return {"body": {"statusCode": 200, "body": "container image invoke not supported locally"}}

        timeout = self._get_effective_timeout(fn)

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                zip_path = os.path.join(tmpdir, "code.zip")
                with open(zip_path, "wb") as f:
                    f.write(fn.code_zip)
                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(tmpdir)

                payload_file = os.path.join(tmpdir, "_payload.json")
                with open(payload_file, "w") as pf:
                    pf.write(payload)

                env_file = os.path.join(tmpdir, "_env.json")
                with open(env_file, "w") as ef:
                    json.dump(fn.environment, ef)

                is_go = fn.runtime.startswith("go") or fn.runtime in ("provided", "provided.al2")

                if fn.runtime.startswith("nodejs"):
                    proc, err = self._execute_nodejs(fn, tmpdir, payload_file)
                    if err:
                        return {"error": True, "body": {"errorMessage": err}}
                elif is_go:
                    proc, err = self._execute_go(fn, tmpdir, payload)
                    if err:
                        return {"error": True, "body": {"errorMessage": err}}
                else:
                    # Default: Python
                    try:
                        proc = self._execute_python(fn, tmpdir, payload_file, env_file)
                    except subprocess.TimeoutExpired:
                        logger.warning("Lambda %s timed out after %ds", fn.name, timeout)
                        return {"error": True, "body": {"errorMessage": f"Task timed out after {timeout} seconds"}}

                try:
                    return self._parse_proc_output(proc, fn)
                except subprocess.TimeoutExpired:
                    logger.warning("Lambda %s timed out after %ds", fn.name, timeout)
                    return {"error": True, "body": {"errorMessage": f"Task timed out after {timeout} seconds"}}

        except subprocess.TimeoutExpired:
            logger.warning("Lambda %s timed out after %ds", fn.name, timeout)
            return {"error": True, "body": {"errorMessage": f"Task timed out after {timeout} seconds"}}
        except Exception as e:
            return {"error": True, "body": {"errorMessage": str(e)}}

    def reset(self):
        self.functions = {}
        self.aliases = {}
        self.event_source_mappings = {}
        self.permissions = {}
        self.layers_store = {}
        self.async_errors = {}
