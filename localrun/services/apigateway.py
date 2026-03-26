"""API Gateway v1 service emulator."""
import json, logging, time, uuid
from flask import Request, Response
from localrun.config import get_config

logger = logging.getLogger("localrun.apigateway")

def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/json")

def _err(msg, status=400):
    return Response(json.dumps({"message": msg}), status, content_type="application/json")

class APIGatewayService:
    def __init__(self):
        self.apis = {}         # id -> api dict
        self.resources = {}    # api_id -> {id: resource}
        self.methods = {}      # f"{api_id}/{resource_id}" -> {method: config}
        self.deployments = {}  # api_id -> [deployments]
        self.stages = {}       # api_id -> {name: stage}
        self.tags = {}         # arn -> {tag: value}
        self.lambda_svc = None  # injected by gateway

    def handle(self, req: Request, path: str) -> Response:
        method = req.method
        parts = path.strip("/").split("/")

        # Tag operations: PUT/GET /tags/{arn}
        if parts[0] == "tags":
            arn = "/".join(parts[1:]) if len(parts) > 1 else ""
            if method == "PUT": return self._tag_resource(req, arn)
            if method == "GET": return self._get_tags(arn)
            if method == "DELETE": return self._untag_resource(req, arn)
            return _err("Not found", 404)

        if not path.startswith("restapis"):
            return _err("Not found", 404)

        if len(parts) == 1:
            if method == "POST": return self._create_api(req)
            if method == "GET": return self._list_apis(req)
            return _err("Not found", 404)

        api_id = parts[1]
        if len(parts) == 2:
            if method == "GET": return self._get_api(api_id)
            if method == "DELETE": return self._delete_api(api_id)
            return _err("Not found", 404)

        sub = parts[2]

        if sub == "resources":
            if len(parts) == 3:
                if method == "GET": return self._list_resources(api_id)
                if method == "POST": return self._create_resource(req, api_id)
            if len(parts) >= 4:
                resource_id = parts[3]
                if len(parts) == 4:
                    if method == "GET": return self._get_resource(api_id, resource_id)
                    if method == "POST": return self._create_resource(req, api_id, parent_id=resource_id)
                if len(parts) >= 5 and parts[4] == "methods":
                    if len(parts) == 5:
                        if method == "GET": return self._list_methods(api_id, resource_id)
                    if len(parts) >= 6:
                        http_method = parts[5].upper()
                        if len(parts) == 6:
                            if method == "PUT": return self._put_method(req, api_id, resource_id, http_method)
                            if method == "GET": return self._get_method(api_id, resource_id, http_method)
                            if method == "DELETE": return self._delete_method(api_id, resource_id, http_method)
                        if len(parts) >= 7 and parts[6] == "integration":
                            if method == "PUT": return self._put_integration(req, api_id, resource_id, http_method)
                            if method == "GET": return self._get_integration(api_id, resource_id, http_method)
                            if method == "DELETE": return self._delete_integration(api_id, resource_id, http_method)

        if sub == "deployments":
            if len(parts) == 3 and method == "POST": return self._create_deployment(req, api_id)

        if sub == "stages":
            if len(parts) == 3:
                if method == "GET": return self._list_stages(api_id)
                if method == "POST": return self._create_stage(req, api_id)

        # Lambda proxy invocation: /{api_id}/{stage}/{resource_path}
        if len(parts) >= 4:
            stage_name = parts[2]
            resource_path = "/" + "/".join(parts[3:])
            stages = self.stages.get(api_id, {})
            if stage_name in stages:
                return self._invoke_proxy(req, api_id, resource_path, method)

        return _err("Not found", 404)

    def _create_api(self, req):
        body = req.get_json(silent=True) or json.loads(req.get_data(as_text=True) or "{}")
        name = body.get("name", "")
        api_id = uuid.uuid4().hex[:10]
        c = get_config()
        self.apis[api_id] = {
            "id": api_id, "name": name,
            "description": body.get("description", ""),
            "createdDate": time.time(),
        }
        root_id = uuid.uuid4().hex[:10]
        self.resources[api_id] = {root_id: {"id": root_id, "path": "/", "parentId": None}}
        logger.info("Created API: %s (%s)", name, api_id)
        return _resp(self.apis[api_id], 201)

    def _list_apis(self, req):
        return _resp({"item": list(self.apis.values())})

    def _get_api(self, api_id):
        api = self.apis.get(api_id)
        if not api: return _err("API not found", 404)
        return _resp(api)

    def _delete_api(self, api_id):
        self.apis.pop(api_id, None)
        self.resources.pop(api_id, None)
        return Response("", 202)

    def _list_resources(self, api_id):
        res = self.resources.get(api_id, {})
        return _resp({"item": list(res.values())})

    def _create_resource(self, req, api_id, parent_id=None):
        body = req.get_json(silent=True) or json.loads(req.get_data(as_text=True) or "{}")
        # parent_id from URL takes precedence over body
        if parent_id is None:
            parent_id = body.get("parentId")
        path_part = body.get("pathPart", "")
        rid = uuid.uuid4().hex[:10]
        parent = self.resources.get(api_id, {}).get(parent_id, {})
        parent_path = parent.get("path", "")
        full_path = f"{parent_path.rstrip('/')}/{path_part}"
        resource = {"id": rid, "parentId": parent_id, "pathPart": path_part, "path": full_path}
        self.resources.setdefault(api_id, {})[rid] = resource
        return _resp(resource, 201)

    def _create_deployment(self, req, api_id):
        did = uuid.uuid4().hex[:10]
        deployment = {"id": did, "createdDate": time.time()}
        self.deployments.setdefault(api_id, []).append(deployment)
        return _resp(deployment, 201)

    def _list_stages(self, api_id):
        return _resp({"item": list(self.stages.get(api_id, {}).values())})

    def _create_stage(self, req, api_id):
        body = req.get_json(silent=True) or json.loads(req.get_data(as_text=True) or "{}")
        name = body.get("stageName", "")
        stage = {"stageName": name, "deploymentId": body.get("deploymentId", ""), "createdDate": time.time()}
        self.stages.setdefault(api_id, {})[name] = stage
        return _resp(stage, 201)

    def _get_resource(self, api_id, resource_id):
        res = self.resources.get(api_id, {}).get(resource_id)
        if not res: return _err("Resource not found", 404)
        return _resp(res)

    def _list_methods(self, api_id, resource_id):
        prefix = f"{api_id}/{resource_id}/"
        methods = {k.split("/")[-1]: v for k, v in self.methods.items() if k.startswith(prefix)}
        return _resp(methods)

    def _method_key(self, api_id, resource_id, http_method):
        return f"{api_id}/{resource_id}/{http_method}"

    def _put_method(self, req, api_id, resource_id, http_method):
        body = req.get_json(silent=True) or json.loads(req.get_data(as_text=True) or "{}")
        key = self._method_key(api_id, resource_id, http_method)
        existing = self.methods.get(key, {})
        existing.update({
            "httpMethod": http_method,
            "authorizationType": body.get("authorizationType", "NONE"),
            "apiKeyRequired": body.get("apiKeyRequired", False),
            "requestParameters": body.get("requestParameters", {}),
        })
        self.methods[key] = existing
        return _resp(existing)

    def _get_method(self, api_id, resource_id, http_method):
        method_cfg = self.methods.get(self._method_key(api_id, resource_id, http_method))
        if not method_cfg: return _err("Method not found", 404)
        return _resp(method_cfg)

    def _delete_method(self, api_id, resource_id, http_method):
        self.methods.pop(self._method_key(api_id, resource_id, http_method), None)
        return Response("", 204)

    def _put_integration(self, req, api_id, resource_id, http_method):
        body = req.get_json(silent=True) or json.loads(req.get_data(as_text=True) or "{}")
        key = self._method_key(api_id, resource_id, http_method)
        if key not in self.methods:
            self.methods[key] = {"httpMethod": http_method}
        integration = {
            "type": body.get("type", "AWS_PROXY"),
            "httpMethod": body.get("httpMethod", http_method),
            "uri": body.get("uri", ""),
            "passthroughBehavior": body.get("passthroughBehavior", "WHEN_NO_MATCH"),
            "requestTemplates": body.get("requestTemplates", {}),
        }
        self.methods[key]["integration"] = integration
        return _resp(integration)

    def _get_integration(self, api_id, resource_id, http_method):
        method_cfg = self.methods.get(self._method_key(api_id, resource_id, http_method))
        if not method_cfg or "integration" not in method_cfg:
            return _err("Integration not found", 404)
        return _resp(method_cfg["integration"])

    def _delete_integration(self, api_id, resource_id, http_method):
        key = self._method_key(api_id, resource_id, http_method)
        if key in self.methods:
            self.methods[key].pop("integration", None)
        return Response("", 204)

    def _tag_resource(self, req, arn):
        body = req.get_json(silent=True) or json.loads(req.get_data(as_text=True) or "{}")
        tags = body.get("tags", {})
        self.tags.setdefault(arn, {}).update(tags)
        return Response("", 204)

    def _get_tags(self, arn):
        return _resp({"tags": self.tags.get(arn, {})})

    def _untag_resource(self, req, arn):
        keys = req.args.getlist("tagKeys")
        existing = self.tags.get(arn, {})
        for k in keys:
            existing.pop(k, None)
        return Response("", 204)

    def reset(self):
        self.apis = {}
        self.resources = {}
        self.methods = {}
        self.deployments = {}
        self.stages = {}
        self.tags = {}

    def _invoke_proxy(self, req, api_id, resource_path, http_method):
        """Find a matching resource and invoke Lambda via AWS_PROXY."""
        if not self.lambda_svc:
            return _err("Lambda not available for proxy", 500)
        resources = self.resources.get(api_id, {})
        matching_resource = None
        for res in resources.values():
            if res.get("path") == resource_path:
                matching_resource = res
                break
        if not matching_resource:
            return _err("No matching resource for " + resource_path, 404)
        key = self._method_key(api_id, matching_resource["id"], http_method)
        method_cfg = self.methods.get(key)
        if not method_cfg or "integration" not in method_cfg:
            return _err("No integration configured", 404)
        integration = method_cfg["integration"]
        if integration.get("type") != "AWS_PROXY":
            return _err("Only AWS_PROXY is supported", 501)
        # Extract Lambda function name from URI
        uri = integration.get("uri", "")
        fn_name = uri.split(":")[-1] if ":" in uri else uri
        fn = self.lambda_svc.functions.get(fn_name)
        if not fn:
            return _err("Lambda function not found: " + fn_name, 404)
        # Build API Gateway proxy event
        event = json.dumps({
            "httpMethod": http_method,
            "path": resource_path,
            "headers": dict(req.headers),
            "queryStringParameters": dict(req.args) or None,
            "body": req.get_data(as_text=True) or None,
            "isBase64Encoded": False,
            "requestContext": {
                "resourceId": matching_resource["id"],
                "httpMethod": http_method,
                "path": resource_path,
            },
        })
        result = self.lambda_svc._execute_function(fn, event)
        # Parse Lambda response
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except Exception:
                return _resp({"body": result})
        if isinstance(result, dict) and "statusCode" in result:
            status = result.get("statusCode", 200)
            body = result.get("body", "")
            headers = result.get("headers", {})
            resp = Response(body, status, content_type="application/json")
            for k, v in headers.items():
                resp.headers[k] = v
            return resp
        return _resp(result)
