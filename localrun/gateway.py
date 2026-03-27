"""API gateway — routes requests to the appropriate service engine."""
import json, logging, time, collections, threading
from flask import Flask, Request, Response, request
from localrun.config import get_config
from localrun.faults import FaultManager
from localrun.state import StateManager
from localrun.plugins import load_entry_points, inject_into_engines

logger = logging.getLogger("localrun.gateway")
fault_manager = FaultManager()

# Ring buffer for request log — keeps last 200 entries
_request_log = collections.deque(maxlen=200)


def _diff_states(snap1, snap2, label1, label2):
    """Compare two state snapshots and return a structured diff per service."""
    all_services = set(snap1.keys()) | set(snap2.keys())
    result = {}
    for svc in all_services:
        s1 = snap1.get(svc, {})
        s2 = snap2.get(svc, {})
        if not isinstance(s1, dict):
            s1 = {}
        if not isinstance(s2, dict):
            s2 = {}
        keys1 = set(s1.keys())
        keys2 = set(s2.keys())
        added = list(keys2 - keys1)
        removed = list(keys1 - keys2)
        changed = []
        for k in keys1 & keys2:
            if s1[k] != s2[k]:
                changed.append(k)
        if added or removed or changed:
            result[svc] = {"added": added, "removed": removed, "changed": changed}
    return {"services": result}


def create_app() -> Flask:
    app = Flask("localrun")
    app.url_map.strict_slashes = False

    from localrun.services.s3 import S3Service
    from localrun.services.sqs import SQSService
    from localrun.services.dynamodb import DynamoDBService
    from localrun.services.sns import SNSService
    from localrun.services.lambda_service import LambdaService
    from localrun.services.iam import IAMService
    from localrun.services.cloudwatch_logs import CloudWatchLogsService
    from localrun.services.sts import STSService
    from localrun.services.secretsmanager import SecretsManagerService
    from localrun.services.ssm import SSMService
    from localrun.services.eventbridge import EventBridgeService
    from localrun.services.cloudformation import CloudFormationService
    from localrun.services.rds import RDSService
    from localrun.services.apigateway import APIGatewayService
    from localrun.services.opensearch import OpenSearchService
    from localrun.services.kinesis import KinesisService
    from localrun.services.cloudwatch_metrics import CloudWatchService
    from localrun.services.stepfunctions import StepFunctionsService
    from localrun.services.ses import SESService
    from localrun.services.cognito import CognitoService
    from localrun.services.kms import KMSService
    from localrun.services.ec2 import EC2Service
    from localrun.services.acm import ACMService
    from localrun.services.route53 import Route53Service

    def _make_engines():
        eng = {
            "s3": S3Service(),
            "sqs": SQSService(),
            "dynamodb": DynamoDBService(),
            "sns": SNSService(),
            "lambda": LambdaService(),
            "iam": IAMService(),
            "logs": CloudWatchLogsService(),
            "sts": STSService(),
            "secretsmanager": SecretsManagerService(),
            "ssm": SSMService(),
            "events": EventBridgeService(),
            "cloudformation": CloudFormationService(),
            "rds": RDSService(),
            "apigateway": APIGatewayService(),
            "opensearch": OpenSearchService(),
            "kinesis": KinesisService(),
            "cloudwatch": CloudWatchService(),
            "stepfunctions": StepFunctionsService(),
            "ses": SESService(),
            "cognito": CognitoService(),
            "kms": KMSService(),
            "ec2": EC2Service(),
            "acm": ACMService(),
            "route53": Route53Service(),
        }
        # Wire cross-service references so SNS/EventBridge can deliver to SQS and Lambda
        eng["sns"].sqs = eng["sqs"]
        eng["sns"].lambda_svc = eng["lambda"]
        eng["events"].sqs = eng["sqs"]
        eng["events"].sns = eng["sns"]
        eng["events"].lambda_svc = eng["lambda"]
        eng["s3"].sqs = eng["sqs"]
        eng["s3"].sns = eng["sns"]
        eng["s3"].lambda_svc = eng["lambda"]
        eng["cloudwatch"].sns = eng["sns"]
        eng["cloudformation"].engines = eng
        eng["apigateway"].lambda_svc = eng["lambda"]
        eng["lambda"].logs_svc = eng["logs"]
        return eng

    engines = _make_engines()

    # Multi-region: primary region uses the engines dict already built
    _region_engines = {}
    _region_engines[get_config().region] = engines

    # Per-service locks for thread safety
    _locks = {svc: threading.Lock() for svc in engines}
    app.config["_locks"] = _locks

    # Rate limiting state
    _rate_counters = {}  # svc -> {"count": int, "window_start": float}
    _rate_lock = threading.Lock()

    def _check_rate_limit(svc):
        limits = get_config().rate_limits if hasattr(get_config(), "rate_limits") else {}
        limit = limits.get(svc, 0)
        if limit <= 0:
            return None  # no limit configured
        now = time.time()
        with _rate_lock:
            entry = _rate_counters.get(svc)
            if entry is None or now - entry["window_start"] >= 60.0:
                _rate_counters[svc] = {"count": 1, "window_start": now}
                return None
            entry["count"] += 1
            if entry["count"] > limit:
                return entry["count"]
        return None

    def _region_from_auth(req):
        auth = req.headers.get("Authorization", "")
        if "Credential=" in auth:
            parts = auth.split("Credential=")[1].split(",")[0].split("/")
            if len(parts) >= 4:
                return parts[2]
        return None

    load_entry_points()
    inject_into_engines(engines)

    # Store engines on the app so other code can reach them (e.g. seed loader)
    app.config["engines"] = engines

    # Initialize state manager and try loading existing state on boot
    config = get_config()
    state_manager = StateManager(config.data_dir)
    state_manager.load_state(engines)

    @app.before_request
    def log_request():
        logger.debug("→ %s %s", request.method, request.url)

    @app.after_request
    def add_headers(resp):
        resp.headers["x-amzn-RequestId"] = __import__("uuid").uuid4().hex
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,HEAD,OPTIONS,PATCH"
        resp.headers["Access-Control-Allow-Headers"] = "*"
        return resp

    @app.route("/health", methods=["GET"])
    @app.route("/_localrun/health", methods=["GET"])
    def health():
        from localrun import __version__
        svc_stats = {}
        for name, engine in engines.items():
            counts = {}
            if hasattr(engine, "buckets"): counts["buckets"] = len(engine.buckets)
            if hasattr(engine, "queues"): counts["queues"] = len(engine.queues)
            if hasattr(engine, "tables"): counts["tables"] = len(engine.tables)
            if hasattr(engine, "topics"): counts["topics"] = len(engine.topics)
            if hasattr(engine, "functions"): counts["functions"] = len(engine.functions)
            if hasattr(engine, "secrets"): counts["secrets"] = len(engine.secrets)
            if hasattr(engine, "parameters"): counts["parameters"] = len(engine.parameters)
            if hasattr(engine, "inbox"): counts["inbox"] = len(engine.inbox)
            svc_stats[name] = counts if counts else "active"
        return {
            "status": "running", "version": __version__,
            "services": svc_stats,
        }

    @app.route("/_localrun/faults", methods=["GET", "POST", "DELETE"])
    def api_faults():
        if request.method == "GET":
            return {"faults": fault_manager.get_all()}
        elif request.method == "POST":
            # Add a fault
            try:
                body = request.get_json(force=True)
            except Exception:
                return Response(json.dumps({"error": "Invalid JSON"}), 400, content_type="application/json")
            fid = fault_manager.add(body)
            return {"id": fid, "message": "Fault added"}
        elif request.method == "DELETE":
            # Remove a fault (by id in query param) or clear all
            fid = request.args.get("id")
            if fid:
                removed = fault_manager.remove(fid)
                return {"message": "Fault removed" if removed else "Fault not found"}
            fault_manager.clear()
            return {"message": "All faults cleared"}

    @app.route("/_localrun/state/save", methods=["POST"])
    def api_state_save():
        if state_manager.save_state(engines):
            return {"message": "State saved successfully"}
        return Response(json.dumps({"error": "Failed to save state. Ensure LOCALRUN_DATA_DIR is set."}), 500, content_type="application/json")

    @app.route("/_localrun/state/load", methods=["POST"])
    def api_state_load():
        if state_manager.load_state(engines):
            return {"message": "State loaded successfully"}
        return Response(json.dumps({"error": "Failed to load state. Ensure LOCALRUN_DATA_DIR is set and file exists."}), 500, content_type="application/json")

    @app.route("/_localrun/state/save/<name>", methods=["POST"])
    def api_state_save_named(name):
        import os
        data_dir = config.data_dir or "."
        path = os.path.join(data_dir, f"localrun_state_{name}.json")
        if state_manager.save_state(engines, path):
            return {"message": f"Snapshot '{name}' saved", "path": path}
        return Response(json.dumps({"error": "Failed to save snapshot"}), 500, content_type="application/json")

    @app.route("/_localrun/state/load/<name>", methods=["POST"])
    def api_state_load_named(name):
        import os
        data_dir = config.data_dir or "."
        path = os.path.join(data_dir, f"localrun_state_{name}.json")
        if state_manager.load_state(engines, path):
            return {"message": f"Snapshot '{name}' loaded"}
        return Response(json.dumps({"error": f"Snapshot '{name}' not found"}), 404, content_type="application/json")

    @app.route("/_localrun/state/snapshots", methods=["GET"])
    def api_state_snapshots():
        import os, glob as _glob
        data_dir = config.data_dir or "."
        pattern = os.path.join(data_dir, "localrun_state_*.json")
        files = _glob.glob(pattern)
        names = []
        for f in sorted(files):
            base = os.path.basename(f)
            # strip prefix and suffix to get just the name
            name = base[len("localrun_state_"):-len(".json")]
            names.append(name)
        return Response(json.dumps({"snapshots": names}), 200, content_type="application/json")

    @app.route("/_localrun/state/diff/<name1>/<name2>", methods=["GET"])
    def api_state_diff(name1, name2):
        import os, json as _json
        data_dir = config.data_dir or "."

        def load_snapshot(name):
            path = os.path.join(data_dir, f"localrun_state_{name}.json")
            if not os.path.exists(path):
                return None
            with open(path) as f:
                return _json.load(f)

        snap1 = load_snapshot(name1)
        snap2 = load_snapshot(name2)

        if snap1 is None:
            return Response(_json.dumps({"error": f"Snapshot '{name1}' not found"}), 404, content_type="application/json")
        if snap2 is None:
            return Response(_json.dumps({"error": f"Snapshot '{name2}' not found"}), 404, content_type="application/json")

        diff = _diff_states(snap1, snap2, name1, name2)
        return Response(_json.dumps(diff), 200, content_type="application/json")

    @app.route("/_localrun/reset", methods=["POST"])
    def api_reset():
        svc = request.args.get("service")
        if svc:
            engine = engines.get(svc)
            if not engine:
                return Response(json.dumps({"error": f"Service '{svc}' not found"}), 404, content_type="application/json")
            if hasattr(engine, "reset"):
                engine.reset()
            return {"reset": True, "service": svc}
        # Reset all services
        for engine in engines.values():
            if hasattr(engine, "reset"):
                engine.reset()
        return {"reset": True}

    @app.route("/_localrun/requests", methods=["GET"])
    def api_request_log():
        svc_filter = request.args.get("service")
        try:
            limit = int(request.args.get("limit", 50))
        except ValueError:
            limit = 50
        entries = list(_request_log)
        if svc_filter:
            entries = [e for e in entries if e.get("service") == svc_filter]
        status_filter = request.args.get("status")
        if status_filter:
            try:
                status_code = int(status_filter)
                entries = [e for e in entries if e.get("status") == status_code]
            except ValueError:
                pass
        return Response(json.dumps({"requests": entries[-limit:]}), 200, content_type="application/json")

    @app.route("/_localrun/regions", methods=["GET"])
    def api_regions():
        return Response(json.dumps({"regions": list(_region_engines.keys())}), 200, content_type="application/json")

    @app.route("/_localrun/resources", methods=["GET"])
    def api_resources():
        """Return a flat list of all resources across all services."""
        svc_filter = request.args.get("service")
        resources = []
        c = get_config()
        for svc_name, engine in engines.items():
            if svc_filter and svc_name != svc_filter:
                continue
            if hasattr(engine, "buckets"):
                for name in engine.buckets:
                    resources.append({"service": "s3", "type": "bucket", "name": name})
            if hasattr(engine, "queues"):
                for q in engine.queues.values():
                    resources.append({"service": "sqs", "type": "queue", "name": q.name, "arn": q.arn})
            if hasattr(engine, "tables"):
                for name, t in engine.tables.items():
                    resources.append({"service": "dynamodb", "type": "table", "name": name, "arn": t.get("TableArn", "")})
            if hasattr(engine, "topics"):
                for arn, t in engine.topics.items():
                    resources.append({"service": "sns", "type": "topic", "name": t.name, "arn": arn})
            if hasattr(engine, "functions"):
                for name, f in engine.functions.items():
                    resources.append({"service": "lambda", "type": "function", "name": name, "arn": f.arn})
            if hasattr(engine, "secrets"):
                for name, s in engine.secrets.items():
                    if not s.get("deleted"):
                        resources.append({"service": "secretsmanager", "type": "secret", "name": s.get("Name", name)})
            if hasattr(engine, "parameters"):
                for name in engine.parameters:
                    resources.append({"service": "ssm", "type": "parameter", "name": name})
            if hasattr(engine, "stacks"):
                for name, s in engine.stacks.items():
                    resources.append({"service": "cloudformation", "type": "stack", "name": name, "status": s.get("StackStatus", "")})
            if hasattr(engine, "streams") and svc_name == "kinesis":
                for name in engine.streams:
                    resources.append({"service": "kinesis", "type": "stream", "name": name})
            if hasattr(engine, "state_machines"):
                for arn, sm in engine.state_machines.items():
                    resources.append({"service": "stepfunctions", "type": "stateMachine", "name": sm.get("name", ""), "arn": arn})
            if hasattr(engine, "roles"):
                for name, r in engine.roles.items():
                    resources.append({"service": "iam", "type": "role", "name": name, "arn": r.get("Arn", "")})
            if hasattr(engine, "user_pools"):
                for pid, pool in engine.user_pools.items():
                    resources.append({"service": "cognito", "type": "userPool", "name": pool.get("Name", ""), "id": pid})
            if hasattr(engine, "log_groups"):
                for name in engine.log_groups:
                    resources.append({"service": "logs", "type": "logGroup", "name": name})
        return Response(json.dumps({"resources": resources, "count": len(resources)}), 200, content_type="application/json")

    @app.route("/_localrun/sns/inbox", methods=["GET"])
    def sns_inbox():
        sns = engines.get("sns")
        if not sns:
            return Response(json.dumps({"error": "SNS not available"}), 404, content_type="application/json")
        return Response(json.dumps({
            "sms": sns.sms_inbox[-50:],
            "email": sns.email_inbox[-50:],
        }), 200, content_type="application/json")

    @app.route("/_localrun/ses/inbox", methods=["GET"])
    def ses_inbox():
        ses = engines.get("ses")
        if not ses:
            return Response(json.dumps({"error": "SES not available"}), 404, content_type="application/json")
        return Response(json.dumps({"emails": ses.inbox[-50:]}), 200, content_type="application/json")

    def _serve_dashboard():
        from localrun.dashboard import DASHBOARD_HTML
        return Response(DASHBOARD_HTML, 200, content_type="text/html")

    @app.route("/dashboard", methods=["GET"])
    @app.route("/dashboard/", methods=["GET"])
    def dashboard_root():
        return _serve_dashboard()

    @app.route("/_localrun/ui", methods=["GET"])
    @app.route("/_localrun/ui/", methods=["GET"])
    def dashboard_ui():
        return _serve_dashboard()

    @app.route("/_localrun/terraform", methods=["GET"])
    def terraform_provider():
        c = get_config()
        endpoint = f"http://localhost:{c.port}"
        services_map = {}
        for svc in c.enabled_services:
            services_map[svc] = endpoint
        return Response(json.dumps({
            "provider": "aws",
            "region": c.region,
            "endpoint": endpoint,
            "services": services_map,
        }), 200, content_type="application/json")

    @app.route("/_localrun/api/state", methods=["GET"])
    def dashboard_api():
        state = {}
        for svc, engine in engines.items():
            if svc == "s3":
                state["s3"] = [{"name": b, "objects": len(o)} for b, o in engine.buckets.items()]
            elif svc == "sqs":
                state["sqs"] = [{"name": q.name, "messages": len(q.messages)} for q in engine.queues.values()]
            elif svc == "dynamodb":
                state["dynamodb"] = [{"name": name, "items": len(engine.table_items.get(name, []))} for name in engine.tables.keys()]
            elif svc == "sns":
                state["sns"] = [{"name": t.name, "subscriptions": len(t.subscriptions)} for t in engine.topics.values()]
            elif svc == "lambda":
                state["lambda"] = [{"name": f.name, "runtime": f.runtime} for f in engine.functions.values()]
            elif svc == "logs":
                state["cloudwatch logs"] = [{"name": g.name, "streams": len(g.streams)} for g in engine.log_groups.values()]
            elif svc == "secretsmanager":
                state["secrets manager"] = [{"name": s["Name"]} for s in engine.secrets.values() if not s.get("deleted")]
            elif svc == "ssm":
                state["ssm"] = [{"name": p["Name"], "type": p["Type"]} for p in engine.parameters.values()]
            elif svc == "events":
                state["eventbridge"] = [{"name": r["Name"], "targets": len(engine.targets.get(r["Name"], []))} for r in engine.rules.values()]
            elif svc == "opensearch":
                state["opensearch"] = [{"name": d, "indices": len(engine.indices)} for d in engine.domains]
            elif svc == "kinesis":
                state["kinesis"] = [{"name": n, "shards": s["shard_count"], "records": len(s["records"])} for n, s in engine.streams.items()]
            elif svc == "cloudwatch":
                state["cloudwatch"] = [{"namespace": ns, "metric": mn, "datapoints": len(pts)} for (ns, mn), pts in engine.metrics.items()]
            elif svc == "stepfunctions":
                state["stepfunctions"] = [{"name": sm["name"], "executions": len([e for e in engine.executions.values() if e["stateMachineArn"] == sm["arn"]])} for sm in engine.state_machines.values()]
        return Response(json.dumps(state), 200, content_type="application/json")

    @app.route("/", defaults={"path": ""}, methods=["GET","POST","PUT","DELETE","HEAD","OPTIONS","PATCH"])
    @app.route("/<path:path>", methods=["GET","POST","PUT","DELETE","HEAD","OPTIONS","PATCH"])
    def route_request(path: str):
        if request.method == "OPTIONS":
            return Response("", 200)

        svc = _detect_service(request, path)
        if not svc:
            return Response(json.dumps({"error": "Cannot detect target service"}), 400, content_type="application/json")

        config = get_config()
        if not config.enabled_services.get(svc, False):
            return Response(json.dumps({"error": f"Service {svc} is disabled"}), 400, content_type="application/json")

        # Multi-region: route to a per-region engine set when the request targets a different region
        region = _region_from_auth(request)
        if region and region != get_config().region:
            if region not in _region_engines:
                _region_engines[region] = _make_engines()
                logger.info("Created engine set for region: %s", region)
            handler = _region_engines[region].get(svc)
        else:
            handler = engines.get(svc)

        if not handler:
            return Response(json.dumps({"error": f"Service {svc} not implemented"}), 501, content_type="application/json")

        # Try to extract action for fault filtering
        target = request.headers.get("X-Amz-Target", "")
        if target and "." in target:
            action = target.split(".")[-1]
        else:
            action = request.args.get("Action") or request.form.get("Action", "")

        # Check for faults
        fault = fault_manager.apply_faults(svc, action)
        if fault:
            logger.warning("Applied fault to %s %s: %s", svc, action, fault)
            status_code = fault.get("status_code", 500)
            body = f'<?xml version="1.0" encoding="UTF-8"?><ErrorResponse><Error><Code>{fault.get("error_type", "InternalFailure")}</Code><Message>{fault.get("message", "Injected fault error")}</Message></Error></ErrorResponse>'
            # For JSON protocols we could return JSON, but many clients handle XML errors fine or we can let botocore process it.
            # To be safe for newer boto3 JSON requests:
            if "json" in (request.content_type or "") or target:
                body = json.dumps({"__type": fault.get("error_type", "InternalFailure"), "message": fault.get("message", "Injected fault error")})
                return Response(body, status_code, content_type="application/x-amz-json-1.0")
            return Response(body, status_code, content_type="application/xml")

        # Check rate limit
        over_limit = _check_rate_limit(svc)
        if over_limit is not None:
            body = json.dumps({"__type": "ThrottlingException", "message": f"Rate exceeded for service {svc}"})
            return Response(body, 429, content_type="application/x-amz-json-1.0")

        logger.info("→ %s %s", svc, path or "/")
        start = time.time()

        # Acquire per-service lock so concurrent requests don't corrupt state
        lock = _locks.get(svc)
        if lock:
            with lock:
                resp = handler.handle(request, path)
        else:
            resp = handler.handle(request, path)

        duration_ms = int((time.time() - start) * 1000)
        _request_log.append({
            "timestamp": time.time(),
            "method": request.method,
            "path": "/" + path,
            "service": svc,
            "action": action,
            "status": resp.status_code,
            "duration_ms": duration_ms,
        })
        return resp

    return app


_TARGET_MAP = {
    "DynamoDB": "dynamodb",
    "DynamoDBStreams": "dynamodb",
    "AmazonSQS": "sqs",
    "AmazonSNS": "sns",
    "AWSCognitoIdentityProviderService": "cognito",
    "Logs": "logs",
    "Kinesis": "kinesis",
    "AWSStepFunctions": "stepfunctions",
    "TrentService": "kms",
    "secretsmanager": "secretsmanager",
    "AmazonSSM": "ssm",
    "AWSEvents": "events",
    "OpenSearchService": "opensearch",
    "es": "opensearch",
    # New boto3 CloudWatch uses JSON protocol with this target prefix
    "GraniteServiceVersion20100801": "cloudwatch",
    "CloudWatch": "cloudwatch",
    "CertificateManager": "acm",
    "ACM": "acm",
}

_PATH_PREFIXES = {
    "2015-03-31/functions": "lambda",
    "2015-03-31/event-source-mappings": "lambda",
    "2017-03-31/tags": "lambda",
    "restapis": "apigateway",
    "v2/apis": "apigateway",
    "2021-01-01/opensearch": "opensearch",
    "2021-01-01/tags": "opensearch",
    "2015-01-01/es": "opensearch",
    "_cluster": "opensearch",
    "_cat": "opensearch",
    "_nodes": "opensearch",
    "_bulk": "opensearch",
    "_search": "opensearch",
    "_mget": "opensearch",
    "_index_template": "opensearch",
    "_aliases": "opensearch",
    "2013-04-01": "route53",
}

_ACTION_SERVICES = {
    "iam": ["CreateRole","DeleteRole","ListRoles","GetRole","CreatePolicy","AttachRolePolicy","CreateUser","DeleteUser","ListUsers","GetUser","DetachRolePolicy","DeletePolicy","ListPolicies"],
    "sts": ["AssumeRole","GetCallerIdentity","GetSessionToken","AssumeRoleWithSAML","AssumeRoleWithWebIdentity"],
    "sns": ["CreateTopic","DeleteTopic","ListTopics","Subscribe","Unsubscribe","Publish","ListSubscriptions","ListSubscriptionsByTopic","GetTopicAttributes","SetTopicAttributes"],
    "cloudformation": ["CreateStack","DeleteStack","ListStacks","DescribeStacks","UpdateStack","DescribeStackResources","DescribeStackEvents","GetTemplate"],
    "rds": ["CreateDBInstance","DeleteDBInstance","DescribeDBInstances","CreateDBCluster","DeleteDBCluster","DescribeDBClusters"],
    "events": ["PutRule","DeleteRule","ListRules","PutTargets","RemoveTargets","PutEvents","DescribeRule"],
    "opensearch": ["CreateDomain","DeleteDomain","DescribeDomain","DescribeDomains","ListDomainNames","AddTags","ListTags","RemoveTags","UpdateDomainConfig","GetDomainNames"],
    "cloudwatch": ["PutMetricData","GetMetricStatistics","GetMetricData","ListMetrics","PutMetricAlarm","DescribeAlarms","SetAlarmState","DeleteAlarms","EnableAlarmActions","DisableAlarmActions"],
    "ses": ["SendEmail","SendRawEmail","VerifyEmailIdentity","VerifyDomainIdentity","ListIdentities","GetSendQuota","GetSendStatistics","DeleteIdentity"],
    "ec2": ["DescribeInstances","RunInstances","TerminateInstances","StartInstances","StopInstances",
            "DescribeInstanceStatus","DescribeVpcs","DescribeSubnets","DescribeSecurityGroups",
            "CreateSecurityGroup","DeleteSecurityGroup","AuthorizeSecurityGroupIngress","AuthorizeSecurityGroupEgress",
            "DescribeKeyPairs","CreateKeyPair","DeleteKeyPair","DescribeImages","DescribeRegions",
            "DescribeAvailabilityZones","DescribeVolumes","CreateVolume","DeleteVolume",
            "DescribeInstanceTypes"],
}


def _detect_service(req, path):
    target = req.headers.get("X-Amz-Target", "")
    if target and "." in target:
        prefix = target.rsplit(".", 1)[0]
        # strip version suffix like DynamoDB_20120810
        clean = prefix.split("_")[0] if "_" in prefix else prefix
        if clean in _TARGET_MAP:
            return _TARGET_MAP[clean]

    auth = req.headers.get("Authorization", "")
    if "Credential=" in auth:
        parts = auth.split("Credential=")[1].split(",")[0].split("/")
        if len(parts) >= 4:
            svc = parts[3]
            svc_map = {
                "s3": "s3", "sqs": "sqs", "dynamodb": "dynamodb", "sns": "sns",
                "lambda": "lambda", "iam": "iam", "logs": "logs",
                "monitoring": "cloudwatch", "sts": "sts",
                "secretsmanager": "secretsmanager", "ssm": "ssm", "events": "events",
                "cloudformation": "cloudformation", "rds": "rds",
                "execute-api": "apigateway", "apigateway": "apigateway",
                "es": "opensearch", "opensearch": "opensearch",
                "kinesis": "kinesis", "states": "stepfunctions",
                "email": "ses", "ses": "ses", "cognito": "cognito",
                "kms": "kms", "ec2": "ec2", "acm": "acm", "route53": "route53",
            }
            if svc in svc_map:
                return svc_map[svc]

    for prefix, svc in _PATH_PREFIXES.items():
        if path.startswith(prefix):
            return svc

    action = req.args.get("Action") or req.form.get("Action", "")
    if not action:
        # Try JSON body for action (e.g. OpenSearch control-plane)
        try:
            _body = req.get_json(force=True, silent=True) or {}
            action = _body.get("Action", "")
        except Exception:
            pass
    if action:
        for svc, actions in _ACTION_SERVICES.items():
            if action in actions:
                return svc

    # OpenSearch data-plane: paths with a second segment that is an OS operation,
    # or a root call (empty path) with JSON content that looks like an OS query.
    _OS_OPS = {
        "_doc", "_create", "_update", "_search", "_bulk", "_count",
        "_mapping", "_mappings", "_settings", "_alias", "_mget",
        "_delete_by_query", "_update_by_query", "_refresh", "_flush",
        "_forcemerge", "_stats",
    }
    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[1] in _OS_OPS:
        return "opensearch"

    # Single-segment path with JSON body or no AWS auth headers → OpenSearch index op
    ct = req.content_type or ""
    has_aws_auth = bool(req.headers.get("Authorization") or req.headers.get("x-amz-content-sha256"))
    if (len(parts) == 1 and parts[0] and not has_aws_auth
            and req.method in ("PUT", "DELETE", "GET", "HEAD")):
        return "opensearch"
    if (len(parts) == 1 and parts[0]
            and "json" in ct
            and req.method in ("PUT", "DELETE", "GET", "HEAD")):
        return "opensearch"

    # Root path with no auth → OpenSearch cluster info
    if not path.strip("/") and not has_aws_auth and req.method == "GET":
        return "opensearch"

    if path.strip("/"):
        return "s3"

    return None
