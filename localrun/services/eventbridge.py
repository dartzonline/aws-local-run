"""EventBridge service emulator."""
import json, logging, time, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import parse_json_body, json_error, new_request_id

logger = logging.getLogger("localrun.events")

def _resp(data, status=200):
    return Response(json.dumps(data, default=str), status, content_type="application/x-amz-json-1.1")

class EventBridgeService:
    def __init__(self):
        self.rules = {}       # name -> rule dict
        self.targets = {}     # rule_name -> [targets]
        self.event_buses = {"default": {"Name": "default"}}
        self.events_log = []  # stored for debugging
        # injected by gateway after all engines are built
        self.sqs = None
        self.sns = None
        self.lambda_svc = None

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        if not action:
            action = req.args.get("Action") or req.form.get("Action", "")
        body = parse_json_body(req)
        handlers = {
            "PutRule": self._put_rule, "DeleteRule": self._delete_rule,
            "ListRules": self._list_rules, "DescribeRule": self._describe_rule,
            "PutTargets": self._put_targets, "RemoveTargets": self._remove_targets,
            "ListTargetsByRule": self._list_targets, "PutEvents": self._put_events,
            "CreateEventBus": self._create_bus, "DeleteEventBus": self._delete_bus,
            "ListEventBuses": self._list_buses,
            "DisableRule": self._disable_rule, "EnableRule": self._enable_rule,
        }
        h = handlers.get(action)
        if not h:
            return json_error("InvalidAction", f"Unknown action: {action}")
        return h(body)

    def _arn(self, name):
        c = get_config()
        return f"arn:aws:events:{c.region}:{c.account_id}:rule/{name}"

    def _put_rule(self, body):
        name = body.get("Name", "")
        if not name: return json_error("ValidationException", "Name required")
        self.rules[name] = {
            "Name": name, "Arn": self._arn(name),
            "State": body.get("State", "ENABLED"),
            "EventPattern": body.get("EventPattern", ""),
            "ScheduleExpression": body.get("ScheduleExpression", ""),
            "Description": body.get("Description", ""),
            "EventBusName": body.get("EventBusName", "default"),
        }
        self.targets.setdefault(name, [])
        logger.info("Put rule: %s", name)
        return _resp({"RuleArn": self.rules[name]["Arn"]})

    def _delete_rule(self, body):
        name = body.get("Name", "")
        self.rules.pop(name, None)
        self.targets.pop(name, None)
        return _resp({})

    def _list_rules(self, body):
        prefix = body.get("NamePrefix", "")
        rules = [r for r in self.rules.values() if not prefix or r["Name"].startswith(prefix)]
        return _resp({"Rules": rules})

    def _describe_rule(self, body):
        name = body.get("Name", "")
        r = self.rules.get(name)
        if not r: return json_error("ResourceNotFoundException", "Rule not found", 404)
        return _resp(r)

    def _put_targets(self, body):
        rule = body.get("Rule", "")
        if rule not in self.rules: return json_error("ResourceNotFoundException", "Rule not found", 404)
        new_targets = body.get("Targets", [])
        existing = {t["Id"]: t for t in self.targets.get(rule, [])}
        for t in new_targets:
            existing[t["Id"]] = t
        self.targets[rule] = list(existing.values())
        return _resp({"FailedEntryCount": 0, "FailedEntries": []})

    def _remove_targets(self, body):
        rule = body.get("Rule", "")
        ids = body.get("Ids", [])
        if rule in self.targets:
            self.targets[rule] = [t for t in self.targets[rule] if t["Id"] not in ids]
        return _resp({"FailedEntryCount": 0, "FailedEntries": []})

    def _list_targets(self, body):
        rule = body.get("Rule", "")
        return _resp({"Targets": self.targets.get(rule, [])})

    def _put_events(self, body):
        entries = body.get("Entries", [])
        results = []
        for e in entries:
            eid = str(uuid.uuid4())
            self.events_log.append({"EventId": eid, **e})
            results.append({"EventId": eid})
            # Try to route this event to matching rule targets
            self._route_event(e)
        return _resp({"FailedEntryCount": 0, "Entries": results})

    def _event_matches_pattern(self, event, pattern_str):
        """Check if an event matches a rule's EventPattern.

        AWS uses subset-matching: every key in the pattern must appear in the event
        with a matching value. Pattern values can be a list of allowed values.
        Supports nested matching (e.g. {"detail": {"type": ["order"]}}).
        """
        if not pattern_str:
            return False
        try:
            pattern = json.loads(pattern_str)
        except Exception:
            return False
        return self._deep_match(pattern, event)

    def _deep_match(self, pattern, data):
        """Recursively check that every key in pattern matches in data."""
        if not isinstance(pattern, dict) or not isinstance(data, dict):
            return False
        for key, expected in pattern.items():
            actual = data.get(key)
            if isinstance(expected, dict):
                # nested match (e.g. pattern={"detail": {"status": ["completed"]}})
                if not self._deep_match(expected, actual if isinstance(actual, dict) else {}):
                    return False
            elif isinstance(expected, list):
                if actual not in expected:
                    return False
            else:
                if actual != expected:
                    return False
        return True

    def _route_event(self, event):
        """Send event to targets of all enabled matching rules."""
        for rule_name, rule in self.rules.items():
            if rule.get("State") != "ENABLED":
                continue
            pattern = rule.get("EventPattern", "")
            if not pattern or not self._event_matches_pattern(event, pattern):
                continue
            for target in self.targets.get(rule_name, []):
                self._dispatch_to_target(target, event)

    def _dispatch_to_target(self, target, event):
        arn = target.get("Arn", "")
        event_body = json.dumps(event)
        if ":sqs:" in arn and self.sqs:
            queue_name = arn.split(":")[-1]
            from localrun.services.sqs import SQSMessage
            from localrun.utils import new_message_id
            queue_url = self.sqs._url(queue_name)
            q = self.sqs.queues.get(queue_url)
            if q:
                q.messages.append(SQSMessage(message_id=new_message_id(), body=event_body))
                logger.info("EventBridge routed to SQS: %s", queue_name)
            else:
                logger.warning("EventBridge: SQS queue not found: %s", queue_name)
        elif ":sns:" in arn and self.sns:
            topic = self.sns.topics.get(arn)
            if topic:
                msg_id = str(uuid.uuid4())
                logger.info("EventBridge routed to SNS topic: %s", arn)
                subject = event.get("detail-type", "EventBridgeEvent")
                for sub in topic.subscriptions:
                    if sub.protocol == "sqs" and self.sqs:
                        self.sns._deliver_to_sqs(arn, sub.endpoint, event_body, msg_id, subject)
                    elif sub.protocol == "lambda" and self.sns.lambda_svc:
                        self.sns._deliver_to_lambda(arn, sub.endpoint, event_body, msg_id, subject)
            else:
                logger.warning("EventBridge: SNS topic not found: %s", arn)
        elif ":lambda:" in arn or ":function:" in arn:
            self._dispatch_to_lambda(arn, event_body)
        else:
            logger.debug("EventBridge: no handler for target ARN: %s", arn)

    def _dispatch_to_lambda(self, fn_arn, event_body):
        if not self.lambda_svc:
            logger.warning("EventBridge: Lambda not wired, cannot invoke %s", fn_arn)
            return
        fn_name = fn_arn.split(":")[-1]
        # strip qualifier if present (e.g. function:myFunc:$LATEST)
        if fn_name.startswith("$") or fn_name.isdigit():
            parts = fn_arn.split(":")
            fn_name = parts[-2] if len(parts) > 1 else fn_name
        fn = self.lambda_svc.functions.get(fn_name)
        if not fn:
            logger.warning("EventBridge: Lambda function not found: %s", fn_name)
            return
        import threading
        t = threading.Thread(
            target=self.lambda_svc._execute_function, args=(fn, event_body), daemon=True
        )
        t.start()
        logger.info("EventBridge routed to Lambda: %s", fn_name)

    def _create_bus(self, body):
        name = body.get("Name", "")
        c = get_config()
        arn = f"arn:aws:events:{c.region}:{c.account_id}:event-bus/{name}"
        self.event_buses[name] = {"Name": name, "Arn": arn}
        return _resp({"EventBusArn": arn})

    def _delete_bus(self, body):
        name = body.get("Name", "")
        if name == "default": return json_error("ValidationException", "Cannot delete default bus")
        self.event_buses.pop(name, None)
        return _resp({})

    def _list_buses(self, body):
        return _resp({"EventBuses": list(self.event_buses.values())})

    def _disable_rule(self, body):
        name = body.get("Name", "")
        if name in self.rules:
            self.rules[name]["State"] = "DISABLED"
        return _resp({})

    def _enable_rule(self, body):
        name = body.get("Name", "")
        if name in self.rules:
            self.rules[name]["State"] = "ENABLED"
        return _resp({})

    def reset(self):
        self.rules = {}
        self.targets = {}
        self.event_buses = {"default": {"Name": "default"}}
        self.events_log = []
