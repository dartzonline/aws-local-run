"""CloudWatch Metrics service emulator."""
import json
import logging
import time
import uuid
from urllib.parse import parse_qs

from flask import Request, Response

from localrun.config import get_config
from localrun.utils import new_request_id

logger = logging.getLogger("localrun.cloudwatch")

CW_NS = "http://monitoring.amazonaws.com/doc/2010-08-01/"


def _xml(action, content):
    body = (
        f'<?xml version="1.0"?>\n'
        f'<{action}Response xmlns="{CW_NS}">\n'
        f"  <{action}Result>\n{content}\n  </{action}Result>\n"
        f"  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>\n"
        f"</{action}Response>"
    )
    return Response(body, 200, content_type="application/xml")


def _ok_xml(action):
    return _xml(action, "")


def _json_resp(data, status=200):
    return Response(json.dumps(data), status, content_type="application/x-amz-json-1.0")


class CloudWatchService:
    def __init__(self):
        # metrics: (namespace, metric_name) -> list of data points
        # each data point: {Timestamp, Value, Unit}
        self.metrics = {}
        # alarms: alarm_name -> alarm dict
        self.alarms = {}

    def handle(self, req, path):
        # New boto3 sends X-Amz-Target with JSON; old protocol uses form-encoded body
        target = req.headers.get("X-Amz-Target", "")
        if target and "." in target:
            action = target.split(".")[-1]
        else:
            action = req.args.get("Action") or req.form.get("Action", "")
            if not action:
                raw_params = parse_qs(req.get_data(as_text=True))
                action = raw_params.get("Action", [""])[0]

        actions = {
            "PutMetricData": self._put_metric_data,
            "GetMetricStatistics": self._get_metric_statistics,
            "GetMetricData": self._get_metric_data,
            "ListMetrics": self._list_metrics,
            "PutMetricAlarm": self._put_metric_alarm,
            "DescribeAlarms": self._describe_alarms,
            "SetAlarmState": self._set_alarm_state,
            "DeleteAlarms": self._delete_alarms,
            "DescribeAlarmsForMetric": self._describe_alarms_for_metric,
            "EnableAlarmActions": self._stub_ok,
            "DisableAlarmActions": self._stub_ok,
        }
        handler = actions.get(action)
        if not handler:
            from localrun.utils import error_response
            return error_response("InvalidAction", f"Invalid action: {action}", 400)
        return handler(req)

    def _is_json(self, req):
        ct = req.content_type or ""
        return "json" in ct

    def _json(self, req):
        try:
            return json.loads(req.get_data(as_text=True) or "{}")
        except Exception:
            return {}

    def _p(self, req):
        params = dict(req.args)
        if req.content_type and "form" in req.content_type:
            params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items():
                params[k] = v[0] if len(v) == 1 else v
        return params

    def _put_metric_data(self, req):
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if self._is_json(req):
            body = self._json(req)
            namespace = body.get("Namespace", "")
            for item in body.get("MetricData", []):
                name = item.get("MetricName", "")
                value = float(item.get("Value", 0))
                unit = item.get("Unit", "None")
                ts = item.get("Timestamp", now)
                key = (namespace, name)
                if key not in self.metrics:
                    self.metrics[key] = []
                self.metrics[key].append({"Timestamp": ts, "Value": value, "Unit": unit})
            logger.info("PutMetricData namespace=%s (%d points)", namespace, len(body.get("MetricData", [])))
            return _json_resp({})
        # Form-encoded (old protocol)
        p = self._p(req)
        namespace = p.get("Namespace", "")
        i = 1
        while f"MetricData.member.{i}.MetricName" in p:
            name = p[f"MetricData.member.{i}.MetricName"]
            value = float(p.get(f"MetricData.member.{i}.Value", 0))
            unit = p.get(f"MetricData.member.{i}.Unit", "None")
            ts = p.get(f"MetricData.member.{i}.Timestamp", now)
            key = (namespace, name)
            if key not in self.metrics:
                self.metrics[key] = []
            self.metrics[key].append({"Timestamp": ts, "Value": value, "Unit": unit})
            i += 1
        logger.info("PutMetricData namespace=%s (%d points)", namespace, i - 1)
        return _ok_xml("PutMetricData")

    def _get_metric_statistics(self, req):
        if self._is_json(req):
            p = self._json(req)
            namespace = p.get("Namespace", "")
            metric_name = p.get("MetricName", "")
            # boto3 sends Unix timestamps (floats); convert to ISO string for comparison
            start_raw = p.get("StartTime")
            end_raw = p.get("EndTime")
            import datetime as _dt
            if start_raw is not None:
                start = _dt.datetime.fromtimestamp(float(start_raw), tz=_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                start = ""
            if end_raw is not None:
                end = _dt.datetime.fromtimestamp(float(end_raw), tz=_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                end = ""
        else:
            p = self._p(req)
            namespace = p.get("Namespace", "")
            metric_name = p.get("MetricName", "")
            start = p.get("StartTime", "")
            end = p.get("EndTime", "")
        key = (namespace, metric_name)
        points = self.metrics.get(key, [])

        # Filter by time range if provided
        if start or end:
            filtered = []
            for pt in points:
                ts = pt["Timestamp"]
                if start and ts < start:
                    continue
                if end and ts > end:
                    continue
                filtered.append(pt)
            points = filtered

        values = [pt["Value"] for pt in points]
        if values:
            count = len(values)
            total = sum(values)
            avg = total / count
            mn = min(values)
            mx = max(values)
        else:
            count, total, avg, mn, mx = 0, 0.0, 0.0, 0.0, 0.0

        dp_xml = ""
        if points:
            dp_xml = (
                "    <member>\n"
                f"      <Timestamp>{points[-1]['Timestamp']}</Timestamp>\n"
                f"      <SampleCount>{count}</SampleCount>\n"
                f"      <Sum>{total}</Sum>\n"
                f"      <Average>{avg}</Average>\n"
                f"      <Minimum>{mn}</Minimum>\n"
                f"      <Maximum>{mx}</Maximum>\n"
                f"      <Unit>{points[-1].get('Unit', 'None')}</Unit>\n"
                "    </member>"
            )

        if self._is_json(req):
            dp = []
            if points:
                dp.append({"Timestamp": points[-1]["Timestamp"], "SampleCount": count,
                           "Sum": total, "Average": avg, "Minimum": mn, "Maximum": mx,
                           "Unit": points[-1].get("Unit", "None")})
            return _json_resp({"Datapoints": dp, "Label": metric_name})
        content = f"    <Datapoints>\n{dp_xml}\n    </Datapoints>\n    <Label>{metric_name}</Label>"
        return _xml("GetMetricStatistics", content)

    def _get_metric_data(self, req):
        if self._is_json(req):
            body = self._json(req)
            results = []
            for q in body.get("MetricDataQueries", []):
                results.append({"Id": q.get("Id", ""), "StatusCode": "Complete",
                                "Timestamps": [], "Values": []})
            return _json_resp({"MetricDataResults": results})
        p = self._p(req)
        # Form-encoded: simplified, just return empty results
        results_xml = ""
        i = 1
        while f"MetricDataQueries.member.{i}.Id" in p:
            qid = p[f"MetricDataQueries.member.{i}.Id"]
            results_xml += (
                f"    <member>\n"
                f"      <Id>{qid}</Id>\n"
                f"      <StatusCode>Complete</StatusCode>\n"
                f"      <Timestamps/>\n"
                f"      <Values/>\n"
                f"    </member>\n"
            )
            i += 1
        content = f"    <MetricDataResults>\n{results_xml}    </MetricDataResults>"
        return _xml("GetMetricData", content)

    def _list_metrics(self, req):
        if self._is_json(req):
            p = self._json(req)
        else:
            p = self._p(req)
        namespace_filter = p.get("Namespace", "")
        name_filter = p.get("MetricName", "")
        results = []
        for (ns, name) in sorted(self.metrics.keys()):
            if namespace_filter and ns != namespace_filter:
                continue
            if name_filter and name != name_filter:
                continue
            results.append({"Namespace": ns, "MetricName": name, "Dimensions": []})
        if self._is_json(req):
            return _json_resp({"Metrics": results})
        metrics_xml = ""
        for m in results:
            metrics_xml += (
                "    <member>\n"
                f"      <Namespace>{m['Namespace']}</Namespace>\n"
                f"      <MetricName>{m['MetricName']}</MetricName>\n"
                "      <Dimensions/>\n"
                "    </member>\n"
            )
        content = f"    <Metrics>\n{metrics_xml}    </Metrics>"
        return _xml("ListMetrics", content)

    def _put_metric_alarm(self, req):
        if self._is_json(req):
            p = self._json(req)
            actions_enabled = p.get("ActionsEnabled", True)
            if isinstance(actions_enabled, str):
                actions_enabled = actions_enabled.lower() == "true"
        else:
            p = self._p(req)
            actions_enabled = p.get("ActionsEnabled", "true").lower() == "true"
        name = p.get("AlarmName", "")
        if not name:
            from localrun.utils import error_response
            return error_response("InvalidParameterValue", "AlarmName required")
        c = get_config()
        arn = f"arn:aws:cloudwatch:{c.region}:{c.account_id}:alarm:{name}"
        self.alarms[name] = {
            "AlarmName": name,
            "AlarmArn": arn,
            "MetricName": p.get("MetricName", ""),
            "Namespace": p.get("Namespace", ""),
            "ComparisonOperator": p.get("ComparisonOperator", ""),
            "Threshold": float(p.get("Threshold", 0)),
            "Period": int(p.get("Period", 60)),
            "EvaluationPeriods": int(p.get("EvaluationPeriods", 1)),
            "Statistic": p.get("Statistic", "Average"),
            "AlarmDescription": p.get("AlarmDescription", ""),
            "StateValue": "INSUFFICIENT_DATA",
            "ActionsEnabled": actions_enabled,
        }
        logger.info("Created alarm: %s", name)
        if self._is_json(req):
            return _json_resp({})
        return _ok_xml("PutMetricAlarm")

    def _describe_alarms(self, req):
        if self._is_json(req):
            p = self._json(req)
            alarm_names = p.get("AlarmNames", [])
            prefix = p.get("AlarmNamePrefix", "")
            state = p.get("StateValue", "")
        else:
            p = self._p(req)
            alarm_names = []
            prefix = p.get("AlarmNamePrefix", "")
            state = p.get("StateValue", "")
        matching = []
        for alarm in self.alarms.values():
            if alarm_names and alarm["AlarmName"] not in alarm_names:
                continue
            if prefix and not alarm["AlarmName"].startswith(prefix):
                continue
            if state and alarm["StateValue"] != state:
                continue
            matching.append(alarm)
        if self._is_json(req):
            return _json_resp({"MetricAlarms": matching})
        alarms_xml = "".join(self._alarm_xml(a) for a in matching)
        content = f"    <MetricAlarms>\n{alarms_xml}    </MetricAlarms>"
        return _xml("DescribeAlarms", content)

    def _describe_alarms_for_metric(self, req):
        if self._is_json(req):
            p = self._json(req)
        else:
            p = self._p(req)
        metric_name = p.get("MetricName", "")
        namespace = p.get("Namespace", "")
        matching = [a for a in self.alarms.values()
                    if a["MetricName"] == metric_name and a["Namespace"] == namespace]
        if self._is_json(req):
            return _json_resp({"MetricAlarms": matching})
        alarms_xml = ""
        for alarm in matching:
            alarms_xml += self._alarm_xml(alarm)
        content = f"    <MetricAlarms>\n{alarms_xml}    </MetricAlarms>"
        return _xml("DescribeAlarmsForMetric", content)

    def _alarm_xml(self, alarm):
        return (
            "      <member>\n"
            f"        <AlarmName>{alarm['AlarmName']}</AlarmName>\n"
            f"        <AlarmArn>{alarm['AlarmArn']}</AlarmArn>\n"
            f"        <MetricName>{alarm['MetricName']}</MetricName>\n"
            f"        <Namespace>{alarm['Namespace']}</Namespace>\n"
            f"        <StateValue>{alarm['StateValue']}</StateValue>\n"
            f"        <ComparisonOperator>{alarm['ComparisonOperator']}</ComparisonOperator>\n"
            f"        <Threshold>{alarm['Threshold']}</Threshold>\n"
            f"        <Period>{alarm['Period']}</Period>\n"
            "      </member>\n"
        )

    def _set_alarm_state(self, req):
        if self._is_json(req):
            p = self._json(req)
        else:
            p = self._p(req)
        name = p.get("AlarmName", "")
        state = p.get("StateValue", "")
        alarm = self.alarms.get(name)
        if not alarm:
            from localrun.utils import error_response
            return error_response("ResourceNotFound", f"Alarm {name} not found", 404)
        alarm["StateValue"] = state
        logger.info("Set alarm %s state to %s", name, state)
        if self._is_json(req):
            return _json_resp({})
        return _ok_xml("SetAlarmState")

    def _delete_alarms(self, req):
        if self._is_json(req):
            p = self._json(req)
            for name in p.get("AlarmNames", []):
                self.alarms.pop(name, None)
                logger.info("Deleted alarm: %s", name)
        else:
            p = self._p(req)
            i = 1
            while f"AlarmNames.member.{i}" in p:
                name = p[f"AlarmNames.member.{i}"]
                self.alarms.pop(name, None)
                logger.info("Deleted alarm: %s", name)
                i += 1
        if self._is_json(req):
            return _json_resp({})
        return _ok_xml("DeleteAlarms")

    def _stub_ok(self, req):
        target = req.headers.get("X-Amz-Target", "")
        if target and "." in target:
            action = target.split(".")[-1]
        else:
            p = self._p(req)
            action = p.get("Action", "")
        return _ok_xml(action)

    def reset(self):
        self.metrics = {}
        self.alarms = {}
