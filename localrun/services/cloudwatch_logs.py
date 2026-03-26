"""CloudWatch Logs service emulator."""
import json, logging, time, uuid
from dataclasses import dataclass, field
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import new_request_id

logger = logging.getLogger("localrun.logs")

def _json_resp(data, status=200):
    return Response(json.dumps(data, default=str), status=status, content_type="application/x-amz-json-1.1")

def _json_err(code, msg, status=400):
    return Response(json.dumps({"__type": code, "message": msg}), status=status, content_type="application/x-amz-json-1.1")

@dataclass
class LogStream:
    name: str; creation_time: int = 0; events: list = field(default_factory=list)
    first_event_ts: int = 0; last_event_ts: int = 0; upload_seq: str = "1"
    def __post_init__(self):
        if not self.creation_time: self.creation_time = int(time.time() * 1000)

@dataclass
class LogGroup:
    name: str; arn: str; creation_time: int = 0; retention_days: int = 0
    streams: dict = field(default_factory=dict)  # stream_name -> LogStream
    tags: dict = field(default_factory=dict)
    def __post_init__(self):
        if not self.creation_time: self.creation_time = int(time.time() * 1000)

class CloudWatchLogsService:
    def __init__(self):
        self.log_groups = {}
        # metric_filters: log_group_name -> list of filter dicts
        self.metric_filters = {}

    def handle(self, req: Request, path: str) -> Response:
        target = req.headers.get("X-Amz-Target", "")
        action = target.split(".")[-1] if "." in target else ""
        body = {}
        try:
            body = json.loads(req.get_data(as_text=True) or "{}")
        except (json.JSONDecodeError, Exception):
            pass
        actions = {
            "CreateLogGroup": self._create_log_group, "DeleteLogGroup": self._delete_log_group,
            "DescribeLogGroups": self._describe_log_groups,
            "CreateLogStream": self._create_log_stream, "DeleteLogStream": self._delete_log_stream,
            "DescribeLogStreams": self._describe_log_streams,
            "PutLogEvents": self._put_log_events, "GetLogEvents": self._get_log_events,
            "FilterLogEvents": self._filter_log_events,
            "PutRetentionPolicy": self._put_retention, "DeleteRetentionPolicy": self._delete_retention,
            "TagLogGroup": self._tag_log_group, "UntagLogGroup": self._untag_log_group,
            "ListTagsLogGroup": self._list_tags_log_group,
            "PutMetricFilter": self._put_metric_filter, "DeleteMetricFilter": self._delete_metric_filter,
            "DescribeMetricFilters": self._describe_metric_filters,
        }
        handler = actions.get(action)
        if not handler: return _json_err("UnknownOperationException", f"Unknown: {action}")
        return handler(body)

    def _create_log_group(self, body):
        name = body.get("logGroupName", "")
        if not name: return _json_err("InvalidParameterException", "logGroupName required")
        if name in self.log_groups: return _json_err("ResourceAlreadyExistsException", "Log group exists")
        c = get_config(); arn = f"arn:aws:logs:{c.region}:{c.account_id}:log-group:{name}"
        self.log_groups[name] = LogGroup(name=name, arn=arn)
        logger.info("Created log group: %s", name)
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _delete_log_group(self, body):
        name = body.get("logGroupName", "")
        if name not in self.log_groups: return _json_err("ResourceNotFoundException", "Log group not found")
        del self.log_groups[name]
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _describe_log_groups(self, body):
        prefix = body.get("logGroupNamePrefix", "")
        groups = []
        for name, group in sorted(self.log_groups.items()):
            if prefix and not name.startswith(prefix): continue
            groups.append({
                "logGroupName": name, "arn": group.arn, "creationTime": group.creation_time,
                "retentionInDays": group.retention_days or None,
                "storedBytes": sum(len(json.dumps(e)) for s in group.streams.values() for e in s.events),
            })
        return _json_resp({"logGroups": groups})

    def _create_log_stream(self, body):
        group_name = body.get("logGroupName", "")
        stream_name = body.get("logStreamName", "")
        group = self.log_groups.get(group_name)
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        if stream_name in group.streams: return _json_err("ResourceAlreadyExistsException", "Stream exists")
        group.streams[stream_name] = LogStream(name=stream_name)
        logger.info("Created log stream: %s/%s", group_name, stream_name)
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _delete_log_stream(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        group.streams.pop(body.get("logStreamName", ""), None)
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _describe_log_streams(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        prefix = body.get("logStreamNamePrefix", "")
        streams = []
        for name, stream in sorted(group.streams.items()):
            if prefix and not name.startswith(prefix): continue
            streams.append({
                "logStreamName": name, "creationTime": stream.creation_time,
                "firstEventTimestamp": stream.first_event_ts, "lastEventTimestamp": stream.last_event_ts,
                "uploadSequenceToken": stream.upload_seq,
                "storedBytes": sum(len(json.dumps(e)) for e in stream.events),
            })
        return _json_resp({"logStreams": streams})

    def _put_log_events(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        stream = group.streams.get(body.get("logStreamName", ""))
        if not stream: return _json_err("ResourceNotFoundException", "Log stream not found")
        events = body.get("logEvents", [])
        for event in events:
            stream.events.append({"timestamp": event.get("timestamp", int(time.time()*1000)), "message": event.get("message", "")})
        if stream.events:
            stream.first_event_ts = stream.events[0]["timestamp"]
            stream.last_event_ts = stream.events[-1]["timestamp"]
        stream.upload_seq = str(int(stream.upload_seq) + 1)
        return _json_resp({"nextSequenceToken": stream.upload_seq})

    def _get_log_events(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        stream = group.streams.get(body.get("logStreamName", ""))
        if not stream: return _json_err("ResourceNotFoundException", "Log stream not found")
        start = body.get("startTime", 0); end = body.get("endTime", int(time.time()*1000)+999999)
        limit = body.get("limit", 10000)
        events = [e for e in stream.events if start <= e["timestamp"] <= end][:limit]
        return _json_resp({"events": events, "nextForwardToken": "f/0", "nextBackwardToken": "b/0"})

    def _filter_log_events(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        pattern = body.get("filterPattern", "").lower()
        stream_names = body.get("logStreamNames", list(group.streams.keys()))
        events = []
        for sn in stream_names:
            stream = group.streams.get(sn)
            if not stream: continue
            for e in stream.events:
                if not pattern or pattern in e["message"].lower():
                    events.append({**e, "logStreamName": sn})
        limit = body.get("limit", 10000)
        return _json_resp({"events": events[:limit], "searchedLogStreams": [{"logStreamName": sn, "searchedCompletely": True} for sn in stream_names]})

    def _put_retention(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        group.retention_days = body.get("retentionInDays", 0)
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _delete_retention(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        group.retention_days = 0
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _tag_log_group(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        group.tags.update(body.get("tags", {}))
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _untag_log_group(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        for k in body.get("tags", []):
            group.tags.pop(k, None)
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _list_tags_log_group(self, body):
        group = self.log_groups.get(body.get("logGroupName", ""))
        if not group: return _json_err("ResourceNotFoundException", "Log group not found")
        return _json_resp({"tags": group.tags})

    def _put_metric_filter(self, body):
        group_name = body.get("logGroupName", "")
        filter_name = body.get("filterName", "")
        if not group_name or not filter_name:
            return _json_err("InvalidParameterException", "logGroupName and filterName required")
        if group_name not in self.metric_filters:
            self.metric_filters[group_name] = []
        # Replace if filter with same name already exists
        self.metric_filters[group_name] = [f for f in self.metric_filters[group_name] if f["filterName"] != filter_name]
        self.metric_filters[group_name].append({
            "filterName": filter_name,
            "filterPattern": body.get("filterPattern", ""),
            "metricTransformations": body.get("metricTransformations", []),
            "logGroupName": group_name,
        })
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _delete_metric_filter(self, body):
        group_name = body.get("logGroupName", "")
        filter_name = body.get("filterName", "")
        if group_name in self.metric_filters:
            self.metric_filters[group_name] = [f for f in self.metric_filters[group_name] if f["filterName"] != filter_name]
        return Response("", 200, content_type="application/x-amz-json-1.1")

    def _describe_metric_filters(self, body):
        group_name = body.get("logGroupName", "")
        filters = self.metric_filters.get(group_name, []) if group_name else []
        if not group_name:
            # Return all filters across all groups
            filters = [f for group_filters in self.metric_filters.values() for f in group_filters]
        return _json_resp({"metricFilters": filters})

    def reset(self):
        self.log_groups = {}
        self.metric_filters = {}
