"""SQS service emulator — supports both XML (query) and JSON (amz-json) protocols."""
import hashlib, json, logging, time, uuid
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import parse_qs
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, md5_hex, new_message_id, new_request_id

logger = logging.getLogger("localrun.sqs")

@dataclass
class SQSMessage:
    message_id: str; body: str; receipt_handle: Optional[str] = None
    md5_of_body: str = ""; sent_timestamp: int = 0; visible_after: float = 0; receive_count: int = 0
    deduplication_id: str = ""; group_id: str = ""
    message_attributes: dict = field(default_factory=dict)
    def __post_init__(self):
        if not self.md5_of_body: self.md5_of_body = md5_hex(self.body.encode())
        if not self.sent_timestamp: self.sent_timestamp = int(time.time() * 1000)

@dataclass
class SQSQueue:
    name: str; url: str; arn: str
    attributes: dict = field(default_factory=dict)
    messages: list = field(default_factory=list)
    tags: dict = field(default_factory=dict)
    is_fifo: bool = False
    # dedup_id -> (message_id, timestamp) — used to reject duplicates within 5 min window
    dedup_ids: dict = field(default_factory=dict)
    # group_id -> list of message_ids
    message_groups: dict = field(default_factory=dict)
    redrive_policy: dict = field(default_factory=dict)
    dlq: object = field(default=None)  # reference to another SQSQueue

    def __post_init__(self):
        self.is_fifo = self.name.endswith(".fifo")
        ts = str(int(time.time()))
        defaults = {"VisibilityTimeout": "30", "DelaySeconds": "0", "MaximumMessageSize": "262144",
                     "MessageRetentionPeriod": "345600", "ReceiveMessageWaitTimeSeconds": "0",
                     "ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0",
                     "CreatedTimestamp": ts, "LastModifiedTimestamp": ts, "QueueArn": self.arn}
        for k, v in defaults.items(): self.attributes.setdefault(k, v)
        if self.is_fifo:
            self.attributes.setdefault("FifoQueue", "true")
            self.attributes.setdefault("ContentBasedDeduplication", "false")


class SQSService:
    def __init__(self):
        self.queues = {}  # url -> SQSQueue

    def _is_json_protocol(self, req):
        ct = req.content_type or ""
        return "json" in ct

    def handle(self, req: Request, path: str) -> Response:
        amz_target = req.headers.get("X-Amz-Target", "")
        if amz_target and "." in amz_target:
            action = amz_target.split(".")[-1]
        else:
            action = req.args.get("Action") or req.form.get("Action", "")
            if not action:
                params = parse_qs(req.get_data(as_text=True))
                action = params.get("Action", [""])[0]
        h = {"CreateQueue": self._create_queue, "DeleteQueue": self._delete_queue,
             "ListQueues": self._list_queues, "GetQueueUrl": self._get_queue_url,
             "GetQueueAttributes": self._get_queue_attributes, "SetQueueAttributes": self._set_queue_attributes,
             "SendMessage": self._send_message, "ReceiveMessage": self._receive_message,
             "DeleteMessage": self._delete_message, "PurgeQueue": self._purge_queue,
             "ChangeMessageVisibility": self._change_visibility, "TagQueue": self._tag_queue,
             "UntagQueue": self._untag_queue, "ListQueueTags": self._list_queue_tags,
             "SendMessageBatch": self._send_message_batch,
             "DeleteMessageBatch": self._delete_message_batch,
             "ChangeMessageVisibilityBatch": self._change_visibility_batch,
             "GetDeadLetterSourceQueues": self._get_dlq_source_queues}
        handler = h.get(action)
        if not handler:
            if self._is_json_protocol(req):
                return Response(json.dumps({"__type": "InvalidAction", "message": f"Action {action} not valid"}), 400, content_type="application/x-amz-json-1.0")
            return error_response("InvalidAction", f"The action {action} is not valid.", 400)
        return handler(req, path)

    def _p(self, req):
        params = dict(req.args)
        if req.content_type and "form" in req.content_type: params.update(req.form.to_dict())
        if self._is_json_protocol(req):
            try:
                jb = json.loads(req.get_data(as_text=True))
                if isinstance(jb, dict): params.update(jb)
            except Exception: pass
        elif not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items(): params[k] = v[0] if len(v)==1 else v
        return params

    def _url(self, name):
        c = get_config(); return f"http://localhost:{c.port}/{c.account_id}/{name}"
    def _arn(self, name):
        c = get_config(); return f"arn:aws:sqs:{c.region}:{c.account_id}:{name}"
    def _find(self, path):
        for url, q in self.queues.items():
            if url.endswith(f"/{path}") or path.endswith(q.name): return q
        return None

    def _resp(self, req, action, xml_content="", json_data=None):
        if self._is_json_protocol(req):
            return Response(json.dumps(json_data or {}, default=str), 200, content_type="application/x-amz-json-1.0")
        body = f'<?xml version="1.0"?>\n<{action}Response xmlns="http://queue.amazonaws.com/doc/2012-11-05/">\n  <{action}Result>\n{xml_content}\n  </{action}Result>\n  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>\n</{action}Response>'
        return Response(body, 200, content_type="application/xml")

    def _err(self, req, code, msg, status=400):
        if self._is_json_protocol(req):
            return Response(json.dumps({"__type": code, "message": msg}), status, content_type="application/x-amz-json-1.0")
        return error_response(code, msg, status)

    def _resolve_queue(self, params, path):
        url = params.get("QueueUrl", "")
        if not url:
            q = self._find(path)
            if q: url = q.url
        return self.queues.get(url)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _parse_message_attributes(self, p):
        """Return dict of name -> {DataType, StringValue} from either JSON or form params."""
        attrs = {}
        # JSON protocol sends MessageAttributes as a dict directly
        if "MessageAttributes" in p and isinstance(p["MessageAttributes"], dict):
            for name, val in p["MessageAttributes"].items():
                if isinstance(val, dict):
                    attrs[name] = {"DataType": val.get("DataType", "String"),
                                   "StringValue": val.get("StringValue", "")}
            return attrs
        # Form/query-string: MessageAttribute.1.Name, MessageAttribute.1.Value.DataType, etc.
        i = 1
        while f"MessageAttribute.{i}.Name" in p:
            name = p[f"MessageAttribute.{i}.Name"]
            dtype = p.get(f"MessageAttribute.{i}.Value.DataType", "String")
            sval = p.get(f"MessageAttribute.{i}.Value.StringValue", "")
            attrs[name] = {"DataType": dtype, "StringValue": sval}
            i += 1
        return attrs

    def _message_attrs_xml(self, msg):
        """Build XML fragment for message attributes."""
        xml = ""
        for name, val in msg.message_attributes.items():
            dtype = val.get("DataType", "String")
            sval = val.get("StringValue", "")
            xml += (f"      <MessageAttribute>"
                    f"<Name>{name}</Name>"
                    f"<Value><DataType>{dtype}</DataType><StringValue>{sval}</StringValue></Value>"
                    f"</MessageAttribute>\n")
        return xml

    def _expire_dedup_ids(self, q):
        """Remove dedup IDs older than 5 minutes."""
        cutoff = time.time() - 300
        expired = [k for k, (_, ts) in q.dedup_ids.items() if ts < cutoff]
        for k in expired:
            del q.dedup_ids[k]

    def _fifo_dedup_id(self, p, body):
        """Return dedup ID from params, or MD5 of body for content-based dedup."""
        did = p.get("MessageDeduplicationId", "")
        if not did:
            did = hashlib.md5(body.encode()).hexdigest()
        return did

    def _wire_dlq(self, queue, redrive_policy):
        """Link queue.dlq to the target queue object if it exists."""
        if not redrive_policy:
            return
        target_arn = redrive_policy.get("deadLetterTargetArn", "")
        if not target_arn:
            return
        for q in self.queues.values():
            if q.arn == target_arn or target_arn.endswith(":" + q.name):
                queue.dlq = q
                return

    def _move_to_dlq(self, src_queue, msg):
        """Move a message to the DLQ and remove from source."""
        if src_queue.dlq is not None:
            src_queue.dlq.messages.append(msg)
        src_queue.messages = [m for m in src_queue.messages if m.message_id != msg.message_id]

    def _check_dlq(self, q, returned_msgs):
        """After receive, move any message that exceeded maxReceiveCount to the DLQ."""
        if not q.redrive_policy:
            return
        max_receives = int(q.redrive_policy.get("maxReceiveCount", 0))
        if max_receives <= 0:
            return
        to_move = [m for m in returned_msgs if m.receive_count >= max_receives]
        for m in to_move:
            logger.info("Moving message %s to DLQ (receive_count=%d)", m.message_id, m.receive_count)
            self._move_to_dlq(q, m)

    # ── queue actions ────────────────────────────────────────────────────────

    def _create_queue(self, req, path):
        p = self._p(req); name = p.get("QueueName", "")
        if not name: return self._err(req, "MissingParameter", "QueueName required")
        url = self._url(name)
        if url not in self.queues:
            attrs = {}
            if "Attributes" in p and isinstance(p["Attributes"], dict):
                attrs = p["Attributes"]
            else:
                i = 1
                while f"Attribute.{i}.Name" in p: attrs[p[f"Attribute.{i}.Name"]] = p.get(f"Attribute.{i}.Value", ""); i+=1
            queue = SQSQueue(name=name, url=url, arn=self._arn(name), attributes=attrs)
            # parse and store RedrivePolicy
            rp_raw = attrs.get("RedrivePolicy", "")
            if rp_raw:
                try:
                    queue.redrive_policy = json.loads(rp_raw) if isinstance(rp_raw, str) else rp_raw
                except Exception:
                    queue.redrive_policy = {}
            self.queues[url] = queue
            self._wire_dlq(queue, queue.redrive_policy)
            logger.info("Created queue: %s", name)
        return self._resp(req, "CreateQueue", f"    <QueueUrl>{url}</QueueUrl>", {"QueueUrl": url})

    def _delete_queue(self, req, path):
        p = self._p(req); url = p.get("QueueUrl", "")
        if not url:
            q = self._find(path)
            if q: url = q.url
        self.queues.pop(url, None)
        return self._resp(req, "DeleteQueue", "", {})

    def _list_queues(self, req, path):
        p = self._p(req); prefix = p.get("QueueNamePrefix", "")
        urls = [q.url for q in self.queues.values() if not prefix or q.name.startswith(prefix)]
        xml = "".join(f"    <QueueUrl>{u}</QueueUrl>\n" for u in urls)
        return self._resp(req, "ListQueues", xml, {"QueueUrls": urls})

    def _get_queue_url(self, req, path):
        p = self._p(req); url = self._url(p.get("QueueName", ""))
        if url not in self.queues: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        return self._resp(req, "GetQueueUrl", f"    <QueueUrl>{url}</QueueUrl>", {"QueueUrl": url})

    def _get_queue_attributes(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        now = time.time()
        q.attributes["ApproximateNumberOfMessages"] = str(sum(1 for m in q.messages if m.visible_after <= now))
        q.attributes["ApproximateNumberOfMessagesNotVisible"] = str(sum(1 for m in q.messages if m.visible_after > now))
        xml = "".join(f"    <Attribute><Name>{k}</Name><Value>{v}</Value></Attribute>\n" for k,v in q.attributes.items())
        return self._resp(req, "GetQueueAttributes", xml, {"Attributes": q.attributes})

    def _set_queue_attributes(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        if "Attributes" in p and isinstance(p["Attributes"], dict):
            q.attributes.update(p["Attributes"])
        else:
            i = 1
            while f"Attribute.{i}.Name" in p: q.attributes[p[f"Attribute.{i}.Name"]] = p.get(f"Attribute.{i}.Value", ""); i+=1
        return self._resp(req, "SetQueueAttributes", "", {})

    # ── message actions ──────────────────────────────────────────────────────

    def _send_message(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        body = p.get("MessageBody", "")
        delay = int(p.get("DelaySeconds", q.attributes.get("DelaySeconds", "0")))
        msg_attrs = self._parse_message_attributes(p)

        if q.is_fifo:
            dedup_id = self._fifo_dedup_id(p, body)
            group_id = p.get("MessageGroupId", "")
            self._expire_dedup_ids(q)
            if dedup_id in q.dedup_ids:
                existing_id, _ = q.dedup_ids[dedup_id]
                logger.info("Dedup hit for %s on queue %s", dedup_id, q.name)
                md5 = md5_hex(body.encode())
                j = {"MessageId": existing_id, "MD5OfMessageBody": md5}
                xml = f"    <MessageId>{existing_id}</MessageId>\n    <MD5OfMessageBody>{md5}</MD5OfMessageBody>"
                return self._resp(req, "SendMessage", xml, j)
            msg = SQSMessage(message_id=new_message_id(), body=body,
                             visible_after=time.time() + delay,
                             deduplication_id=dedup_id, group_id=group_id,
                             message_attributes=msg_attrs)
            q.dedup_ids[dedup_id] = (msg.message_id, time.time())
            if group_id not in q.message_groups:
                q.message_groups[group_id] = []
            q.message_groups[group_id].append(msg.message_id)
        else:
            msg = SQSMessage(message_id=new_message_id(), body=body,
                             visible_after=time.time() + delay,
                             message_attributes=msg_attrs)

        q.messages.append(msg)
        logger.info("Sent message to %s", q.name)
        j = {"MessageId": msg.message_id, "MD5OfMessageBody": msg.md5_of_body}
        xml = f"    <MessageId>{msg.message_id}</MessageId>\n    <MD5OfMessageBody>{msg.md5_of_body}</MD5OfMessageBody>"
        return self._resp(req, "SendMessage", xml, j)

    def _receive_message(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        max_msgs = int(p.get("MaxNumberOfMessages", "1"))
        vt = int(p.get("VisibilityTimeout", q.attributes.get("VisibilityTimeout", "30")))

        # collect requested attribute names so we know what to return
        requested_attr_names = set()
        if "MessageAttributeNames" in p:
            raw = p["MessageAttributeNames"]
            if isinstance(raw, list):
                for n in raw: requested_attr_names.add(n)
            else:
                requested_attr_names.add(raw)
        else:
            i = 1
            while f"MessageAttributeName.{i}" in p:
                requested_attr_names.add(p[f"MessageAttributeName.{i}"]); i += 1

        now = time.time(); xml = ""; msgs_json = []; count = 0; returned = []

        if q.is_fifo:
            # find which group IDs have in-flight messages
            inflight_groups = set()
            for m in q.messages:
                if m.visible_after > now and m.group_id:
                    inflight_groups.add(m.group_id)
            for msg in q.messages:
                if count >= max_msgs: break
                if msg.visible_after > now: continue
                # skip groups that currently have in-flight messages
                if msg.group_id and msg.group_id in inflight_groups: continue
                msg.visible_after = now + vt; msg.receive_count += 1
                msg.receipt_handle = str(uuid.uuid4()); count += 1
                if msg.group_id:
                    inflight_groups.add(msg.group_id)
                returned.append(msg)
        else:
            for msg in q.messages:
                if count >= max_msgs: break
                if msg.visible_after > now: continue
                msg.visible_after = now + vt; msg.receive_count += 1
                msg.receipt_handle = str(uuid.uuid4()); count += 1
                returned.append(msg)

        want_all_attrs = "All" in requested_attr_names or ".*" in requested_attr_names

        for msg in returned:
            attrs_xml = ""
            attrs_json = {}
            if want_all_attrs or requested_attr_names:
                for name, val in msg.message_attributes.items():
                    if want_all_attrs or name in requested_attr_names:
                        attrs_xml += (f"      <MessageAttribute>"
                                      f"<Name>{name}</Name>"
                                      f"<Value><DataType>{val.get('DataType','String')}</DataType>"
                                      f"<StringValue>{val.get('StringValue','')}</StringValue></Value>"
                                      f"</MessageAttribute>\n")
                        attrs_json[name] = val
            xml += (f"    <Message>"
                    f"<MessageId>{msg.message_id}</MessageId>"
                    f"<ReceiptHandle>{msg.receipt_handle}</ReceiptHandle>"
                    f"<MD5OfBody>{msg.md5_of_body}</MD5OfBody>"
                    f"<Body>{msg.body}</Body>"
                    f"{attrs_xml}"
                    f"</Message>\n")
            entry = {"MessageId": msg.message_id, "ReceiptHandle": msg.receipt_handle,
                     "MD5OfBody": msg.md5_of_body, "Body": msg.body}
            if attrs_json:
                entry["MessageAttributes"] = attrs_json
            msgs_json.append(entry)

        self._check_dlq(q, returned)
        return self._resp(req, "ReceiveMessage", xml, {"Messages": msgs_json} if msgs_json else {})

    def _delete_message(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        rh = p.get("ReceiptHandle", ""); q.messages = [m for m in q.messages if m.receipt_handle != rh]
        return self._resp(req, "DeleteMessage", "", {})

    def _purge_queue(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        q.messages.clear()
        return self._resp(req, "PurgeQueue", "", {})

    def _change_visibility(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        rh = p.get("ReceiptHandle", ""); vt = int(p.get("VisibilityTimeout", "30"))
        for m in q.messages:
            if m.receipt_handle == rh: m.visible_after = time.time() + vt; break
        return self._resp(req, "ChangeMessageVisibility", "", {})

    def _tag_queue(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        if "Tags" in p and isinstance(p["Tags"], dict):
            q.tags.update(p["Tags"])
        else:
            i = 1
            while f"Tag.{i}.Key" in p: q.tags[p[f"Tag.{i}.Key"]] = p.get(f"Tag.{i}.Value", ""); i+=1
        return self._resp(req, "TagQueue", "", {})

    def _untag_queue(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        keys = p.get("TagKeys", [])
        if isinstance(keys, list):
            for k in keys: q.tags.pop(k, None)
        else:
            i = 1
            while f"TagKey.{i}" in p: q.tags.pop(p[f"TagKey.{i}"], None); i+=1
        return self._resp(req, "UntagQueue", "", {})

    def _list_queue_tags(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        xml = "".join(f"    <Tag><Key>{k}</Key><Value>{v}</Value></Tag>\n" for k,v in q.tags.items())
        return self._resp(req, "ListQueueTags", xml, {"Tags": q.tags})

    def _send_message_batch(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        entries = p.get("Entries", []) if isinstance(p.get("Entries"), list) else []
        if not entries:
            # Query-string format: SendMessageBatchRequestEntry.1.Id, .MessageBody, etc.
            i = 1
            while f"SendMessageBatchRequestEntry.{i}.Id" in p:
                entries.append({
                    "Id": p[f"SendMessageBatchRequestEntry.{i}.Id"],
                    "MessageBody": p.get(f"SendMessageBatchRequestEntry.{i}.MessageBody", ""),
                    "DelaySeconds": p.get(f"SendMessageBatchRequestEntry.{i}.DelaySeconds", "0"),
                })
                i += 1
        success = []; xml_success = ""
        for entry in entries:
            entry_id = entry.get("Id", "")
            body_str = entry.get("MessageBody", "")
            delay = int(entry.get("DelaySeconds", q.attributes.get("DelaySeconds", "0")))
            msg = SQSMessage(message_id=new_message_id(), body=body_str, visible_after=time.time() + delay)
            q.messages.append(msg)
            success.append({"Id": entry_id, "MessageId": msg.message_id, "MD5OfMessageBody": msg.md5_of_body})
            xml_success += f"    <SendMessageBatchResultEntry><Id>{entry_id}</Id><MessageId>{msg.message_id}</MessageId><MD5OfMessageBody>{msg.md5_of_body}</MD5OfMessageBody></SendMessageBatchResultEntry>\n"
        return self._resp(req, "SendMessageBatch", xml_success, {"Successful": success, "Failed": []})

    def _delete_message_batch(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        entries = p.get("Entries", []) if isinstance(p.get("Entries"), list) else []
        if not entries:
            i = 1
            while f"DeleteMessageBatchRequestEntry.{i}.Id" in p:
                entries.append({
                    "Id": p[f"DeleteMessageBatchRequestEntry.{i}.Id"],
                    "ReceiptHandle": p.get(f"DeleteMessageBatchRequestEntry.{i}.ReceiptHandle", ""),
                })
                i += 1
        success = []; xml_success = ""
        handles_to_delete = {e["ReceiptHandle"] for e in entries}
        q.messages = [m for m in q.messages if m.receipt_handle not in handles_to_delete]
        for entry in entries:
            success.append({"Id": entry["Id"]})
            xml_success += f"    <DeleteMessageBatchResultEntry><Id>{entry['Id']}</Id></DeleteMessageBatchResultEntry>\n"
        return self._resp(req, "DeleteMessageBatch", xml_success, {"Successful": success, "Failed": []})

    def _change_visibility_batch(self, req, path):
        p = self._p(req); q = self._resolve_queue(p, path)
        if not q: return self._err(req, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
        entries = p.get("Entries", []) if isinstance(p.get("Entries"), list) else []
        if not entries:
            i = 1
            while f"ChangeMessageVisibilityBatchRequestEntry.{i}.Id" in p:
                entries.append({
                    "Id": p[f"ChangeMessageVisibilityBatchRequestEntry.{i}.Id"],
                    "ReceiptHandle": p.get(f"ChangeMessageVisibilityBatchRequestEntry.{i}.ReceiptHandle", ""),
                    "VisibilityTimeout": p.get(f"ChangeMessageVisibilityBatchRequestEntry.{i}.VisibilityTimeout", "30"),
                })
                i += 1
        success = []; xml_success = ""
        for entry in entries:
            rh = entry["ReceiptHandle"]; vt = int(entry["VisibilityTimeout"])
            for m in q.messages:
                if m.receipt_handle == rh:
                    m.visible_after = time.time() + vt; break
            success.append({"Id": entry["Id"]})
            xml_success += f"    <ChangeMessageVisibilityBatchResultEntry><Id>{entry['Id']}</Id></ChangeMessageVisibilityBatchResultEntry>\n"
        return self._resp(req, "ChangeMessageVisibilityBatch", xml_success, {"Successful": success, "Failed": []})

    def _get_dlq_source_queues(self, req, path):
        # stub — returns empty list
        return self._resp(req, "GetDeadLetterSourceQueues", "", {"queueUrls": []})

    def reset(self):
        self.queues = {}
