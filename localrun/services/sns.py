"""SNS service emulator."""
import json, logging, time, uuid
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import parse_qs
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, new_request_id

logger = logging.getLogger("localrun.sns")

@dataclass
class SNSTopic:
    name: str; arn: str; attributes: dict = field(default_factory=dict)
    subscriptions: list = field(default_factory=list)

@dataclass
class SNSSubscription:
    subscription_arn: str; topic_arn: str; protocol: str; endpoint: str
    attributes: dict = field(default_factory=dict)

class SNSService:
    def __init__(self):
        self.topics = {}  # arn -> SNSTopic
        self._sub_counter = 0
        self.sqs = None  # injected by gateway after all engines are created

    def handle(self, req: Request, path: str) -> Response:
        action = req.args.get("Action") or req.form.get("Action", "")
        if not action:
            params = parse_qs(req.get_data(as_text=True))
            action = params.get("Action", [""])[0]
        actions = {
            "CreateTopic": self._create_topic, "DeleteTopic": self._delete_topic,
            "ListTopics": self._list_topics, "GetTopicAttributes": self._get_topic_attrs,
            "SetTopicAttributes": self._set_topic_attrs, "Subscribe": self._subscribe,
            "Unsubscribe": self._unsubscribe, "Publish": self._publish,
            "ListSubscriptions": self._list_subscriptions,
            "ListSubscriptionsByTopic": self._list_subs_by_topic,
        }
        handler = actions.get(action)
        if not handler:
            return error_response("InvalidAction", f"Invalid action: {action}", 400)
        return handler(req)

    def _p(self, req):
        params = dict(req.args)
        if req.content_type and "form" in req.content_type: params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items(): params[k] = v[0] if len(v)==1 else v
        return params

    def _xml(self, action, content):
        body = f'<?xml version="1.0"?>\n<{action}Response xmlns="http://sns.amazonaws.com/doc/2010-03-31/">\n  <{action}Result>\n{content}\n  </{action}Result>\n  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>\n</{action}Response>'
        return Response(body, 200, content_type="application/xml")

    def _create_topic(self, req):
        p = self._p(req); name = p.get("Name", "")
        if not name: return error_response("InvalidParameter", "Name required", 400)
        c = get_config(); arn = f"arn:aws:sns:{c.region}:{c.account_id}:{name}"
        if arn not in self.topics:
            self.topics[arn] = SNSTopic(name=name, arn=arn)
            logger.info("Created topic: %s", name)
        return self._xml("CreateTopic", f"    <TopicArn>{arn}</TopicArn>")

    def _delete_topic(self, req):
        p = self._p(req); arn = p.get("TopicArn", "")
        self.topics.pop(arn, None)
        return self._xml("DeleteTopic", "")

    def _list_topics(self, req):
        xml = "    <Topics>\n"
        for arn in sorted(self.topics.keys()):
            xml += f"      <member><TopicArn>{arn}</TopicArn></member>\n"
        xml += "    </Topics>"
        return self._xml("ListTopics", xml)

    def _get_topic_attrs(self, req):
        p = self._p(req); arn = p.get("TopicArn", "")
        topic = self.topics.get(arn)
        if not topic: return error_response("NotFound", "Topic not found", 404)
        xml = "    <Attributes>\n"
        for k, v in {"TopicArn": arn, "DisplayName": topic.name, **topic.attributes}.items():
            xml += f"      <entry><key>{k}</key><value>{v}</value></entry>\n"
        xml += "    </Attributes>"
        return self._xml("GetTopicAttributes", xml)

    def _set_topic_attrs(self, req):
        p = self._p(req); arn = p.get("TopicArn", "")
        topic = self.topics.get(arn)
        if not topic: return error_response("NotFound", "Topic not found", 404)
        name = p.get("AttributeName", ""); value = p.get("AttributeValue", "")
        if name: topic.attributes[name] = value
        return self._xml("SetTopicAttributes", "")

    def _subscribe(self, req):
        p = self._p(req)
        topic_arn = p.get("TopicArn", ""); protocol = p.get("Protocol", ""); endpoint = p.get("Endpoint", "")
        topic = self.topics.get(topic_arn)
        if not topic: return error_response("NotFound", "Topic not found", 404)
        self._sub_counter += 1
        sub_arn = f"{topic_arn}:{self._sub_counter}"
        sub = SNSSubscription(subscription_arn=sub_arn, topic_arn=topic_arn, protocol=protocol, endpoint=endpoint)
        topic.subscriptions.append(sub)
        logger.info("Subscribed %s to %s via %s", endpoint, topic.name, protocol)
        return self._xml("Subscribe", f"    <SubscriptionArn>{sub_arn}</SubscriptionArn>")

    def _unsubscribe(self, req):
        p = self._p(req); sub_arn = p.get("SubscriptionArn", "")
        for topic in self.topics.values():
            topic.subscriptions = [s for s in topic.subscriptions if s.subscription_arn != sub_arn]
        return self._xml("Unsubscribe", "")

    def _publish(self, req):
        p = self._p(req)
        topic_arn = p.get("TopicArn", ""); message = p.get("Message", ""); subject = p.get("Subject", "")
        topic = self.topics.get(topic_arn)
        if not topic: return error_response("NotFound", "Topic not found", 404)
        msg_id = str(uuid.uuid4())
        logger.info("Published to %s: %s (subs: %d)", topic.name, msg_id, len(topic.subscriptions))
        # Deliver to SQS subscribers if the SQS engine has been wired in
        for sub in topic.subscriptions:
            if sub.protocol == "sqs" and self.sqs:
                self._deliver_to_sqs(topic_arn, sub.endpoint, message, msg_id, subject)
            elif sub.protocol not in ("sqs",):
                logger.debug("SNS delivery not implemented for protocol: %s", sub.protocol)
        return self._xml("Publish", f"    <MessageId>{msg_id}</MessageId>")

    def _deliver_to_sqs(self, topic_arn, endpoint_arn, message, msg_id, subject):
        # endpoint_arn looks like: arn:aws:sqs:region:account:queue-name
        queue_name = endpoint_arn.split(":")[-1]
        queue_url = self.sqs._url(queue_name)
        q = self.sqs.queues.get(queue_url)
        if not q:
            logger.warning("SNS: SQS queue not found for endpoint %s", endpoint_arn)
            return
        # Build the standard SNS notification envelope that SQS subscribers receive
        from localrun.utils import iso_timestamp
        envelope = json.dumps({
            "Type": "Notification",
            "MessageId": msg_id,
            "TopicArn": topic_arn,
            "Subject": subject,
            "Message": message,
            "Timestamp": iso_timestamp(),
            "SignatureVersion": "1",
            "Signature": "FAKESIGNATURE",
            "SigningCertURL": "",
            "UnsubscribeURL": "",
        })
        from localrun.services.sqs import SQSMessage
        import time
        q.messages.append(SQSMessage(message_id=str(uuid.uuid4()), body=envelope))
        logger.info("SNS delivered to SQS queue %s", queue_name)

    def _list_subscriptions(self, req):
        xml = "    <Subscriptions>\n"
        for topic in self.topics.values():
            for sub in topic.subscriptions:
                xml += f"      <member><SubscriptionArn>{sub.subscription_arn}</SubscriptionArn><TopicArn>{sub.topic_arn}</TopicArn><Protocol>{sub.protocol}</Protocol><Endpoint>{sub.endpoint}</Endpoint><Owner>000000000000</Owner></member>\n"
        xml += "    </Subscriptions>"
        return self._xml("ListSubscriptions", xml)

    def _list_subs_by_topic(self, req):
        p = self._p(req); topic_arn = p.get("TopicArn", "")
        topic = self.topics.get(topic_arn)
        if not topic: return error_response("NotFound", "Topic not found", 404)
        xml = "    <Subscriptions>\n"
        for sub in topic.subscriptions:
            xml += f"      <member><SubscriptionArn>{sub.subscription_arn}</SubscriptionArn><TopicArn>{sub.topic_arn}</TopicArn><Protocol>{sub.protocol}</Protocol><Endpoint>{sub.endpoint}</Endpoint></member>\n"
        xml += "    </Subscriptions>"
        return self._xml("ListSubscriptionsByTopic", xml)

    def reset(self):
        self.topics = {}
        self._sub_counter = 0
