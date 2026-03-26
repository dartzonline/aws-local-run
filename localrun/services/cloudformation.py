"""CloudFormation service emulator (stub)."""
import json, logging, time, uuid
from flask import Request, Response
from localrun.config import get_config
from localrun.utils import error_response, new_request_id

logger = logging.getLogger("localrun.cloudformation")

class CloudFormationService:
    def __init__(self):
        self.stacks = {}  # name -> stack dict
        # injected by gateway for resource provisioning
        self.engines = {}

    def handle(self, req: Request, path: str) -> Response:
        action = req.args.get("Action") or req.form.get("Action", "")
        handlers = {
            "CreateStack": self._create, "DeleteStack": self._delete,
            "ListStacks": self._list, "DescribeStacks": self._describe,
            "DescribeStackResources": self._describe_resources,
            "DescribeStackEvents": self._describe_events,
            "GetTemplate": self._get_template,
        }
        h = handlers.get(action)
        if not h:
            return error_response("InvalidAction", f"Action {action} not valid", 400)
        return h(req)

    def _p(self, req):
        from urllib.parse import parse_qs
        params = dict(req.args)
        if req.content_type and "form" in req.content_type:
            params.update(req.form.to_dict())
        if not params.get("Action"):
            for k, v in parse_qs(req.get_data(as_text=True)).items():
                params[k] = v[0] if len(v) == 1 else v
        return params

    def _xml(self, action, content):
        body = f'<?xml version="1.0"?>\n<{action}Response xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">\n  <{action}Result>\n{content}\n  </{action}Result>\n  <ResponseMetadata><RequestId>{new_request_id()}</RequestId></ResponseMetadata>\n</{action}Response>'
        return Response(body, 200, content_type="application/xml")

    def _arn(self, name, sid):
        c = get_config()
        return f"arn:aws:cloudformation:{c.region}:{c.account_id}:stack/{name}/{sid}"

    def _create(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        if not name: return error_response("ValidationError", "StackName required", 400)
        if name in self.stacks: return error_response("AlreadyExistsException", f"Stack {name} exists", 400)
        sid = str(uuid.uuid4())
        template_body = p.get("TemplateBody", "")
        self.stacks[name] = {
            "StackName": name, "StackId": self._arn(name, sid),
            "StackStatus": "CREATE_COMPLETE", "CreationTime": time.time(),
            "TemplateBody": template_body,
            "Parameters": [], "Resources": [], "Events": [],
        }
        # Provision resources from template
        self._provision_resources(name, template_body)
        logger.info("Created stack: %s", name)
        return self._xml("CreateStack", f"    <StackId>{self.stacks[name]['StackId']}</StackId>")

    def _delete(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        self.stacks.pop(name, None)
        return self._xml("DeleteStack", "")

    def _list(self, req):
        xml = ""
        for s in self.stacks.values():
            xml += f"""    <member>
      <StackName>{s['StackName']}</StackName>
      <StackId>{s['StackId']}</StackId>
      <StackStatus>{s['StackStatus']}</StackStatus>
    </member>\n"""
        return self._xml("ListStacks", f"  <StackSummaries>\n{xml}  </StackSummaries>")

    def _describe(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        s = self.stacks.get(name)
        if not s: return error_response("ValidationError", f"Stack {name} not found", 400)
        return self._xml("DescribeStacks", f"""  <Stacks>
    <member>
      <StackName>{s['StackName']}</StackName>
      <StackId>{s['StackId']}</StackId>
      <StackStatus>{s['StackStatus']}</StackStatus>
    </member>
  </Stacks>""")

    def _describe_resources(self, req):
        return self._xml("DescribeStackResources", "  <StackResources></StackResources>")

    def _describe_events(self, req):
        return self._xml("DescribeStackEvents", "  <StackEvents></StackEvents>")

    def _get_template(self, req):
        p = self._p(req)
        name = p.get("StackName", "")
        s = self.stacks.get(name)
        tmpl = s["TemplateBody"] if s else ""
        return self._xml("GetTemplate", f"    <TemplateBody>{tmpl}</TemplateBody>")

    def reset(self):
        self.stacks = {}

    def _provision_resources(self, stack_name, template_body):
        """Parse template body and create resources in the matching service engines."""
        if not template_body:
            return
        try:
            template = json.loads(template_body)
        except Exception:
            try:
                import yaml
                template = yaml.safe_load(template_body)
            except Exception:
                logger.debug("Could not parse CloudFormation template as JSON or YAML")
                return
        resources = template.get("Resources", {})
        created = []
        for logical_id, res in resources.items():
            res_type = res.get("Type", "")
            props = res.get("Properties", {})
            try:
                result = self._create_resource(res_type, props, logical_id)
                if result:
                    created.append(result)
            except Exception as e:
                logger.warning("CloudFormation: failed to provision %s (%s): %s", logical_id, res_type, e)
        if stack_name in self.stacks:
            self.stacks[stack_name]["Resources"] = created
        logger.info("CloudFormation provisioned %d resources for stack %s", len(created), stack_name)

    def _create_resource(self, res_type, props, logical_id):
        c = get_config()
        if res_type == "AWS::S3::Bucket":
            s3 = self.engines.get("s3")
            if s3:
                bucket_name = props.get("BucketName", logical_id.lower())
                s3.buckets.setdefault(bucket_name, {})
                logger.info("CloudFormation created S3 bucket: %s", bucket_name)
                return {"LogicalResourceId": logical_id, "ResourceType": res_type, "PhysicalResourceId": bucket_name}

        elif res_type == "AWS::SQS::Queue":
            sqs = self.engines.get("sqs")
            if sqs:
                queue_name = props.get("QueueName", logical_id)
                url = sqs._url(queue_name)
                if url not in sqs.queues:
                    from localrun.services.sqs import SQSQueue
                    arn = "arn:aws:sqs:" + c.region + ":" + c.account_id + ":" + queue_name
                    sqs.queues[url] = SQSQueue(name=queue_name, url=url, arn=arn)
                logger.info("CloudFormation created SQS queue: %s", queue_name)
                return {"LogicalResourceId": logical_id, "ResourceType": res_type, "PhysicalResourceId": queue_name}

        elif res_type == "AWS::DynamoDB::Table":
            dynamodb = self.engines.get("dynamodb")
            if dynamodb:
                table_name = props.get("TableName", logical_id)
                if table_name not in dynamodb.tables:
                    key_schema = props.get("KeySchema", [{"AttributeName": "id", "KeyType": "HASH"}])
                    attr_defs = props.get("AttributeDefinitions", [{"AttributeName": "id", "AttributeType": "S"}])
                    dynamodb.tables[table_name] = {
                        "TableName": table_name,
                        "TableArn": "arn:aws:dynamodb:" + c.region + ":" + c.account_id + ":table/" + table_name,
                        "KeySchema": key_schema,
                        "AttributeDefinitions": attr_defs,
                        "CreationDateTime": time.time(),
                        "ProvisionedThroughput": props.get("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}),
                    }
                    if props.get("GlobalSecondaryIndexes"):
                        dynamodb.tables[table_name]["GlobalSecondaryIndexes"] = props["GlobalSecondaryIndexes"]
                    if props.get("LocalSecondaryIndexes"):
                        dynamodb.tables[table_name]["LocalSecondaryIndexes"] = props["LocalSecondaryIndexes"]
                    dynamodb.table_items[table_name] = []
                logger.info("CloudFormation created DynamoDB table: %s", table_name)
                return {"LogicalResourceId": logical_id, "ResourceType": res_type, "PhysicalResourceId": table_name}

        elif res_type == "AWS::SNS::Topic":
            sns = self.engines.get("sns")
            if sns:
                topic_name = props.get("TopicName", logical_id)
                arn = "arn:aws:sns:" + c.region + ":" + c.account_id + ":" + topic_name
                if arn not in sns.topics:
                    from localrun.services.sns import SNSTopic
                    sns.topics[arn] = SNSTopic(name=topic_name, arn=arn)
                logger.info("CloudFormation created SNS topic: %s", topic_name)
                return {"LogicalResourceId": logical_id, "ResourceType": res_type, "PhysicalResourceId": arn}

        elif res_type == "AWS::SSM::Parameter":
            ssm = self.engines.get("ssm")
            if ssm:
                param_name = props.get("Name", "/" + logical_id)
                ssm.parameters[param_name] = {
                    "Name": param_name,
                    "Type": props.get("Type", "String"),
                    "Value": props.get("Value", ""),
                    "Version": 1,
                    "LastModifiedDate": time.time(),
                    "ARN": "arn:aws:ssm:" + c.region + ":" + c.account_id + ":parameter" + param_name,
                    "Description": props.get("Description", ""),
                    "Tags": [],
                }
                logger.info("CloudFormation created SSM parameter: %s", param_name)
                return {"LogicalResourceId": logical_id, "ResourceType": res_type, "PhysicalResourceId": param_name}

        return None
