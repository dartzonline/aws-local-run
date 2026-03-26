# LocalRun

**Run AWS services locally.** LocalRun is a lightweight, pure-Python AWS service emulator. It runs 18 AWS services on a single port with zero external dependencies — no Docker, no JVM, just `pip install` and go.

Built for developers who need fast, offline AWS testing without the overhead of full cloud emulators.

## Why LocalRun?

- **Zero setup** — `pip install` and run. No Docker required.
- **Single port** — all 18 services on `:4566`, just like production endpoint routing.
- **Drop-in compatible** — works with `boto3`, AWS CLI, and any AWS SDK. Just set `endpoint_url`.
- **Fast** — starts in under a second. In-memory storage, no cold starts.
- **Pure Python** — easy to extend, debug, and contribute to.

## Supported Services

| Service | Operations |
|---------|-----------|
| **S3** | Buckets, objects, copy, multi-delete, list v2, range downloads, multipart upload, pagination |
| **SQS** | Queues, messages, purge, attributes, batch send/delete/visibility, tags |
| **DynamoDB** | Tables, items, query, scan, batch ops, update expressions, transactions |
| **SNS** | Topics, subscriptions, publish, SNS→SQS fanout delivery |
| **Lambda** | Functions, invoke (sync + async), aliases, event source mappings, permissions, tags |
| **IAM** | Roles, policies, users (stub) |
| **CloudWatch Logs** | Log groups, streams, events, retention, tags, metric filters |
| **CloudWatch Metrics** | Put/get metrics, alarms, list metrics, set alarm state |
| **STS** | GetCallerIdentity, AssumeRole, GetSessionToken |
| **Secrets Manager** | Secrets CRUD, versioning, tags |
| **SSM Parameter Store** | Parameters CRUD, get-by-path, versioning, tags |
| **EventBridge** | Rules, targets, events, event buses, SQS/SNS routing |
| **CloudFormation** | Stacks CRUD, describe, templates (stub) |
| **RDS** | DB instances, clusters CRUD (stub) |
| **API Gateway** | REST APIs, resources, deployments, stages, methods, integrations |
| **OpenSearch** | Domains (control-plane), indices, documents, search, bulk, aggregations |
| **Kinesis** | Streams, shards, put/get records, shard iterators, list shards |
| **Step Functions** | State machines, executions, history, tags |

## Quick Start

```bash
# Install from PyPI
pip install aws-local-run

# Start the emulator
aws-local-run start
```

LocalRun is now running at `http://localhost:4566`.

## Using LocalRun in an Existing Project

Point any existing AWS project at LocalRun by overriding the endpoint URL. No code changes to your business logic required.

### Step 1: Start LocalRun

```bash
aws-local-run start
```

### Step 2: Configure your app

**Option A — Environment variables (recommended for existing projects):**

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Now run your app as usual — all AWS calls route to LocalRun
python manage.py runserver
```

**Option B — boto3 configuration in code:**

```python
import boto3

session = boto3.Session(
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# Create clients with the LocalRun endpoint
s3 = session.client("s3", endpoint_url="http://localhost:4566")
sqs = session.client("sqs", endpoint_url="http://localhost:4566")
dynamodb = session.client("dynamodb", endpoint_url="http://localhost:4566")
```

**Option C — AWS CLI:**

```bash
aws --endpoint-url http://localhost:4566 s3 ls
aws --endpoint-url http://localhost:4566 sqs list-queues
aws --endpoint-url http://localhost:4566 sts get-caller-identity
```

### Example: Flask App with S3 + SQS

Here's a real-world example — a Flask app that uploads files to S3 and sends job messages to SQS, running entirely against LocalRun:

```python
import boto3, os

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                  aws_access_key_id="test", aws_secret_access_key="test",
                  region_name="us-east-1")
sqs = boto3.client("sqs", endpoint_url=ENDPOINT,
                   aws_access_key_id="test", aws_secret_access_key="test",
                   region_name="us-east-1")

# Setup
s3.create_bucket(Bucket="uploads")
queue_url = sqs.create_queue(QueueName="jobs")["QueueUrl"]

# Upload a file and enqueue a processing job
s3.put_object(Bucket="uploads", Key="report.pdf", Body=open("report.pdf", "rb"))
sqs.send_message(QueueUrl=queue_url, MessageBody='{"file": "report.pdf", "action": "process"}')

# Worker: poll for jobs
messages = sqs.receive_message(QueueUrl=queue_url)
for msg in messages.get("Messages", []):
    print(f"Processing: {msg['Body']}")
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
```

### Example: pytest Integration

Use LocalRun in your test suite with a simple fixture:

```python
# conftest.py
import pytest, subprocess, time, requests

@pytest.fixture(scope="session", autouse=True)
def localrun():
    proc = subprocess.Popen(["aws-local-run", "start", "--port", "4566"])
    for _ in range(20):
        try:
            if requests.get("http://localhost:4566/health", timeout=1).ok:
                break
        except Exception:
            time.sleep(0.2)
    yield
    proc.terminate()

@pytest.fixture
def s3():
    import boto3
    return boto3.client("s3", endpoint_url="http://localhost:4566",
                        aws_access_key_id="test", aws_secret_access_key="test",
                        region_name="us-east-1")
```

```python
# test_uploads.py
def test_upload_and_download(s3):
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="file.txt", Body=b"hello")
    resp = s3.get_object(Bucket="test-bucket", Key="file.txt")
    assert resp["Body"].read() == b"hello"
```

## CLI

```bash
aws-local-run start                      # Start server
aws-local-run start --port 5000          # Custom port
aws-local-run start --services s3,sqs    # Only specific services
aws-local-run start --debug              # Debug logging
aws-local-run start --seed seed.json     # Pre-create resources from file
aws-local-run status                     # Check if running
aws-local-run services                   # List all services
aws-local-run wait                       # Wait until server is ready
aws-local-run wait --timeout 60          # Custom timeout (seconds)
aws-local-run fault list                 # List active fault injections
aws-local-run fault add --service s3 --action GetObject --type error --status 500
aws-local-run fault add --service sqs --action ReceiveMessage --type latency --delay 2000
aws-local-run fault clear                # Remove all faults
aws-local-run fault clear --id <uuid>    # Remove a specific fault
```

### Seed File

Pre-create resources at startup with a JSON seed file:

```json
{
  "s3": {"buckets": ["my-bucket", "my-other-bucket"]},
  "sqs": {"queues": ["jobs", "jobs-dlq"]},
  "dynamodb": {
    "tables": [
      {"name": "users", "key": "id", "type": "S"},
      {"name": "orders", "key": "order_id", "type": "S"}
    ]
  },
  "ssm": {
    "parameters": [
      {"name": "/app/db_url", "value": "postgres://localhost/mydb"},
      {"name": "/app/secret_key", "value": "dev-secret"}
    ]
  }
}
```

```bash
aws-local-run start --seed ./seed.json
```

### Wait for Ready

Useful in CI pipelines and Docker entrypoints:

```bash
aws-local-run start &
aws-local-run wait --timeout 30
# Server is ready, run tests now
```

## Fault Injection

LocalRun has a built-in fault injection API for chaos testing. Faults can inject errors or latency into specific service actions.

```bash
# Inject a 500 error on S3 GetObject (50% of the time)
aws-local-run fault add --service s3 --action GetObject --type error --status 500 --probability 0.5

# Inject 2 second latency on SQS receive
aws-local-run fault add --service sqs --action ReceiveMessage --type latency --delay 2000

# List active faults
aws-local-run fault list

# Remove a fault by ID
aws-local-run fault clear --id <uuid>

# Remove all faults
aws-local-run fault clear
```

You can also manage faults via HTTP:

```bash
# Add a fault
curl -X POST http://localhost:4566/_localrun/faults \
  -H "Content-Type: application/json" \
  -d '{"service": "s3", "action": "GetObject", "type": "error", "status": 500, "probability": 0.5}'

# List faults
curl http://localhost:4566/_localrun/faults

# Remove a fault
curl -X DELETE "http://localhost:4566/_localrun/faults/<id>"
```

## State Persistence

LocalRun can save and restore all in-memory state so you don't have to re-create resources after restart.

Set `LOCALRUN_DATA_DIR` to a directory and state is automatically saved/loaded:

```bash
LOCALRUN_DATA_DIR=/tmp/localrun-state aws-local-run start
```

Or use the HTTP API to save/load on demand:

```bash
# Save state
curl -X POST http://localhost:4566/_localrun/state/save

# Load state
curl -X POST http://localhost:4566/_localrun/state/load
```

### Named Snapshots

You can save and restore named snapshots to switch between different setups:

```bash
# Save the current state as "baseline"
curl -X POST http://localhost:4566/_localrun/state/save/baseline

# Restore it later
curl -X POST http://localhost:4566/_localrun/state/load/baseline

# See what snapshots are available
curl http://localhost:4566/_localrun/state/snapshots
```

State is stored as JSON (`localrun_state.json`) in `LOCALRUN_DATA_DIR`. Named snapshots are stored as `localrun_state_<name>.json`.

## Request Log

LocalRun keeps a ring buffer of the last 200 requests for debugging:

```bash
# All recent requests
curl http://localhost:4566/_localrun/requests

# Filter by service
curl "http://localhost:4566/_localrun/requests?service=s3"

# Limit results
curl "http://localhost:4566/_localrun/requests?service=sqs&limit=10"
```

Each entry contains: `timestamp`, `method`, `path`, `service`, `action`, `status`, `duration_ms`.

## Reset

Reset all in-memory state without restarting the server:

```bash
# Reset all services
curl -X POST http://localhost:4566/_localrun/reset

# Reset one service
curl -X POST "http://localhost:4566/_localrun/reset?service=sqs"
```

Useful for test isolation.

## SNS → SQS Delivery

When you subscribe an SQS queue to an SNS topic, messages published to the topic are delivered to the queue automatically:

```python
sns = boto3.client("sns", endpoint_url="http://localhost:4566", ...)
sqs = boto3.client("sqs", endpoint_url="http://localhost:4566", ...)

topic = sns.create_topic(Name="events")["TopicArn"]
queue_url = sqs.create_queue(QueueName="handler")["QueueUrl"]
queue_arn = sqs.get_queue_attributes(
    QueueUrl=queue_url, AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]

sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=queue_arn)
sns.publish(TopicArn=topic, Message="hello")

# Message arrives in the queue wrapped in an SNS envelope
msgs = sqs.receive_message(QueueUrl=queue_url)["Messages"]
import json
payload = json.loads(msgs[0]["Body"])
print(payload["Message"])  # "hello"
```

## EventBridge Routing

EventBridge rules with SQS or SNS targets actually deliver events:

```python
events = boto3.client("events", endpoint_url="http://localhost:4566", ...)

events.put_rule(Name="my-rule", EventPattern='{"source": ["my.app"]}', State="ENABLED")
events.put_targets(Rule="my-rule", Targets=[{"Id": "1", "Arn": queue_arn}])

events.put_events(Entries=[{
    "Source": "my.app",
    "DetailType": "Order",
    "Detail": '{"order_id": "123"}'
}])

# The event arrives in the SQS queue
```

## Kinesis

```python
kinesis = boto3.client("kinesis", endpoint_url="http://localhost:4566", ...)

kinesis.create_stream(StreamName="events", ShardCount=2)
kinesis.put_record(StreamName="events", Data=b"hello", PartitionKey="key1")

iterator = kinesis.get_shard_iterator(
    StreamName="events",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)["ShardIterator"]

records = kinesis.get_records(ShardIterator=iterator)["Records"]
```

## Step Functions

Step Functions stores state machine definitions and auto-succeeds executions. Useful for testing wiring without building an ASL interpreter:

```python
sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4566", ...)

sm = sfn.create_state_machine(
    name="my-workflow",
    definition='{"Comment": "my workflow"}',
    roleArn="arn:aws:iam::000000000000:role/sfn-role",
)["stateMachineArn"]

execution = sfn.start_execution(
    stateMachineArn=sm,
    input='{"key": "value"}',
)["executionArn"]

desc = sfn.describe_execution(executionArn=execution)
print(desc["status"])  # SUCCEEDED
```

## S3 Extras

### Range Downloads

```python
resp = s3.get_object(Bucket="my-bucket", Key="large-file.bin", Range="bytes=0-999")
first_1k = resp["Body"].read()
```

### Multipart Upload

```python
upload = s3.create_multipart_upload(Bucket="my-bucket", Key="big.bin")
uid = upload["UploadId"]

p1 = s3.upload_part(Bucket="my-bucket", Key="big.bin", UploadId=uid, PartNumber=1, Body=b"x" * 5242880)
p2 = s3.upload_part(Bucket="my-bucket", Key="big.bin", UploadId=uid, PartNumber=2, Body=b"y" * 1000)

s3.complete_multipart_upload(
    Bucket="my-bucket", Key="big.bin", UploadId=uid,
    MultipartUpload={"Parts": [
        {"PartNumber": 1, "ETag": p1["ETag"]},
        {"PartNumber": 2, "ETag": p2["ETag"]},
    ]},
)
```

## DynamoDB Transactions

```python
dynamodb = boto3.client("dynamodb", endpoint_url="http://localhost:4566", ...)

dynamodb.transact_write_items(TransactItems=[
    {"Put": {"TableName": "orders", "Item": {"id": {"S": "1"}, "status": {"S": "new"}}}},
    {"Put": {"TableName": "orders", "Item": {"id": {"S": "2"}, "status": {"S": "new"}}}},
])

result = dynamodb.transact_get_items(TransactItems=[
    {"Get": {"TableName": "orders", "Key": {"id": {"S": "1"}}}},
])
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `LOCALRUN_HOST` | `0.0.0.0` | Bind host |
| `LOCALRUN_PORT` | `4566` | Bind port |
| `LOCALRUN_REGION` | `us-east-1` | AWS region |
| `LOCALRUN_ACCOUNT_ID` | `000000000000` | Account ID |
| `LOCALRUN_DATA_DIR` | (none) | State persistence directory |
| `LOCALRUN_DEBUG` | `false` | Debug logging |

## Docker

```bash
docker build -t localrun .
docker run -p 4566:4566 localrun

# Or with docker-compose
docker-compose up
```

## Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

266 integration tests across 18 services.

## Project Structure

```
localrun/
├── __init__.py           # Package version
├── cli.py                # Click CLI
├── config.py             # Configuration
├── gateway.py            # Request routing
├── state.py              # JSON state persistence
├── utils.py              # Shared utilities
└── services/
    ├── s3.py             ├── sts.py
    ├── sqs.py            ├── secretsmanager.py
    ├── dynamodb.py       ├── ssm.py
    ├── sns.py            ├── eventbridge.py
    ├── lambda_service.py ├── cloudformation.py
    ├── iam.py            ├── rds.py
    ├── cloudwatch_logs.py├── apigateway.py
    ├── cloudwatch_metrics.py ├── opensearch.py
    ├── kinesis.py        └── stepfunctions.py
```

## License

MIT
