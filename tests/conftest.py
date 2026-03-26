"""Pytest fixtures for LocalRun integration tests."""
import threading, time
import boto3, pytest

@pytest.fixture(scope="session", autouse=True)
def localrun_server():
    from localrun.config import LocalRunConfig, set_config
    from localrun.gateway import create_app
    config = LocalRunConfig(host="127.0.0.1", port=14566)
    set_config(config)
    app = create_app()
    t = threading.Thread(target=lambda: app.run(host="127.0.0.1", port=14566, debug=False, use_reloader=False), daemon=True)
    t.start()
    for _ in range(20):
        try:
            import requests
            if requests.get("http://127.0.0.1:14566/health", timeout=1).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)
    yield

ENDPOINT = "http://127.0.0.1:14566"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test", "region_name": "us-east-1"}

def _client(service):
    return boto3.client(service, endpoint_url=ENDPOINT, **CREDS)

@pytest.fixture
def s3_client(): return _client("s3")
@pytest.fixture
def sqs_client(): return _client("sqs")
@pytest.fixture
def dynamodb_client(): return _client("dynamodb")
@pytest.fixture
def sns_client(): return _client("sns")
@pytest.fixture
def iam_client(): return _client("iam")
@pytest.fixture
def sts_client(): return _client("sts")
@pytest.fixture
def logs_client(): return _client("logs")
@pytest.fixture
def secretsmanager_client(): return _client("secretsmanager")
@pytest.fixture
def ssm_client(): return _client("ssm")
@pytest.fixture
def events_client(): return _client("events")
@pytest.fixture
def opensearch_client(): return _client("opensearch")
@pytest.fixture
def rds_client(): return _client("rds")
@pytest.fixture
def lambda_client(): return _client("lambda")
@pytest.fixture
def kinesis_client(): return _client("kinesis")
@pytest.fixture
def cloudwatch_client(): return _client("cloudwatch")
@pytest.fixture
def stepfunctions_client(): return _client("stepfunctions")
@pytest.fixture
def kms_client(): return _client("kms")
@pytest.fixture
def ec2_client(): return _client("ec2")
@pytest.fixture
def acm_client(): return _client("acm")
@pytest.fixture
def route53_client(): return _client("route53")
