"""Configuration."""
import os
from dataclasses import dataclass, field

ALL_SERVICES = [
    "s3", "sqs", "dynamodb", "sns", "lambda", "iam", "logs",
    "sts", "secretsmanager", "ssm", "events", "cloudformation", "rds", "apigateway",
    "opensearch", "kinesis", "cloudwatch", "stepfunctions",
]


@dataclass
class LocalRunConfig:
    host: str = "0.0.0.0"
    port: int = 4566
    region: str = "us-east-1"
    account_id: str = "000000000000"
    access_key: str = "test"
    secret_key: str = "test"
    data_dir: str = ""
    debug: bool = False
    enabled_services: dict = field(default_factory=lambda: {s: True for s in ALL_SERVICES})

    @classmethod
    def from_env(cls):
        c = cls()
        c.host = os.environ.get("LOCALRUN_HOST", c.host)
        c.port = int(os.environ.get("LOCALRUN_PORT", c.port))
        c.region = os.environ.get("LOCALRUN_REGION", c.region)
        c.account_id = os.environ.get("LOCALRUN_ACCOUNT_ID", c.account_id)
        c.data_dir = os.environ.get("LOCALRUN_DATA_DIR", c.data_dir)
        c.debug = os.environ.get("LOCALRUN_DEBUG", "").lower() in ("1", "true", "yes")
        return c


_config = None


def get_config():
    global _config
    if _config is None:
        _config = LocalRunConfig.from_env()
    return _config


def set_config(c):
    global _config
    _config = c
