"""Configuration."""
import os
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("localrun.config")

ALL_SERVICES = [
    "s3", "sqs", "dynamodb", "sns", "lambda", "iam", "logs",
    "sts", "secretsmanager", "ssm", "events", "cloudformation", "rds", "apigateway",
    "opensearch", "kinesis", "cloudwatch", "stepfunctions", "ses", "cognito",
    "kms", "ec2", "acm", "route53",
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
    # rate_limits: per-service max requests per minute (0 = unlimited)
    # Example: {"s3": 1000, "dynamodb": 500}
    rate_limits: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls):
        c = cls()
        c.host = os.environ.get("LOCALRUN_HOST", c.host)
        c.port = int(os.environ.get("LOCALRUN_PORT", c.port))
        c.region = os.environ.get("LOCALRUN_REGION", c.region)
        c.account_id = os.environ.get("LOCALRUN_ACCOUNT_ID", c.account_id)
        c.data_dir = os.environ.get("LOCALRUN_DATA_DIR", c.data_dir)
        c.debug = os.environ.get("LOCALRUN_DEBUG", "").lower() in ("1", "true", "yes")
        # Try loading localrun.yaml from cwd if it exists
        yaml_path = os.path.join(os.getcwd(), "localrun.yaml")
        if os.path.isfile(yaml_path):
            c._load_yaml(yaml_path)
        return c

    @classmethod
    def from_yaml(cls, path):
        c = cls.from_env()
        c._load_yaml(path)
        return c

    def _load_yaml(self, path):
        try:
            import yaml
        except ImportError:
            logger.debug("pyyaml not installed, skipping config file: %s", path)
            return
        try:
            with open(path) as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning("Failed to load config file %s: %s", path, e)
            return
        if "host" in data:
            self.host = data["host"]
        if "port" in data:
            self.port = int(data["port"])
        if "region" in data:
            self.region = data["region"]
        if "account_id" in data:
            self.account_id = str(data["account_id"])
        if "data_dir" in data:
            self.data_dir = data["data_dir"]
        if "debug" in data:
            self.debug = bool(data["debug"])
        if "rate_limits" in data and isinstance(data["rate_limits"], dict):
            self.rate_limits = {k: int(v) for k, v in data["rate_limits"].items()}
        if "services" in data:
            svc_list = data["services"]
            if isinstance(svc_list, list):
                for s in ALL_SERVICES:
                    self.enabled_services[s] = False
                for s in svc_list:
                    self.enabled_services[s.strip()] = True
        logger.info("Loaded config from %s", path)


def load_config_file(path=None):
    """Load config from a localrun.yaml/yml/json file. Returns a dict."""
    import os
    if path is None:
        for fname in ("localrun.yaml", "localrun.yml", "localrun.json"):
            if os.path.isfile(fname):
                path = fname
                break
    if path is None:
        return {}
    if path.endswith(".json"):
        import json
        try:
            with open(path) as f:
                return json.load(f) or {}
        except Exception as e:
            logger.warning("Failed to load config file %s: %s", path, e)
            return {}
    try:
        import yaml
    except ImportError:
        raise ImportError("pyyaml is required for YAML config files: pip install pyyaml")
    try:
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("Failed to load config file %s: %s", path, e)
        return {}


def merge_config(base_config, file_config):
    """Merge file_config values into base_config. file_config has lower precedence than CLI."""
    if not file_config:
        return base_config
    if "port" in file_config and base_config.port == 4566:
        base_config.port = int(file_config["port"])
    if "region" in file_config and base_config.region == "us-east-1":
        base_config.region = file_config["region"]
    if "debug" in file_config and not base_config.debug:
        base_config.debug = bool(file_config["debug"])
    if "services" in file_config:
        svc_list = file_config["services"]
        if isinstance(svc_list, list):
            for s in ALL_SERVICES:
                base_config.enabled_services[s] = False
            for s in svc_list:
                base_config.enabled_services[s.strip()] = True
    return base_config


_config = None


def get_config():
    global _config
    if _config is None:
        _config = LocalRunConfig.from_env()
    return _config


def set_config(c):
    global _config
    _config = c
