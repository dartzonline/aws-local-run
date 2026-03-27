"""CLI entry point."""
import json
import logging
import time
import click
import requests
from localrun import __version__
from localrun.config import ALL_SERVICES

LOGO = (
    "\033[36m\n"
    "    __    ____  _________    __    ____  __  ___   __\n"
    "   / /   / __ \\/ ____/   |  / /   / __ \\/ / / / | / /\n"
    "  / /   / / / / /   / /| | / /   / /_/ / / / /  |/ /\n"
    " / /___/ /_/ / /___/ ___ |/ /___/ _, _/ /_/ / /|  /\n"
    "/_____/\\____/\\____/_/  |_/_____/_/ |_|\\____/_/ |_/\n"
    "\033[0m"
)

MENU = f"""{LOGO}
\033[90m  AWS Local Emulator  v{__version__}\033[0m

  \033[1mUsage:\033[0m  aws-local-run <command> [options]

  \033[1mCommands:\033[0m
    \033[32mstart\033[0m       Start the LocalRun server
    \033[32mstatus\033[0m      Check if LocalRun is running
    \033[32mservices\033[0m    List all supported AWS services
    \033[32mfault\033[0m       Manage fault injection rules
    \033[32mwait\033[0m        Wait for LocalRun to be ready
    \033[32m--version\033[0m   Show version
    \033[32m--help\033[0m      Show this message

  \033[1mQuick start:\033[0m
    \033[90m$\033[0m aws-local-run start
    \033[90m$\033[0m aws-local-run start --port 5000 --services s3,sqs,dynamodb
    \033[90m$\033[0m aws-local-run status
"""


@click.group(invoke_without_command=True)
@click.version_option(__version__, prog_name="aws-local-run")
@click.pass_context
def main(ctx):
    """LocalRun — run AWS services locally."""
    if ctx.invoked_subcommand is None:
        click.echo(MENU)


@main.command()
@click.option("--port", default=4566, help="Port to listen on")
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--services", default=None, help="Comma-separated service list")
@click.option("--data-dir", default=None, help="Data persistence directory")
@click.option("--seed", default=None, help="Seed file (JSON) to pre-create resources")
@click.option("--config", "config_file", default=None, help="YAML config file path")
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.option("--reload", is_flag=True, help="Auto-reload on config file changes")
@click.option("--watch", default=None, help="Directory to watch for Lambda hot reload")
def start(port, host, services, data_dir, seed, config_file, debug, reload, watch):
    """Start the LocalRun server."""
    from localrun.config import LocalRunConfig, set_config
    from localrun.gateway import create_app

    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")

    config = LocalRunConfig(host=host, port=port, debug=debug)
    if config_file:
        config._load_yaml(config_file)
    if services:
        for s in ALL_SERVICES:
            config.enabled_services[s] = False
        for s in services.split(","):
            config.enabled_services[s.strip()] = True
    if data_dir:
        config.data_dir = data_dir
    set_config(config)

    click.echo(LOGO)
    svc_list = ", ".join(s for s, on in config.enabled_services.items() if on)
    click.echo(f"  \033[90mAWS Local Emulator  v{__version__}\033[0m\n")
    click.echo(f"  \033[32m➜\033[0m  http://{host}:{port}")
    click.echo(f"  \033[32m➜\033[0m  Services: {svc_list}\n")

    app = create_app()

    if seed:
        _load_seed_file(seed, app.config.get("engines", {}))

    if watch:
        from localrun.watcher import LambdaWatcher
        lambda_svc = app.config.get("engines", {}).get("lambda")
        if lambda_svc:
            watcher = LambdaWatcher(watch, lambda_svc)
            watcher.start()
            click.echo(f"  \033[32m➜\033[0m  Watching {watch} for Lambda hot reload\n")

    # Graceful shutdown: save state on SIGINT/SIGTERM
    import signal
    def _shutdown(signum, frame):
        click.echo("\n  Shutting down...")
        if config.data_dir:
            from localrun.state import StateManager
            sm = StateManager(config.data_dir)
            sm.save_state(app.config.get("engines", {}))
            click.echo("  State saved.")
        raise SystemExit(0)
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Load plugins from LOCALRUN_PLUGINS dir if set
    import os
    plugin_dir = os.environ.get("LOCALRUN_PLUGINS")
    if plugin_dir and os.path.isdir(plugin_dir):
        _load_plugins(plugin_dir, app)

    app.run(host=host, port=port, debug=debug, use_reloader=reload)


def _load_plugins(plugin_dir, app):
    """Load Python plugin files from a directory.
    
    Each plugin can define a register(app, engines) function.
    """
    import importlib.util
    import os
    engines = app.config.get("engines", {})
    for filename in sorted(os.listdir(plugin_dir)):
        if not filename.endswith(".py"):
            continue
        filepath = os.path.join(plugin_dir, filename)
        try:
            spec = importlib.util.spec_from_file_location(filename[:-3], filepath)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            if hasattr(mod, "register"):
                mod.register(app, engines)
            click.echo(f"  Plugin loaded: {filename}")
        except Exception as e:
            click.echo(f"  Plugin error: {filename}: {e}")


def _load_seed_file(seed_path, engines):
    """Pre-create resources by calling engine methods directly — no HTTP needed."""
    try:
        with open(seed_path) as f:
            seed = json.load(f)
    except Exception as e:
        click.echo(f"\033[31m✖\033[0m Failed to read seed file: {e}")
        return

    s3 = engines.get("s3")
    sqs = engines.get("sqs")
    dynamodb = engines.get("dynamodb")
    ssm = engines.get("ssm")

    # S3 buckets
    for bucket in seed.get("s3", {}).get("buckets", []):
        try:
            if s3:
                s3.buckets.setdefault(bucket, {})
            click.echo(f"  Seed: S3 bucket '{bucket}'")
        except Exception as e:
            click.echo(f"  Seed warning: S3 bucket '{bucket}': {e}")

    # SQS queues — reuse the internal _create_queue logic
    for queue_name in seed.get("sqs", {}).get("queues", []):
        try:
            if sqs:
                from localrun.config import get_config
                c = get_config()
                url = sqs._url(queue_name)
                if url not in sqs.queues:
                    from localrun.services.sqs import SQSQueue
                    arn = f"arn:aws:sqs:{c.region}:{c.account_id}:{queue_name}"
                    sqs.queues[url] = SQSQueue(name=queue_name, url=url, arn=arn)
            click.echo(f"  Seed: SQS queue '{queue_name}'")
        except Exception as e:
            click.echo(f"  Seed warning: SQS queue '{queue_name}': {e}")

    # DynamoDB tables
    from localrun.config import get_config
    for tbl in seed.get("dynamodb", {}).get("tables", []):
        try:
            if dynamodb:
                name = tbl["name"]
                key = tbl.get("key", "id")
                key_type = tbl.get("type", "S")
                c = get_config()
                dynamodb.tables[name] = {
                    "TableName": name,
                    "TableArn": f"arn:aws:dynamodb:{c.region}:{c.account_id}:table/{name}",
                    "TableStatus": "ACTIVE",
                    "KeySchema": [{"AttributeName": key, "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": key, "AttributeType": key_type}],
                    "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
                }
                dynamodb.table_items[name] = []
            click.echo(f"  Seed: DynamoDB table '{name}'")
        except Exception as e:
            click.echo(f"  Seed warning: DynamoDB table: {e}")

    # SSM parameters
    import time
    for param in seed.get("ssm", {}).get("parameters", []):
        try:
            if ssm:
                from localrun.config import get_config
                c = get_config()
                pname = param["name"]
                ssm.parameters[pname] = {
                    "Name": pname,
                    "Type": param.get("type", "String"),
                    "Value": param["value"],
                    "Version": 1,
                    "LastModifiedDate": time.time(),
                    "ARN": f"arn:aws:ssm:{c.region}:{c.account_id}:parameter{pname}",
                    "Description": param.get("description", ""),
                    "Tags": [],
                }
            click.echo(f"  Seed: SSM parameter '{param['name']}'")
        except Exception as e:
            click.echo(f"  Seed warning: SSM parameter '{param.get('name', '?')}': {e}")


@main.command()
@click.option("--port", default=4566)
def status(port):
    """Check if LocalRun is running."""
    try:
        r = requests.get(f"http://localhost:{port}/health", timeout=2)
        click.echo(f"\033[32m✔\033[0m LocalRun is running (status {r.status_code})")
    except Exception:
        click.echo(f"\033[31m✖\033[0m LocalRun is not running on port {port}")


@main.command()
@click.option("--port", default=4566, help="Port to check")
@click.option("--timeout", default=30, help="Max seconds to wait")
def wait(port, timeout):
    """Wait until LocalRun is ready to accept requests."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"http://localhost:{port}/health", timeout=1)
            if r.status_code == 200:
                click.echo(f"\033[32m✔\033[0m LocalRun is ready on port {port}")
                raise SystemExit(0)
        except requests.exceptions.ConnectionError:
            pass
        except SystemExit:
            raise
        except Exception:
            pass
        time.sleep(0.5)
    click.echo(f"\033[31m✖\033[0m Timed out waiting for LocalRun on port {port}")
    raise SystemExit(1)


@main.command()
def services():
    """List all supported AWS services."""
    click.echo(LOGO)
    click.echo(f"  \033[90mAWS Local Emulator  v{__version__}\033[0m\n")
    click.echo("  \033[1mSupported Services (24):\033[0m\n")
    all_svc = [
        ("s3",             "Buckets, objects, versioning, ACLs, lifecycle, event notifications"),
        ("sqs",            "Queues, messages, FIFO, DLQ, batch ops, long polling"),
        ("dynamodb",       "Tables, items, GSI/LSI, expressions, streams, TTL"),
        ("sns",            "Topics, subscriptions, publish, SQS/Lambda delivery"),
        ("lambda",         "Functions, Python/Node.js/Go, layers, async invoke, CloudWatch Logs"),
        ("iam",            "Roles, policies, users, groups, inline policies, instance profiles"),
        ("logs",           "Log groups, streams, events, metric filters, tags"),
        ("sts",            "GetCallerIdentity, AssumeRole, GetSessionToken"),
        ("secretsmanager", "Secrets CRUD, versioning, tags"),
        ("ssm",            "Parameters CRUD, get-by-path, tags"),
        ("events",         "Rules, targets, event buses, SQS/SNS/Lambda routing"),
        ("cloudformation", "Stacks CRUD, templates, resource provisioning"),
        ("rds",            "DB instances, clusters (stub)"),
        ("apigateway",     "REST APIs, resources, methods, integrations, stages"),
        ("opensearch",     "Domains, indices, search, bulk, aggregations"),
        ("kinesis",        "Streams, shards, put/get records, shard iterators"),
        ("cloudwatch",     "Metrics, statistics, alarms, SNS triggering"),
        ("stepfunctions",  "State machines, executions (auto-succeed), tags"),
        ("ses",            "SendEmail, SendRawEmail, identities, local inbox"),
        ("cognito",        "User pools, sign-up, sign-in, tokens"),
        ("kms",            "Keys, aliases, encrypt/decrypt, data key generation"),
        ("ec2",            "Instances, VPCs, subnets, security groups, key pairs, volumes"),
        ("acm",            "Certificate management, tags"),
        ("route53",        "Hosted zones, record sets"),
    ]
    for name, desc in all_svc:
        click.echo(f"    \033[32m•\033[0m {name:<18} \033[90m{desc}\033[0m")
    click.echo()


@main.group()
def fault():
    """Manage fault injection rules."""
    pass


@fault.command("list")
@click.option("--port", default=4566)
def fault_list(port):
    """List all active fault rules."""
    try:
        r = requests.get(f"http://localhost:{port}/_localrun/faults", timeout=5)
        faults = r.json().get("faults", [])
        if not faults:
            click.echo("No active faults.")
            return
        for f in faults:
            click.echo(f"  [{f.get('id','')}] {f.get('service','*')}/{f.get('action','*')} "
                       f"type={f.get('type')} prob={f.get('probability',1.0)}")
    except Exception as e:
        click.echo(f"\033[31m✖\033[0m Could not connect: {e}")


@fault.command("add")
@click.option("--port", default=4566)
@click.option("--service", default=None, help="Service name (e.g. s3)")
@click.option("--action", default=None, help="Action name (e.g. GetObject)")
@click.option("--type", "fault_type", default="error", help="error or latency")
@click.option("--status", "status_code", default=500, help="HTTP status code for error type")
@click.option("--delay", default=2.0, help="Delay in seconds for latency type")
@click.option("--probability", default=1.0, help="Fault probability 0.0-1.0")
def fault_add(port, service, action, fault_type, status_code, delay, probability):
    """Add a fault injection rule."""
    body = {"type": fault_type, "probability": probability}
    if service:
        body["service"] = service
    if action:
        body["action"] = action
    if fault_type == "error":
        body["status_code"] = status_code
    if fault_type == "latency":
        body["delay"] = delay
    try:
        r = requests.post(f"http://localhost:{port}/_localrun/faults", json=body, timeout=5)
        data = r.json()
        click.echo(f"\033[32m✔\033[0m Fault added: id={data.get('id')}")
    except Exception as e:
        click.echo(f"\033[31m✖\033[0m Could not connect: {e}")


@fault.command("clear")
@click.option("--port", default=4566)
@click.option("--id", "fault_id", default=None, help="Specific fault ID to remove")
def fault_clear(port, fault_id):
    """Remove a fault rule (or all faults if no ID given)."""
    try:
        url = f"http://localhost:{port}/_localrun/faults"
        if fault_id:
            url += f"?id={fault_id}"
        r = requests.delete(url, timeout=5)
        click.echo(f"\033[32m✔\033[0m {r.json().get('message')}")
    except Exception as e:
        click.echo(f"\033[31m✖\033[0m Could not connect: {e}")


@main.command()
@click.option("--port", default=4566)
@click.option("--output", default=None, help="Output file (default: stdout)")
@click.option("--service", default=None, help="Export only a specific service")
def export(port, output, service):
    """Export current state as a CloudFormation-compatible JSON template."""
    try:
        url = f"http://localhost:{port}/_localrun/resources"
        if service:
            url += f"?service={service}"
        r = requests.get(url, timeout=5)
        resources = r.json().get("resources", [])
    except Exception as e:
        click.echo(f"\033[31m✖\033[0m Could not connect: {e}")
        raise SystemExit(1)

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "LocalRun state export",
        "Resources": {}
    }

    for res in resources:
        svc = res.get("service", "")
        name = res.get("name", "")
        logical_id = "".join(c for c in name if c.isalnum())[:64] or "Resource"
        if svc == "s3":
            template["Resources"][logical_id + "Bucket"] = {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": name}
            }
        elif svc == "sqs":
            template["Resources"][logical_id + "Queue"] = {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": name}
            }
        elif svc == "dynamodb":
            template["Resources"][logical_id + "Table"] = {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {"TableName": name, "BillingMode": "PAY_PER_REQUEST",
                               "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                               "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
            }
        elif svc == "sns":
            template["Resources"][logical_id + "Topic"] = {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": name}
            }

    out = json.dumps(template, indent=2)
    if output:
        with open(output, "w") as f:
            f.write(out)
        click.echo(f"\033[32m✔\033[0m Template written to {output}")
    else:
        click.echo(out)


@main.command()
@click.option("--port", default=4566)
@click.option("--service", default=None, help="Filter by service name")
@click.option("--limit", default=20, help="Max results")
def resources(port, service, limit):
    """List all resources currently in LocalRun."""
    try:
        url = f"http://localhost:{port}/_localrun/resources"
        if service:
            url += f"?service={service}"
        r = requests.get(url, timeout=5)
        data = r.json()
    except Exception as e:
        click.echo(f"\033[31m✖\033[0m Could not connect: {e}")
        raise SystemExit(1)
    res_list = data.get("resources", [])[:limit]
    if not res_list:
        click.echo("No resources found.")
        return
    click.echo(f"  {'SERVICE':<16} {'TYPE':<20} {'NAME'}")
    click.echo(f"  {'-------':<16} {'----':<20} {'----'}")
    for res in res_list:
        click.echo(f"  {res.get('service',''):<16} {res.get('type',''):<20} {res.get('name','')}")


@main.command()
@click.option("--port", default=4566, help="Port to check")
@click.option("--host", default="localhost")
def doctor(port, host):
    """Diagnose LocalRun configuration and connectivity."""
    ok = click.style("OK", fg="green")
    err = click.style("FAIL", fg="red")

    click.echo("LocalRun Doctor\n")

    # Check health endpoint
    health_data = {}
    try:
        r = requests.get(f"http://{host}:{port}/health", timeout=2)
        if r.status_code == 200:
            health_data = r.json()
            version = health_data.get("version", "?")
            click.echo(f"  [{ok}] Server running at {host}:{port} (version {version})")
        else:
            click.echo(f"  [{err}] Server returned status {r.status_code}")
    except Exception as e:
        click.echo(f"  [{err}] Cannot connect to {host}:{port}: {e}")
        click.echo("       Fix: run 'aws-local-run start'")

    # Check requests endpoint
    try:
        r = requests.get(f"http://{host}:{port}/_localrun/requests", timeout=2)
        if r.status_code == 200:
            click.echo(f"  [{ok}] Request log endpoint available")
        else:
            click.echo(f"  [{err}] Request log endpoint returned {r.status_code}")
    except Exception as e:
        click.echo(f"  [{err}] Request log unavailable: {e}")

    # Print service health
    if health_data.get("services"):
        click.echo("\n  Services:")
        for svc, info in health_data["services"].items():
            click.echo(f"    [{ok}] {svc:<20} {info}")

    # Check env vars
    import os
    click.echo("\n  Environment:")
    for var in ("AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"):
        val = os.environ.get(var)
        if val:
            click.echo(f"  [{ok}] {var}={val[:30]}")
        else:
            click.echo(f"  [{err}] {var} not set")

    # Check boto3
    try:
        import boto3
        click.echo(f"\n  [{ok}] boto3 {boto3.__version__}")
    except ImportError:
        click.echo(f"\n  [{err}] boto3 not installed (pip install boto3)")

    # Check pyyaml
    try:
        import yaml
        click.echo(f"  [{ok}] pyyaml installed")
    except ImportError:
        click.echo(f"  [{err}] pyyaml not installed (pip install pyyaml) — needed for YAML config files")

    # Check for config files
    for fname in ("localrun.yaml", "localrun.yml", "localrun.json"):
        if os.path.isfile(fname):
            click.echo(f"  [{ok}] Config file found: {fname}")
            break
    else:
        click.echo(f"  [   ] No localrun.yaml/json found in current directory")

    click.echo("\nDone.")


_ALL_24_SERVICES = [
    "s3", "sqs", "dynamodb", "sns", "lambda", "iam", "logs", "sts",
    "secretsmanager", "ssm", "events", "cloudformation", "rds", "apigateway",
    "opensearch", "kinesis", "cloudwatch", "stepfunctions", "ses", "cognito",
    "kms", "ec2", "acm", "route53",
]


@main.command("terraform-config")
@click.option("--port", default=4566)
@click.option("--region", default="us-east-1")
def terraform_config(port, region):
    """Print Terraform AWS provider config for LocalRun."""
    endpoint = f"http://localhost:{port}"
    lines = [
        'provider "aws" {',
        f'  region                      = "{region}"',
        '  access_key                  = "test"',
        '  secret_key                  = "test"',
        '  skip_credentials_validation = true',
        '  skip_metadata_api_check     = true',
        '  skip_requesting_account_id  = true',
        '',
        '  endpoints {',
    ]
    svc_map = {
        "s3": "s3", "sqs": "sqs", "dynamodb": "dynamodb", "sns": "sns",
        "lambda": "lambda", "iam": "iam", "logs": "cloudwatchlogs",
        "sts": "sts", "secretsmanager": "secretsmanager", "ssm": "ssm",
        "events": "cloudwatchevents", "cloudformation": "cloudformation",
        "rds": "rds", "apigateway": "apigateway", "opensearch": "opensearch",
        "kinesis": "kinesis", "cloudwatch": "cloudwatch",
        "stepfunctions": "sfn", "ses": "ses", "cognito": "cognitoidp",
        "kms": "kms", "ec2": "ec2", "acm": "acm", "route53": "route53",
    }
    for svc, tf_name in svc_map.items():
        lines.append(f'    {tf_name:<24} = "{endpoint}"')
    lines.extend(['  }', '}'])
    click.echo("\n".join(lines))


@main.command("terraform-init")
@click.option("--dir", "target_dir", default=".", help="Directory to write localrun.tf")
@click.option("--port", default=4566)
@click.option("--region", default="us-east-1")
@click.option("--cdktf", is_flag=True, help="Print CDK for Terraform TypeScript instead")
def terraform_init(target_dir, port, region, cdktf):
    """Write localrun.tf to a directory (or print CDKTF snippet)."""
    import os
    endpoint = f"http://localhost:{port}"

    if cdktf:
        snippet = f"""// CDK for Terraform — LocalRun provider config
import {{ AwsProvider }} from "@cdktf/provider-aws/lib/provider";

new AwsProvider(this, "LocalRun", {{
  region: "{region}",
  accessKey: "test",
  secretKey: "test",
  skipCredentialsValidation: true,
  skipMetadataApiCheck: "true",
  skipRequestingAccountId: true,
  endpoints: [{{
    s3: "{endpoint}",
    sqs: "{endpoint}",
    dynamodb: "{endpoint}",
    sns: "{endpoint}",
    lambda: "{endpoint}",
    iam: "{endpoint}",
  }}],
}});"""
        click.echo(snippet)
        return

    tf_content = f"""provider "aws" {{
  region                      = "{region}"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {{
    s3               = "{endpoint}"
    sqs              = "{endpoint}"
    dynamodb         = "{endpoint}"
    sns              = "{endpoint}"
    lambda           = "{endpoint}"
    iam              = "{endpoint}"
    cloudwatchlogs   = "{endpoint}"
    sts              = "{endpoint}"
    secretsmanager   = "{endpoint}"
    ssm              = "{endpoint}"
    cloudwatchevents = "{endpoint}"
    cloudformation   = "{endpoint}"
    rds              = "{endpoint}"
    apigateway       = "{endpoint}"
    opensearch       = "{endpoint}"
    kinesis          = "{endpoint}"
    cloudwatch       = "{endpoint}"
    sfn              = "{endpoint}"
    ses              = "{endpoint}"
    cognitoidp       = "{endpoint}"
    kms              = "{endpoint}"
    ec2              = "{endpoint}"
    acm              = "{endpoint}"
    route53          = "{endpoint}"
  }}
}}
"""
    out_path = os.path.join(target_dir, "localrun.tf")
    with open(out_path, "w") as f:
        f.write(tf_content)
    click.echo(f"Written: {out_path}")
