"""CLI entry point."""
import json
import logging
import time
import click
import requests
from localrun import __version__
from localrun.config import ALL_SERVICES

LOGO = """
\033[36m
    __    ____  _________    __    ____  __  ___   __
   / /   / __ \/ ____/   |  / /   / __ \/ / / / | / /
  / /   / / / / /   / /| | / /   / /_/ / / / /  |/ /
 / /___/ /_/ / /___/ ___ |/ /___/ _, _/ /_/ / /|  /
/_____/\____/\____/_/  |_/_____/_/ |_|\____/_/ |_/
\033[0m"""

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
@click.option("--debug", is_flag=True, help="Enable debug logging")
def start(port, host, services, data_dir, seed, debug):
    """Start the LocalRun server."""
    from localrun.config import LocalRunConfig, set_config
    from localrun.gateway import create_app

    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")

    config = LocalRunConfig(host=host, port=port, debug=debug)
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

    app.run(host=host, port=port, debug=debug, use_reloader=False)


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
    click.echo("  \033[1mSupported Services (18):\033[0m\n")
    all_svc = [
        ("s3",             "Buckets, objects, range, pagination, multipart upload"),
        ("sqs",            "Queues, messages, batch ops, purge, visibility"),
        ("dynamodb",       "Tables, items, query, scan, transactions"),
        ("sns",            "Topics, subscriptions, publish, SQS delivery"),
        ("lambda",         "Functions, invoke via subprocess, async invoke"),
        ("iam",            "Roles, policies, users (stub)"),
        ("logs",           "Log groups, streams, events, metric filters, tags"),
        ("sts",            "GetCallerIdentity, AssumeRole, GetSessionToken"),
        ("secretsmanager", "Secrets CRUD, versioning, tags"),
        ("ssm",            "Parameters CRUD, get-by-path, tags"),
        ("events",         "Rules, targets, event buses, SQS/SNS routing"),
        ("cloudformation", "Stacks CRUD, templates (stub)"),
        ("rds",            "DB instances, clusters (stub)"),
        ("apigateway",     "REST APIs, resources, methods, integrations, stages"),
        ("opensearch",     "Domains, indices, search, bulk, aggregations"),
        ("kinesis",        "Streams, shards, put/get records, iterators"),
        ("cloudwatch",     "Metrics, statistics, alarms, set alarm state"),
        ("stepfunctions",  "State machines, executions (auto-succeed), tags"),
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
