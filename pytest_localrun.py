"""pytest-localrun — auto-start LocalRun as a test fixture.

Install with:  pip install .  (or pip install pytest-localrun once published)
Then in your conftest.py, import the fixtures:

  from pytest_localrun import localrun_server, localrun_port

Or just install the package and pytest discovers the plugin automatically
via the entry point in pyproject.toml.

Usage in tests:
  def test_my_thing(localrun_server):
      import boto3
      s3 = boto3.client("s3", endpoint_url=localrun_server)
      ...
"""

import subprocess
import sys
import time
import pytest
import requests


DEFAULT_PORT = 14566


def _wait_for_server(port, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"http://127.0.0.1:{port}/health", timeout=1)
            if r.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        except Exception:
            pass
        time.sleep(0.3)
    return False


@pytest.fixture(scope="session")
def localrun_port():
    """Return the port LocalRun is running on."""
    return DEFAULT_PORT


@pytest.fixture(scope="session")
def localrun_server():
    """Start LocalRun and return its base URL.

    If a server is already running on DEFAULT_PORT, reuse it.
    Otherwise start one as a subprocess and stop it after the session.
    """
    base_url = f"http://127.0.0.1:{DEFAULT_PORT}"

    # Check if already running
    try:
        r = requests.get(f"{base_url}/health", timeout=1)
        if r.status_code == 200:
            yield base_url
            return
    except Exception:
        pass

    # Start a new server
    proc = subprocess.Popen(
        [sys.executable, "-m", "localrun.cli", "start", "--port", str(DEFAULT_PORT), "--host", "127.0.0.1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    ready = _wait_for_server(DEFAULT_PORT, timeout=20)
    if not ready:
        proc.terminate()
        proc.wait()
        raise RuntimeError(f"LocalRun did not start on port {DEFAULT_PORT}")

    yield base_url

    proc.terminate()
    proc.wait()


@pytest.fixture(autouse=False)
def reset_localrun(localrun_server):
    """Reset all LocalRun state before a test.

    Use this fixture when you need a clean slate for each test.
    """
    requests.post(f"{localrun_server}/_localrun/reset", timeout=5)
    yield
    # No teardown needed — state is reset at the start of next test
