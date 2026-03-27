"""Tests for the doctor and terraform-config CLI commands."""
import pytest
from click.testing import CliRunner
from localrun.cli import main


class TestDoctor:
    def test_doctor_runs_without_error(self, localrun_server):
        runner = CliRunner()
        result = runner.invoke(main, ["doctor", "--port", "14566"])
        # doctor should exit 0 and output something useful
        assert result.exit_code == 0
        assert "LocalRun Doctor" in result.output or "Doctor" in result.output

    def test_terraform_config_outputs_provider_block(self):
        runner = CliRunner()
        result = runner.invoke(main, ["terraform-config", "--port", "4566"])
        assert result.exit_code == 0
        assert "provider" in result.output
        assert "aws" in result.output
        assert "skip_credentials_validation" in result.output
