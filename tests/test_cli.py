import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bluesky_ingest.cli import MetricsServer, launch
from bluesky_ingest.config import Settings
from bluesky_ingest.metrics import Metrics, create_app

import uvicorn


class CliTests(unittest.TestCase):
    def run_cli(self, output, *args):
        return subprocess.run(
            [sys.executable, "-m", "bluesky_ingest.cli", "--no-metrics", *args],
            env={**os.environ, "RAW_DIR": str(output)},
            text=True, capture_output=True, timeout=10,
        )

    def test_replay_command(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_cli(directory, "--replay", "tests/fixtures/events.jsonl")
            self.assertEqual(result.returncode, 0, result.stderr)
            lines = [line for p in Path(directory).glob("*.jsonl") for line in p.read_text().splitlines()]
            self.assertEqual(len(lines), 6)
            final = json.loads(result.stderr.splitlines()[-1])
            self.assertEqual(final["events_persisted"], 6)
            self.assertEqual(final["connection_state"], "stopped")

    def test_limit_and_invalid_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_cli(directory, "--replay", "tests/fixtures/events.jsonl", "--limit", "2")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stderr.splitlines()[-1])["events_persisted"], 2)
            result = self.run_cli(directory, "--limit", "-1")
            self.assertEqual(result.returncode, 2)

    def test_missing_replay_is_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_cli(directory, "--replay", str(Path(directory) / "missing.jsonl"))
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stderr.splitlines()[-1])["connection_state"], "failed")


class ServerFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_bind_failure_becomes_regular_task_error(self):
        server = MetricsServer(uvicorn.Config(create_app(Metrics())))
        async def fail(self, sockets=None):
            raise SystemExit(1)
        with patch.object(uvicorn.Server, "serve", fail):
            with self.assertRaises(RuntimeError):
                await server.serve()

    async def test_server_failure_drains_ingestion_and_reports_error(self):
        async def fail(self, sockets=None):
            raise RuntimeError("synthetic metrics failure")
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(MetricsServer, "serve", fail), patch("bluesky_ingest.cli.log_event") as log:
                with self.assertRaises(RuntimeError):
                    await launch(Settings(raw_dir=Path(directory)), Path("tests/fixtures/events.jsonl"), 0, True)
                name = log.call_args.args[0]
                metrics = log.call_args.kwargs
                self.assertEqual(name, "final_metrics")
                self.assertEqual(metrics["events_received"], metrics["events_persisted"])
                self.assertEqual(metrics["connection_state"], "failed")
                self.assertEqual(metrics["errors"], 1)
