"""Shared helpers for the logistic-map CLI tests."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEST_DIR = ROOT / "test"
SAMPLE_DIR = TEST_DIR / "sample"
MANIFEST_PATH = SAMPLE_DIR / "manifest.json"
CLI = ROOT / "scripts" / "logistic_map.py"
PYTHON = sys.executable


def run_cli(
    args: list[str],
    db: Path,
    cwd: Path = ROOT,
    timeout: int = 180,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [PYTHON, str(CLI), "--db", str(db), *[str(item) for item in args]],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


class GraphTestCase(unittest.TestCase):
    """A test case with a temporary, initialised LadybugDB database."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="logistic-test-")
        self.tmp = Path(self._tmp.name)
        self.db = self.tmp / "graph.lbdb"
        proc = run_cli(["init"], self.db, cwd=self.tmp)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def cli(self, *args, cwd: Path | None = None, expect: int | None = 0) -> subprocess.CompletedProcess:
        proc = run_cli(list(args), self.db, cwd=cwd or self.tmp)
        if expect is not None:
            self.assertEqual(
                proc.returncode,
                expect,
                f"command {args} exited {proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
            )
        return proc

    def export(self) -> dict:
        out = self.tmp / "export.json"
        self.cli("export", "--out", out)
        return json.loads(out.read_text(encoding="utf-8"))

    def add_flow(self, flow_id: str = "flow:test", name: str = "Test flow",
                 entities=()) -> str:
        self.cli("add-flow", "--id", flow_id, "--name", name)
        for entity in entities:
            self.cli("flow", "use", "--id", flow_id, "--entity", entity)
        return flow_id

    def report(self, *extra) -> dict:
        proc = self.cli("report", "--format", "json", *extra)
        return json.loads(proc.stdout)

    def stats(self) -> dict:
        proc = self.cli("stats", "--json")
        return json.loads(proc.stdout)
