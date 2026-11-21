"""Per-sample workspace and the only writer to the logistic-map database."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from .config import SKILL_CLI, WORK_DIR


class MapSession:
    """Owns the Kuzu database and JSON artefacts for one evaluated case."""

    def __init__(self, spec: dict) -> None:
        self.spec = spec
        self.case_dir = Path(spec["case_dir"])
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix=f"{spec['id']}-", dir=WORK_DIR))
        self.db_path = self.root / "graph.kuzu"
        self.export_path = self.root / "graph.json"
        self.call_count = 0
        self.apply_count = 0

    @classmethod
    def from_metadata(cls, metadata: dict) -> "MapSession":
        """Rebuild the session paths from sample metadata (used by scorers)."""
        session = cls.__new__(cls)
        session.spec = metadata["spec"]
        session.case_dir = Path(metadata["case_dir"])
        session.root = Path(metadata["work_dir"])
        session.db_path = session.root / "graph.kuzu"
        session.export_path = session.root / "graph.json"
        session.call_count = int(metadata.get("cli_calls", 0))
        session.apply_count = 0
        return session

    def _run(self, args: list[str], timeout: int = 180) -> subprocess.CompletedProcess:
        command = [
            sys.executable,
            str(SKILL_CLI),
            "--db",
            str(self.db_path),
            *[str(item) for item in args],
        ]
        env = os.environ.copy()
        env.setdefault("NO_COLOR", "1")
        env["PYTHONIOENCODING"] = "utf-8"
        self.call_count += 1
        try:
            return subprocess.run(
                command,
                cwd=str(self.case_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )
        except subprocess.TimeoutExpired as exc:
            return subprocess.CompletedProcess(
                command, 124, stdout=exc.stdout or "", stderr=f"timed out after {timeout}s"
            )

    def run_cli(self, args: list[str], timeout: int = 180) -> str:
        proc = self._run(args, timeout=timeout)
        output = "\n".join(
            part.strip() for part in (proc.stdout or "", proc.stderr or "") if part.strip()
        )
        return f"exit={proc.returncode}\n{output}".strip()

    def init(self) -> str:
        return self.run_cli(["init"])

    def export(self) -> tuple[dict | None, str]:
        if self.export_path.exists():
            self.export_path.unlink()
        log = self.run_cli(["export", "--out", str(self.export_path)])
        if not self.export_path.exists():
            return None, log
        try:
            return json.loads(self.export_path.read_text(encoding="utf-8")), log
        except (json.JSONDecodeError, OSError) as exc:
            return None, f"{log}\ninvalid export: {exc}"

    def report(self) -> dict:
        proc = self._run(["report", "--format", "json", "--check-files"], timeout=180)
        try:
            return json.loads(proc.stdout)
        except (json.JSONDecodeError, TypeError):
            return {
                "error": "\n".join(
                    part for part in (proc.stdout or "", proc.stderr or "") if part
                ).strip()
            }

    def cleanup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)
