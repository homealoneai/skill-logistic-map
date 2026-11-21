#!/usr/bin/env python
"""Copy the newest eval snapshot per case into test/sample and write a manifest.

Run with the eval environment, which has Inspect AI and Kuzu:

    eval/.venv/bin/python test/scripts/extract_samples.py
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from inspect_ai.log import read_eval_log

ROOT = Path(__file__).resolve().parents[2]
EVAL_DIR = ROOT / "eval"
SAMPLE_DIR = ROOT / "test" / "sample"
CASES_DIR = EVAL_DIR / "cases"
LOG_DIR = EVAL_DIR / "logs"
CLI = ROOT / "scripts" / "logistic_map.py"
MANIFEST = SAMPLE_DIR / "manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        default="",
        help="comma-separated case ids (default: every case file in eval/cases)",
    )
    return parser.parse_args()


def load_cases(selection: str) -> dict[str, dict]:
    cases = {}
    for path in sorted(CASES_DIR.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        cases[spec["id"]] = spec
    if selection:
        wanted = [item.strip() for item in selection.split(",") if item.strip()]
        unknown = [item for item in wanted if item not in cases]
        if unknown:
            raise SystemExit(f"unknown case(s): {', '.join(unknown)}")
        cases = {item: cases[item] for item in wanted}
    return cases


def newest_snapshots(case_ids: set[str]) -> dict[str, tuple[dict, Path]]:
    found: dict[str, tuple[dict, Path]] = {}
    logs = sorted(LOG_DIR.glob("*.eval"), key=lambda path: path.stat().st_mtime, reverse=True)
    for log_path in logs:
        log = read_eval_log(str(log_path))
        for sample in log.samples or []:
            sample_id = str(sample.id)
            if sample_id in found or sample_id not in case_ids:
                continue
            snapshot = (sample.metadata or {}).get("snapshot")
            if (isinstance(snapshot, dict) and snapshot.get("nodes")
                    and snapshot.get("schema_version") == 7):
                found[sample_id] = (snapshot, log_path)
        if len(found) == len(case_ids):
            break
    missing = sorted(case_ids - set(found))
    if missing:
        raise SystemExit(f"no snapshot found in eval/logs for: {', '.join(missing)}")
    return found


def run_cli(args: list[str], db: Path, cwd: Path) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        [sys.executable, str(CLI), "--db", str(db), *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"CLI failed ({' '.join(args)}):\n{proc.stdout}\n{proc.stderr}"
        )
    return proc


def _valid_ref(case_dir: Path, reference: str) -> bool:
    """Keep only refs that resolve to a file and an existing line."""
    raw = str(reference).strip()
    head, _, tail = raw.rpartition(":")
    path, line = (head, int(tail)) if head and tail.isdigit() else (raw, None)
    target = case_dir / path
    if not target.is_file():
        return False
    if line is None:
        return True
    try:
        with target.open("rb") as handle:
            total = sum(1 for _ in handle)
    except OSError:
        return False
    return 1 <= line <= total


def sanitize_refs(snapshot: dict, case_dir: Path) -> None:
    """Drop refs the fixture test would reject (missing file or line).

    The eval scorer tolerates a small ratio of invalid references; the
    deterministic fixture tests do not. Dropping is honest: the remaining refs
    are still exact, and no line number is invented.
    """
    for collection in (snapshot.get("nodes") or [], snapshot.get("edges") or []):
        for item in collection:
            refs = item.get("refs")
            if refs:
                item["refs"] = [ref for ref in refs if _valid_ref(case_dir, ref)]


def validate_sample(sample_path: Path, case_dir: Path) -> dict:
    with tempfile.TemporaryDirectory(prefix="logistic-sample-") as tmp:
        db = Path(tmp) / "graph.kuzu"
        run_cli(["init"], db, case_dir)
        run_cli(["apply", "--file", str(sample_path)], db, case_dir)
        report = json.loads(run_cli(["report", "--format", "json", "--check-files"], db, case_dir).stdout)
        counts = json.loads(run_cli(["stats", "--json"], db, case_dir).stdout)
    summary = report["summary"]
    return {
        "report": summary["findings"],
        "by_code": summary["by_code"],
        "stats": {
            "nodes": counts["nodes"],
            "edges": counts["edges"],
            "kinds": counts["kinds"],
            "edge_kinds": counts["edge_kinds"],
        },
    }


def main() -> int:
    args = parse_args()
    cases = load_cases(args.only)
    snapshots = newest_snapshots(set(cases))
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "samples": {},
    }

    for sample_id in sorted(cases):
        spec = cases[sample_id]
        snapshot, log_path = snapshots[sample_id]
        case_dir = (EVAL_DIR / spec["path"]).resolve()
        sanitize_refs(snapshot, case_dir)
        sample_path = SAMPLE_DIR / f"{sample_id}.json"
        sample_path.write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

        checks = validate_sample(sample_path, case_dir)

        nodes = snapshot.get("nodes") or []
        edges = snapshot.get("edges") or []
        kinds = Counter(node.get("kind") for node in nodes if node.get("kind") != "field")
        edges_by_kind = Counter(edge.get("kind") for edge in edges)
        contract_kinds = ("input", "entry", "storage")
        recorded = {
            "title": spec.get("title", sample_id),
            "case_path": spec["path"],
            "source_log": log_path.name,
            "counts": {
                "nodes": len(nodes),
                "contracts": sum(node.get("kind") in contract_kinds for node in nodes),
                "flows": sum(node.get("kind") == "flow" for node in nodes),
                "entrypoints": sum(node.get("kind") == "entrypoint" for node in nodes),
                "related": sum(edge.get("kind") == "related" for edge in edges),
                "rules": sum(node.get("kind") == "rule" for node in nodes),
                "clusters": len({str(node.get("cluster") or "").strip()
                                 for node in nodes
                                 if node.get("kind") != "field"
                                 and str(node.get("cluster") or "").strip()}),
                "fields": sum(1 for node in nodes if node.get("kind") == "field"),
                "edges": len(edges),
                "kinds": dict(sorted(kinds.items())),
                "edge_kinds": dict(sorted(edges_by_kind.items())),
            },
            "report": checks["report"],
            "by_code": checks["by_code"],
            "stats": checks["stats"],
        }
        manifest["samples"][sample_id] = recorded
        print(
            f"{sample_id:<8} nodes={recorded['counts']['nodes']:<4} "
            f"edges={recorded['counts']['edges']:<4} "
            f"report={recorded['report']}"
        )

    MANIFEST.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"\nwrote {len(manifest['samples'])} samples and {MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
