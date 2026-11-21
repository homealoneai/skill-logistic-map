#!/usr/bin/env python
"""Import a sample graph into a scratch database and open its HTML map.

    .logistic-map/.venv/bin/python test/scripts/open_sample.py --sample go
    .logistic-map/.venv/bin/python test/scripts/open_sample.py --sample web --no-open
    .logistic-map/.venv/bin/python test/scripts/open_sample.py --list
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEST_DIR = ROOT / "test"
SAMPLE_DIR = TEST_DIR / "sample"
MANIFEST = SAMPLE_DIR / "manifest.json"
BUILD_DIR = TEST_DIR / ".build"
CLI = ROOT / "scripts" / "logistic_map.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", default="go", help="sample id (default: go)")
    parser.add_argument("--out", default="", help="output directory (default: test/.build/<sample>)")
    parser.add_argument("--no-open", action="store_true", help="render without opening the browser")
    parser.add_argument("--list", action="store_true", help="list the available samples")
    return parser.parse_args()


def run_cli(args: list[str], db: Path, cwd: Path) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        [sys.executable, str(CLI), "--db", str(db), *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(f"CLI failed ({' '.join(args)}):\n{proc.stdout}\n{proc.stderr}")
    return proc


def load_manifest() -> dict:
    if not MANIFEST.is_file():
        raise SystemExit(
            f"missing {MANIFEST}; run `make samples` (or "
            "eval/.venv/bin/python test/scripts/extract_samples.py)"
        )
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def main() -> int:
    args = parse_args()
    manifest = load_manifest()
    samples = manifest["samples"]

    if args.list:
        for sample_id, meta in samples.items():
            counts = meta["counts"]
            report = meta["report"]
            print(
                f"{sample_id:<8} {counts['nodes']:>4} nodes  {counts['edges']:>4} edges  "
                f"{report['error']} errors  {report['warn']} warnings"
            )
        return 0

    if args.sample not in samples:
        raise SystemExit(
            f"unknown sample {args.sample!r}; available: {', '.join(samples)}"
        )

    meta = samples[args.sample]
    case_dir = (ROOT / "eval" / meta["case_path"]).resolve()
    sample_path = (SAMPLE_DIR / f"{args.sample}.json").resolve()

    out_dir = Path(args.out) if args.out else BUILD_DIR / args.sample
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "graph.kuzu"
    html_path = out_dir / "graph.html"

    run_cli(["init"], db_path, case_dir)
    run_cli(["apply", "--file", str(sample_path), "--json"], db_path, case_dir)
    report = json.loads(run_cli(["report", "--format", "json", "--check-files"], db_path, case_dir).stdout)
    run_cli(["render", "--out", str(html_path), "--title", f"Sample: {args.sample}"], db_path, case_dir)

    summary = report["summary"]
    print(
        f"{args.sample}: {summary['contracts']} contracts, {summary['fields']} fields, "
        f"{summary['edges']} edges"
    )
    print(
        f"report: {summary['findings']['error']} errors, "
        f"{summary['findings']['warn']} warnings, {summary['findings']['info']} info"
    )
    print(f"html:   {html_path}")
    print(f"db:     {db_path}")

    if summary["findings"]["error"]:
        print("warning: the sample has structural errors; inspect the report", file=sys.stderr)

    if not args.no_open:
        webbrowser.open(html_path.as_uri())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
