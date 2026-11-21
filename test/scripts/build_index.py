#!/usr/bin/env python
"""Build test/.build/index.html with links to every sample map.

    .logistic-map/.venv/bin/python test/scripts/build_index.py
    .logistic-map/.venv/bin/python test/scripts/build_index.py --force
    .logistic-map/.venv/bin/python test/scripts/build_index.py --no-build
"""

from __future__ import annotations

import argparse
import html
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEST_DIR = ROOT / "test"
SAMPLE_DIR = TEST_DIR / "sample"
BUILD_DIR = TEST_DIR / ".build"
MANIFEST = SAMPLE_DIR / "manifest.json"
OPEN_SAMPLE = TEST_DIR / "scripts" / "open_sample.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true",
                        help="rebuild every sample graph before writing the index")
    parser.add_argument("--no-build", action="store_true",
                        help="only write the index, do not render missing graphs")
    return parser.parse_args()


def load_samples() -> dict:
    if not MANIFEST.is_file():
        raise SystemExit(
            f"missing {MANIFEST}; run `make samples` first")
    return json.loads(MANIFEST.read_text(encoding="utf-8"))["samples"]


def ensure_graph(sample_id: str, force: bool, no_build: bool) -> bool:
    target = BUILD_DIR / sample_id / "graph.html"
    if no_build:
        return target.is_file()
    if force or not target.is_file():
        proc = subprocess.run(
            [sys.executable, str(OPEN_SAMPLE), "--sample", sample_id, "--no-open"],
            cwd=str(ROOT), capture_output=True, text=True,
        )
        if proc.returncode != 0:
            print(f"{sample_id}: build failed\n{proc.stdout}\n{proc.stderr}",
                  file=sys.stderr)
            return False
    return target.is_file()


def render(samples: dict, available: dict[str, bool]) -> str:
    cards = []
    for sample_id in sorted(samples):
        meta = samples[sample_id]
        counts = meta.get("counts", {})
        report = meta.get("report", {})
        errors = int(report.get("error", 0))
        warnings = int(report.get("warn", 0))
        status = "error" if errors else ("warn" if warnings else "ok")
        label = html.escape(str(meta.get("title") or sample_id))
        href = f"./{sample_id}/graph.html"
        link = (f'<a class="open" href="{href}">Open map</a>'
                if available.get(sample_id) else
                '<span class="missing">not built</span>')
        cards.append(f"""
        <article class="card {status}">
          <div class="card-head">
            <h2>{label}</h2>
            <code>{html.escape(sample_id)}</code>
          </div>
          <div class="metrics">
            <span><b>{counts.get('nodes', 0)}</b> nodes</span>
            <span><b>{counts.get('edges', 0)}</b> edges</span>
            <span><b>{counts.get('flows', 0)}</b> flows</span>
            <span><b>{counts.get('rules', 0)}</b> rules</span>
            <span><b>{counts.get('clusters', 0)}</b> clusters</span>
          </div>
          <div class="report">
            <span class="pill {status}">{errors} errors</span>
            <span class="pill {('warn' if warnings else 'ok')}">{warnings} warnings</span>
          </div>
          {link}
        </article>""")
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Logistic map samples</title>
<style>
  :root {{
    --bg: #080d18; --panel: #0d1526; --line: #1e293b; --text: #e2e8f0;
    --muted: #94a3b8; --ok: #34d399; --warn: #fbbf24; --error: #fb7185;
    --accent: #22d3ee;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 40px; background: var(--bg); color: var(--text);
    font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  }}
  header {{ max-width: 1100px; margin: 0 auto 24px; }}
  h1 {{ margin: 0 0 6px; font-size: 22px; }}
  .sub {{ color: var(--muted); }}
  .grid {{
    max-width: 1100px; margin: 0 auto; display: grid; gap: 14px;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  }}
  .card {{
    background: var(--panel); border: 1px solid var(--line); border-radius: 12px;
    padding: 18px; display: flex; flex-direction: column; gap: 12px;
  }}
  .card.error {{ border-color: rgba(251,113,133,.5); }}
  .card.warn {{ border-color: rgba(251,191,36,.4); }}
  .card-head {{ display: flex; justify-content: space-between; gap: 10px; align-items: baseline; }}
  h2 {{ margin: 0; font-size: 15px; }}
  code {{ color: var(--muted); font-size: 11px; }}
  .metrics {{ display: flex; flex-wrap: wrap; gap: 10px; color: var(--muted); font-size: 12px; }}
  .metrics b {{ color: var(--text); font-size: 13px; }}
  .report {{ display: flex; gap: 8px; }}
  .pill {{ font-size: 11px; border: 1px solid currentColor; border-radius: 999px; padding: 2px 8px; }}
  .pill.ok {{ color: var(--ok); }}
  .pill.warn {{ color: var(--warn); }}
  .pill.error {{ color: var(--error); }}
  .open {{
    color: var(--accent); text-decoration: none; font-weight: 600;
    border-bottom: 1px solid rgba(34,211,238,.35); align-self: flex-start;
  }}
  .open:hover {{ color: #a5f3fc; }}
  .missing {{ color: var(--muted); font-size: 12px; }}
  footer {{ max-width: 1100px; margin: 26px auto 0; color: var(--muted); font-size: 11px; }}
</style>
</head>
<body>
<header>
  <h1>Logistic map samples</h1>
  <div class="sub">Open any generated graph. Rebuild with <code>make index</code>.</div>
</header>
<main class="grid">{''.join(cards)}
</main>
<footer>Generated {html.escape(generated)} · {len(samples)} samples</footer>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    samples = load_samples()
    available = {}
    for sample_id in samples:
        available[sample_id] = ensure_graph(
            sample_id, force=args.force, no_build=args.no_build)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    index_path = BUILD_DIR / "index.html"
    index_path.write_text(render(samples, available), encoding="utf-8")
    print(index_path)
    missing = [sample_id for sample_id, ok in available.items() if not ok]
    if missing:
        print(f"missing graphs: {', '.join(missing)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
