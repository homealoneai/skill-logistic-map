"""Load the evaluation cases (one JSON file per repository slice)."""

from __future__ import annotations

import json
from pathlib import Path

from .config import CASES_DIR, EVAL_DIR


def available_cases() -> list[str]:
    return sorted(path.stem for path in CASES_DIR.glob("*.json"))


def load_specs(selection: str | None = None) -> list[dict]:
    available = {name: CASES_DIR / f"{name}.json" for name in available_cases()}
    if not available:
        raise SystemExit(f"no cases found in {CASES_DIR}")

    if selection:
        ids = [item.strip() for item in selection.split(",") if item.strip()]
        unknown = [item for item in ids if item not in available]
        if unknown:
            raise SystemExit(
                f"unknown case(s): {', '.join(unknown)}; available: {', '.join(available)}"
            )
        paths = [available[item] for item in ids]
    else:
        paths = list(available.values())

    specs = []
    for path in paths:
        spec = json.loads(path.read_text(encoding="utf-8"))
        case_dir = (EVAL_DIR / spec["path"]).resolve()
        if not case_dir.is_dir():
            raise SystemExit(f"case {spec['id']}: directory not found: {case_dir}")
        spec["case_dir"] = str(case_dir)
        specs.append(spec)
    return specs
