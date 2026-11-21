#!/usr/bin/env python
"""Run the logistic-map evaluation against the cases in eval/cases."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from inspect_ai import eval as inspect_eval

EVAL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(EVAL_DIR))

from logistic_eval.config import LOGS_DIR, load_settings  # noqa: E402
from logistic_eval.report import build_report, write_reports  # noqa: E402
from logistic_eval.specs import available_cases  # noqa: E402
from logistic_eval.task import logistic_extraction  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cases",
        default="",
        help=f"comma-separated case ids (default: all of {', '.join(available_cases())})",
    )
    parser.add_argument("--limit", type=int, default=None, help="run only the first N samples")
    parser.add_argument("--max-messages", type=int, default=90, help="agent message budget")
    parser.add_argument("--log-dir", default=str(LOGS_DIR), help="Inspect log directory")
    parser.add_argument(
        "--report-dir",
        default=str(EVAL_DIR / "reports"),
        help="directory for the Markdown/JSON report",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="skip writing the Markdown/JSON report",
    )
    parser.add_argument(
        "--min-structural",
        type=float,
        default=0.75,
        help="fail when a sample scores below this on deterministic checks",
    )
    parser.add_argument(
        "--min-judge",
        type=float,
        default=0.60,
        help="fail when a sample scores below this on the LLM judge",
    )
    parser.add_argument(
        "--display",
        default="plain",
        choices=("full", "conversation", "rich", "plain", "log", "none"),
        help="Inspect progress display",
    )
    return parser.parse_args()


def summarize(log, min_structural: float, min_judge: float) -> bool:
    samples = log.samples or []
    if not samples:
        print("no samples were scored; inspect the log for errors", file=sys.stderr)
        return False

    header = f"{'case':<10} {'structural':>11} {'judge':>7}  status"
    print("\n" + header)
    print("-" * len(header))
    ok = True

    for sample in samples:
        scores = sample.scores or {}
        structural = scores.get("structural_scorer")
        judge = scores.get("judge_scorer")
        structural_value = float(structural.value) if structural else 0.0
        judge_value = float(judge.value) if judge else 0.0
        failed = structural_value < min_structural or judge_value < min_judge
        ok = ok and not failed
        status = "FAIL" if failed else "ok"
        print(f"{str(sample.id):<10} {structural_value:>11.3f} {judge_value:>7.3f}  {status}")

        if failed:
            if structural and structural_value < min_structural:
                print(f"  structural: {(structural.explanation or '')[:600]}")
            if judge and judge_value < min_judge:
                print(f"  judge: {(judge.explanation or '')[:600]}")

    return ok


def main() -> int:
    args = parse_args()
    load_dotenv(EVAL_DIR / ".env")
    settings = load_settings()

    for key, value in (
        ("LITELLM_BASE_URL", settings.base_url),
        ("LITELLM_API_KEY", settings.api_key),
    ):
        os.environ[key] = value

    log_dir = Path(args.log_dir).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)

    print(f"extractor: {settings.extractor}")
    print(f"judge:     {settings.judge}")
    print(f"logs:      {log_dir}")

    logs = inspect_eval(
        logistic_extraction(cases=args.cases, max_messages=args.max_messages),
        model=settings.extractor,
        model_roles={"grader": settings.judge},
        log_dir=str(log_dir),
        limit=args.limit,
        display=args.display,
    )

    ok = True
    for log in logs:
        ok = summarize(log, args.min_structural, args.min_judge) and ok

    if not args.no_report:
        report = build_report(
            logs, settings, log_dir, args.min_structural, args.min_judge
        )
        md_path, json_path = write_reports(report, Path(args.report_dir).expanduser())
        print(f"\nreport: {md_path}")
        print(f"        {json_path}")

    print(f"\nview results: {EVAL_DIR / '.venv' / 'bin' / 'inspect'} view --log-dir {log_dir}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
