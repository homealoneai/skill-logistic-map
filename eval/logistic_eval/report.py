"""Build a Markdown and JSON report from Inspect eval logs."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings

CONTRACT_KINDS = ("input", "entry", "storage")
CONTRACT_PLURALS = {"input": "inputs", "entry": "entries", "storage": "storages"}


def _number(value) -> float | None:
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _sample_record(sample, min_structural: float, min_judge: float) -> dict:
    scores = sample.scores or {}
    structural = scores.get("structural_scorer")
    judge = scores.get("judge_scorer")
    structural_value = _number(structural.value) if structural else None
    judge_value = _number(judge.value) if judge else None
    passed = (
        structural_value is not None
        and judge_value is not None
        and structural_value >= min_structural
        and judge_value >= min_judge
    )

    metadata = sample.metadata or {}
    snapshot = metadata.get("snapshot") or {}
    nodes = snapshot.get("nodes") or []
    contracts = [node for node in nodes if node.get("kind") in CONTRACT_KINDS]
    fields = [node for node in nodes if node.get("kind") == "field"]
    rules = [node for node in nodes if node.get("kind") == "rule"]
    externals = [node for node in nodes if node.get("kind") == "external"]
    edges = snapshot.get("edges") or []
    report = (metadata.get("report") or {}).get("summary") or {}
    kinds = {kind: sum(1 for node in contracts if node.get("kind") == kind) for kind in CONTRACT_KINDS}
    clusters = Counter(
        str(node.get("cluster") or "").strip()
        for node in nodes
        if node.get("kind") != "field" and str(node.get("cluster") or "").strip()
    )

    failed_checks = []
    if structural:
        failed_checks = [
            {"name": check.get("name"), "detail": check.get("detail", "")}
            for check in (structural.metadata or {}).get("checks", [])
            if not check.get("ok")
        ]

    judge_metadata = (judge.metadata or {}) if judge else {}
    reason = judge_metadata.get("parsed", {}).get("reason") if judge_metadata.get("parsed") else None
    if not reason and judge:
        reason = judge.explanation

    return {
        "id": str(sample.id),
        "status": "pass" if passed else "fail",
        "structural": {
            "score": structural_value,
            "failed_checks": failed_checks,
        },
        "judge": {
            "score": judge_value,
            "clustering": judge_metadata.get("clustering"),
            "reason": reason or "",
            "problems": list(judge_metadata.get("problems") or []),
        },
        "graph": {
            "contracts": kinds,
            "fields": len(fields),
            "rules": len(rules),
            "externals": len(externals),
            "edges": len(edges),
            "clusters": dict(clusters.most_common()),
            "report": report,
        },
        "agent": {
            "messages": len(sample.messages or []),
            "cli_calls": metadata.get("cli_calls"),
            "error": metadata.get("agent_error"),
        },
    }


def build_report(
    logs: list,
    settings: Settings,
    log_dir: Path,
    min_structural: float,
    min_judge: float,
) -> dict:
    cases = []
    token_totals: dict[str, dict] = {}
    log_paths = []

    for log in logs:
        location = getattr(log, "location", None)
        if location:
            log_paths.append(str(location))
        for sample in log.samples or []:
            cases.append(_sample_record(sample, min_structural, min_judge))
        for model, usage in (getattr(log.stats, "model_usage", None) or {}).items():
            data = token_totals.setdefault(
                model,
                {"total_tokens": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0},
            )
            data["total_tokens"] += int(usage.total_tokens or 0)
            data["input_tokens"] += int(usage.input_tokens or 0)
            data["output_tokens"] += int(usage.output_tokens or 0)
            if usage.total_cost:
                data["cost"] += float(usage.total_cost)

    passed = sum(1 for case in cases if case["status"] == "pass")
    structural_scores = [case["structural"]["score"] for case in cases if case["structural"]["score"] is not None]
    judge_scores = [case["judge"]["score"] for case in cases if case["judge"]["score"] is not None]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "result": {
            "cases": len(cases),
            "passed": passed,
            "failed": len(cases) - passed,
            "ok": len(cases) > 0 and passed == len(cases),
        },
        "thresholds": {"structural": min_structural, "judge": min_judge},
        "models": {"extractor": settings.extractor, "judge": settings.judge},
        "log_dir": str(log_dir),
        "inspect_logs": log_paths,
        "token_usage": token_totals,
        "means": {
            "structural": round(sum(structural_scores) / len(structural_scores), 4) if structural_scores else None,
            "judge": round(sum(judge_scores) / len(judge_scores), 4) if judge_scores else None,
        },
        "cases": cases,
    }


def _cell(value) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_markdown(report: dict) -> str:
    result = report["result"]
    thresholds = report["thresholds"]
    models = report["models"]
    means = report["means"]

    lines = [
        "# Logistic map evaluation report",
        "",
        f"- Generated: {report['generated_at']}",
        f"- Extractor: `{models['extractor']}`",
        f"- Judge: `{models['judge']}`",
        f"- Thresholds: structural >= {thresholds['structural']}, judge >= {thresholds['judge']}",
        f"- Result: **{result['passed']}/{result['cases']} passed**",
        f"- Structural mean: {means['structural']} | Judge mean: {means['judge']}",
        "",
    ]

    if report["token_usage"]:
        lines.append("## Token usage")
        lines.append("")
        lines.append("| model | total | input | output | cost |")
        lines.append("|---|---:|---:|---:|---:|")
        for model, usage in report["token_usage"].items():
            cost = "-" if not usage["cost"] else f"${usage['cost']:.4f}"
            lines.append(
                f"| `{model}` | {usage['total_tokens']:,} | {usage['input_tokens']:,} | "
                f"{usage['output_tokens']:,} | {cost} |"
            )
        lines.append("")

    lines.append("## Cases")
    lines.append("")
    lines.append("| case | structural | judge | status |")
    lines.append("|---|---:|---:|---|")
    for case in report["cases"]:
        lines.append(
            f"| {_cell(case['id'])} | {case['structural']['score']} | "
            f"{case['judge']['score']} | {case['status']} |"
        )
    lines.append("")

    for case in report["cases"]:
        graph = case["graph"]
        agent = case["agent"]
        report_summary = graph["report"]
        findings = report_summary.get("findings", {}) if isinstance(report_summary, dict) else {}
        kinds = graph["contracts"]
        graph_text = ", ".join(
            f"{count} {(CONTRACT_PLURALS.get(kind, kind + 's') if count != 1 else kind)}"
            for kind, count in kinds.items()
        )
        fields_text = f"{graph['fields']} field{'' if graph['fields'] == 1 else 's'}"
        rules_text = f"{graph.get('rules', 0)} rule{'' if graph.get('rules') == 1 else 's'}"
        externals_text = (f"{graph.get('externals', 0)} external "
                          f"{'system' if graph.get('externals') == 1 else 'systems'}")
        edges_text = f"{graph['edges']} edge{'' if graph['edges'] == 1 else 's'}"
        lines.extend(
            [
                f"### {case['id']} — {case['status']}",
                "",
                f"- Graph: {graph_text}, {fields_text}, {rules_text}, "
                f"{externals_text}, {edges_text}",
                f"- Clusters: {', '.join(f'{name}={count}' for name, count in graph.get('clusters', {}).items()) or 'none'}",
                f"- Skill report: {findings.get('error', 0)} errors, "
                f"{findings.get('warn', 0)} warnings, {findings.get('info', 0)} info",
                f"- Agent: {agent['messages']} messages, {agent['cli_calls']} CLI calls, "
                f"error: {agent['error'] or 'none'}",
                f"- Structural score: {case['structural']['score']}",
            ]
        )
        if case["structural"]["failed_checks"]:
            lines.append("- Failed structural checks:")
            for check in case["structural"]["failed_checks"]:
                detail = f" — {_cell(check['detail'])}" if check.get("detail") else ""
                lines.append(f"  - {_cell(check['name'])}{detail}")
        else:
            lines.append("- Failed structural checks: none")

        lines.append(f"- Judge score: {case['judge']['score']}")
        if case["judge"].get("clustering") is not None:
            lines.append(f"- Judge clustering: {case['judge']['clustering']}")
        if case["judge"]["reason"]:
            lines.append(f"- Judge reason: {_cell(case['judge']['reason'])}")
        if case["judge"]["problems"]:
            lines.append("- Judge problems:")
            for problem in case["judge"]["problems"]:
                lines.append(f"  - {_cell(problem)}")
        lines.append("")

    if report["inspect_logs"]:
        lines.append("## Inspect logs")
        lines.append("")
        for path in report["inspect_logs"]:
            lines.append(f"- `{path}`")
        lines.append("")

    return "\n".join(lines)


def write_reports(report: dict, directory: Path) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    markdown = render_markdown(report)

    md_path = directory / f"report-{stamp}.md"
    json_path = directory / f"report-{stamp}.json"
    md_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    (directory / "latest.md").write_text(markdown, encoding="utf-8")
    (directory / "latest.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return md_path, json_path
