#!/usr/bin/env python
"""Offline smoke test for the evaluation harness.

Uses Inspect's mockllm provider to drive one sample through the real tools,
scorers and cleanup. No gateway or credentials are required.

    .venv/bin/python smoke_test.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from inspect_ai import eval as inspect_eval
from inspect_ai.model import ChatMessageAssistant, ModelOutput, get_model
from inspect_ai.tool import ToolCall

sys.path.insert(0, str(Path(__file__).resolve().parent))

from logistic_eval.config import WORK_DIR  # noqa: E402
from logistic_eval.task import logistic_extraction  # noqa: E402

CASE = "go"


def tool_call(name: str, arguments: dict, index: int) -> ModelOutput:
    return ModelOutput.from_message(
        ChatMessageAssistant(
            content=f"step {index}",
            tool_calls=[ToolCall(id=f"call-{index}", function=name, arguments=arguments)],
        )
    )


def extractor_outputs() -> list[ModelOutput]:
    graph = {
        "nodes": [
            {
                "kind": "entrypoint",
                "id": "entrypoint:get-products",
                "name": "GET /catalog/products",
                "description": "HTTP GET /catalog/products",
                "cluster": "catalog-service",
                "file_path": "controller/controller.go",
                "refs": ["controller/controller.go:58"],
            },
            {
                "kind": "entrypoint",
                "id": "entrypoint:catalog-screen",
                "name": "Catalog screen",
                "description": "UI activation that requests the products route",
                "cluster": "catalog-ui",
                "file_path": "ui/catalog.tsx",
                "refs": ["ui/catalog.tsx:12"],
            },
            {
                "kind": "input",
                "id": "input:get-products",
                "name": "GET /catalog/products",
                "cluster": "catalog-service",
                "file_path": "controller/controller.go",
                "refs": ["controller/controller.go:58"],
            },
            {
                "kind": "entry",
                "id": "entry:product-list",
                "name": "product list",
                "stage": "response",
                "cluster": "catalog-service",
                "file_path": "controller/controller.go",
                "refs": ["controller/controller.go:58"],
            },
            {
                "kind": "storage",
                "id": "storage:catalog",
                "name": "catalog products",
                "location": "mysql://catalog.products",
                "store_type": "sql",
                "cluster": "catalog-service",
                "file_path": "repository/repository.go",
                "refs": ["repository/repository.go:24"],
            },
            {
                "kind": "flow",
                "id": "flow:list-products",
                "name": "List products",
                "description": "HTTP read path",
                "cluster": "catalog-service",
                "file_path": "controller/controller.go",
            },
            {
                "kind": "rule",
                "id": "rule:product-limit",
                "name": "Product page limit",
                "description": "A product page returns at most 10 products",
                "cluster": "catalog-service",
                "file_path": "controller/controller.go",
                "refs": ["controller/controller.go:61"],
            },
        ],
        "fields": [
            {
                "owner": "input:get-products",
                "path": "tags",
                "type": "string",
                "file_path": "controller/controller.go",
                "refs": ["controller/controller.go:61"],
            }
        ],
        "edges": [
            {
                "kind": "trigger",
                "from": "entrypoint:get-products",
                "to": "flow:list-products",
            },
            {
                "kind": "related",
                "from": "entrypoint:catalog-screen",
                "to": "entrypoint:get-products",
                "description": "screen requests the product list",
                "refs": ["ui/catalog.tsx:14"],
            },
            {
                "kind": "rule",
                "from": "flow:list-products",
                "to": "rule:product-limit",
                "refs": ["controller/controller.go:61"],
            },
            {
                "kind": "use",
                "from": "flow:list-products",
                "to": "input:get-products",
            },
            {
                "kind": "use",
                "from": "flow:list-products",
                "to": "entry:product-list",
            },
            {
                "kind": "use",
                "from": "flow:list-products",
                "to": "storage:catalog",
            },
            {
                "kind": "transform",
                "flow_id": "flow:list-products",
                "from": "input:get-products",
                "to": "entry:product-list",
                "note": "database rows -> API response",
            },
            {
                "kind": "flow",
                "flow_id": "flow:list-products",
                "from": "entry:product-list",
                "to": "storage:catalog",
            },
        ],
    }
    return [
        tool_call("apply_graph", {"graph": graph}, 1),
        tool_call("run_logistic_map", {"args": ["report"]}, 2),
        ModelOutput.from_message(ChatMessageAssistant(content="map complete")),
    ]


def judge_outputs() -> list[ModelOutput]:
    verdict = {
        "score": 0.8,
        "coverage": 0.8,
        "accuracy": 0.8,
        "references": 0.8,
        "problems": ["smoke test map is intentionally tiny"],
        "reason": "Smoke test judge response.",
    }
    return [ModelOutput.from_content(model="mockllm/judge", content=json.dumps(verdict))]


def main() -> int:
    extractor = get_model("mockllm/extractor", custom_outputs=extractor_outputs())
    judge = get_model("mockllm/judge", custom_outputs=judge_outputs())

    logs = inspect_eval(
        logistic_extraction(cases=CASE),
        model=extractor,
        model_roles={"grader": judge},
        log_dir=str(WORK_DIR / "smoke-logs"),
        display="none",
    )

    log = logs[0]
    sample = (log.samples or [None])[0]
    if sample is None:
        print("FAIL: no sample was produced", file=sys.stderr)
        return 1

    scores = sample.scores or {}
    structural = scores.get("structural_scorer")
    judged = scores.get("judge_scorer")
    print(f"structural: {structural.value if structural else 'missing'}")
    print(f"judge:      {judged.value if judged else 'missing'}")
    if structural:
        print(structural.explanation)
    if judged:
        print(judged.explanation)

    metadata = sample.metadata or {}
    report = metadata.get("report") or {}
    errors = [f for f in report.get("findings", []) if f.get("severity") == "error"]
    work_dir = metadata.get("work_dir")

    failures = []
    if not structural or float(structural.value) <= 0:
        failures.append("structural scorer did not run")
    if not judged or float(judged.value) != 0.8:
        failures.append("judge scorer did not parse the verdict")
    if errors:
        failures.append(f"unexpected structural errors: {errors}")
    if work_dir and Path(work_dir).exists():
        failures.append(f"work dir was not cleaned: {work_dir}")

    if failures:
        print("\nFAIL\n" + "\n".join(f"- {item}" for item in failures), file=sys.stderr)
        return 1
    print("\nOK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
