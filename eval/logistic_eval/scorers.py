"""Deterministic structural checks and the LLM judge for extracted maps."""

from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

from inspect_ai.model import GenerateConfig, get_model
from inspect_ai.scorer import Score, Scorer, Target, mean, scorer, stderr
from inspect_ai.solver import TaskState

CONTRACT_KINDS = ("input", "entry", "storage")
VALID_ID = re.compile(r"^[a-z]+:[^\s#]+$|^[a-z]+:[^\s#]+#[^\s#]+$")


def _normalize_entry_point(value: str) -> str:
    text = (value or "").lower()
    text = re.sub(r"\{[^/{}]+\}", "{param}", text)
    text = re.sub(r":([a-z_][a-z0-9_-]*)", "{param}", text)
    text = re.sub(r"[-_]+", "", text)
    return re.sub(r"\s+", "", text)


def _file_part(reference: str) -> str:
    raw = str(reference).strip()
    head, _, tail = raw.rpartition(":")
    if head and tail.isdigit():
        return head
    return raw


def _snapshot(state: TaskState) -> dict | None:
    value = state.metadata.get("snapshot")
    return value if isinstance(value, dict) else None


@scorer(metrics=[mean(), stderr()])
def structural_scorer() -> Scorer:
    async def score(state: TaskState, target: Target) -> Score:
        spec = state.metadata["spec"]
        expect = spec.get("expect", {})
        case_dir = Path(state.metadata["case_dir"])
        checks: list[dict] = []

        def check(name: str, ok: bool, detail: str = "") -> None:
            checks.append({"name": name, "ok": bool(ok), "detail": detail})

        snapshot = _snapshot(state)
        check("snapshot exported", snapshot is not None,
              "" if snapshot else (state.metadata.get("export_log") or "no snapshot")[:500])
        agent_error = state.metadata.get("agent_error")
        check(
            "agent finished within budget",
            not agent_error,
            str(agent_error)[:300],
        )
        if snapshot is None:
            return Score(
                value=0.0,
                explanation="the extractor produced no parsable graph snapshot",
                metadata={"checks": checks},
            )

        nodes = snapshot.get("nodes") or []
        edges = snapshot.get("edges") or []
        contracts = [node for node in nodes if node.get("kind") in CONTRACT_KINDS]
        fields = [node for node in nodes if node.get("kind") == "field"]
        counts = Counter(node.get("kind") for node in contracts)
        node_ids = [node.get("id") for node in nodes]

        check("graph is not empty", bool(contracts))
        check("ids are unique", len(node_ids) == len(set(node_ids)))
        check(
            "ids are well formed",
            all(isinstance(node_id, str) and VALID_ID.match(node_id) for node_id in node_ids),
            ", ".join(str(node_id) for node_id in node_ids if not VALID_ID.match(str(node_id)))[:300],
        )
        check(
            "contract ids match their kind",
            all(
                str(node.get("id", "")).split(":", 1)[0] == node.get("kind")
                for node in contracts
            ),
            ", ".join(
                str(node.get("id"))
                for node in contracts
                if str(node.get("id", "")).split(":", 1)[0] != node.get("kind")
            )[:300],
        )

        contract_ids = {node.get("id") for node in contracts}
        entry_ids = {node.get("id") for node in nodes
                     if node.get("kind") == "entry"}
        flows = [node for node in nodes if node.get("kind") == "flow"]
        flow_ids = {node.get("id") for node in flows}
        entrypoints = [node for node in nodes if node.get("kind") == "entrypoint"]
        entrypoint_ids = {node.get("id") for node in entrypoints}
        uses_by_flow = defaultdict(set)
        triggered_by = defaultdict(set)
        related_by_entrypoint = defaultdict(set)
        for edge in edges:
            if edge.get("kind") == "use":
                uses_by_flow[edge.get("from")].add(edge.get("to"))
            elif edge.get("kind") == "trigger":
                triggered_by[edge.get("from")].add(edge.get("to"))
            elif edge.get("kind") == "related":
                related_by_entrypoint[edge.get("from")].add(edge.get("to"))
        data_edges = [edge for edge in edges
                      if edge.get("kind") in ("flow", "transform")]
        edge_endpoints = {edge.get("from") for edge in data_edges} | {
            edge.get("to") for edge in data_edges
        }
        check(
            "edges resolve to contracts",
            all(endpoint in contract_ids for endpoint in edge_endpoints),
            ", ".join(str(item) for item in edge_endpoints - contract_ids)[:300],
        )
        check(
            "use edges resolve to a flow and a contract",
            all(edge.get("from") in flow_ids and edge.get("to") in contract_ids
                for edge in edges if edge.get("kind") == "use"),
            "use edge points outside the graph",
        )
        trigger_edges = [edge for edge in edges if edge.get("kind") == "trigger"]
        check(
            "trigger edges resolve EntryPoint to Flow",
            all(edge.get("from") in entrypoint_ids and edge.get("to") in flow_ids
                for edge in trigger_edges),
            "trigger edge points outside EntryPoint/Flow",
        )
        related_edges = [edge for edge in edges if edge.get("kind") == "related"]
        check(
            "related edges resolve EntryPoint to EntryPoint",
            all(edge.get("from") in entrypoint_ids and edge.get("to") in entrypoint_ids
                for edge in related_edges),
            "related edge points outside EntryPoint",
        )
        check(
            "related edges carry description and code evidence",
            all(str(edge.get("description") or "").strip() and edge.get("refs")
                for edge in related_edges),
            "related edge without description or refs",
        )

        externals = [node for node in nodes if node.get("kind") == "external"]
        external_ids = {node.get("id") for node in externals}
        calls_edges = [edge for edge in edges if edge.get("kind") == "calls"]
        delivers_edges = [edge for edge in edges
                          if edge.get("kind") == "delivers-to"]
        check(
            "calls edges resolve Flow to External System",
            all(edge.get("from") in flow_ids and edge.get("to") in external_ids
                for edge in calls_edges),
            "calls edge points outside Flow/External",
        )
        check(
            "delivers-to edges resolve Entry to External System",
            all(edge.get("from") in entry_ids and edge.get("to") in external_ids
                for edge in delivers_edges),
            "delivers-to edge points outside Entry/External",
        )
        check(
            "delivers-to edges carry a flow_id to a known flow",
            all((edge.get("flow_id") or "") in flow_ids
                for edge in delivers_edges),
            "delivers-to edge without a known flow",
        )
        check(
            "delivers-to entries are used by their flow",
            all(edge.get("from") in uses_by_flow.get(edge.get("flow_id"), set())
                for edge in delivers_edges),
            "delivers-to entry is not USEd by the flow",
        )

        def reaches_trigger(start: str) -> bool:
            stack, seen = [start], set()
            while stack:
                current = stack.pop()
                if current in seen:
                    continue
                seen.add(current)
                if triggered_by.get(current):
                    return True
                stack.extend(related_by_entrypoint.get(current, ()))
            return False

        check(
            "entrypoints have code evidence and reach a flow",
            all((node.get("file_path") or node.get("refs")) and reaches_trigger(node.get("id"))
                for node in entrypoints),
            ", ".join(node.get("id", "") for node in entrypoints
                      if not (node.get("file_path") or node.get("refs"))
                      or not reaches_trigger(node.get("id")))[:300],
        )
        check("at least one entrypoint", bool(entrypoints))
        check(
            "inputs contain no execution metadata",
            all(not any(key in node for key in ("entry_point", "channel"))
                for node in contracts if node.get("kind") == "input"),
        )
        min_entrypoints = int(expect.get("min_entrypoints", 1))
        check(f"at least {min_entrypoints} entrypoint(s)",
              len(entrypoints) >= min_entrypoints, f"found {len(entrypoints)}")

        clustered_nodes = contracts + flows + entrypoints

        def node_cluster(node: dict) -> str:
            return str(node.get("cluster") or "").strip()

        missing_clusters = [node.get("id") for node in clustered_nodes
                            if not node_cluster(node)]
        check(
            "every contract, flow and entrypoint has a cluster",
            not missing_clusters,
            ", ".join(str(item) for item in missing_clusters)[:300],
        )
        cluster_label = re.compile(r"^[a-z0-9][a-z0-9._:/@-]{0,59}$")
        bad_clusters = [
            f"{node.get('id')}={node_cluster(node)}"
            for node in clustered_nodes
            if node_cluster(node) and not cluster_label.match(node_cluster(node))
        ]
        check(
            "clusters are stable lowercase labels",
            not bad_clusters,
            ", ".join(bad_clusters)[:300],
        )
        by_id = {node.get("id"): node for node in nodes}
        mismatched = []
        for edge in edges:
            if edge.get("kind") != "trigger":
                continue
            source = by_id.get(edge.get("from")) or {}
            target = by_id.get(edge.get("to")) or {}
            source_cluster, target_cluster = node_cluster(source), node_cluster(target)
            if source_cluster and target_cluster and source_cluster != target_cluster:
                mismatched.append(
                    f"{edge.get('from')} ({source_cluster}) -> "
                    f"{edge.get('to')} ({target_cluster})")
        check(
            "triggered flow shares the entrypoint cluster",
            not mismatched,
            ", ".join(mismatched)[:300],
        )
        cluster_counts = Counter(node_cluster(node) for node in clustered_nodes
                                 if node_cluster(node))
        check(
            "clusters group more than one node",
            len(clustered_nodes) < 6 or any(count >= 2 for count in cluster_counts.values()),
            f"cluster counts: {dict(cluster_counts)}",
        )

        rules = [node for node in nodes if node.get("kind") == "rule"]
        rule_ids = {node.get("id") for node in rules}
        rule_edges = [edge for edge in edges if edge.get("kind") == "rule"]
        check(
            "rule edges resolve to Flow or EntryPoint and Rule",
            all(edge.get("from") in flow_ids | entrypoint_ids
                and edge.get("to") in rule_ids for edge in rule_edges),
            ", ".join(
                f"{edge.get('from')}->{edge.get('to')}" for edge in rule_edges
                if edge.get("from") not in flow_ids | entrypoint_ids
                or edge.get("to") not in rule_ids)[:300],
        )
        check(
            "rules have a description and code evidence",
            all(str(node.get("description") or "").strip()
                and (node.get("file_path") or node.get("refs"))
                for node in rules),
            ", ".join(str(node.get("id")) for node in rules
                      if not str(node.get("description") or "").strip()
                      or not (node.get("file_path") or node.get("refs")))[:300],
        )
        attached_rules = {edge.get("to") for edge in rule_edges}
        check(
            "rules are attached to a flow or entrypoint",
            all(node.get("id") in attached_rules for node in rules),
            ", ".join(str(node.get("id")) for node in rules
                      if node.get("id") not in attached_rules)[:300],
        )
        check(
            "passages belong to a declared flow",
            all(edge.get("flow_id") in flow_ids for edge in data_edges),
            ", ".join(sorted({str(edge.get("flow_id")) for edge in data_edges
                              if edge.get("flow_id") not in flow_ids}))[:300],
        )
        check(
            "passage endpoints are used by their flow",
            all(edge.get("from") in uses_by_flow[edge.get("flow_id")]
                and edge.get("to") in uses_by_flow[edge.get("flow_id")]
                for edge in data_edges),
            "a passage uses an entity the flow does not USE",
        )
        node_ids_set = {node.get("id") for node in nodes}
        containment_endpoints = {
            edge.get("from") for edge in edges if edge.get("kind") == "contains"
        } | {edge.get("to") for edge in edges if edge.get("kind") == "contains"}
        check(
            "containment edges resolve to nodes",
            all(endpoint in node_ids_set for endpoint in containment_endpoints),
            ", ".join(str(item) for item in containment_endpoints - node_ids_set)[:300],
        )
        check(
            "fields have an owner",
            all(
                str(field.get("id", "")).split("#", 1)[0] in contract_ids
                for field in fields
            ),
            "field owner is not a registered contract",
        )

        for kind in CONTRACT_KINDS:
            minimum = int(expect.get(f"min_{kind}s", 0))
            check(f"at least {minimum} {kind}(s)", counts[kind] >= minimum, f"found {counts[kind]}")
        min_fields = int(expect.get("min_fields", 0))
        declared_fields = [
            field for field in fields if field.get("source") != "inferred"
        ]
        check(
            f"at least {min_fields} declared field(s)",
            len(declared_fields) >= min_fields,
            f"found {len(declared_fields)} declared ({len(fields)} total)",
        )
        min_edges = int(expect.get("min_edges", 0))
        check(
            f"at least {min_edges} data edge(s)",
            len(data_edges) >= min_edges,
            f"found {len(data_edges)} flow/transform edges",
        )
        min_flows = int(expect.get("min_flows", 1))
        check(
            f"at least {min_flows} flow(s)",
            len(flows) >= min_flows,
            f"found {len(flows)}",
        )
        check(
            "every flow uses at least one entity",
            all(uses_by_flow.get(flow_id) for flow_id in flow_ids),
            ", ".join(sorted(flow_id for flow_id in flow_ids
                             if not uses_by_flow.get(flow_id)))[:300],
        )

        entry_points = [_normalize_entry_point(str(node.get("name", "")))
                        for node in entrypoints]
        for expected in expect.get("entry_points", []):
            needle = _normalize_entry_point(expected)
            check(
                f"entrypoint covers {expected}",
                any(needle in point for point in entry_points),
                f"recorded entrypoints: {', '.join(str(node.get('name')) for node in entrypoints)[:400]}",
            )

        recorded: set[str] = set()
        references: list[str] = []
        for node in nodes:
            if node.get("file_path"):
                recorded.add(str(node["file_path"]))
                references.append(str(node["file_path"]))
            for ref in node.get("refs") or []:
                recorded.add(_file_part(ref))
                references.append(str(ref))

        for expected_file in expect.get("files", []):
            check(
                f"references {expected_file}",
                any(
                    item == expected_file or item.endswith("/" + expected_file)
                    for item in recorded
                ),
                f"recorded files: {', '.join(sorted(recorded))[:400]}",
            )

        check("records at least one reference", bool(references), "no --file/--ref recorded")
        invalid = [
            ref
            for ref in references
            if not (case_dir / _file_part(ref)).exists()
        ]
        ratio = len(invalid) / len(references) if references else 1.0
        check(
            "references resolve on disk",
            bool(references) and ratio <= 0.2,
            f"{len(invalid)}/{len(references)} missing, e.g. {', '.join(invalid[:5])}",
        )

        report = state.metadata.get("report") or {}
        findings = report.get("findings") or []
        errors = [finding for finding in findings if finding.get("severity") == "error"]
        check(
            "no structural errors in report",
            not errors,
            "; ".join(str(finding.get("message")) for finding in errors)[:400],
        )
        check(
            "no significant warnings in report",
            len([f for f in findings if f.get("severity") == "warn"]) <= 2,
            f"{len([f for f in findings if f.get('severity') == 'warn'])} warnings",
        )

        passed = sum(1 for item in checks if item["ok"])
        value = passed / len(checks) if checks else 0.0
        failed = [item for item in checks if not item["ok"]]
        explanation = f"{passed}/{len(checks)} structural checks passed"
        if failed:
            explanation += "\n" + "\n".join(
                f"- {item['name']}: {item['detail']}" for item in failed
            )
        return Score(
            value=round(value, 4),
            explanation=explanation,
            metadata={"checks": checks, "failed": [item["name"] for item in failed]},
        )

    return score


def _digest(snapshot: dict, limit_contracts: int = 80, limit_fields: int = 100) -> str:
    nodes = snapshot.get("nodes") or []
    edges = snapshot.get("edges") or []
    contracts = [node for node in nodes if node.get("kind") in CONTRACT_KINDS]
    fields = [node for node in nodes if node.get("kind") == "field"]

    lines = ["CONTRACTS"]
    for node in contracts[:limit_contracts]:
        attributes = (node.get("description") if node.get("kind") == "entrypoint"
                      else node.get("location")
                      or node.get("stage") or "")
        refs = ", ".join((node.get("refs") or [])[:3])
        lines.append(
            f"- {node.get('kind')} {node.get('id')} | cluster={node.get('cluster', '')} "
            f"| {attributes} | file={node.get('file_path', '')}"
            + (f" | refs={refs}" if refs else "")
        )
    if len(contracts) > limit_contracts:
        lines.append(f"- ... {len(contracts) - limit_contracts} more contracts")

    entrypoints = [node for node in nodes if node.get("kind") == "entrypoint"]
    if entrypoints:
        lines.append("ENTRYPOINTS")
        for node in entrypoints:
            refs = ", ".join((node.get("refs") or [])[:3])
            lines.append(
                f"- {node.get('id')} | {node.get('name', '')} | "
                f"{node.get('description', '')} | cluster={node.get('cluster', '')} "
                f"| file={node.get('file_path', '')}"
                + (f" | refs={refs}" if refs else "")
            )

    rules = [node for node in nodes if node.get("kind") == "rule"]
    if rules:
        lines.append("RULES")
        for node in rules:
            refs = ", ".join((node.get("refs") or [])[:2])
            lines.append(
                f"- {node.get('id')} | {node.get('name', '')} | "
                f"cluster={node.get('cluster', '')} | {node.get('description', '')}"
                + (f" | refs={refs}" if refs else "")
            )

    externals = [node for node in nodes if node.get("kind") == "external"]
    if externals:
        lines.append("EXTERNAL SYSTEMS")
        for node in externals:
            refs = ", ".join((node.get("refs") or [])[:2])
            lines.append(
                f"- {node.get('id')} | {node.get('name', '')} | "
                f"provider={node.get('provider', '')} | "
                f"protocol={node.get('protocol', '')} | "
                f"cluster={node.get('cluster', '')} | {node.get('description', '')}"
                + (f" | refs={refs}" if refs else "")
            )
    external_links = [edge for edge in edges
                      if edge.get("kind") in ("calls", "delivers-to")]
    if external_links:
        lines.append("EXTERNAL LINKS")
        for edge in external_links:
            suffix = (f" flow={edge.get('flow_id')}"
                      if edge.get("flow_id") else "")
            lines.append(
                f"- {edge.get('kind')} {edge.get('from')} -> {edge.get('to')}{suffix}")

    lines.append("FIELDS")
    for field in fields[:limit_fields]:
        refs = ", ".join((field.get("refs") or [])[:2])
        lines.append(
            f"- {field.get('id')} | type={field.get('type', '?')} | "
            f"required={field.get('required', '?')}" + (f" | refs={refs}" if refs else "")
        )
    if len(fields) > limit_fields:
        lines.append(f"- ... {len(fields) - limit_fields} more fields")

    flows = [node for node in nodes if node.get("kind") == "flow"]
    if flows:
        lines.append("FLOWS")
        for flow in flows:
            used = sorted(edge.get("to") for edge in edges
                          if edge.get("kind") == "use"
                          and edge.get("from") == flow.get("id"))
            lines.append(
                f"- {flow.get('id')} | {flow.get('name', '')} | "
                f"cluster={flow.get('cluster', '')} | uses={', '.join(used)}")

    lines.append("EDGES")
    for edge in edges:
        note = f" | note={edge['note']}" if edge.get("note") else ""
        flow = f" | flow={edge['flow_id']}" if edge.get("flow_id") else ""
        refs = ", ".join((edge.get("refs") or [])[:2])
        ref_text = f" | refs={refs}" if refs else ""
        lines.append(f"- {edge.get('from')} -{edge.get('kind')}-> "
                     f"{edge.get('to')}{flow}{note}{ref_text}")
    return "\n".join(lines)


def _evidence(spec: dict, snapshot: dict, max_chars: int = 45_000) -> str:
    case_dir = Path(spec["case_dir"])
    chunks: list[str] = []
    used = 0

    candidate_files: list[str] = list(spec.get("context_files", []))
    for node in snapshot.get("nodes") or []:
        if node.get("file_path"):
            candidate_files.append(str(node["file_path"]))
        for ref in node.get("refs") or []:
            candidate_files.append(_file_part(ref))

    seen: set[str] = set()
    for name in candidate_files:
        if name in seen:
            continue
        seen.add(name)
        path = (case_dir / name).resolve()
        if case_dir.resolve() not in path.parents or not path.is_file():
            continue
        if path.stat().st_size > 120_000:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        excerpt = text[:8000]
        if used + len(excerpt) > max_chars:
            break
        used += len(excerpt)
        chunks.append(f"### {name}\n```\n{excerpt}\n```")
        if len(seen) >= 10:
            break

    return "\n\n".join(chunks)


def _tree(spec: dict, max_entries: int = 120) -> str:
    case_dir = Path(spec["case_dir"])
    skip = {".git", "node_modules", ".venv", "target", "build", "dist"}
    lines: list[str] = []
    for path in sorted(case_dir.rglob("*")):
        if len(lines) >= max_entries:
            break
        if any(part in skip for part in path.relative_to(case_dir).parts):
            continue
        depth = len(path.relative_to(case_dir).parts)
        if depth > 3:
            continue
        suffix = "/" if path.is_dir() else ""
        lines.append(f"{'  ' * (depth - 1)}{path.name}{suffix}")
    return "\n".join(lines)


def _judge_prompt(spec: dict, snapshot: dict) -> str:
    report = snapshot.get("__report") or {}
    findings = (report.get("findings") or [])[:25]
    findings_text = "\n".join(
        f"- [{item.get('severity')}] {item.get('code')}: {item.get('message')}"
        for item in findings
    ) or "- none"

    return f"""\
You are the judge for a data-flow extraction evaluation. Grade the map below
against the actual repository slice.

Case: {spec['title']}
Scope: {spec['scope']}

Case-specific rubric:
{spec.get('rubric', 'No extra rubric.')}

Repository tree (depth <= 3):
```
{_tree(spec)}
```

Extracted map:
```
{_digest(snapshot)}
```

Skill report findings:
{findings_text}

Source excerpts (only the files the map referenced plus the context files):
{_evidence(spec, snapshot)}

Grade these dimensions from 0 to 1:
- coverage: the important input entities, intermediate data shapes and storages
  of the scoped code are present, grouped into the flow(s) that use them.
- accuracy: EntryPoint activation evidence and TRIGGER direction, storage/stage,
  fields, edge kinds (flow vs transform vs compose), USE membership, flow ids
  and data direction match the code. Inventing triggers or queues lowers this.
  Distinct positions on a path are deliberately distinct contract nodes even
  when they share fields or a file; do not penalize that by itself, only when
  the same position is duplicated. A directional RELATED edge between
  EntryPoints is valid when an outer activation such as a screen or action
  causes another activation; it must carry a description and evidence. RULE
  nodes are valid only when the code actually enforces the stated limit or
  condition; a vague or invented rule lowers this. External Systems are valid
  only when the code calls a service outside the mapped repositories; CALLS
  links the flow, DELIVERS_TO names the flow that carries the payload.
- references: the recorded file:line references point at the declarations the
  map claims.
- clustering: every contract, flow and entrypoint is assigned to a cluster that
  matches its service, repository or module boundary. Code from the same
  boundary shares one label; different deployables use different labels. Labels
  describe a real grouping, not one cluster per node and not an invented
  organization.

Then return ONLY a JSON object, no prose:
{{"score": 0.0, "coverage": 0.0, "accuracy": 0.0, "references": 0.0,
  "clustering": 0.0, "problems": ["..."], "reason": "one paragraph"}}

`score` is the overall usefulness of the map and must not exceed the average of
the four dimensions. Use below 0.5 when the map invents contracts, invents
clusters or misses the main entry points of the scope.
"""


def _parse_json_object(text: str) -> dict | None:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        value = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


@scorer(metrics=[mean(), stderr()])
def judge_scorer() -> Scorer:
    async def score(state: TaskState, target: Target) -> Score:
        spec = state.metadata["spec"]
        snapshot = _snapshot(state)
        if not snapshot:
            return Score(
                value=0.0,
                explanation="no map to judge",
                reason="scoring_failed",
                metadata={},
            )

        grader = get_model(role="grader", required=True)
        config = GenerateConfig(max_tokens=1800)
        temperature = os.environ.get("JUDGE_TEMPERATURE")
        if temperature:
            config = GenerateConfig(max_tokens=1800, temperature=float(temperature))

        prompt = _judge_prompt(spec, {**snapshot, "__report": state.metadata.get("report")})
        try:
            output = await grader.generate(prompt, config=config)
        except Exception as exc:
            return Score(
                value=0.0,
                explanation=f"judge call failed: {type(exc).__name__}: {exc}",
                reason="grader_failed",
                metadata={"model": grader.name},
            )

        parsed = _parse_json_object(output.completion)
        if parsed is None:
            return Score(
                value=0.0,
                explanation="judge did not return a JSON object",
                reason="invalid_response_format",
                metadata={"model": grader.name, "raw": output.completion[:4000]},
            )

        def number(key: str, default: float | None = None) -> float | None:
            value = parsed.get(key, default)
            try:
                return min(1.0, max(0.0, float(value)))
            except (TypeError, ValueError):
                return default

        dimensions = [number("coverage"), number("accuracy"),
                      number("references"), number("clustering")]
        known = [item for item in dimensions if item is not None]
        fallback = sum(known) / len(known) if known else 0.0
        value = number("score", fallback)
        problems = parsed.get("problems") or []
        if not isinstance(problems, list):
            problems = [str(problems)]

        explanation = str(parsed.get("reason") or "").strip() or "no reason returned"
        if problems:
            explanation += "\nproblems:\n" + "\n".join(f"- {item}" for item in problems)

        return Score(
            value=round(value if value is not None else fallback, 4),
            explanation=explanation,
            answer=str(parsed["score"]) if "score" in parsed else None,
            metadata={
                "model": grader.name,
                "coverage": number("coverage"),
                "accuracy": number("accuracy"),
                "references": number("references"),
                "clustering": number("clustering"),
                "problems": problems,
                "parsed": parsed,
            },
        )

    return score
