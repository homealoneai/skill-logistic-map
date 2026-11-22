"""Deterministic validation of the sample graphs copied from the eval runs."""

from __future__ import annotations

import json
import unittest
from collections import Counter
from pathlib import Path

from helpers import MANIFEST_PATH, ROOT, SAMPLE_DIR, GraphTestCase, load_manifest

try:
    SAMPLES = load_manifest()["samples"]
except (FileNotFoundError, json.JSONDecodeError):
    SAMPLES = {}

CONTRACT_KINDS = ("input", "entry", "storage")


def _edge_key(edge: dict) -> tuple:
    return (edge.get("kind"), edge.get("from"), edge.get("to"),
            edge.get("flow_id") or "")


def _file_and_line(reference: str) -> tuple[str, int | None]:
    raw = str(reference)
    head, _, tail = raw.rpartition(":")
    if head and tail.isdigit():
        return head, int(tail)
    return raw, None


@unittest.skipIf(not SAMPLES, f"no samples in {MANIFEST_PATH}; run `make samples`")
class SampleFixtureTests(GraphTestCase):
    def check_sample(self, sample_id: str) -> None:
        meta = SAMPLES[sample_id]
        sample_path = SAMPLE_DIR / f"{sample_id}.json"
        self.assertTrue(sample_path.is_file(), sample_path)
        data = json.loads(sample_path.read_text(encoding="utf-8"))
        case_dir = (ROOT / "eval" / meta["case_path"]).resolve()
        self.assertTrue(case_dir.is_dir(), case_dir)

        nodes = data.get("nodes") or []
        edges = data.get("edges") or []
        by_id = {node["id"]: node for node in nodes}
        contracts = {
            node["id"] for node in nodes if node.get("kind") in CONTRACT_KINDS
        }
        flows = [node for node in nodes if node.get("kind") == "flow"]
        entrypoints = [node for node in nodes if node.get("kind") == "entrypoint"]
        entrypoint_ids = {node["id"] for node in entrypoints}
        rules = [node for node in nodes if node.get("kind") == "rule"]
        rule_ids = {node["id"] for node in rules}
        fields = [node for node in nodes if node.get("kind") == "field"]

        # --- file integrity -------------------------------------------------
        self.assertEqual(data.get("schema_version"), 8)
        self.assertEqual(len(by_id), len(nodes), "duplicate node ids in the sample")
        kinds = Counter(node.get("kind") for node in nodes if node.get("kind") != "field")
        edge_kinds = Counter(edge.get("kind") for edge in edges)
        self.assertEqual(len(nodes), meta["counts"]["nodes"])
        self.assertEqual(len(edges), meta["counts"]["edges"])
        self.assertEqual(len(contracts), meta["counts"]["contracts"])
        self.assertEqual(len(flows), meta["counts"]["flows"])
        self.assertGreaterEqual(len(flows), 1, "every sample must include a flow")
        self.assertEqual(len(entrypoints), meta["counts"]["entrypoints"])
        self.assertGreaterEqual(
            len(entrypoints), 1, "every sample must include an entrypoint")
        self.assertEqual(len(rules), meta["counts"].get("rules", 0))
        externals = [node for node in nodes if node.get("kind") == "external"]
        external_ids = {node["id"] for node in externals}
        self.assertEqual(len(externals), meta["counts"].get("externals", 0))
        self.assertEqual(
            len([edge for edge in edges if edge.get("kind") == "related"]),
            meta["counts"].get("related", 0),
        )
        clusters = {str(node.get("cluster") or "").strip()
                    for node in nodes if node.get("kind") != "field"
                    and str(node.get("cluster") or "").strip()}
        self.assertEqual(len(clusters), meta["counts"].get("clusters", 0))
        for node in nodes:
            if node.get("kind") == "field":
                continue
            self.assertTrue(str(node.get("cluster") or "").strip(),
                            f"{node['id']} has no cluster")
        self.assertEqual(len(fields), meta["counts"]["fields"])
        self.assertEqual(dict(sorted(kinds.items())), meta["counts"]["kinds"])
        self.assertEqual(dict(sorted(edge_kinds.items())), meta["counts"]["edge_kinds"])

        for node in nodes:
            node_id = node["id"]
            if node.get("kind") in CONTRACT_KINDS:
                self.assertEqual(node_id.split(":", 1)[0], node["kind"], node_id)
            elif node.get("kind") == "flow":
                self.assertEqual(node_id.split(":", 1)[0], "flow", node_id)
            elif node.get("kind") == "entrypoint":
                self.assertEqual(node_id.split(":", 1)[0], "entrypoint", node_id)
                self.assertTrue(node.get("file_path") or node.get("refs"), node_id)
            elif node.get("kind") == "rule":
                self.assertEqual(node_id.split(":", 1)[0], "rule", node_id)
                self.assertTrue(node.get("description"), node_id)
                self.assertTrue(node.get("file_path") or node.get("refs"), node_id)
            elif node.get("kind") == "external":
                self.assertEqual(node_id.split(":", 1)[0], "external", node_id)
                self.assertTrue(node.get("file_path") or node.get("refs"), node_id)
            else:
                self.assertEqual(node.get("kind"), "field", node_id)
                self.assertIn("#", node_id)
                self.assertIn(node_id.split("#", 1)[0], contracts, node_id)

        for edge in edges:
            if edge.get("kind") in ("compose", "transform", "flow"):
                self.assertIn(edge["from"], contracts, edge)
                self.assertIn(edge["to"], contracts, edge)
                if edge["kind"] in ("transform", "flow"):
                    self.assertIn(edge.get("flow_id"), {flow["id"] for flow in flows}, edge)
            elif edge.get("kind") == "use":
                self.assertIn(edge["from"], {flow["id"] for flow in flows}, edge)
                self.assertIn(edge["to"], contracts, edge)
            elif edge.get("kind") == "trigger":
                self.assertIn(edge["from"], entrypoint_ids, edge)
                self.assertIn(edge["to"], {flow["id"] for flow in flows}, edge)
            elif edge.get("kind") == "related":
                self.assertIn(edge["from"], entrypoint_ids, edge)
                self.assertIn(edge["to"], entrypoint_ids, edge)
                self.assertTrue(edge.get("description"), edge)
                self.assertTrue(edge.get("refs"), edge)
            elif edge.get("kind") == "rule":
                self.assertIn(edge["from"], entrypoint_ids | {flow["id"] for flow in flows}, edge)
                self.assertIn(edge["to"], rule_ids, edge)
            elif edge.get("kind") == "calls":
                self.assertIn(edge["from"], {flow["id"] for flow in flows}, edge)
                self.assertIn(edge["to"], external_ids, edge)
            elif edge.get("kind") == "delivers-to":
                self.assertIn(
                    edge["from"],
                    {contract["id"] for contract in contracts
                     if contract["kind"] == "entry"}, edge)
                self.assertIn(edge["to"], external_ids, edge)
                self.assertIn(edge.get("flow_id"), {flow["id"] for flow in flows}, edge)

        for rule in rules:
            self.assertTrue(
                any(edge.get("kind") == "rule" and edge.get("to") == rule["id"]
                    for edge in edges),
                f"{rule['id']} is not attached")

        triggered = {edge["to"] for edge in edges if edge.get("kind") == "trigger"}
        for flow in flows:
            self.assertIn(flow["id"], triggered, f"{flow['id']} has no entrypoint")

        relates = {}
        for edge in edges:
            if edge.get("kind") == "related":
                relates.setdefault(edge["from"], set()).add(edge["to"])

        def reaches_trigger(start: str) -> bool:
            stack, seen = [start], set()
            while stack:
                current = stack.pop()
                if current in seen:
                    continue
                seen.add(current)
                if any(edge["from"] == current and edge.get("kind") == "trigger"
                       for edge in edges):
                    return True
                stack.extend(relates.get(current, ()))
            return False

        for entrypoint in entrypoints:
            self.assertTrue(
                reaches_trigger(entrypoint["id"]),
                f"{entrypoint['id']} reaches no flow")

        contains = {
            (edge["from"], edge["to"]) for edge in edges if edge.get("kind") == "contains"
        }
        for field in fields:
            owner = field["id"].split("#", 1)[0]
            path = field.get("path", "")
            parent = owner if "." not in path else f"{owner}#{path.rsplit('.', 1)[0]}"
            self.assertIn((parent, field["id"]), contains, field["id"])

        for node in nodes:
            references = []
            if node.get("file_path"):
                references.append(node["file_path"])
            references.extend(node.get("refs") or [])
            for reference in references:
                file_name, line = _file_and_line(reference)
                path = case_dir / file_name
                self.assertTrue(path.is_file(), f"{sample_id}: {reference} does not exist")
                if line is not None:
                    with path.open("rb") as handle:
                        total = sum(1 for _ in handle)
                    self.assertLessEqual(line, total, f"{sample_id}: {reference} beyond EOF")

        # --- CLI round trip -------------------------------------------------
        self.cli("apply", "--file", sample_path, "--json", cwd=case_dir)
        exported = self.export()
        exported_ids = {node["id"] for node in exported["nodes"]}
        self.assertEqual(exported_ids, set(by_id))
        self.assertEqual(
            Counter(_edge_key(edge) for edge in exported["edges"]),
            Counter(_edge_key(edge) for edge in edges),
        )

        for original in nodes:
            current = next(node for node in exported["nodes"] if node["id"] == original["id"])
            for key in (
                "kind", "name", "location", "store_type",
                "stage", "path", "type", "required", "source", "owner", "file_path",
            ):
                if key in original:
                    self.assertEqual(current.get(key), original[key], f"{original['id']}.{key}")
            self.assertEqual(current.get("refs", []), original.get("refs", []))

        # --- report and stats ----------------------------------------------
        report = json.loads(
            self.cli("report", "--format", "json", "--check-files", cwd=case_dir).stdout
        )
        summary = report["summary"]
        self.assertEqual(summary["findings"], meta["report"])
        self.assertEqual(summary["by_code"], meta["by_code"])
        self.assertEqual(summary["findings"]["error"], 0, report["findings"])
        self.assertNotIn("orphan_field", summary["by_code"])
        self.assertNotIn("compose_cycle", summary["by_code"])
        self.assertNotIn("flow_cycle", summary["by_code"])
        self.assertNotIn("edge_without_flow", summary["by_code"])
        self.assertNotIn("edge_entity_not_used", summary["by_code"])

        stats = json.loads(self.cli("stats", "--json", cwd=case_dir).stdout)
        self.assertEqual(stats["nodes"], meta["stats"]["nodes"])
        self.assertEqual(stats["edges"], meta["stats"]["edges"])
        self.assertEqual(stats["kinds"], meta["stats"]["kinds"])
        self.assertEqual(stats["edge_kinds"], meta["stats"]["edge_kinds"])

        # --- HTML rendering -------------------------------------------------
        html_path = self.tmp / f"{sample_id}.html"
        self.cli(
            "render", "--out", html_path, "--title", f"Sample: {sample_id}",
            cwd=case_dir,
        )
        html = html_path.read_text(encoding="utf-8")
        self.assertGreater(len(html), 20_000)
        self.assertIn(next(iter(contracts)), html)
        self.assertNotIn("__DATA__", html)


def _make_test(sample_id: str):
    def test(self) -> None:
        self.check_sample(sample_id)

    return test


for _sample_id in sorted(SAMPLES):
    setattr(SampleFixtureTests, f"test_sample_{_sample_id}", _make_test(_sample_id))


if __name__ == "__main__":
    unittest.main()
