"""Deterministic tests for the logistic_map.py CLI."""

from __future__ import annotations

import json
import subprocess
import unittest

from helpers import PYTHON, GraphTestCase, run_cli


class InitAndGraphTests(GraphTestCase):
    def test_init_creates_empty_graph(self) -> None:
        self.assertTrue(self.db.exists())
        stats = self.stats()
        self.assertEqual(stats["nodes"], 0)
        self.assertEqual(stats["edges"], 0)

    def test_contract_field_and_edge_roundtrip(self) -> None:
        self.cli(
            "add-node", "--kind", "input", "--id", "input:orders.created",
            "--name", "orders.created", "--file", "consumer.py",
            "--ref", "consumer.py:24", "--ref", "queues.yaml:12",
        )
        self.cli(
            "add-node", "--kind", "entry", "--id", "entry:order-validated",
            "--name", "order-validated", "--stage", "validated",
            "--file", "validate.py",
        )
        self.cli(
            "add-node", "--kind", "storage", "--id", "storage:orders",
            "--location", "postgres://orders.orders", "--store-type", "sql",
            "--file", "repository.py",
        )
        self.cli(
            "add-field", "--owner", "input:orders.created", "--path", "customer.email",
            "--type", "str", "--file", "consumer.py", "--ref", "consumer.py:30",
        )
        self.cli("add-flow", "--id", "flow:orders", "--name", "Orders")
        for entity in ("input:orders.created", "entry:order-validated",
                       "storage:orders"):
            self.cli("flow", "use", "--id", "flow:orders", "--entity", entity)
        self.cli(
            "add-edge", "--kind", "transform",
            "--from", "input:orders.created", "--to", "entry:order-validated",
            "--flow", "flow:orders", "--note", "total: string -> number",
        )
        self.cli(
            "add-edge", "--kind", "flow",
            "--from", "entry:order-validated", "--to", "storage:orders",
            "--flow", "flow:orders",
        )

        payload = self.export()
        by_id = {node["id"]: node for node in payload["nodes"]}
        self.assertIn("input:orders.created", by_id)
        self.assertIn("entry:order-validated", by_id)
        self.assertIn("flow:orders", by_id)
        self.assertIn("input:orders.created#customer", by_id)
        self.assertIn("input:orders.created#customer.email", by_id)

        self.assertNotIn("channel", by_id["input:orders.created"])
        self.assertNotIn("entry_point", by_id["input:orders.created"])
        self.assertEqual(by_id["input:orders.created"]["file_path"], "consumer.py")
        self.assertEqual(by_id["input:orders.created"]["refs"], ["consumer.py:24", "queues.yaml:12"])
        self.assertEqual(by_id["entry:order-validated"]["stage"], "validated")
        self.assertNotIn("entry_point", by_id["entry:order-validated"])

        leaf = by_id["input:orders.created#customer.email"]
        self.assertEqual(leaf["type"], "str")
        self.assertEqual(leaf["source"], "declared")
        self.assertEqual(leaf["refs"], ["consumer.py:30"])

        parent = by_id["input:orders.created#customer"]
        self.assertEqual(parent["source"], "inferred")
        self.assertEqual(parent["path"], "customer")

        edges = {(edge["kind"], edge["from"], edge["to"]): edge for edge in payload["edges"]}
        self.assertIn(("contains", "input:orders.created", "input:orders.created#customer"), edges)
        self.assertIn(
            ("contains", "input:orders.created#customer", "input:orders.created#customer.email"),
            edges,
        )
        transform = edges[("transform", "input:orders.created", "entry:order-validated")]
        self.assertEqual(transform["note"], "total: string -> number")
        self.assertEqual(transform["flow_id"], "flow:orders")
        self.assertIn(("flow", "entry:order-validated", "storage:orders"), edges)
        self.assertIn(("use", "flow:orders", "input:orders.created"), edges)
        self.assertEqual(by_id["flow:orders"]["name"], "Orders")

    def test_parent_fields_are_created_as_inferred(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:shipments")
        self.cli(
            "add-field", "--owner", "input:shipments", "--path", "shipping.address.city",
            "--type", "string",
        )
        by_id = {node["id"]: node for node in self.export()["nodes"]}
        self.assertEqual(by_id["input:shipments#shipping"]["source"], "inferred")
        self.assertEqual(by_id["input:shipments#shipping.address"]["source"], "inferred")
        self.assertEqual(by_id["input:shipments#shipping.address.city"]["source"], "declared")

        self.cli(
            "add-field", "--owner", "input:shipments", "--path", "shipping.address",
            "--type", "object",
        )
        by_id = {node["id"]: node for node in self.export()["nodes"]}
        parent = by_id["input:shipments#shipping.address"]
        self.assertEqual(parent["source"], "declared")
        self.assertEqual(parent["type"], "object")

    def test_remove_node_deletes_owned_fields_and_edges(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:orders")
        self.cli("add-node", "--kind", "entry", "--id", "entry:handle")
        self.cli("add-field", "--owner", "input:orders", "--path", "id", "--type", "string")
        self.cli("add-edge", "--kind", "compose", "--from", "input:orders", "--to", "entry:handle")

        result = self.cli("remove-node", "--id", "input:orders", expect=0)
        self.assertIn("removed", result.stdout)

        payload = self.export()
        ids = {node["id"] for node in payload["nodes"]}
        self.assertEqual(ids, {"entry:handle"})
        self.assertEqual(payload["edges"], [])

    def test_remove_edge(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a")
        self.cli("add-node", "--kind", "entry", "--id", "entry:b")
        self.cli("add-edge", "--kind", "compose", "--from", "input:a", "--to", "entry:b")
        self.cli("remove-edge", "--kind", "compose", "--from", "input:a", "--to", "entry:b")
        self.assertEqual(self.export()["edges"], [])
        self.cli(
            "remove-edge", "--kind", "compose", "--from", "input:a", "--to", "entry:b",
            expect=2,
        )

    def test_stats_counts_nodes_edges_and_kinds(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a")
        self.cli("add-node", "--kind", "entry", "--id", "entry:b")
        self.cli("add-field", "--owner", "input:a", "--path", "id", "--type", "string")
        self.cli("add-edge", "--kind", "compose", "--from", "input:a", "--to", "entry:b")

        stats = self.stats()
        self.assertEqual(stats["nodes"], 3)
        self.assertEqual(stats["edges"], 2)
        self.assertEqual(stats["kinds"], {"entry": 1, "field": 1, "input": 1})
        self.assertEqual(stats["edge_kinds"], {"compose": 1, "contains": 1})

    def test_migrate_db_from_snapshot(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a",
                 "--name", "a", "--cluster", "svc")
        self.cli("add-flow", "--id", "flow:f", "--name", "F", "--cluster", "svc")
        self.cli("flow", "use", "--id", "flow:f", "--entity", "input:a")
        self.cli("add-entrypoint", "--id", "entrypoint:e", "--name", "E",
                 "--description", "activation", "--cluster", "svc")
        self.cli("add-edge", "--kind", "trigger", "--from", "entrypoint:e",
                 "--to", "flow:f")
        snapshot = self.tmp / "snapshot.json"
        self.cli("export", "--out", snapshot)

        target = self.tmp / "migrated.lbdb"
        result = self.cli("migrate-db", "--from", snapshot, "--to", target)
        self.assertIn("migrated", result.stdout)
        exported = self.tmp / "migrated.json"
        run_cli(["export", "--out", exported], target, cwd=self.tmp)
        payload = json.loads(exported.read_text(encoding="utf-8"))
        self.assertEqual(
            {node["id"] for node in payload["nodes"]},
            {"input:a", "flow:f", "entrypoint:e"})
        self.assertIn(
            ("trigger", "entrypoint:e", "flow:f"),
            {(edge["kind"], edge["from"], edge["to"]) for edge in payload["edges"]})

        blocked = self.cli("migrate-db", "--from", snapshot, "--to", target,
                           expect=2)
        self.assertIn("already exists", blocked.stderr)
        self.cli("migrate-db", "--from", snapshot, "--to", target, "--force")
        info = json.loads(run_cli(["doctor", "--json"], target, cwd=self.tmp).stdout)
        self.assertIn("ladybug", info)


class ReportTests(GraphTestCase):
    def test_type_aliases_do_not_conflict(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a")
        self.cli("add-node", "--kind", "entry", "--id", "entry:b")
        self.cli("add-field", "--owner", "input:a", "--path", "total", "--type", "int")
        self.cli("add-field", "--owner", "entry:b", "--path", "total", "--type", "integer")
        self.add_flow(entities=["input:a", "entry:b"])
        self.cli("add-edge", "--kind", "flow", "--from", "input:a", "--to", "entry:b",
                 "--flow", "flow:test")

        report = self.report()
        self.assertNotIn("type_conflict", report["summary"]["by_code"])

        self.cli("add-field", "--owner", "entry:b", "--path", "total", "--type", "string")
        report = self.report()
        conflicts = [item for item in report["findings"] if item["code"] == "type_conflict"]
        self.assertEqual(len(conflicts), 1)
        self.assertIn("total", conflicts[0]["message"])

    def test_missing_field_severity_depends_on_target(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a")
        self.cli("add-node", "--kind", "entry", "--id", "entry:b")
        self.cli("add-node", "--kind", "storage", "--id", "storage:c")
        for path in ("id", "name"):
            self.cli("add-field", "--owner", "input:a", "--path", path, "--type", "string")
        self.cli("add-field", "--owner", "entry:b", "--path", "id", "--type", "string")
        self.cli("add-field", "--owner", "storage:c", "--path", "id", "--type", "string")
        self.add_flow(entities=["input:a", "entry:b", "storage:c"])
        self.cli("add-edge", "--kind", "flow", "--from", "input:a", "--to", "entry:b",
                 "--flow", "flow:test")
        self.cli("add-edge", "--kind", "flow", "--from", "entry:b", "--to", "storage:c",
                 "--flow", "flow:test")

        report = self.report()
        entry_findings = [
            item for item in report["findings"]
            if item["code"] == "missing_field" and item["edge"]["to"] == "entry:b"
        ]
        self.assertEqual(len(entry_findings), 1)
        self.assertEqual(entry_findings[0]["severity"], "warn")
        self.assertIn("name", entry_findings[0]["message"])

        self.cli("add-field", "--owner", "entry:b", "--path", "name", "--type", "string")
        report = self.report()
        storage_findings = [
            item for item in report["findings"]
            if item["code"] == "missing_field" and item["edge"]["to"] == "storage:c"
        ]
        self.assertEqual(len(storage_findings), 1)
        self.assertEqual(storage_findings[0]["severity"], "info")

    def test_compose_cycle_is_an_error(self) -> None:
        for node_id in ("entry:a", "entry:b", "entry:c"):
            self.cli("add-node", "--kind", "entry", "--id", node_id)
        self.cli("add-edge", "--kind", "compose", "--from", "entry:a", "--to", "entry:b")
        self.cli("add-edge", "--kind", "compose", "--from", "entry:b", "--to", "entry:c")
        self.cli("add-edge", "--kind", "compose", "--from", "entry:c", "--to", "entry:a")

        report = self.report()
        cycles = [item for item in report["findings"] if item["code"] == "compose_cycle"]
        self.assertEqual(len(cycles), 1)
        self.assertEqual(cycles[0]["severity"], "error")
        self.assertEqual(report["summary"]["findings"]["error"], 1)

    def test_check_files_detects_missing_reference(self) -> None:
        self.cli(
            "add-node", "--kind", "input", "--id", "input:a",
            "--file", "missing.py",
        )
        report = self.report("--check-files")
        self.assertIn("missing_file", report["summary"]["by_code"])

        (self.tmp / "missing.py").write_text("pass\n", encoding="utf-8")
        report = self.report("--check-files")
        self.assertNotIn("missing_file", report["summary"]["by_code"])


class ApplyTests(GraphTestCase):
    def test_batch_apply_is_transactional(self) -> None:
        invalid = self.tmp / "invalid.json"
        invalid.write_text(
            json.dumps(
                {
                    "nodes": [
                        {"kind": "input", "id": "input:a"},
                        {"kind": "entry", "id": "entry:b"},
                    ],
                    "edges": [
                        {"kind": "flow", "from": "input:a", "to": "entry:ghost"}
                    ],
                }
            ),
            encoding="utf-8",
        )
        proc = self.cli("apply", "--file", invalid, expect=2)
        self.assertIn("does not exist", proc.stdout + proc.stderr)
        self.assertEqual(self.export()["nodes"], [])

        valid = self.tmp / "valid.json"
        valid.write_text(
            json.dumps(
                {
                    "nodes": [
                        {"kind": "input", "id": "input:a", "entry_point": "GET /a"},
                        {"kind": "entry", "id": "entry:b", "stage": "normalized"},
                    ],
                    "fields": [
                        {"owner": "input:a", "path": "id", "type": "string"}
                    ],
                    "edges": [
                        {"kind": "flow", "from": "input:a", "to": "entry:b"}
                    ],
                }
            ),
            encoding="utf-8",
        )
        result = self.cli("apply", "--file", valid, "--json")
        counts = json.loads(result.stdout)
        self.assertEqual(counts["nodes"], 2)
        self.assertEqual(counts["fields"], 1)
        self.assertEqual(counts["edges"], 1)

        payload = self.export()
        self.assertEqual(len(payload["nodes"]), 3)
        self.assertEqual(len(payload["edges"]), 2)


class QueryTests(GraphTestCase):
    def test_query_is_read_only(self) -> None:
        proc = self.cli(
            "query", "--cypher", "MATCH (n) DETACH DELETE n", expect=2
        )
        self.assertIn("read-only", proc.stderr.lower())

        self.cli("add-node", "--kind", "input", "--id", "input:a")
        result = self.cli("query", "--cypher", "MATCH (n:Input) RETURN n.id", "--json")
        self.assertEqual(json.loads(result.stdout), [{"n.id": "input:a"}])

    def test_invalid_inputs_are_rejected(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "Input:Bad ID", expect=2)
        self.cli("add-node", "--kind", "input", "--id", "input:ok")
        self.cli(
            "add-field", "--owner", "input:missing", "--path", "id", expect=2
        )
        self.cli("add-field", "--owner", "input:ok", "--path", "a..b", expect=2)
        self.cli(
            "add-edge", "--kind", "contains", "--from", "input:ok", "--to", "input:ok",
            expect=2,
        )


class FlowTests(GraphTestCase):
    def test_flow_uses_entities_and_passages(self) -> None:
        for kind, node_id in (
            ("storage", "storage:orders"),
            ("storage", "storage:customers"),
            ("storage", "storage:shipments"),
            ("entry", "entry:sp-ship-order"),
            ("input", "input:ignored"),
        ):
            self.cli("add-node", "--kind", kind, "--id", node_id)
        self.add_flow("flow:test", "Ship order", entities=[
            "storage:orders", "storage:customers",
            "entry:sp-ship-order", "storage:shipments",
        ])
        for source, target in (
            ("storage:orders", "entry:sp-ship-order"),
            ("storage:customers", "entry:sp-ship-order"),
            ("entry:sp-ship-order", "storage:shipments"),
        ):
            self.cli("add-edge", "--kind", "flow", "--from", source,
                     "--to", target, "--flow", "flow:test")
        # composition is not a path and the entity is not USEd by the flow
        self.cli(
            "add-edge", "--kind", "compose",
            "--from", "input:ignored", "--to", "storage:orders",
        )

        result = self.cli("flow", "show", "flow:test", "--json")
        payload = json.loads(result.stdout)
        self.assertEqual(payload["flow"]["id"], "flow:test")
        self.assertEqual(len(payload["steps"]), 4)
        self.assertEqual(len(payload["feeds"]), 3)
        self.assertEqual(len(payload["joins"]), 1)
        contract_ids = {step["contract_id"] for step in payload["steps"]}
        self.assertNotIn("input:ignored", contract_ids)
        join = payload["joins"][0]
        self.assertEqual(join["step"], "entry:sp-ship-order")
        self.assertEqual(
            set(join["sources"]),
            {"storage:orders", "storage:customers"},
        )
        roles = {step["contract_id"]: step["role"] for step in payload["steps"]}
        self.assertEqual(roles["storage:orders"], "source")
        self.assertEqual(roles["storage:shipments"], "write")

    def test_same_pair_can_belong_to_several_flows(self) -> None:
        for kind, node_id in (("input", "input:a"), ("entry", "entry:b")):
            self.cli("add-node", "--kind", kind, "--id", node_id)
        self.add_flow("flow:one", "One", entities=["input:a", "entry:b"])
        self.add_flow("flow:two", "Two", entities=["input:a", "entry:b"])
        for flow_id in ("flow:one", "flow:two"):
            self.cli("add-edge", "--kind", "flow", "--from", "input:a",
                     "--to", "entry:b", "--flow", flow_id)

        payload = self.export()
        passages = {(edge.get("flow_id"), edge["from"], edge["to"])
                    for edge in payload["edges"] if edge["kind"] == "flow"}
        self.assertEqual(passages, {
            ("flow:one", "input:a", "entry:b"),
            ("flow:two", "input:a", "entry:b"),
        })
        self.cli("remove-edge", "--kind", "flow", "--from", "input:a",
                 "--to", "entry:b", "--flow", "flow:one")
        payload = self.export()
        remaining = {(edge.get("flow_id"), edge["from"], edge["to"])
                     for edge in payload["edges"] if edge["kind"] == "flow"}
        self.assertEqual(remaining, {("flow:two", "input:a", "entry:b")})

    def test_passage_needs_a_flow_and_use_is_reported(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a")
        self.cli("add-node", "--kind", "entry", "--id", "entry:b")
        proc = self.cli("add-edge", "--kind", "flow", "--from", "input:a",
                        "--to", "entry:b", expect=2)
        self.assertIn("--flow", proc.stderr)
        self.add_flow("flow:f", entities=["input:a"])
        self.cli("add-edge", "--kind", "flow", "--from", "input:a",
                 "--to", "entry:b", "--flow", "flow:f")
        codes = self.report()["summary"]["by_code"]
        self.assertIn("edge_entity_not_used", codes)

    def test_flow_without_entities_is_a_warning(self) -> None:
        self.add_flow("flow:empty", "Empty")
        report = self.report()
        self.assertIn("flow_without_entities", report["summary"]["by_code"])
        self.assertIn("flow_without_path", report["summary"]["by_code"])

    def test_removing_a_flow_keeps_entities(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a")
        self.cli("add-node", "--kind", "entry", "--id", "entry:b")
        self.add_flow("flow:f", "F", entities=["input:a", "entry:b"])
        self.cli("add-edge", "--kind", "flow", "--from", "input:a",
                 "--to", "entry:b", "--flow", "flow:f")
        result = self.cli("flow", "remove", "--id", "flow:f", "--json")
        self.assertEqual(json.loads(result.stdout)["removed_edges"], 1)

        payload = self.export()
        kinds = {node["id"]: node["kind"] for node in payload["nodes"]}
        self.assertEqual(kinds, {"input:a": "input", "entry:b": "entry"})
        self.assertEqual(payload["edges"], [])


class SemanticsTests(GraphTestCase):
    def test_entry_is_a_data_entity(self) -> None:
        self.cli("add-node", "--kind", "entry", "--id", "entry:x", "--stage", "validated")
        node = next(item for item in self.export()["nodes"] if item["id"] == "entry:x")
        self.assertEqual(node["stage"], "validated")

    def test_entrypoint_triggers_only_flows(self) -> None:
        self.cli("add-entrypoint", "--id", "entrypoint:http-orders",
                 "--name", "POST /orders", "--description", "HTTP request",
                 "--ref", "handler.py:10")
        self.cli("add-node", "--kind", "input", "--id", "input:orders")
        self.add_flow("flow:create-order", "Create order", entities=["input:orders"])
        self.cli("add-edge", "--kind", "trigger", "--from", "entrypoint:http-orders",
                 "--to", "flow:create-order")
        payload = self.export()
        nodes = {node["id"]: node for node in payload["nodes"]}
        self.assertEqual(nodes["entrypoint:http-orders"]["refs"], ["handler.py:10"])
        self.assertNotIn("entry_point", nodes["input:orders"])
        self.assertIn(("trigger", "entrypoint:http-orders", "flow:create-order"),
                      {(e["kind"], e["from"], e["to"]) for e in payload["edges"]})
        bad = self.cli("add-edge", "--kind", "trigger", "--from", "input:orders",
                       "--to", "flow:create-order", expect=2)
        self.assertIn("EntryPoint to a Flow", bad.stderr)
        removed = self.cli("remove-node", "--id", "entrypoint:http-orders", "--json")
        self.assertEqual(json.loads(removed.stdout)["removed_edges"], 1)
        self.assertFalse(any(edge["kind"] == "trigger" for edge in self.export()["edges"]))

    def test_entrypoint_without_trigger_is_reported(self) -> None:
        self.cli("add-entrypoint", "--id", "entrypoint:orphan", "--name", "orphan",
                 "--description", "Unwired activation", "--file", "handler.py")
        codes = self.report()["summary"]["by_code"]
        self.assertIn("entrypoint_without_flow", codes)

    def test_cluster_is_stored_and_reported(self) -> None:
        for kind, node_id in (("input", "input:orders"), ("entry", "entry:validated")):
            self.cli("add-node", "--kind", kind, "--id", node_id,
                     "--cluster", "orders-service")
        self.cli("add-node", "--kind", "storage", "--id", "storage:orders-db",
                 "--location", "postgres://orders", "--store-type", "sql",
                 "--cluster", "orders-db")
        self.cli("add-entrypoint", "--id", "entrypoint:orders-http",
                 "--name", "POST /orders", "--description", "HTTP route",
                 "--cluster", "orders-service", "--ref", "handler.py:8")
        self.cli("add-flow", "--id", "flow:create-order", "--name", "Create order",
                 "--cluster", "orders-service")
        for entity in ("input:orders", "entry:validated", "storage:orders-db"):
            self.cli("flow", "use", "--id", "flow:create-order", "--entity", entity)
        self.cli("add-edge", "--kind", "trigger", "--from", "entrypoint:orders-http",
                 "--to", "flow:create-order")

        nodes = {node["id"]: node for node in self.export()["nodes"]}
        self.assertEqual(nodes["entrypoint:orders-http"]["cluster"], "orders-service")
        self.assertEqual(nodes["flow:create-order"]["cluster"], "orders-service")
        self.assertEqual(nodes["input:orders"]["cluster"], "orders-service")
        report = self.report()
        self.assertEqual(report["summary"]["clusters"],
                         ["orders-db", "orders-service"])
        self.assertNotIn("cluster_mismatch", report["summary"]["by_code"])
        listed = json.loads(self.cli("list", "--cluster", "orders-db",
                                     "--json").stdout)
        self.assertEqual([node["id"] for node in listed], ["storage:orders-db"])

        self.cli("add-flow", "--id", "flow:other", "--name", "Other",
                 "--cluster", "billing")
        self.cli("add-edge", "--kind", "trigger", "--from", "entrypoint:orders-http",
                 "--to", "flow:other")
        self.assertIn("cluster_mismatch", self.report()["summary"]["by_code"])

    def test_rule_attaches_to_flow_and_entrypoint(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:orders")
        self.add_flow("flow:search", "Search", entities=["input:orders"])
        self.cli("add-entrypoint", "--id", "entrypoint:search",
                 "--name", "GET /search", "--description", "HTTP search",
                 "--ref", "handler.py:4")
        self.cli("add-rule", "--id", "rule:search-limit", "--name", "Search limit",
                 "--description", "Returns at most 10 products",
                 "--cluster", "catalog", "--ref", "handler.py:8")
        self.cli("add-edge", "--kind", "rule", "--from", "flow:search",
                 "--to", "rule:search-limit")
        self.cli("add-edge", "--kind", "rule", "--from", "entrypoint:search",
                 "--to", "rule:search-limit")
        node = next(item for item in self.export()["nodes"]
                    if item["id"] == "rule:search-limit")
        self.assertEqual(node["description"], "Returns at most 10 products")
        self.assertEqual(node["cluster"], "catalog")
        report = self.report()
        self.assertEqual(report["summary"]["rules"], 1)
        self.assertNotIn("orphan_rule", report["summary"]["by_code"])

        bad = self.cli("add-edge", "--kind", "rule", "--from", "rule:search-limit",
                       "--to", "input:orders", expect=2)
        self.assertIn("Flow or EntryPoint to a Rule", bad.stderr)
        missing = self.cli("add-rule", "--id", "rule:blank", "--name", "blank",
                           expect=2)
        self.assertIn("--description", missing.stderr)

        removed = self.cli("remove-node", "--id", "rule:search-limit", "--json")
        self.assertEqual(json.loads(removed.stdout)["removed_edges"], 2)
        self.assertFalse(any(edge["kind"] == "rule" for edge in self.export()["edges"]))

    def test_related_entrypoints_are_directional_and_named_by_description(self) -> None:
        for name in ("screen", "http"):
            self.cli("add-entrypoint", "--id", f"entrypoint:{name}", "--name", name,
                     "--description", f"{name} activation", "--file", f"{name}.py",
                     "--ref", f"{name}.py:1")
        self.cli("add-node", "--kind", "input", "--id", "input:orders")
        self.add_flow("flow:create-order", "Create order", entities=["input:orders"])
        self.cli("add-edge", "--kind", "trigger", "--from", "entrypoint:http",
                 "--to", "flow:create-order")

        for description, ref in (
            ("screen invokes the route", "screen.py:8"),
            ("screen refreshes the route", "screen.py:12"),
        ):
            self.cli("add-edge", "--kind", "related", "--from", "entrypoint:screen",
                     "--to", "entrypoint:http", "--description", description,
                     "--ref", ref)
        self.cli("add-edge", "--kind", "related", "--from", "entrypoint:http",
                 "--to", "entrypoint:screen", "--description", "route updates screen",
                 "--ref", "http.py:20")

        related = [edge for edge in self.export()["edges"] if edge["kind"] == "related"]
        self.assertEqual(
            {(edge["from"], edge["to"], edge["description"]) for edge in related},
            {
                ("entrypoint:screen", "entrypoint:http", "screen invokes the route"),
                ("entrypoint:screen", "entrypoint:http", "screen refreshes the route"),
                ("entrypoint:http", "entrypoint:screen", "route updates screen"),
            },
        )
        self.assertNotIn("entrypoint_without_flow", self.report()["summary"]["by_code"])

        bad = self.cli("add-edge", "--kind", "related", "--from", "entrypoint:screen",
                       "--to", "input:orders", "--description", "wrong", "--ref", "x.py:1",
                       expect=2)
        self.assertIn("another EntryPoint", bad.stderr)
        self.cli("add-edge", "--kind", "related", "--from", "entrypoint:screen",
                 "--to", "entrypoint:http", "--ref", "screen.py:1", expect=2)
        self.cli("add-edge", "--kind", "related", "--from", "entrypoint:screen",
                 "--to", "entrypoint:http", "--description", "no evidence", expect=2)

        self.cli("remove-edge", "--kind", "related", "--from", "entrypoint:screen",
                 "--to", "entrypoint:http", "--description", "screen invokes the route")
        remaining = [edge for edge in self.export()["edges"] if edge["kind"] == "related"]
        self.assertEqual(len(remaining), 2)

    def test_transform_without_note_warns(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:a")
        self.cli("add-node", "--kind", "entry", "--id", "entry:b")
        self.add_flow(entities=["input:a", "entry:b"])
        self.cli(
            "add-edge", "--kind", "transform",
            "--from", "input:a", "--to", "entry:b", "--flow", "flow:test",
        )
        report = self.report()
        findings = [item for item in report["findings"]
                    if item["code"] == "transform_without_note"]
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0]["severity"], "warn")

        self.cli(
            "add-edge", "--kind", "transform", "--from", "input:a",
            "--to", "entry:b", "--flow", "flow:test",
            "--note", "id: int -> string",
        )
        self.assertNotIn(
            "transform_without_note", self.report()["summary"]["by_code"]
        )

    def test_apply_ignores_v1_entry_attributes(self) -> None:
        payload = self.tmp / "legacy.json"
        payload.write_text(json.dumps({
            "nodes": [
                {"kind": "entry", "id": "entry:b", "entry_point": "fn()",
                 "entry_type": "function"},
            ],
            "edges": [],
        }), encoding="utf-8")
        self.cli("apply", "--file", payload)
        node = next(item for item in self.export()["nodes"] if item["id"] == "entry:b")
        self.assertNotIn("entry_point", node)
        self.assertNotIn("entry_type", node)

    def test_adopts_and_removes_unassigned_passages(self) -> None:
        payload = self.tmp / "unassigned.json"
        payload.write_text(json.dumps({
            "nodes": [
                {"kind": "input", "id": "input:a"},
                {"kind": "entry", "id": "entry:b"},
            ],
            "edges": [
                {"kind": "flow", "from": "input:a", "to": "entry:b"},
                {"kind": "transform", "from": "input:a", "to": "entry:b",
                 "note": "old"},
            ],
        }), encoding="utf-8")
        self.cli("apply", "--file", payload)
        self.add_flow("flow:f", "F", entities=["input:a", "entry:b"])

        adopted = self.cli("add-edge", "--kind", "flow", "--from", "input:a",
                           "--to", "entry:b", "--flow", "flow:f", "--json")
        self.assertTrue(json.loads(adopted.stdout)["adopted"])

        edges = {(edge["kind"], edge.get("flow_id") or ""): edge
                 for edge in self.export()["edges"] if edge["kind"] != "contains"}
        self.assertIn(("flow", "flow:f"), edges)
        self.assertNotIn(("flow", ""), edges)
        self.assertIn(("transform", ""), edges)

        self.cli("remove-edge", "--kind", "transform", "--from", "input:a",
                 "--to", "entry:b")
        kinds = {(edge["kind"], edge.get("flow_id") or "")
                 for edge in self.export()["edges"]}
        self.assertNotIn(("transform", ""), kinds)

    def test_migrate_converts_v1_pipeline_edges(self) -> None:
        path = self.tmp / "v1" / "graph.lbdb"
        path.parent.mkdir(parents=True)
        script = r"""
import json, sys
import ladybug as lb
path = sys.argv[1]
db = lb.Database(path)
conn = lb.Connection(db)
conn.execute("CREATE NODE TABLE Field(id STRING, name STRING, path STRING, type STRING, description STRING, required BOOLEAN, source STRING, repo STRING, commit_sha STRING, file_path STRING, refs STRING[], PRIMARY KEY(id))")
conn.execute("CREATE NODE TABLE Input(id STRING, name STRING, description STRING, entry_point STRING, channel STRING, repo STRING, commit_sha STRING, file_path STRING, refs STRING[], PRIMARY KEY(id))")
conn.execute("CREATE NODE TABLE Storage(id STRING, name STRING, description STRING, location STRING, store_type STRING, repo STRING, commit_sha STRING, file_path STRING, refs STRING[], PRIMARY KEY(id))")
conn.execute("CREATE NODE TABLE Entry(id STRING, name STRING, description STRING, entry_point STRING, entry_type STRING, repo STRING, commit_sha STRING, file_path STRING, refs STRING[], PRIMARY KEY(id))")
pairs = "FROM Input TO Entry, FROM Input TO Storage, FROM Input TO Input, FROM Entry TO Entry, FROM Entry TO Storage, FROM Entry TO Input, FROM Storage TO Entry, FROM Storage TO Storage, FROM Storage TO Input"
conn.execute("CREATE REL TABLE CONTAINS(FROM Input TO Field, FROM Storage TO Field, FROM Entry TO Field, FROM Field TO Field, refs STRING[], file_path STRING)")
conn.execute(f"CREATE REL TABLE COMPOSE({pairs}, refs STRING[], file_path STRING, note STRING)")
conn.execute(f"CREATE REL TABLE TRANSFORM({pairs}, refs STRING[], file_path STRING, note STRING)")
conn.execute("CREATE (:Input {id:'input:a', name:'a'})")
conn.execute("CREATE (:Input {id:'input:legacy', name:'legacy', entry_point:'POST /legacy', channel:'http'})")
conn.execute("CREATE (:Entry {id:'entry:b', name:'b', entry_point:'fn()', entry_type:'function'})")
conn.execute("CREATE (:Storage {id:'storage:c', name:'c'})")
conn.execute("MATCH (a:Input {id:'input:a'}),(b:Entry {id:'entry:b'}) CREATE (a)-[:COMPOSE {note:'pipeline'}]->(b)")
conn.execute("MATCH (b:Entry {id:'entry:b'}),(c:Storage {id:'storage:c'}) CREATE (b)-[:COMPOSE]->(c)")
json.dump({"schema_version": 1}, open(sys.argv[2], "w"))
"""
        proc = subprocess.run(
            [PYTHON, "-c", script, str(path), str(path.parent / "meta.json")],
            capture_output=True, text=True, cwd=str(self.tmp),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)

        old = run_cli(["report"], path, cwd=self.tmp)
        self.assertEqual(old.returncode, 2, old.stdout + old.stderr)
        self.assertIn("migrate", old.stderr)

        migrated = run_cli(["migrate"], path, cwd=self.tmp)
        self.assertEqual(migrated.returncode, 0, migrated.stderr)
        self.assertIn("converted 2 compose", migrated.stdout)
        info = json.loads(run_cli(["doctor", "--json"], path, cwd=self.tmp).stdout)
        self.assertIn("RELATED", info["tables"])
        self.assertIn("RULE_REL", info["tables"])

        export_path = self.tmp / "migrated.json"
        exported = run_cli(["export", "--out", str(export_path)], path, cwd=self.tmp)
        self.assertEqual(exported.returncode, 0, exported.stderr)
        payload = json.loads(export_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], 7)
        entry = next(item for item in payload["nodes"] if item["id"] == "entry:b")
        self.assertNotIn("entry_point", entry)
        legacy_input = next(item for item in payload["nodes"] if item["id"] == "input:legacy")
        self.assertNotIn("entry_point", legacy_input)
        self.assertNotIn("channel", legacy_input)
        self.assertFalse(any(edge["kind"] == "trigger" for edge in payload["edges"]))
        self.assertEqual(
            [(edge["kind"], edge["from"], edge["to"]) for edge in payload["edges"]],
            [
                ("flow", "entry:b", "storage:c"),
                ("flow", "input:a", "entry:b"),
            ],
        )
        self.assertTrue(all("flow_id" not in edge for edge in payload["edges"]))

        # composition is a valid edge and must survive a second migrate
        added = run_cli(
            ["add-edge", "--kind", "compose", "--from", "input:a", "--to", "entry:b"],
            path, cwd=self.tmp,
        )
        self.assertEqual(added.returncode, 0, added.stderr)
        again = run_cli(["migrate"], path, cwd=self.tmp)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertIn("already at schema v7", again.stdout)
        run_cli(["export", "--out", str(export_path)], path, cwd=self.tmp)
        payload = json.loads(export_path.read_text(encoding="utf-8"))
        self.assertIn(
            ("compose", "input:a", "entry:b"),
            [(edge["kind"], edge["from"], edge["to"]) for edge in payload["edges"]],
        )

        clustered = run_cli(
            ["add-node", "--kind", "input", "--id", "input:clustered",
             "--cluster", "legacy-service"],
            path, cwd=self.tmp,
        )
        self.assertEqual(clustered.returncode, 0, clustered.stderr)
        run_cli(["export", "--out", str(export_path)], path, cwd=self.tmp)
        payload = json.loads(export_path.read_text(encoding="utf-8"))
        node = next(item for item in payload["nodes"] if item["id"] == "input:clustered")
        self.assertEqual(node["cluster"], "legacy-service")


class ExportMdTests(GraphTestCase):
    def test_export_md_writes_entities_flows_and_index(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:orders",
                 "--file", "consumer.py")
        self.cli("add-node", "--kind", "entry", "--id", "entry:validated",
                 "--stage", "validated")
        self.cli("add-node", "--kind", "entry", "--id", "entry:header",
                 "--stage", "header")
        self.cli("add-node", "--kind", "storage", "--id", "storage:orders",
                 "--location", "postgres://orders.orders", "--store-type", "sql")
        self.cli("add-field", "--owner", "input:orders", "--path", "total",
                 "--type", "string")
        self.cli("add-field", "--owner", "entry:validated", "--path", "total",
                 "--type", "number")
        self.add_flow("flow:orders", "Orders", entities=[
            "input:orders", "entry:validated", "storage:orders", "entry:header",
        ])
        self.cli("add-entrypoint", "--id", "entrypoint:orders", "--name", "POST /orders",
                 "--description", "HTTP orders route", "--ref", "handler.py:8")
        self.cli("add-entrypoint", "--id", "entrypoint:orders-screen",
                 "--name", "Orders screen", "--description", "UI activation",
                 "--ref", "screen.py:4")
        self.cli("add-edge", "--kind", "trigger", "--from", "entrypoint:orders",
                 "--to", "flow:orders")
        self.cli("add-edge", "--kind", "related", "--from", "entrypoint:orders-screen",
                 "--to", "entrypoint:orders",
                 "--description", "screen submits the order form", "--ref", "screen.py:9")
        self.cli("add-edge", "--kind", "transform", "--from", "input:orders",
                 "--to", "entry:validated", "--note", "total: string -> number",
                 "--flow", "flow:orders")
        self.cli("add-edge", "--kind", "flow", "--from", "entry:validated",
                 "--to", "storage:orders", "--flow", "flow:orders")
        self.cli("add-edge", "--kind", "compose", "--from", "entry:validated",
                 "--to", "entry:header")
        self.cli("add-rule", "--id", "rule:order-limit", "--name", "Order limit",
                 "--description", "At most 20 items per order",
                 "--ref", "validate.py:9")
        self.cli("add-edge", "--kind", "rule", "--from", "flow:orders",
                 "--to", "rule:order-limit")

        out = self.tmp / "docs"
        result = self.cli("export-md", "--out", out, "--title", "Orders docs")
        self.assertIn("markdown files", result.stdout)

        readme = (out / "README.md").read_text(encoding="utf-8")
        self.assertIn("# Orders docs", readme)
        self.assertIn("## Entries (2)", readme)
        self.assertIn("entities/input/orders.md", readme)
        self.assertIn("flows/orders.md", readme)
        self.assertIn("## EntryPoints (2)", readme)
        self.assertIn("entrypoints/orders.md", readme)
        self.assertIn("## Rules (1)", readme)
        self.assertIn("rules/order-limit.md", readme)
        rule_doc = (out / "rules" / "order-limit.md").read_text(encoding="utf-8")
        self.assertIn("At most 20 items per order", rule_doc)
        self.assertIn("../flows/orders.md", rule_doc)
        entrypoint_doc = (out / "entrypoints" / "orders.md").read_text(encoding="utf-8")
        self.assertIn("POST /orders", entrypoint_doc)
        self.assertIn("flow:orders", entrypoint_doc)
        screen_doc = (out / "entrypoints" / "orders-screen.md").read_text(encoding="utf-8")
        self.assertIn("## Related entrypoints", screen_doc)
        self.assertIn("../entrypoints/orders.md", screen_doc)
        self.assertIn("screen submits the order form", screen_doc)

        entity = (out / "entities" / "input" / "orders.md").read_text(encoding="utf-8")
        self.assertIn("`input:orders`", entity)
        self.assertIn("`total`", entity)
        self.assertIn("`transform`", entity)
        self.assertIn("Data leaving", entity)

        entry = (out / "entities" / "entry" / "validated.md").read_text(encoding="utf-8")
        self.assertIn("Composed of:", entry)
        self.assertIn("../entry/header.md", entry)
        self.assertIn("## Flows", entry)
        self.assertIn("../../flows/orders.md", entry)

        flow = (out / "flows" / "orders.md").read_text(encoding="utf-8")
        self.assertIn("## Triggered by", flow)
        self.assertIn("../entrypoints/orders.md", flow)
        self.assertIn("## Path", flow)
        self.assertIn("## Passages", flow)
        self.assertIn("## Rules", flow)
        self.assertIn("../rules/order-limit.md", flow)
        self.assertIn("storage/orders.md", flow)
        self.assertIn("| `transform` |", flow)

        # the output directory is managed: stale generated pages are removed
        first = {path.relative_to(out): path.read_text(encoding="utf-8")
                 for path in out.rglob("*.md")}
        stale = out / "entities" / "entry" / "ghost.md"
        stale.write_text("# stale\n", encoding="utf-8")
        self.cli("export-md", "--out", out, "--title", "Orders docs")
        self.assertFalse(stale.exists())
        second = {path.relative_to(out): path.read_text(encoding="utf-8")
                  for path in out.rglob("*.md")}
        self.assertEqual(first, second)


class RenderTests(GraphTestCase):
    def test_render_writes_standalone_html(self) -> None:
        self.cli("add-node", "--kind", "input", "--id", "input:render-check")
        self.add_flow("flow:render", "Render flow", entities=["input:render-check"])
        self.cli("add-entrypoint", "--id", "entrypoint:render-screen",
                 "--name", "Render screen", "--description", "UI activation",
                 "--ref", "screen.py:1")
        self.cli("add-entrypoint", "--id", "entrypoint:render-http",
                 "--name", "Render HTTP", "--description", "HTTP activation",
                 "--ref", "route.py:1")
        self.cli("add-edge", "--kind", "trigger", "--from", "entrypoint:render-http",
                 "--to", "flow:render")
        self.cli("add-edge", "--kind", "related", "--from", "entrypoint:render-screen",
                 "--to", "entrypoint:render-http",
                 "--description", "screen invokes the route", "--ref", "screen.py:5")
        self.cli("add-rule", "--id", "rule:render-limit", "--name", "Render limit",
                 "--description", "Renders at most 10 items",
                 "--cluster", "render-ui", "--ref", "screen.py:9")
        self.cli("add-edge", "--kind", "rule", "--from", "flow:render",
                 "--to", "rule:render-limit", "--ref", "screen.py:9")
        out = self.tmp / "map.html"
        result = self.cli("render", "--out", out, "--title", "Render check")
        self.assertIn("rendered", result.stdout)
        self.assertTrue(out.is_file())
        html = out.read_text(encoding="utf-8")
        self.assertGreater(len(html), 20_000)
        self.assertIn("input:render-check", html)
        self.assertIn("Render check", html)
        self.assertIn("function drawLanes", html)
        self.assertIn("flow:render", html)
        self.assertIn("entrypointFocus", html)
        self.assertIn("data-kind=\"entrypoint\"", html)
        self.assertIn("trigger:", html)
        self.assertIn("related:", html)
        self.assertIn("data-rel=\"related\"", html)
        self.assertIn("Related entrypoints", html)
        self.assertIn("Reached flows", html)
        self.assertIn("renderRulePanel", html)
        self.assertIn("data-kind=\"rule\"", html)
        self.assertIn("data-rel=\"rule\"", html)
        self.assertIn("id=\"clusterFilter\"", html)
        self.assertIn("nodeCluster", html)
        self.assertIn("All clusters", html)
        self.assertIn("id=\"panelClose\"", html)
        self.assertIn("setPanelOpen", html)
        self.assertIn("id=\"legendClose\"", html)
        self.assertIn("id=\"legendOpen\"", html)
        self.assertNotIn("__DATA__", html)


class BranchTests(GraphTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cli("add-node", "--kind", "input", "--id", "input:base",
                 "--name", "base")

    def node_ids(self) -> set:
        return {node["id"] for node in self.export()["nodes"]}

    def branch_rows(self) -> dict:
        proc = self.cli("branch", "list", "--json")
        payload = json.loads(proc.stdout)
        return {row["name"]: row for row in payload["branches"]}

    def test_branch_is_isolated_and_applied_to_master(self) -> None:
        self.cli("branch", "create", "feat-a")
        self.cli("add-node", "--kind", "input", "--id", "input:from-a",
                 "--name", "A")
        self.assertEqual(self.node_ids(), {"input:base", "input:from-a"})
        self.cli("branch", "use", "master")
        self.assertEqual(self.node_ids(), {"input:base"})
        self.cli("branch", "use", "feat-a")
        self.assertIn("input:from-a", self.node_ids())

        result = self.cli("branch", "apply", "feat-a")
        self.assertIn("applied feat-a", result.stdout)
        self.assertEqual(self.node_ids(), {"input:base", "input:from-a"})
        self.assertFalse((self.tmp / "graph.lbdb.feat-a").exists())
        rows = self.branch_rows()
        self.assertEqual(rows["feat-a"]["status"], "applied")
        status = json.loads(self.cli("branch", "status", "--json").stdout)
        self.assertEqual(status["active"], "master")
        self.assertEqual(status["revision"], 1)

    def test_applying_one_branch_stales_the_others(self) -> None:
        self.cli("branch", "create", "feat-a")
        self.cli("add-node", "--kind", "input", "--id", "input:only-a",
                 "--name", "A")
        self.cli("branch", "create", "feat-b")
        self.assertEqual(self.node_ids(), {"input:base"})
        self.cli("branch", "apply", "feat-a")

        rows = self.branch_rows()
        self.assertEqual(rows["feat-a"]["status"], "applied")
        self.assertEqual(rows["feat-b"]["status"], "stale")
        self.assertIn("stale", rows["feat-b"]["reason"])

        blocked = self.cli("branch", "apply", "feat-b", expect=2)
        self.assertIn("stale", blocked.stderr)
        blocked = self.cli("branch", "use", "feat-b", expect=2)
        self.assertIn("cannot be used", blocked.stderr)

        self.cli("branch", "create", "feat-c")
        self.cli("add-node", "--kind", "input", "--id", "input:only-c",
                 "--name", "C")
        self.cli("branch", "apply", "feat-c")
        self.assertEqual(self.node_ids(),
                         {"input:base", "input:only-a", "input:only-c"})

    def test_branch_always_starts_from_master(self) -> None:
        self.cli("branch", "create", "feat-a")
        self.cli("add-node", "--kind", "input", "--id", "input:only-a",
                 "--name", "A")
        self.cli("branch", "create", "feat-b")
        self.assertEqual(self.node_ids(), {"input:base"})
        rows = self.branch_rows()
        self.assertEqual(rows["feat-a"]["base_revision"],
                         rows["feat-b"]["base_revision"])

    def test_editing_master_invalidates_open_branches(self) -> None:
        self.cli("branch", "create", "feat-a")
        self.cli("branch", "use", "master")
        self.cli("add-node", "--kind", "input", "--id", "input:on-master",
                 "--name", "master edit")
        blocked = self.cli("branch", "apply", "feat-a", expect=2)
        self.assertIn("stale", blocked.stderr)
        self.assertEqual(self.branch_rows()["feat-a"]["status"], "stale")

    def test_drop_and_reserved_names(self) -> None:
        self.cli("branch", "create", "feat-a")
        dropped = self.cli("branch", "drop", "feat-a")
        self.assertIn("feat-a", dropped.stdout)
        self.assertEqual(self.branch_rows(), {})
        self.assertFalse((self.tmp / "graph.lbdb.feat-a").exists())

        self.cli("branch", "create", "master", expect=2)
        self.cli("branch", "create", "Bad Name", expect=2)
        missing = self.cli("branch", "apply", "nope", expect=2)
        self.assertIn("does not exist", missing.stderr)


if __name__ == "__main__":
    unittest.main()
