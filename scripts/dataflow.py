#!/usr/bin/env python3
"""Kuzu-backed data-flow map for application contracts.

Schema v1
---------
Contract nodes: Input, Entry, Storage.
Field nodes belong to exactly one contract node.

  Input|Entry|Storage -CONTAINS-> Field          contract fields
  Field               -CONTAINS-> Field          nested fields
  Input|Entry|Storage -COMPOSE-> Input|Entry|Storage    structural pipeline
  Input|Entry|Storage -TRANSFORM-> Input|Entry|Storage  data transformation

The CLI is the only supported writer. Reads are exposed through `query`
(read-only guard) and the canned commands `list`, `report`, `stats`, `export`.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

SCHEMA_VERSION = 1
DEFAULT_DB = os.environ.get("DATAFLOW_DB", ".dataflow/graph.kuzu")
DEFAULT_RENDER = "graph.html"

CONTRACT_KINDS = ("input", "entry", "storage")
ALL_KINDS = ("input", "entry", "storage", "field")

LABELS = {"input": "Input", "entry": "Entry", "storage": "Storage", "field": "Field"}
RELS = {"contains": "CONTAINS", "compose": "COMPOSE", "transform": "TRANSFORM"}

NODE_PROPS = {
    "input": ["name", "description", "entry_point", "channel",
              "repo", "commit_sha", "file_path", "refs"],
    "entry": ["name", "description", "entry_point", "entry_type",
              "repo", "commit_sha", "file_path", "refs"],
    "storage": ["name", "description", "location", "store_type",
                "repo", "commit_sha", "file_path", "refs"],
    "field": ["name", "path", "type", "description", "required", "source",
              "repo", "commit_sha", "file_path", "refs"],
}

DDL = [
    """CREATE NODE TABLE IF NOT EXISTS Field(
        id STRING, name STRING, path STRING, type STRING, description STRING,
        required BOOLEAN, source STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Input(
        id STRING, name STRING, description STRING, entry_point STRING, channel STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Storage(
        id STRING, name STRING, description STRING, location STRING, store_type STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Entry(
        id STRING, name STRING, description STRING, entry_point STRING, entry_type STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE REL TABLE IF NOT EXISTS CONTAINS(
        FROM Input TO Field, FROM Storage TO Field, FROM Entry TO Field,
        FROM Field TO Field,
        refs STRING[], file_path STRING)""",
    """CREATE REL TABLE IF NOT EXISTS COMPOSE(
        FROM Input TO Entry, FROM Input TO Storage, FROM Input TO Input,
        FROM Entry TO Entry, FROM Entry TO Storage, FROM Entry TO Input,
        FROM Storage TO Entry, FROM Storage TO Storage, FROM Storage TO Input,
        refs STRING[], file_path STRING, note STRING)""",
    """CREATE REL TABLE IF NOT EXISTS TRANSFORM(
        FROM Input TO Entry, FROM Input TO Storage, FROM Input TO Input,
        FROM Entry TO Entry, FROM Entry TO Storage, FROM Entry TO Input,
        FROM Storage TO Entry, FROM Storage TO Storage, FROM Storage TO Input,
        refs STRING[], file_path STRING, note STRING)""",
]

TYPE_ALIASES = {
    "int": "integer", "int32": "integer", "int64": "integer", "integer": "integer",
    "long": "integer", "bigint": "integer", "smallint": "integer",
    "str": "string", "text": "string", "varchar": "string", "char": "string",
    "bool": "boolean", "boolean": "boolean",
    "float": "number", "double": "number", "decimal": "number", "number": "number",
    "numeric": "number", "bigdecimal": "number",
    "dict": "object", "map": "object", "object": "object", "struct": "object",
    "list": "array", "array": "array", "[]": "array",
    "ts": "timestamp", "datetime": "timestamp", "date-time": "timestamp",
}

SEVERITY_RANK = {"error": 0, "warn": 1, "info": 2}


class DataflowError(Exception):
    pass


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@lru_cache(maxsize=8)
def git_value(name: str) -> str:
    commands = {
        "repo": ["git", "remote", "get-url", "origin"],
        "commit": ["git", "rev-parse", "--short", "HEAD"],
    }
    if name not in commands:
        return ""
    try:
        out = subprocess.run(commands[name], capture_output=True, text=True, timeout=5)
    except Exception:
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9._@/-]+", "-", value)
    return value.strip("-")


def parse_refs(values) -> list:
    refs = []
    for value in values or []:
        for part in str(value).split(","):
            part = part.strip()
            if part:
                refs.append(part)
    return refs


def clean(row):
    if not isinstance(row, dict):
        return {}
    return {k: v for k, v in row.items() if not str(k).startswith("_")}


def kind_of(node_id: str) -> str:
    if "#" in node_id:
        # Field ids are <contract-id>#<path>; the contract id contains a colon.
        return "field"
    if ":" not in node_id:
        raise DataflowError(f"invalid id {node_id!r}: expected <kind>:<slug>")
    kind = node_id.split(":", 1)[0]
    if kind not in ALL_KINDS:
        raise DataflowError(f"invalid id {node_id!r}: unknown kind {kind!r}")
    return kind


def owner_of(field_id: str) -> str:
    return field_id.split("#", 1)[0] if "#" in field_id else ""


def validate_node_id(node_id: str, kind: str) -> None:
    if kind_of(node_id) != kind:
        raise DataflowError(f"id {node_id!r} does not start with {kind}:")
    if not re.match(r"^[a-z]+:[^\s#]+$", node_id):
        raise DataflowError(
            f"invalid id {node_id!r}: use lowercase <kind>:<slug> without spaces or '#'")


def validate_path(path: str) -> None:
    if not path or "#" in path or re.search(r"\s", path):
        raise DataflowError(f"invalid field path {path!r}")
    if path.startswith(".") or path.endswith(".") or ".." in path:
        raise DataflowError(f"invalid field path {path!r}")


def norm_type(value: str) -> str:
    value = (value or "").strip().lower()
    return TYPE_ALIASES.get(value, value)


def is_contract(node_id: str) -> bool:
    return kind_of(node_id) in CONTRACT_KINDS


# --------------------------------------------------------------------------
# database
# --------------------------------------------------------------------------

def load_kuzu():
    try:
        import kuzu  # noqa: F401
    except Exception as exc:  # pragma: no cover - depends on environment
        raise DataflowError(
            "kuzu is not installed for this Python interpreter "
            f"({sys.executable}). Run scripts/bootstrap.sh, or "
            "`python3 -m venv .dataflow/.venv && .dataflow/.venv/bin/pip install kuzu`."
        ) from exc
    return kuzu


class DB:
    def __init__(self, path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        kuzu = load_kuzu()
        self._db = kuzu.Database(str(self.path))
        self.conn = kuzu.Connection(self._db)

    def run(self, cypher: str, params=None):
        result = self.conn.execute(cypher, params or {})
        columns = result.get_column_names()
        rows = []
        while result.has_next():
            rows.append(result.get_next())
        result.close()
        return columns, rows

    def tables(self) -> set:
        try:
            return {row[1] for row in self.run("CALL show_tables() RETURN *")[1]}
        except Exception:
            return set()

    def meta_path(self) -> Path:
        return self.path.parent / "meta.json"


def open_db(args, initialize: bool = False) -> DB:
    path = Path(args.db).expanduser()
    if not initialize and not path.exists():
        raise DataflowError(
            f"no data-flow database at {path}; run `init` first")
    db = DB(path)
    if not initialize and "Field" not in db.tables():
        raise DataflowError(
            f"{path} exists but has no data-flow schema; run `init` first")
    return db


# --------------------------------------------------------------------------
# writes
# --------------------------------------------------------------------------

def get_node(db: DB, kind: str, node_id: str):
    _, rows = db.run(f"MATCH (n:{LABELS[kind]} {{id:$id}}) RETURN n", {"id": node_id})
    return clean(rows[0][0]) if rows else None


def merge_node(db: DB, kind: str, node_id: str, props: dict) -> None:
    keys = NODE_PROPS[kind]
    sets = ", ".join(f"n.{key}=${key}" for key in keys)
    params = {"id": node_id}
    params.update({key: props.get(key) for key in keys})
    db.run(f"MERGE (n:{LABELS[kind]} {{id:$id}}) SET {sets}", params)


def merge_edge(db: DB, kind: str, src: str, dst: str, props: dict) -> None:
    src_kind, dst_kind = kind_of(src), kind_of(dst)
    base = (f"MATCH (a:{LABELS[src_kind]} {{id:$from}}), "
            f"(b:{LABELS[dst_kind]} {{id:$to}}) MERGE (a)-[r:{RELS[kind]}]->(b)")
    if props:
        sets = ", ".join(f"r.{key}=${key}" for key in props)
        base += " SET " + sets
    params = {"from": src, "to": dst}
    params.update(props)
    db.run(base, params)


def upsert_contract(db: DB, kind: str, node_id=None, name=None, description=None,
                    attrs=None, refs=None, repo=None, commit_sha=None,
                    file_path=None) -> dict:
    if kind not in CONTRACT_KINDS:
        raise DataflowError(f"kind must be one of {CONTRACT_KINDS}")
    attrs = dict(attrs or {})
    unknown = [key for key in attrs if key not in NODE_PROPS[kind]]
    if unknown:
        raise DataflowError(f"unknown attributes for {kind}: {', '.join(unknown)}")
    if node_id:
        validate_node_id(node_id, kind)
    else:
        if not name:
            raise DataflowError("--name or --id is required")
        node_id = f"{kind}:{slugify(name)}"
        validate_node_id(node_id, kind)

    existing = get_node(db, kind, node_id) or {}
    props = {}
    for key in NODE_PROPS[kind]:
        default = [] if key == "refs" else ""
        props[key] = existing.get(key, default)
        if props[key] is None:
            props[key] = default

    if name:
        props["name"] = name
    elif not props.get("name"):
        props["name"] = node_id.split(":", 1)[1]
    if description is not None:
        props["description"] = description
    if refs is not None:
        props["refs"] = refs
    if repo is not None:
        props["repo"] = repo
    elif not props.get("repo"):
        props["repo"] = git_value("repo")
    if commit_sha is not None:
        props["commit_sha"] = commit_sha
    elif not props.get("commit_sha"):
        props["commit_sha"] = git_value("commit")
    if file_path is not None:
        props["file_path"] = file_path
    for key, value in attrs.items():
        if value is not None:
            props[key] = value

    merge_node(db, kind, node_id, props)
    return {"id": node_id, "created": not existing}


def _create_field_node(db: DB, field_id: str, name: str, path: str, source: str,
                       repo: str, commit_sha: str, file_path: str) -> None:
    props = {
        "name": name, "path": path, "type": "", "description": "",
        "required": False, "source": source,
        "repo": repo, "commit_sha": commit_sha, "file_path": file_path, "refs": [],
    }
    merge_node(db, "field", field_id, props)


def upsert_field(db: DB, owner: str, path: str, name=None, field_type=None,
                 description=None, required=None, refs=None, repo=None,
                 commit_sha=None, file_path=None, source=None) -> dict:
    owner_kind = kind_of(owner)
    if owner_kind not in CONTRACT_KINDS:
        raise DataflowError(f"field owner must be a contract node, got {owner!r}")
    if not get_node(db, owner_kind, owner):
        raise DataflowError(f"owner {owner!r} does not exist; add the contract node first")
    validate_path(path)

    segments = path.split(".")
    parent, parent_kind = owner, owner_kind
    leaf_id = f"{owner}#{path}"
    created_leaf = False

    for index, segment in enumerate(segments, start=1):
        sub_path = ".".join(segments[:index])
        field_id = f"{owner}#{sub_path}"
        is_leaf = index == len(segments)
        existing = get_node(db, "field", field_id)

        if is_leaf:
            created_leaf = not existing
            props = dict(existing) if existing else {}
            props.setdefault("name", segment)
            props.setdefault("path", sub_path)
            props.setdefault("type", "")
            props.setdefault("description", "")
            props.setdefault("required", False)
            props.setdefault("source", source or "declared")
            props.setdefault("repo", repo if repo is not None else git_value("repo"))
            props.setdefault("commit_sha",
                             commit_sha if commit_sha is not None else git_value("commit"))
            props.setdefault("file_path", file_path or "")
            props.setdefault("refs", [])
            if name:
                props["name"] = name
            if field_type is not None:
                props["type"] = field_type
            if description is not None:
                props["description"] = description
            if required is not None:
                props["required"] = bool(required)
            if refs is not None:
                props["refs"] = refs
            if repo is not None:
                props["repo"] = repo
            if commit_sha is not None:
                props["commit_sha"] = commit_sha
            if file_path is not None:
                props["file_path"] = file_path
            props["source"] = source or "declared"
            merge_node(db, "field", field_id, props)
            edge_props = {}
            if refs is not None:
                edge_props["refs"] = refs
            if file_path is not None:
                edge_props["file_path"] = file_path
            merge_edge(db, "contains", parent, field_id, edge_props)
        else:
            if not existing:
                _create_field_node(
                    db, field_id, segment, sub_path, "inferred",
                    repo if repo is not None else git_value("repo"),
                    commit_sha if commit_sha is not None else git_value("commit"),
                    file_path or "")
            merge_edge(db, "contains", parent, field_id, {})

        parent, parent_kind = field_id, "field"

    return {"id": leaf_id, "created": created_leaf, "owner": owner, "path": path}


def upsert_edge(db: DB, kind: str, src: str, dst: str, refs=None,
                file_path=None, note=None) -> dict:
    if kind not in RELS:
        raise DataflowError(f"edge kind must be one of {tuple(RELS)}")
    src_kind, dst_kind = kind_of(src), kind_of(dst)

    if kind in ("compose", "transform"):
        if src_kind not in CONTRACT_KINDS or dst_kind not in CONTRACT_KINDS:
            raise DataflowError(f"{kind} edges connect contract nodes only")
    else:
        allowed = ((src_kind in CONTRACT_KINDS and dst_kind == "field") or
                   (src_kind == "field" and dst_kind == "field"))
        if not allowed:
            raise DataflowError("contains edges go from a contract or Field to a Field")
        if src_kind == "field" and owner_of(src) != owner_of(dst):
            raise DataflowError("nested contains edges must stay inside the same contract")

    for node_id in (src, dst):
        if not get_node(db, kind_of(node_id), node_id):
            raise DataflowError(f"node {node_id!r} does not exist")
    if src == dst:
        raise DataflowError("self edges are not allowed")

    _, rows = db.run(
        f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-[r:{RELS[kind]}]->"
        f"(b:{LABELS[dst_kind]} {{id:$to}}) RETURN r",
        {"from": src, "to": dst})
    existing = clean(rows[0][0]) if rows else None

    props = {}
    if refs is not None:
        props["refs"] = refs
    if file_path is not None:
        props["file_path"] = file_path
    if note is not None and kind != "contains":
        props["note"] = note
    merge_edge(db, kind, src, dst, props)
    return {"from": src, "to": dst, "kind": kind, "created": not existing}


def remove_node(db: DB, node_id: str) -> dict:
    kind = kind_of(node_id)
    if not get_node(db, kind, node_id):
        raise DataflowError(f"node {node_id!r} does not exist")
    removed_fields = 0
    if kind in CONTRACT_KINDS:
        _, rows = db.run("MATCH (f:Field) RETURN f.id")
        owned = [row[0] for row in rows if owner_of(row[0]) == node_id]
        for field_id in owned:
            db.run("MATCH (f:Field {id:$id}) DETACH DELETE f", {"id": field_id})
            removed_fields += 1
    db.run(f"MATCH (n:{LABELS[kind]} {{id:$id}}) DETACH DELETE n", {"id": node_id})
    return {"id": node_id, "removed_fields": removed_fields}


def remove_edge(db: DB, kind: str, src: str, dst: str) -> dict:
    if kind not in RELS:
        raise DataflowError(f"edge kind must be one of {tuple(RELS)}")
    src_kind, dst_kind = kind_of(src), kind_of(dst)
    _, rows = db.run(
        f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-[r:{RELS[kind]}]->"
        f"(b:{LABELS[dst_kind]} {{id:$to}}) RETURN count(r)",
        {"from": src, "to": dst})
    count = rows[0][0] if rows else 0
    if not count:
        raise DataflowError(f"edge {src} -[{kind}]-> {dst} does not exist")
    db.run(
        f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-[r:{RELS[kind]}]->"
        f"(b:{LABELS[dst_kind]} {{id:$to}}) DELETE r",
        {"from": src, "to": dst})
    return {"from": src, "to": dst, "kind": kind, "removed": count}


# --------------------------------------------------------------------------
# reads
# --------------------------------------------------------------------------

def load_graph(db: DB):
    nodes = {}
    for kind in ALL_KINDS:
        _, rows = db.run(f"MATCH (n:{LABELS[kind]}) RETURN n")
        for (row,) in rows:
            props = clean(row)
            props["kind"] = kind
            nodes[props["id"]] = props

    edges = []
    for kind, rel in RELS.items():
        _, rows = db.run(
            f"MATCH (a)-[r:{rel}]->(b) RETURN a.id, b.id, r")
        for src, dst, props in rows:
            edge = {"kind": kind, "from": src, "to": dst}
            edge.update(clean(props))
            edges.append(edge)

    for edge in edges:
        edge.setdefault("refs", [])
        edge["refs"] = edge.get("refs") or []
    return nodes, edges


def graph_payload(db: DB) -> dict:
    nodes, edges = load_graph(db)
    node_list = []
    for node in sorted(nodes.values(), key=lambda item: (item["kind"], item["id"])):
        item = {k: v for k, v in node.items() if v not in ("", None, [])}
        item.setdefault("refs", [])
        if node["kind"] == "field":
            item["owner"] = owner_of(node["id"])
        node_list.append(item)
    edge_list = []
    for edge in sorted(edges, key=lambda item: (item["kind"], item["from"], item["to"])):
        item = {k: v for k, v in edge.items() if v not in ("", None, [])}
        item.setdefault("refs", [])
        edge_list.append(item)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "repo": git_value("repo"),
        "commit": git_value("commit"),
        "nodes": node_list,
        "edges": edge_list,
    }


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------

def _prune_descendants(paths):
    paths = set(paths)
    return sorted(path for path in paths
                  if not any(path.startswith(other + ".") for other in paths if other != path))


def build_report(db: DB, check_files: bool = False) -> dict:
    nodes, edges = load_graph(db)
    findings = []
    contracts = {nid: node for nid, node in nodes.items() if node["kind"] in CONTRACT_KINDS}
    fields = {nid: node for nid, node in nodes.items() if node["kind"] == "field"}

    def add(code, severity, message, nodes_involved=None, edge=None, refs=None,
            data=None):
        findings.append({
            "code": code,
            "severity": severity,
            "message": message,
            "nodes": nodes_involved or [],
            "edge": edge,
            "refs": refs or [],
            "data": data or {},
        })

    # adjacency
    degree = Counter()
    out_by_node = defaultdict(list)
    in_by_node = defaultdict(list)
    compose_out = Counter()
    for edge in edges:
        degree[edge["from"]] += 1
        degree[edge["to"]] += 1
        out_by_node[edge["from"]].append(edge)
        in_by_node[edge["to"]].append(edge)
        if edge["kind"] == "compose":
            compose_out[edge["from"]] += 1

    # fields per owner
    fields_by_owner = defaultdict(dict)
    for field_id, field in fields.items():
        fields_by_owner[owner_of(field_id)][field.get("path", "")] = field

    # 1. compose cycles
    adjacency = defaultdict(list)
    for edge in edges:
        if edge["kind"] == "compose" and edge["from"] in contracts and edge["to"] in contracts:
            adjacency[edge["from"]].append(edge["to"])
    seen_cycles = set()
    cycle_list = []
    color = {}

    def visit(node, stack):
        color[node] = 1
        stack.append(node)
        for nxt in adjacency.get(node, []):
            state = color.get(nxt, 0)
            if state == 1:
                index = stack.index(nxt)
                cycle = tuple(stack[index:])
                pivot = cycle.index(min(cycle))
                canonical = cycle[pivot:] + cycle[:pivot]
                if canonical not in seen_cycles:
                    seen_cycles.add(canonical)
                    cycle_list.append(canonical)
            elif state == 0:
                visit(nxt, stack)
        stack.pop()
        color[node] = 2

    for start_node in sorted(adjacency):
        if color.get(start_node, 0) == 0:
            visit(start_node, [])
    for cycle in cycle_list[:20]:
        add("compose_cycle", "error",
            "compose cycle: " + " -> ".join(cycle + (cycle[0],)),
            nodes_involved=list(cycle), data={"cycle": list(cycle)})

    # 2. contract completeness
    for node_id, node in sorted(contracts.items()):
        missing = []
        if node["kind"] in ("input", "entry") and not node.get("entry_point"):
            missing.append("entry_point")
        if node["kind"] == "storage" and not node.get("location"):
            missing.append("location")
        if not node.get("file_path") and not node.get("refs"):
            missing.append("file_path/refs")
        if missing:
            add("incomplete_contract", "info",
                f"{node_id} is missing {', '.join(missing)}",
                nodes_involved=[node_id], refs=node.get("refs") or [])

    # 3. field ownership / typing
    contains_edges = {(edge["from"], edge["to"]) for edge in edges
                      if edge["kind"] == "contains"}
    inferred = 0
    for field_id, field in sorted(fields.items()):
        path = field.get("path", "")
        owner = owner_of(field_id)
        parent = owner if "." not in path else owner + "#" + path.rsplit(".", 1)[0]
        if (parent, field_id) not in contains_edges:
            add("orphan_field", "error",
                f"field {field_id} is not linked to its parent {parent}",
                nodes_involved=[field_id, parent] if parent in nodes else [field_id])
        if field.get("source") == "inferred":
            inferred += 1
        elif not field.get("type"):
            add("field_without_type", "info",
                f'field "{field.get("path")}" of {owner_of(field_id)} has no type',
                nodes_involved=[field_id], refs=field.get("refs") or [])
    if inferred:
        add("inferred_fields", "info",
            f"{inferred} parent field(s) were inferred from dotted paths; "
            "review their types", data={"count": inferred})

    # 4. pipeline shape
    for node_id, node in sorted(contracts.items()):
        if degree[node_id] == 0:
            add("isolated_contract", "warn",
                f"{node_id} is not connected to any other node",
                nodes_involved=[node_id], refs=node.get("refs") or [])
            continue
        if node["kind"] == "input":
            if compose_out[node_id] > 1:
                add("input_multiple_compose", "warn",
                    f"{node_id} has {compose_out[node_id]} outgoing compose edges; "
                    "the model allows at most 1 (use transform for fan-out)",
                    nodes_involved=[node_id])
            if not out_by_node[node_id]:
                add("unconsumed_input", "warn",
                    f"{node_id} has no downstream compose/transform edge",
                    nodes_involved=[node_id], refs=node.get("refs") or [])
        if node["kind"] == "storage" and not in_by_node[node_id]:
            add("unproduced_storage", "warn",
                f"{node_id} has no upstream compose/transform edge",
                nodes_involved=[node_id], refs=node.get("refs") or [])
        if node["kind"] == "entry" and in_by_node[node_id] and not out_by_node[node_id]:
            add("entry_dead_end", "info",
                f"{node_id} receives data but has no downstream edge",
                nodes_involved=[node_id], refs=node.get("refs") or [])

    # 5. contract diffs along compose/transform edges
    compose_pairs = {(edge["from"], edge["to"]) for edge in edges
                     if edge["kind"] == "compose"}
    for edge in sorted(edges, key=lambda item: (item["kind"], item["from"], item["to"])):
        if edge["kind"] not in ("compose", "transform"):
            continue
        if edge["kind"] == "transform" and (edge["from"], edge["to"]) in compose_pairs:
            # the compose edge already expresses this pair; avoid duplicate findings
            continue
        src_fields = fields_by_owner.get(edge["from"], {})
        dst_fields = fields_by_owner.get(edge["to"], {})
        if not src_fields or not dst_fields:
            continue
        dst_kind = nodes.get(edge["to"], {}).get("kind")
        severity = "warn" if edge["kind"] == "compose" and dst_kind != "storage" else "info"
        src_node = nodes.get(edge["from"], {})
        dst_node = nodes.get(edge["to"], {})

        missing = _prune_descendants(set(src_fields) - set(dst_fields))
        for path in missing:
            field = src_fields[path]
            type_note = f" ({field.get('type')})" if field.get("type") else ""
            add("missing_field", severity,
                f'field "{path}"{type_note} exists in {edge["from"]} '
                f'but not in {edge["to"]}',
                nodes_involved=[edge["from"], edge["to"]],
                edge={"kind": edge["kind"], "from": edge["from"], "to": edge["to"]},
                refs=(field.get("refs") or []) + (dst_node.get("refs") or []),
                data={"field": path, "type": field.get("type", ""),
                      "edge_kind": edge["kind"]})

        for path in sorted(set(src_fields) & set(dst_fields)):
            src_type = norm_type(src_fields[path].get("type"))
            dst_type = norm_type(dst_fields[path].get("type"))
            if not src_type or not dst_type or src_type == dst_type:
                continue
            add("type_conflict", severity,
                f'field "{path}" is {src_fields[path].get("type")} in {edge["from"]} '
                f'but {dst_fields[path].get("type")} in {edge["to"]}',
                nodes_involved=[edge["from"], edge["to"]],
                edge={"kind": edge["kind"], "from": edge["from"], "to": edge["to"]},
                refs=(dst_fields[path].get("refs") or []) + (src_node.get("refs") or []),
                data={"field": path, "from_type": src_fields[path].get("type", ""),
                      "to_type": dst_fields[path].get("type", ""),
                      "edge_kind": edge["kind"]})

    # 6. optional filesystem checks (one finding per missing path)
    if check_files:
        seen_paths = {}
        for node_id, node in sorted(list(contracts.items()) + list(fields.items())):
            candidates = []
            if node.get("file_path"):
                candidates.append(node["file_path"])
            for ref in node.get("refs") or []:
                candidates.append(ref.rsplit(":", 1)[0]
                                  if re.search(r":\d+$", ref) else ref)
            for candidate in candidates:
                if not candidate or candidate in seen_paths:
                    continue
                seen_paths[candidate] = node_id
                if not Path(candidate).exists():
                    add("missing_file", "warn",
                        f"{node_id} references a file that does not exist: {candidate}",
                        nodes_involved=[node_id], refs=[candidate])

    findings.sort(key=lambda item: (SEVERITY_RANK[item["severity"]], item["code"],
                                    item["message"]))
    by_severity = Counter(item["severity"] for item in findings)
    by_code = Counter(item["code"] for item in findings)
    return {
        "generated_at": now_iso(),
        "db": str(db.path),
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "contracts": len(contracts),
            "fields": len(fields),
            "edges": len(edges),
            "findings": {key: by_severity.get(key, 0) for key in ("error", "warn", "info")},
            "by_code": dict(sorted(by_code.items())),
        },
        "findings": findings,
    }


def format_report_text(report: dict) -> str:
    summary = report["summary"]
    lines = [
        f"Data flow map: {summary['contracts']} contracts, {summary['fields']} fields, "
        f"{summary['edges']} edges",
        f"DB: {report['db']}",
        "",
    ]
    counts = summary["findings"]
    if not report["findings"]:
        lines.append("No findings. The graph is consistent under the current rules.")
        return "\n".join(lines)

    for severity in ("error", "warn", "info"):
        group = [item for item in report["findings"] if item["severity"] == severity]
        if not group:
            continue
        lines.append(f"{severity.upper()} ({len(group)})")
        for item in group:
            lines.append(f"  [{item['code']}] {item['message']}")
            if item["refs"]:
                lines.append(f"      refs: {', '.join(item['refs'][:6])}")
        lines.append("")

    lines.append(f"Summary: {counts['error']} errors, {counts['warn']} warnings, "
                 f"{counts['info']} infos")
    return "\n".join(lines).rstrip()


def format_report_markdown(report: dict) -> str:
    summary = report["summary"]
    lines = [
        "# Data flow report",
        "",
        f"- Contracts: **{summary['contracts']}**",
        f"- Fields: **{summary['fields']}**",
        f"- Edges: **{summary['edges']}**",
        f"- Findings: **{summary['findings']['error']} errors**, "
        f"**{summary['findings']['warn']} warnings**, "
        f"**{summary['findings']['info']} infos**",
        f"- Generated: {report['generated_at']}",
        "",
    ]
    if not report["findings"]:
        lines.append("No findings.")
        return "\n".join(lines)
    for severity, title in (("error", "Errors"), ("warn", "Warnings"), ("info", "Info")):
        group = [item for item in report["findings"] if item["severity"] == severity]
        if not group:
            continue
        lines.append(f"## {title} ({len(group)})")
        lines.append("")
        for item in group:
            lines.append(f"- `{item['code']}` — {item['message']}")
            if item["refs"]:
                lines.append(f"  - refs: {', '.join(f'`{ref}`' for ref in item['refs'][:6])}")
        lines.append("")
    return "\n".join(lines).rstrip()


# --------------------------------------------------------------------------
# render
# --------------------------------------------------------------------------

def render_html(db: DB, out_path: Path, title: str) -> Path:
    template_path = Path(__file__).resolve().parent.parent / "assets" / "graph.html"
    if not template_path.exists():
        raise DataflowError(f"visualization template not found: {template_path}")
    payload = graph_payload(db)
    payload["title"] = title
    data = json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")
    html = template_path.read_text(encoding="utf-8").replace("__DATA__", data)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------

def write_meta(db: DB) -> None:
    kuzu_version = ""
    try:
        from importlib.metadata import version
        kuzu_version = version("kuzu")
    except Exception:
        pass
    meta = {
        "schema_version": SCHEMA_VERSION,
        "updated_at": now_iso(),
        "kuzu": kuzu_version,
        "tool": "data-flow-map",
    }
    db.meta_path().write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")


def ensure_gitignore(db: DB) -> None:
    parent = db.path.parent
    gitignore = parent / ".gitignore"
    if gitignore.exists():
        return
    gitignore.write_text(
        "# Kuzu database, venv and generated artifacts\n"
        "*\n"
        "!.gitignore\n"
        "!exports/\n"
        "!exports/**\n",
        encoding="utf-8")


def cmd_init(args) -> int:
    db = open_db(args, initialize=True)
    for statement in DDL:
        db.run(statement)
    write_meta(db)
    ensure_gitignore(db)
    _, rows = db.run("CALL show_tables() RETURN *")
    tables = sorted(row[1] for row in rows)
    payload = {"ok": True, "db": str(db.path), "schema_version": SCHEMA_VERSION,
               "tables": tables}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"initialized {db.path} (schema v{SCHEMA_VERSION})")
        print("tables: " + ", ".join(tables))
        print(f"next: {db.path.parent}/exports/ for git-friendly snapshots")
    return 0


def cmd_doctor(args) -> int:
    info = {"python": sys.version.split()[0], "executable": sys.executable}
    try:
        from importlib.metadata import version
        info["kuzu"] = version("kuzu")
    except Exception:
        info["kuzu"] = ""
    info["git_repo"] = git_value("repo")
    info["git_commit"] = git_value("commit")
    db_path = Path(args.db).expanduser()
    info["db"] = str(db_path)
    info["exists"] = db_path.exists()
    if db_path.exists():
        try:
            db = DB(db_path)
            info["tables"] = sorted(db.tables())
            if db.meta_path().exists():
                try:
                    info["meta"] = json.loads(db.meta_path().read_text(encoding="utf-8"))
                except Exception:
                    info["meta"] = None
            if "Field" in info["tables"]:
                nodes, edges = load_graph(db)
                info["nodes"] = len(nodes)
                info["edges"] = len(edges)
        except Exception as exc:
            info["error"] = f"{type(exc).__name__}: {exc}"
    if args.json:
        print(json.dumps(info, indent=2))
    else:
        print(f"python:  {info['python']} ({info['executable']})")
        print(f"kuzu:    {info['kuzu'] or 'MISSING'}")
        print(f"db:      {info['db']} ({'found' if info['exists'] else 'missing'})")
        print(f"git:     {info['git_repo'] or '-'} @ {info['git_commit'] or '-'}")
        if info.get("tables"):
            print("tables:  " + ", ".join(info["tables"]))
        if info.get("nodes") is not None:
            print(f"graph:   {info['nodes']} nodes, {info['edges']} edges")
    return 0 if info["kuzu"] else 1


def cmd_add_node(args) -> int:
    if args.kind not in CONTRACT_KINDS:
        raise DataflowError("kind must be input, entry or storage")
    attrs = {}
    for key in ("entry_point", "channel", "location", "store_type", "entry_type"):
        value = getattr(args, key)
        if value is not None:
            attrs[key] = value
    db = open_db(args)
    result = upsert_contract(
        db, args.kind, node_id=args.id, name=args.name,
        description=args.description, attrs=attrs,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        repo=args.repo, commit_sha=args.commit, file_path=args.file)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{'created' if result['created'] else 'updated'} {result['id']}")
    return 0


def cmd_add_field(args) -> int:
    db = open_db(args)
    result = upsert_field(
        db, args.owner, args.path, name=args.name, field_type=args.type,
        description=args.description,
        required=True if args.required else None,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        repo=args.repo, commit_sha=args.commit, file_path=args.file)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{'created' if result['created'] else 'updated'} {result['id']}")
    return 0


def cmd_add_edge(args) -> int:
    db = open_db(args)
    result = upsert_edge(
        db, args.kind, args.src, args.dst,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        file_path=args.file, note=args.note)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        label = "created" if result["created"] else "updated"
        print(f"{label} {args.src} -[{args.kind}]-> {args.dst}")
    return 0


def cmd_remove_node(args) -> int:
    db = open_db(args)
    result = remove_node(db, args.id)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        extra = f" (+{result['removed_fields']} fields)" if result["removed_fields"] else ""
        print(f"removed {result['id']}{extra}")
    return 0


def cmd_remove_edge(args) -> int:
    db = open_db(args)
    result = remove_edge(db, args.kind, args.src, args.dst)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"removed {args.src} -[{args.kind}]-> {args.dst}")
    return 0


def cmd_apply(args) -> int:
    path = Path(args.file)
    if not path.exists():
        raise DataflowError(f"file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DataflowError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DataflowError("apply expects a JSON object with nodes/fields/edges")

    db = open_db(args)
    counts = {"nodes": 0, "fields": 0, "edges": 0}
    db.run("BEGIN TRANSACTION")
    try:
        node_items = payload.get("nodes", [])
        # contracts first: field owners must exist before fields are inserted
        for item in node_items:
            if item.get("kind") == "field":
                continue
            kind = item.get("kind")
            if kind not in CONTRACT_KINDS:
                raise DataflowError(f"invalid node kind in apply: {kind!r}")
            attrs = {key: item.get(key) for key in
                     ("entry_point", "channel", "location", "store_type", "entry_type")
                     if item.get(key) is not None}
            upsert_contract(
                db, kind, node_id=item.get("id"), name=item.get("name"),
                description=item.get("description"), attrs=attrs,
                refs=item.get("refs"), repo=item.get("repo"),
                commit_sha=item.get("commit_sha") or item.get("commit"),
                file_path=item.get("file_path") or item.get("file"))
            counts["nodes"] += 1
        for item in node_items:
            if item.get("kind") != "field":
                continue
            field_id = item.get("id") or ""
            owner = item.get("owner") or owner_of(field_id)
            path = item.get("path") or (
                field_id.split("#", 1)[1] if "#" in field_id else "")
            if not owner or not path:
                raise DataflowError("field nodes need owner and path")
            upsert_field(
                db, owner, path, name=item.get("name"),
                field_type=item.get("type"), description=item.get("description"),
                required=item.get("required") if "required" in item else None,
                refs=item.get("refs"), repo=item.get("repo"),
                commit_sha=item.get("commit_sha") or item.get("commit"),
                file_path=item.get("file_path") or item.get("file"),
                source=item.get("source"))
            counts["fields"] += 1
        for item in payload.get("fields", []):
            if not item.get("owner") or not item.get("path"):
                raise DataflowError("each field needs owner and path")
            upsert_field(
                db, item["owner"], item["path"], name=item.get("name"),
                field_type=item.get("type"), description=item.get("description"),
                required=item.get("required") if "required" in item else None,
                refs=item.get("refs"), repo=item.get("repo"),
                commit_sha=item.get("commit_sha") or item.get("commit"),
                file_path=item.get("file_path") or item.get("file"))
            counts["fields"] += 1
        for item in payload.get("edges", []):
            if not item.get("kind") or not item.get("from") or not item.get("to"):
                raise DataflowError("each edge needs kind, from and to")
            upsert_edge(
                db, item["kind"], item["from"], item["to"],
                refs=item.get("refs"), file_path=item.get("file_path"),
                note=item.get("note"))
            counts["edges"] += 1
        db.run("COMMIT")
    except Exception:
        try:
            db.run("ROLLBACK")
        except Exception:
            pass
        raise
    counts["ok"] = True
    if args.json:
        print(json.dumps(counts, indent=2))
    else:
        print(f"applied {counts['nodes']} nodes, {counts['fields']} fields, "
              f"{counts['edges']} edges")
    return 0


def cmd_list(args) -> int:
    db = open_db(args)
    if args.kind and args.kind not in ALL_KINDS:
        raise DataflowError(f"kind must be one of {ALL_KINDS}")
    nodes, _ = load_graph(db)
    selected = []
    for node_id, node in nodes.items():
        if args.kind and node["kind"] != args.kind:
            continue
        if args.owner and owner_of(node_id) != args.owner:
            continue
        if args.search:
            haystack = " ".join(str(node.get(key, "")) for key in
                                ("id", "name", "path", "entry_point", "location"))
            if args.search.lower() not in haystack.lower():
                continue
        selected.append(node)
    selected.sort(key=lambda item: (item["kind"], item["id"]))
    if args.json:
        print(json.dumps(selected, indent=2, ensure_ascii=False))
        return 0
    if not selected:
        print("no nodes")
        return 0
    for node in selected:
        detail = ""
        if node["kind"] == "field":
            detail = f'{node.get("path")} : {node.get("type") or "?"}'
        elif node["kind"] == "storage":
            detail = node.get("location") or "-"
        else:
            detail = node.get("entry_point") or "-"
        suffix = f"  [{node.get('file_path')}]" if node.get("file_path") else ""
        print(f"{node['id']:<42} {detail}{suffix}")
    print(f"\n{len(selected)} node(s)")
    return 0


READ_BLOCKLIST = re.compile(
    r"\b(CREATE|MERGE|SET|DELETE|DETACH|DROP|ALTER|COPY|BEGIN|COMMIT|ROLLBACK|"
    r"INSTALL|LOAD|ATTACH|USE|EXPORT|IMPORT)\b", re.IGNORECASE)
SAFE_CALL = re.compile(
    r"\bCALL\s+(show_tables|table_info|db_version|current_setting)\b", re.IGNORECASE)


def assert_read_only(cypher: str) -> None:
    stripped = SAFE_CALL.sub("CALL safe", cypher)
    match = READ_BLOCKLIST.search(stripped)
    if match:
        raise DataflowError(
            f"query is read-only; {match.group(1).upper()} is not allowed")


def cmd_query(args) -> int:
    assert_read_only(args.cypher)
    db = open_db(args)
    params = {}
    for item in args.param or []:
        if "=" not in item:
            raise DataflowError(f"invalid --param {item!r}; use key=json")
        key, raw = item.split("=", 1)
        try:
            params[key] = json.loads(raw)
        except json.JSONDecodeError:
            params[key] = raw
    columns, rows = db.run(args.cypher, params)
    if args.json:
        print(json.dumps([dict(zip(columns, row)) for row in rows],
                         indent=2, ensure_ascii=False))
        return 0
    if not rows:
        print("(no rows)")
        return 0
    limited = rows[:args.max_rows]
    print(" | ".join(columns))
    print("-+-".join("-" * len(column) for column in columns))
    for row in limited:
        cells = []
        for value in row:
            if isinstance(value, list):
                text = ",".join(str(item) for item in value)
            elif isinstance(value, dict):
                text = json.dumps(value, ensure_ascii=False)
            else:
                text = "" if value is None else str(value)
            cells.append(text if len(text) <= 60 else text[:57] + "...")
        print(" | ".join(cells))
    if len(rows) > len(limited):
        print(f"\n... {len(rows) - len(limited)} more row(s); use --max-rows or --json")
    print(f"\n{len(rows)} row(s)")
    return 0


def cmd_report(args) -> int:
    db = open_db(args)
    report = build_report(db, check_files=args.check_files)
    if args.format == "json":
        print(json.dumps(report, indent=2, ensure_ascii=False))
    elif args.format == "md":
        print(format_report_markdown(report))
    else:
        print(format_report_text(report))
    threshold = {"info": 2, "warn": 1, "error": 0}.get(args.fail_on)
    if threshold is None:
        return 0
    worst = min((SEVERITY_RANK[item["severity"]] for item in report["findings"]),
                default=99)
    return 1 if worst <= threshold else 0


def cmd_export(args) -> int:
    db = open_db(args)
    payload = graph_payload(db)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
                   encoding="utf-8")
    if args.json:
        print(json.dumps({"ok": True, "out": str(out),
                          "nodes": len(payload["nodes"]),
                          "edges": len(payload["edges"])}, indent=2))
    else:
        print(f"exported {len(payload['nodes'])} nodes and {len(payload['edges'])} "
              f"edges to {out}")
    return 0


def cmd_render(args) -> int:
    db = open_db(args)
    out = Path(args.out) if args.out else db.path.parent / DEFAULT_RENDER
    path = render_html(db, out, args.title)
    if args.json:
        print(json.dumps({"ok": True, "out": str(path)}, indent=2))
    else:
        print(f"rendered {path}")
    return 0


def cmd_stats(args) -> int:
    db = open_db(args)
    nodes, edges = load_graph(db)
    contracts = [node for node in nodes.values() if node["kind"] in CONTRACT_KINDS]
    fields = [node for node in nodes.values() if node["kind"] == "field"]
    kinds = Counter(node["kind"] for node in nodes.values())
    edge_kinds = Counter(edge["kind"] for edge in edges)
    channels = Counter(node.get("channel") or "?" for node in contracts
                       if node["kind"] == "input")
    stores = Counter(node.get("store_type") or "?" for node in contracts
                     if node["kind"] == "storage")
    per_contract = Counter(owner_of(field_id) for field_id in nodes if "#" in field_id)
    payload = {
        "nodes": len(nodes), "edges": len(edges),
        "kinds": dict(kinds), "edge_kinds": dict(edge_kinds),
        "channels": dict(channels), "store_types": dict(stores),
        "fields_per_contract": dict(per_contract.most_common(10)),
    }
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    print(f"nodes:  {len(nodes)} ({', '.join(f'{k}={v}' for k, v in sorted(kinds.items()))})")
    print(f"edges:  {len(edges)} ({', '.join(f'{k}={v}' for k, v in sorted(edge_kinds.items()))})")
    if channels:
        print("input channels: " + ", ".join(f"{k}={v}" for k, v in channels.most_common()))
    if stores:
        print("storage types:  " + ", ".join(f"{k}={v}" for k, v in stores.most_common()))
    if per_contract:
        print("fields per contract:")
        for owner, count in per_contract.most_common(10):
            print(f"  {owner:<42} {count}")
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS,
                        help=f"Kuzu database path (default: {DEFAULT_DB})")
    common.add_argument("--json", action="store_true", default=argparse.SUPPRESS,
                        help="machine-readable output")

    parser = argparse.ArgumentParser(
        prog="dataflow.py",
        description="Kuzu-backed data-flow map for application contracts.")
    # Global forms are accepted before the subcommand; the per-command
    # duplicates in `common` accept the same options after it.
    parser.add_argument("--db", default=None,
                        help=f"Kuzu database path (default: {DEFAULT_DB})")
    parser.add_argument("--json", action="store_true", default=None,
                        help="machine-readable output")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", parents=[common], help="create the graph schema")
    init.set_defaults(func=cmd_init)

    doctor = sub.add_parser("doctor", parents=[common], help="check environment and database")
    doctor.set_defaults(func=cmd_doctor)

    add_node = sub.add_parser("add-node", parents=[common],
                              help="create or update a contract node")
    add_node.add_argument("--kind", required=True, choices=CONTRACT_KINDS)
    add_node.add_argument("--id", help="explicit id, e.g. input:orders.created")
    add_node.add_argument("--name")
    add_node.add_argument("--description")
    add_node.add_argument("--entry-point", dest="entry_point")
    add_node.add_argument("--channel", help="input channel: queue, http, file, grpc, cron")
    add_node.add_argument("--location", help="storage location, e.g. postgres://orders")
    add_node.add_argument("--store-type", dest="store_type",
                          help="storage type: sql, redis, object, file, cache")
    add_node.add_argument("--entry-type", dest="entry_type",
                          help="entry type: function, handler, worker, job")
    add_node.add_argument("--repo")
    add_node.add_argument("--commit", dest="commit")
    add_node.add_argument("--file", dest="file")
    add_node.add_argument("--ref", action="append", help="reference file:line (repeatable)")
    add_node.set_defaults(func=cmd_add_node)

    add_field = sub.add_parser("add-field", parents=[common],
                               help="create or update a field and its nested parents")
    add_field.add_argument("--owner", required=True, help="contract id that owns the field")
    add_field.add_argument("--path", required=True, help="dotted path, e.g. customer.id")
    add_field.add_argument("--name")
    add_field.add_argument("--type", dest="type", help="declared type, e.g. string, integer")
    add_field.add_argument("--description")
    add_field.add_argument("--required", action="store_true")
    add_field.add_argument("--repo")
    add_field.add_argument("--commit", dest="commit")
    add_field.add_argument("--file", dest="file")
    add_field.add_argument("--ref", action="append", help="reference file:line (repeatable)")
    add_field.set_defaults(func=cmd_add_field)

    add_edge = sub.add_parser("add-edge", parents=[common],
                              help="create or update a relationship")
    add_edge.add_argument("--kind", required=True, choices=tuple(RELS))
    add_edge.add_argument("--from", dest="src", required=True)
    add_edge.add_argument("--to", dest="dst", required=True)
    add_edge.add_argument("--ref", action="append", help="reference file:line (repeatable)")
    add_edge.add_argument("--file", dest="file")
    add_edge.add_argument("--note", help="free-form note (compose/transform)")
    add_edge.set_defaults(func=cmd_add_edge)

    remove_node = sub.add_parser("remove-node", parents=[common],
                                 help="delete a node and its owned fields")
    remove_node.add_argument("--id", required=True)
    remove_node.set_defaults(func=cmd_remove_node)

    remove_edge = sub.add_parser("remove-edge", parents=[common], help="delete an edge")
    remove_edge.add_argument("--kind", required=True, choices=tuple(RELS))
    remove_edge.add_argument("--from", dest="src", required=True)
    remove_edge.add_argument("--to", dest="dst", required=True)
    remove_edge.set_defaults(func=cmd_remove_edge)

    apply = sub.add_parser("apply", parents=[common],
                           help="apply a JSON batch of nodes, fields and edges")
    apply.add_argument("--file", required=True)
    apply.set_defaults(func=cmd_apply)

    list_cmd = sub.add_parser("list", parents=[common], help="list nodes")
    list_cmd.add_argument("--kind", choices=ALL_KINDS)
    list_cmd.add_argument("--owner", help="filter fields by contract id")
    list_cmd.add_argument("--search", help="case-insensitive substring filter")
    list_cmd.set_defaults(func=cmd_list)

    query = sub.add_parser("query", parents=[common], help="run a read-only Cypher query")
    query.add_argument("--cypher", required=True)
    query.add_argument("--param", action="append", help="key=json (repeatable)")
    query.add_argument("--max-rows", type=int, default=100)
    query.set_defaults(func=cmd_query)

    report = sub.add_parser("report", parents=[common],
                            help="find missing, conflicting and orphaned information")
    report.add_argument("--format", choices=("text", "md", "json"), default="text")
    report.add_argument("--fail-on", choices=("none", "error", "warn", "info"),
                        default="none")
    report.add_argument("--check-files", action="store_true",
                        help="verify that file_path/refs exist on disk")
    report.set_defaults(func=cmd_report)

    export = sub.add_parser("export", parents=[common], help="write a JSON snapshot")
    export.add_argument("--out", default=".dataflow/exports/graph.json")
    export.set_defaults(func=cmd_export)

    render = sub.add_parser("render", parents=[common], help="write a standalone HTML graph")
    render.add_argument("--out")
    render.add_argument("--title", default="Data flow map")
    render.set_defaults(func=cmd_render)

    stats = sub.add_parser("stats", parents=[common], help="print graph counters")
    stats.set_defaults(func=cmd_stats)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "db", None) is None:
        args.db = DEFAULT_DB
    if getattr(args, "json", None) is None:
        args.json = False
    try:
        return args.func(args)
    except DataflowError as exc:
        if getattr(args, "json", False):
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # surface Kuzu errors without a traceback
        if getattr(args, "json", False):
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
