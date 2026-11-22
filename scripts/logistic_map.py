#!/usr/bin/env python3
"""LadybugDB-backed logistic map for application data flow.

Schema v7
---------
Contracts are data entities: Input arrives, Entry is an intermediate entity,
Storage is persisted data. Field nodes belong to exactly one contract node.
EntryPoint nodes are code-level activations. Flow nodes group the entities and
passages of one explicit path. Rule nodes describe limits or conditions that a
flow or entrypoint follows. Contracts, flows, entrypoints and rules may carry a
free-form `cluster` label for grouping; fields inherit their owner's cluster.

  EntryPoint          -TRIGGER-> Flow                  the activation starts the flow
  EntryPoint          -RELATED-> EntryPoint           directional activation relation
  Flow|EntryPoint     -RULE-> Rule                    a rule the activation follows
  Flow                -USES-> Input|Entry|Storage      the flow uses this entity
  Input|Entry|Storage -CONTAINS-> Field          contract fields
  Field               -CONTAINS-> Field          nested fields
  Input|Entry|Storage -COMPOSE-> Input|Entry|Storage    whole -> part (composition)
  Input|Entry|Storage -TRANSFORM {flow_id}-> Input|Entry|Storage  data changes shape
  Input|Entry|Storage -FLOW {flow_id}-> Input|Entry|Storage       data passes unchanged

`compose` is containment, never execution order. `flow_id` on FLOWS/TRANSFORM
identifies the flow the passage belongs to; the same pair can appear in several
flows. `USE` only says which entities participate; `TRIGGER` never carries data.

Schema v1 stored pipeline edges as `compose`; schema v2 had derived flows in a
second database; v3 removed Entry code attributes; v4/v5 separated activations
from data and added RELATED; v6 added the cluster label; v7 added Rule.
`migrate` converts v1
compose edges to unassigned FLOWS passages, drops legacy Input activation
attributes, and never invents an EntryPoint, TRIGGER, RELATED or cluster.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

SCHEMA_VERSION = 8
DEFAULT_DB = os.environ.get("LOGISTIC_MAP_DB", ".logistic-map/graph.lbdb")
DEFAULT_RENDER = "graph.html"

CONTRACT_KINDS = ("input", "entry", "storage")
ALL_KINDS = ("input", "entry", "storage", "flow", "entrypoint",
             "rule", "external", "field")
DATA_EDGE_KINDS = ("flow", "transform")
FLOW_SCOPED_EDGE_KINDS = DATA_EDGE_KINDS + ("delivers-to",)

LABELS = {"input": "Input", "entry": "Entry", "storage": "Storage",
          "flow": "Flow", "entrypoint": "EntryPoint", "rule": "Rule",
          "external": "External", "field": "Field"}
PLURALS = {"input": "Inputs", "entry": "Entries", "storage": "Storages",
           "flow": "Flows", "entrypoint": "EntryPoints", "rule": "Rules",
           "external": "External Systems", "field": "Fields"}
RELS = {"contains": "CONTAINS", "compose": "COMPOSE",
        "transform": "TRANSFORM", "flow": "FLOWS", "use": "USES",
        "trigger": "TRIGGER", "related": "RELATED", "rule": "RULE_REL",
        "calls": "CALLS", "delivers-to": "DELIVERS_TO"}

NODE_PROPS = {
    "input": ["name", "description", "cluster", "repo", "commit_sha",
              "file_path", "refs"],
    "entry": ["name", "description", "stage", "cluster",
              "repo", "commit_sha", "file_path", "refs"],
    "storage": ["name", "description", "location", "store_type", "cluster",
                "repo", "commit_sha", "file_path", "refs"],
    "flow": ["name", "description", "cluster",
             "repo", "commit_sha", "file_path", "refs"],
    "entrypoint": ["name", "description", "cluster",
                   "repo", "commit_sha", "file_path", "refs"],
    "rule": ["name", "description", "cluster",
             "repo", "commit_sha", "file_path", "refs"],
    "external": ["name", "description", "provider", "protocol", "location",
                 "cluster", "repo", "commit_sha", "file_path", "refs"],
    "field": ["name", "path", "type", "description", "required", "source",
              "repo", "commit_sha", "file_path", "refs"],
}

# Fields belong to their owner's cluster; every other node carries its own.
CLUSTER_KINDS = tuple(kind for kind in NODE_PROPS if kind != "field")

TRANSFORM_REL_DDL = """CREATE REL TABLE IF NOT EXISTS TRANSFORM(
        FROM Input TO Entry, FROM Input TO Storage, FROM Input TO Input,
        FROM Entry TO Entry, FROM Entry TO Storage, FROM Entry TO Input,
        FROM Storage TO Entry, FROM Storage TO Storage, FROM Storage TO Input,
        flow_id STRING, refs STRING[], file_path STRING, note STRING)"""
FLOW_REL_DDL = """CREATE REL TABLE IF NOT EXISTS FLOWS(
        FROM Input TO Entry, FROM Input TO Storage, FROM Input TO Input,
        FROM Entry TO Entry, FROM Entry TO Storage, FROM Entry TO Input,
        FROM Storage TO Entry, FROM Storage TO Storage, FROM Storage TO Input,
        flow_id STRING, refs STRING[], file_path STRING, note STRING)"""
USE_REL_DDL = """CREATE REL TABLE IF NOT EXISTS USES(
        FROM Flow TO Input, FROM Flow TO Entry, FROM Flow TO Storage,
        refs STRING[], file_path STRING, note STRING)"""

DDL = [
    """CREATE NODE TABLE IF NOT EXISTS Field(
        id STRING, name STRING, path STRING, type STRING, description STRING,
        required BOOLEAN, source STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Input(
        id STRING, name STRING, description STRING, cluster STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Storage(
        id STRING, name STRING, description STRING, location STRING, store_type STRING,
        cluster STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Flow(
        id STRING, name STRING, description STRING, cluster STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS EntryPoint(
        id STRING, name STRING, description STRING, cluster STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Entry(
        id STRING, name STRING, description STRING, stage STRING, cluster STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS Rule(
        id STRING, name STRING, description STRING, cluster STRING,
        repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
        PRIMARY KEY(id))""",
    """CREATE NODE TABLE IF NOT EXISTS External(
        id STRING, name STRING, description STRING, provider STRING,
        protocol STRING, location STRING, cluster STRING,
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
    TRANSFORM_REL_DDL,
    FLOW_REL_DDL,
    USE_REL_DDL,
    """CREATE REL TABLE IF NOT EXISTS TRIGGER(
        FROM EntryPoint TO Flow, refs STRING[], file_path STRING, note STRING)""",
    """CREATE REL TABLE IF NOT EXISTS RELATED(
        FROM EntryPoint TO EntryPoint,
        description STRING, refs STRING[], file_path STRING, note STRING)""",
    """CREATE REL TABLE IF NOT EXISTS RULE_REL(
        FROM Flow TO Rule, FROM EntryPoint TO Rule,
        refs STRING[], file_path STRING, note STRING)""",
    """CREATE REL TABLE IF NOT EXISTS CALLS(
        FROM Flow TO External,
        refs STRING[], file_path STRING, note STRING)""",
    """CREATE REL TABLE IF NOT EXISTS DELIVERS_TO(
        FROM Entry TO External,
        flow_id STRING, refs STRING[], file_path STRING, note STRING)""",
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


class LogisticMapError(Exception):
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
        raise LogisticMapError(f"invalid id {node_id!r}: expected <kind>:<slug>")
    kind = node_id.split(":", 1)[0]
    if kind not in ALL_KINDS:
        raise LogisticMapError(f"invalid id {node_id!r}: unknown kind {kind!r}")
    return kind


def owner_of(field_id: str) -> str:
    return field_id.split("#", 1)[0] if "#" in field_id else ""


def validate_node_id(node_id: str, kind: str) -> None:
    if kind_of(node_id) != kind:
        raise LogisticMapError(f"id {node_id!r} does not start with {kind}:")
    if not re.match(r"^[a-z]+:[^\s#]+$", node_id):
        raise LogisticMapError(
            f"invalid id {node_id!r}: use lowercase <kind>:<slug> without spaces or '#'")


def validate_path(path: str) -> None:
    if not path or "#" in path or re.search(r"\s", path):
        raise LogisticMapError(f"invalid field path {path!r}")
    if path.startswith(".") or path.endswith(".") or ".." in path:
        raise LogisticMapError(f"invalid field path {path!r}")


def norm_type(value: str) -> str:
    value = (value or "").strip().lower()
    return TYPE_ALIASES.get(value, value)


def is_contract(node_id: str) -> bool:
    return kind_of(node_id) in CONTRACT_KINDS


# --------------------------------------------------------------------------
# database
# --------------------------------------------------------------------------

def load_ladybug():
    try:
        import ladybug  # noqa: F401
    except Exception as exc:  # pragma: no cover - depends on environment
        raise LogisticMapError(
            "ladybug is not installed for this Python interpreter "
            f"({sys.executable}). Run scripts/bootstrap.sh, or "
            "`python3 -m venv .logistic-map/.venv && "
            ".logistic-map/.venv/bin/pip install ladybug`."
        ) from exc
    return ladybug


def load_kuzu():
    """Legacy engine, only used by `migrate-db` in a subprocess."""
    try:
        import kuzu  # noqa: F401
    except Exception as exc:  # pragma: no cover - depends on environment
        raise LogisticMapError(
            "reading a Kuzu database requires the archived kuzu package; "
            "install it temporarily with `pip install kuzu==0.11.3`, or "
            "export the graph to JSON with the old CLI and use "
            "`migrate-db --from snapshot.json`."
        ) from exc
    return kuzu


class DB:
    def __init__(self, path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        ladybug = load_ladybug()
        self._db = ladybug.Database(str(self.path))
        self.conn = ladybug.Connection(self._db)

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

    def close(self) -> None:
        for target in (getattr(self, "conn", None), getattr(self, "_db", None)):
            try:
                target.close()
            except Exception:
                pass

    def meta_path(self) -> Path:
        return self.path.parent / "meta.json"


class KuzuDB:
    """Read-only adapter for importing a legacy Kuzu database.

    Kuzu and LadybugDB register the same native types, so the legacy file is
    read in a separate process; this class is only instantiated there.
    """

    def __init__(self, path):
        kuzu = load_kuzu()
        self.path = Path(path).expanduser()
        self._db = kuzu.Database(str(self.path), read_only=True)
        self.conn = kuzu.Connection(self._db)

    def run(self, cypher: str, params=None):
        result = self.conn.execute(cypher, params or {})
        columns = result.get_column_names()
        rows = []
        while result.has_next():
            rows.append(result.get_next())
        result.close()
        return columns, rows

    def close(self) -> None:
        for target in (getattr(self, "conn", None), getattr(self, "_db", None)):
            try:
                target.close()
            except Exception:
                pass


def read_meta(db: DB) -> dict:
    path = db.meta_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def db_schema_version(db: DB) -> int:
    try:
        return int(read_meta(db).get("schema_version", 0))
    except (TypeError, ValueError):
        return 0


def open_db(args, initialize: bool = False, allow_old: bool = False) -> DB:
    path = Path(args.db).expanduser()
    if not initialize and not path.exists():
        legacy = path.with_suffix(".kuzu")
        hint = ""
        if path.suffix == ".lbdb" and legacy.exists():
            hint = (f" found legacy {legacy}; run `migrate-db --from {legacy} "
                    f"--to {path}` to convert it.")
        raise LogisticMapError(
            f"no logistic-map database at {path}; run `init` first.{hint}")
    db = DB(path)
    if not initialize and "Field" not in db.tables():
        raise LogisticMapError(
            f"{path} exists but has no logistic-map schema; run `init` first")
    if not initialize and not allow_old:
        version = db_schema_version(db)
        if version != SCHEMA_VERSION:
            found = f"v{version}" if version else "an unknown version"
            raise LogisticMapError(
                f"{path} uses {found} of the schema; this CLI writes "
                f"v{SCHEMA_VERSION}. Run `migrate` to convert it; legacy "
                "Input activation attributes are dropped without inventing "
                "EntryPoints or triggers.")
    return db


# --------------------------------------------------------------------------
# branches
# --------------------------------------------------------------------------

BRANCH_MASTER = "master"
BRANCH_OPEN = "open"
BRANCH_APPLIED = "applied"
BRANCH_STALE = "stale"
BRANCH_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,59}$")


def branch_state_path(master: Path) -> Path:
    return master.parent / f"{master.name}.branches.json"


def branch_db_path(master: Path, name: str) -> Path:
    return master.parent / f"{master.name}.{name}"


def read_branch_state(master: Path) -> dict:
    path = branch_state_path(master)
    state = {"version": 1, "revision": 0, "active": BRANCH_MASTER,
             "branches": {}}
    if not path.exists():
        return state
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return state
    if not isinstance(data, dict):
        return state
    state.update({key: data[key] for key in state if key in data})
    if not isinstance(state.get("branches"), dict):
        state["branches"] = {}
    return state


def write_branch_state(master: Path, state: dict) -> None:
    path = branch_state_path(master)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n",
                   encoding="utf-8")
    tmp.replace(path)


def branch_name(value: str) -> str:
    name = str(value or "").strip()
    if name == BRANCH_MASTER:
        raise LogisticMapError(
            "master is the main line; use `branch use master` instead")
    if not BRANCH_NAME_RE.fullmatch(name):
        raise LogisticMapError(
            "branch names must match [a-z0-9][a-z0-9._-]* "
            "and be at most 60 characters")
    return name


def graph_fingerprint(db: DB) -> str:
    nodes, edges = load_graph(db)
    node_list = [dict(nodes[node_id])
                 for node_id in sorted(nodes, key=lambda key: (nodes[key]["kind"], key))]
    edge_list = sorted(
        (dict(edge) for edge in edges),
        key=lambda edge: json.dumps(edge, sort_keys=True, default=str))
    payload = {"nodes": node_list, "edges": edge_list}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def master_fingerprint(master: Path) -> str:
    db = DB(master)
    try:
        return graph_fingerprint(db)
    finally:
        db.close()


def refresh_stale_branches(master: Path, state: dict) -> bool:
    """Mark open branches whose base no longer matches the master line."""
    if not master.exists():
        return False
    fingerprint = None
    changed = False
    for branch in state.get("branches", {}).values():
        if branch.get("status") != BRANCH_OPEN:
            continue
        if fingerprint is None:
            fingerprint = master_fingerprint(master)
        if branch.get("base_fingerprint") != fingerprint:
            branch["status"] = BRANCH_STALE
            branch["stale_reason"] = (
                "stale: master changed after this branch was created; "
                "create a new branch from the current master")
            changed = True
    return changed


def resolve_active_db(master: Path) -> Path:
    state = read_branch_state(master)
    active = state.get("active") or BRANCH_MASTER
    if active == BRANCH_MASTER:
        return master
    branch = state.get("branches", {}).get(active)
    if not branch or branch.get("status") != BRANCH_OPEN:
        raise LogisticMapError(
            f"active branch {active!r} is not open; run `branch use master`")
    path = branch_db_path(master, active)
    if not path.exists():
        raise LogisticMapError(
            f"active branch {active!r} is missing its database at {path}; "
            "run `branch use master`")
    return path


def _branch_rows(state: dict) -> list:
    rows = []
    for name in sorted(state.get("branches", {})):
        branch = state["branches"][name]
        rows.append({
            "name": name,
            "status": branch.get("status", BRANCH_OPEN),
            "base_revision": branch.get("base_revision", 0),
            "created_at": branch.get("created_at", ""),
            "applied_at": branch.get("applied_at", ""),
            "reason": branch.get("stale_reason", ""),
        })
    return rows


def copy_db(src: Path, dst: Path) -> None:
    """Copy a graph database, whether it is a single file or a directory."""
    if src.is_dir():
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)
    for suffix in (".wal", ".lock"):
        sidecar = Path(str(src) + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, Path(str(dst) + suffix))


def remove_db(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()
    for suffix in (".wal", ".lock"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists():
            sidecar.unlink()


def _print_branch_list(state: dict) -> None:
    print(f"revision: {state.get('revision', 0)}  "
          f"active: {state.get('active', BRANCH_MASTER)}")
    rows = _branch_rows(state)
    if not rows:
        print("no branches")
        return
    for row in rows:
        line = (f"{row['name']:<28} {row['status']:<8} "
                f"base r{row['base_revision']}")
        if row["reason"]:
            line += f"  {row['reason']}"
        print(line)


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
    identity = ""
    if kind in FLOW_SCOPED_EDGE_KINDS and props.get("flow_id") is not None:
        # A passage is identified by (kind, from, to, flow): the same pair can
        # participate in several flows.
        identity = " {flow_id:$flow_id}"
    elif kind == "related" and props.get("description"):
        # Two entrypoints may be related in different ways. The description is
        # the identity of the relation, so both directions and variants coexist.
        identity = " {description:$description}"
    base = (f"MATCH (a:{LABELS[src_kind]} {{id:$from}}), "
            f"(b:{LABELS[dst_kind]} {{id:$to}}) MERGE (a)-[r:{RELS[kind]}{identity}]->(b)")
    if props:
        sets = ", ".join(f"r.{key}=${key}" for key in props)
        base += " SET " + sets
    params = {"from": src, "to": dst}
    params.update(props)
    db.run(base, params)


def upsert_contract(db: DB, kind: str, node_id=None, name=None, description=None,
                    attrs=None, refs=None, repo=None, commit_sha=None,
                    file_path=None, cluster=None) -> dict:
    if kind not in CONTRACT_KINDS:
        raise LogisticMapError(f"kind must be one of {CONTRACT_KINDS}")
    attrs = dict(attrs or {})
    unknown = [key for key in attrs if key not in NODE_PROPS[kind]]
    if unknown:
        raise LogisticMapError(f"unknown attributes for {kind}: {', '.join(unknown)}")
    if node_id:
        validate_node_id(node_id, kind)
    else:
        if not name:
            raise LogisticMapError("--name or --id is required")
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
    if cluster is not None:
        props["cluster"] = str(cluster).strip()
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
        raise LogisticMapError(f"field owner must be a contract node, got {owner!r}")
    if not get_node(db, owner_kind, owner):
        raise LogisticMapError(f"owner {owner!r} does not exist; add the contract node first")
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


def upsert_flow(db: DB, node_id=None, name=None, description=None,
                refs=None, repo=None, commit_sha=None, file_path=None,
                cluster=None) -> dict:
    if node_id:
        validate_node_id(node_id, "flow")
    else:
        if not name:
            raise LogisticMapError("--name or --id is required")
        node_id = f"flow:{slugify(name)}"
        validate_node_id(node_id, "flow")
    existing = get_node(db, "flow", node_id) or {}
    props = {}
    for key in NODE_PROPS["flow"]:
        default = [] if key == "refs" else ""
        props[key] = existing.get(key, default)
        if props[key] is None:
            props[key] = default
    props["name"] = name or props.get("name") or node_id.split(":", 1)[1]
    if description is not None:
        props["description"] = description
    if cluster is not None:
        props["cluster"] = str(cluster).strip()
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
    merge_node(db, "flow", node_id, props)
    return {"id": node_id, "created": not existing}


def upsert_entrypoint(db: DB, node_id=None, name=None, description=None,
                      refs=None, repo=None, commit_sha=None, file_path=None,
                      cluster=None) -> dict:
    if node_id:
        validate_node_id(node_id, "entrypoint")
    else:
        if not name:
            raise LogisticMapError("--name or --id is required")
        node_id = f"entrypoint:{slugify(name)}"
        validate_node_id(node_id, "entrypoint")
    existing = get_node(db, "entrypoint", node_id) or {}
    props = {}
    for key in NODE_PROPS["entrypoint"]:
        default = [] if key == "refs" else ""
        props[key] = existing.get(key, default)
        if props[key] is None:
            props[key] = default
    props["name"] = name or props.get("name") or node_id.split(":", 1)[1]
    if description is not None:
        props["description"] = description
    if cluster is not None:
        props["cluster"] = str(cluster).strip()
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
    merge_node(db, "entrypoint", node_id, props)
    return {"id": node_id, "created": not existing}


def upsert_rule(db: DB, node_id=None, name=None, description=None,
                refs=None, repo=None, commit_sha=None, file_path=None,
                cluster=None) -> dict:
    if node_id:
        validate_node_id(node_id, "rule")
    else:
        if not name:
            raise LogisticMapError("--name or --id is required")
        node_id = f"rule:{slugify(name)}"
        validate_node_id(node_id, "rule")
    existing = get_node(db, "rule", node_id) or {}
    props = {}
    for key in NODE_PROPS["rule"]:
        default = [] if key == "refs" else ""
        props[key] = existing.get(key, default)
        if props[key] is None:
            props[key] = default
    props["name"] = name or props.get("name") or node_id.split(":", 1)[1]
    if description is not None:
        props["description"] = description
    if not str(props.get("description") or "").strip():
        raise LogisticMapError("rules need --description explaining the rule")
    if cluster is not None:
        props["cluster"] = str(cluster).strip()
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
    merge_node(db, "rule", node_id, props)
    return {"id": node_id, "created": not existing}


def upsert_external(db: DB, node_id=None, name=None, description=None,
                    provider=None, protocol=None, location=None,
                    refs=None, repo=None, commit_sha=None, file_path=None,
                    cluster=None) -> dict:
    if node_id:
        validate_node_id(node_id, "external")
    else:
        if not name:
            raise LogisticMapError("--name or --id is required")
        node_id = f"external:{slugify(name)}"
        validate_node_id(node_id, "external")
    existing = get_node(db, "external", node_id) or {}
    props = {}
    for key in NODE_PROPS["external"]:
        default = [] if key == "refs" else ""
        props[key] = existing.get(key, default)
        if props[key] is None:
            props[key] = default
    props["name"] = name or props.get("name") or node_id.split(":", 1)[1]
    for key, value in (("description", description), ("provider", provider),
                       ("protocol", protocol), ("location", location)):
        if value is not None:
            props[key] = value
    if cluster is not None:
        props["cluster"] = str(cluster).strip()
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
    merge_node(db, "external", node_id, props)
    return {"id": node_id, "created": not existing}


def upsert_edge(db: DB, kind: str, src: str, dst: str, refs=None,
                file_path=None, note=None, flow_id=None,
                description=None, allow_unassigned: bool = False) -> dict:
    if kind not in RELS:
        raise LogisticMapError(f"edge kind must be one of {tuple(RELS)}")
    src_kind, dst_kind = kind_of(src), kind_of(dst)

    if kind == "use":
        if flow_id:
            raise LogisticMapError("use edges belong to the flow endpoint; omit --flow")
        if src_kind != "flow" or dst_kind not in CONTRACT_KINDS:
            raise LogisticMapError("use edges go from a Flow to an Input, Entry or Storage")
    elif kind == "trigger":
        if flow_id:
            raise LogisticMapError("trigger edges do not use --flow")
        if src_kind != "entrypoint" or dst_kind != "flow":
            raise LogisticMapError("trigger edges go from an EntryPoint to a Flow")
    elif kind == "related":
        if flow_id:
            raise LogisticMapError("related edges do not use --flow")
        if src_kind != "entrypoint" or dst_kind != "entrypoint":
            raise LogisticMapError(
                "related edges go from an EntryPoint to another EntryPoint")
        if not description or not str(description).strip():
            raise LogisticMapError(
                "related edges need --description explaining the connection")
        if not refs:
            raise LogisticMapError(
                "related edges need at least one --ref file:line as evidence")
    elif kind == "rule":
        if flow_id:
            raise LogisticMapError("rule edges do not use --flow")
        if src_kind not in ("flow", "entrypoint") or dst_kind != "rule":
            raise LogisticMapError(
                "rule edges go from a Flow or EntryPoint to a Rule")
    elif kind == "calls":
        if flow_id:
            raise LogisticMapError("calls edges do not use --flow")
        if src_kind != "flow" or dst_kind != "external":
            raise LogisticMapError(
                "calls edges go from a Flow to an External System")
    elif kind == "delivers-to":
        if src_kind != "entry" or dst_kind != "external":
            raise LogisticMapError(
                "delivers-to edges go from an Entry to an External System")
        if not flow_id:
            raise LogisticMapError(
                "delivers-to edges need --flow <flow:id>; the delivery belongs "
                "to a flow")
        validate_node_id(flow_id, "flow")
        if not get_node(db, "flow", flow_id):
            raise LogisticMapError(
                f"flow {flow_id!r} does not exist; run add-flow first")
        _, rows = db.run(
            f"MATCH (f:{LABELS['flow']} {{id:$flow}})-[r:{RELS['use']}]->"
            f"(e:{LABELS['entry']} {{id:$entry}}) RETURN count(r)",
            {"flow": flow_id, "entry": src})
        if not rows or not rows[0][0]:
            raise LogisticMapError(
                f"{flow_id} does not use {src}; run `flow use` first")
    elif kind != "contains":
        if src_kind not in CONTRACT_KINDS or dst_kind not in CONTRACT_KINDS:
            raise LogisticMapError(f"{kind} edges connect contract nodes only")
        if kind in DATA_EDGE_KINDS:
            if not flow_id and not allow_unassigned:
                raise LogisticMapError(
                    f"{kind} edges need --flow <flow:id>; every passage belongs "
                    "to a flow")
            if flow_id:
                validate_node_id(flow_id, "flow")
                if not get_node(db, "flow", flow_id):
                    raise LogisticMapError(
                        f"flow {flow_id!r} does not exist; run add-flow first")
    else:
        if flow_id:
            raise LogisticMapError("contains edges do not belong to a flow")
        allowed = ((src_kind in CONTRACT_KINDS and dst_kind == "field") or
                   (src_kind == "field" and dst_kind == "field"))
        if not allowed:
            raise LogisticMapError("contains edges go from a contract or Field to a Field")
        if src_kind == "field" and owner_of(src) != owner_of(dst):
            raise LogisticMapError("nested contains edges must stay inside the same contract")

    for node_id in (src, dst):
        if not get_node(db, kind_of(node_id), node_id):
            raise LogisticMapError(f"node {node_id!r} does not exist")
    if src == dst:
        raise LogisticMapError("self edges are not allowed")
    if kind != "related" and description not in (None, ""):
        raise LogisticMapError("--description is only used by related edges")

    props = {}
    if kind in FLOW_SCOPED_EDGE_KINDS:
        props["flow_id"] = flow_id or ""
    if kind == "related":
        props["description"] = str(description).strip()
    if refs is not None:
        props["refs"] = refs
    if file_path is not None:
        props["file_path"] = file_path
    if note is not None and kind != "contains":
        props["note"] = note

    query = (f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-[r:{RELS[kind]}]->"
             f"(b:{LABELS[dst_kind]} {{id:$to}}) RETURN r")
    params = {"from": src, "to": dst}
    if kind == "related":
        query = (f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-"
                 f"[r:{RELS[kind]} {{description:$description}}]->"
                 f"(b:{LABELS[dst_kind]} {{id:$to}}) RETURN r")
        params["description"] = props["description"]
    _, rows = db.run(query, params)
    already = [row[0] for row in rows]
    if kind in FLOW_SCOPED_EDGE_KINDS:
        wanted = flow_id or ""
        existing = next((clean(row) for row in already
                         if (clean(row).get("flow_id") or "") == wanted), None)
    else:
        existing = clean(already[0]) if already else None

    if kind in DATA_EDGE_KINDS and flow_id and not existing:
        # Adopt an unassigned passage left by a migration instead of creating
        # a duplicate next to it.
        unassigned = next((clean(row) for row in already
                           if not (clean(row).get("flow_id") or "")), None)
        if unassigned is not None:
            sets = ["r.flow_id=$flow_id"]
            params = {"from": src, "to": dst, "flow_id": flow_id}
            for key, value in props.items():
                if key == "flow_id":
                    continue
                sets.append(f"r.{key}=${key}")
                params[key] = value
            db.run(
                f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-"
                f"[r:{RELS[kind]} {{flow_id:''}}]->"
                f"(b:{LABELS[dst_kind]} {{id:$to}}) SET " + ", ".join(sets),
                params)
            return {"from": src, "to": dst, "kind": kind, "created": False,
                    "adopted": True, "flow_id": flow_id}

    merge_edge(db, kind, src, dst, props)
    result = {"from": src, "to": dst, "kind": kind, "created": not existing}
    if kind in FLOW_SCOPED_EDGE_KINDS:
        result["flow_id"] = flow_id or ""
    if kind == "related":
        result["description"] = props["description"]
    return result


def remove_node(db: DB, node_id: str) -> dict:
    kind = kind_of(node_id)
    if not get_node(db, kind, node_id):
        raise LogisticMapError(f"node {node_id!r} does not exist")
    removed_fields = 0
    removed_edges = 0
    if kind in CONTRACT_KINDS:
        _, rows = db.run("MATCH (f:Field) RETURN f.id")
        owned = [row[0] for row in rows if owner_of(row[0]) == node_id]
        for field_id in owned:
            db.run("MATCH (f:Field {id:$id}) DETACH DELETE f", {"id": field_id})
            removed_fields += 1
    if kind == "entrypoint":
        _, counts = db.run(
            f"MATCH (n:{LABELS['entrypoint']} {{id:$id}})-[r:{RELS['trigger']}]->() "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        _, counts = db.run(
            f"MATCH (n:{LABELS['entrypoint']} {{id:$id}})-[r:{RELS['related']}]->() "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        _, counts = db.run(
            f"MATCH ()-[r:{RELS['related']}]->(n:{LABELS['entrypoint']} {{id:$id}}) "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        _, counts = db.run(
            f"MATCH (n:{LABELS['entrypoint']} {{id:$id}})-[r:{RELS['rule']}]->() "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
    if kind == "flow":
        _, counts = db.run(
            f"MATCH ()-[r:{RELS['flow']} {{flow_id:$id}}]->() RETURN count(r)",
            {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        db.run(f"MATCH ()-[r:{RELS['flow']} {{flow_id:$id}}]->() DELETE r",
               {"id": node_id})
        _, counts = db.run(
            f"MATCH ()-[r:{RELS['transform']} {{flow_id:$id}}]->() RETURN count(r)",
            {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        db.run(f"MATCH ()-[r:{RELS['transform']} {{flow_id:$id}}]->() DELETE r",
               {"id": node_id})
        _, counts = db.run(
            f"MATCH (n:{LABELS['flow']} {{id:$id}})-[r:{RELS['rule']}]->() "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        _, counts = db.run(
            f"MATCH ()-[r:{RELS['delivers-to']} {{flow_id:$id}}]->() RETURN count(r)",
            {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        db.run(f"MATCH ()-[r:{RELS['delivers-to']} {{flow_id:$id}}]->() DELETE r",
               {"id": node_id})
        _, counts = db.run(
            f"MATCH (n:{LABELS['flow']} {{id:$id}})-[r:{RELS['calls']}]->() "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
    if kind == "rule":
        _, counts = db.run(
            f"MATCH ()-[r:{RELS['rule']}]->(n:{LABELS['rule']} {{id:$id}}) "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
    if kind == "external":
        _, counts = db.run(
            f"MATCH ()-[r:{RELS['calls']}]->(n:{LABELS['external']} {{id:$id}}) "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
        _, counts = db.run(
            f"MATCH ()-[r:{RELS['delivers-to']}]->(n:{LABELS['external']} {{id:$id}}) "
            "RETURN count(r)", {"id": node_id})
        removed_edges += counts[0][0] if counts else 0
    db.run(f"MATCH (n:{LABELS[kind]} {{id:$id}}) DETACH DELETE n", {"id": node_id})
    return {"id": node_id, "removed_fields": removed_fields,
            "removed_edges": removed_edges}


def remove_edge(db: DB, kind: str, src: str, dst: str, flow_id=None,
                description=None) -> dict:
    if kind not in RELS:
        raise LogisticMapError(f"edge kind must be one of {tuple(RELS)}")
    src_kind, dst_kind = kind_of(src), kind_of(dst)
    if kind in FLOW_SCOPED_EDGE_KINDS:
        if kind == "delivers-to" and not flow_id:
            raise LogisticMapError(
                "remove-edge --kind delivers-to needs --flow to identify the delivery")
        if flow_id:
            match = f"[r:{RELS[kind]} {{flow_id:$flow_id}}]"
            params = {"from": src, "to": dst, "flow_id": flow_id}
        else:
            # no --flow: target the unassigned passage left by a migration
            match = f"[r:{RELS[kind]} {{flow_id:''}}]"
            params = {"from": src, "to": dst}
    elif kind == "use":
        if flow_id:
            raise LogisticMapError("use edges use the flow in --from; omit --flow")
        match = f"[r:{RELS[kind]}]"
        params = {"from": src, "to": dst}
    elif kind == "related":
        if flow_id:
            raise LogisticMapError("related edges do not use --flow")
        if not description or not str(description).strip():
            raise LogisticMapError(
                "remove-edge --kind related needs --description to identify the relation")
        match = f"[r:{RELS[kind]} {{description:$description}}]"
        params = {"from": src, "to": dst,
                  "description": str(description).strip()}
    else:
        if flow_id:
            raise LogisticMapError(f"{kind} edges do not belong to a flow")
        match = f"[r:{RELS[kind]}]"
        params = {"from": src, "to": dst}
    _, rows = db.run(
        f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-{match}->"
        f"(b:{LABELS[dst_kind]} {{id:$to}}) RETURN count(r)",
        params)
    count = rows[0][0] if rows else 0
    if not count:
        raise LogisticMapError(f"edge {src} -[{kind}]-> {dst} does not exist")
    db.run(
        f"MATCH (a:{LABELS[src_kind]} {{id:$from}})-{match}->"
        f"(b:{LABELS[dst_kind]} {{id:$to}}) DELETE r",
        params)
    result = {"from": src, "to": dst, "kind": kind, "removed": count}
    if kind in FLOW_SCOPED_EDGE_KINDS:
        result["flow_id"] = flow_id
    if kind == "related":
        result["description"] = description
    return result


# --------------------------------------------------------------------------
# reads
# --------------------------------------------------------------------------

def cluster_of(node: dict, nodes: dict) -> str:
    """Resolve the effective cluster; a field inherits it from its owner."""
    cluster = str(node.get("cluster") or "").strip()
    if cluster:
        return cluster
    if node.get("kind") == "field":
        owner = nodes.get(owner_of(node["id"])) or {}
        return str(owner.get("cluster") or "").strip()
    return ""


def load_graph(db: DB):
    nodes = {}
    for kind in ALL_KINDS:
        _, rows = db.run(f"MATCH (n:{LABELS[kind]}) RETURN n")
        for (row,) in rows:
            props = clean(row)
            # Legacy tables (schema v1) may carry columns the model no longer
            # uses; ignore anything outside the current property set.
            props = {key: value for key, value in props.items()
                     if key == "id" or key in NODE_PROPS[kind]}
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
    flows = {nid: node for nid, node in nodes.items() if node["kind"] == "flow"}
    rules = {nid: node for nid, node in nodes.items() if node["kind"] == "rule"}
    externals = {nid: node for nid, node in nodes.items()
                 if node["kind"] == "external"}

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

    # adjacency (USE is flow membership, not a data relation)
    degree = Counter()
    out_by_node = defaultdict(list)
    in_by_node = defaultdict(list)
    uses = defaultdict(set)
    triggers = defaultdict(set)
    relates = defaultdict(set)
    for edge in edges:
        if edge["kind"] == "use":
            uses[edge["from"]].add(edge["to"])
            continue
        if edge["kind"] == "trigger":
            triggers[edge["from"]].add(edge["to"])
            continue
        if edge["kind"] == "related":
            relates[edge["from"]].add(edge["to"])
            continue
        degree[edge["from"]] += 1
        degree[edge["to"]] += 1
        out_by_node[edge["from"]].append(edge)
        in_by_node[edge["to"]].append(edge)
    passages = [edge for edge in edges if edge["kind"] in DATA_EDGE_KINDS]

    # fields per owner
    fields_by_owner = defaultdict(dict)
    for field_id, field in fields.items():
        fields_by_owner[owner_of(field_id)][field.get("path", "")] = field

    def cycles_of(kind: str, flow_id=None):
        adjacency = defaultdict(list)
        for edge in edges:
            if edge["kind"] != kind:
                continue
            if flow_id is not None and (edge.get("flow_id") or "") != flow_id:
                continue
            if edge["from"] in nodes and edge["to"] in nodes:
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
        return cycle_list

    # 1. acyclic structure
    for cycle in cycles_of("compose")[:20]:
        add("compose_cycle", "error",
            "compose cycle: " + " -> ".join(cycle + (cycle[0],)),
            nodes_involved=list(cycle), data={"cycle": list(cycle)})
    for flow_id in sorted(flows):
        for cycle in cycles_of("flow", flow_id)[:20]:
            add("flow_cycle", "warn",
                f"{flow_id}: flow cycle: " + " -> ".join(cycle + (cycle[0],)),
                nodes_involved=list(cycle),
                data={"cycle": list(cycle), "flow_id": flow_id})

    # 2. flow structure
    for edge in sorted(passages, key=lambda item: (item["kind"], item["from"],
                                                   item["to"], item.get("flow_id", ""))):
        flow_id = edge.get("flow_id") or ""
        label = {"kind": edge["kind"], "from": edge["from"], "to": edge["to"]}
        if flow_id:
            label["flow_id"] = flow_id
        if not flow_id:
            add("edge_without_flow", "warn",
                f"{edge['kind']} {edge['from']} -> {edge['to']} belongs to no flow",
                nodes_involved=[edge["from"], edge["to"]], edge=label,
                refs=edge.get("refs") or [])
            continue
        if flow_id not in flows:
            add("unknown_flow", "error",
                f"{edge['kind']} {edge['from']} -> {edge['to']} references "
                f"unknown flow {flow_id}",
                nodes_involved=[edge["from"], edge["to"]], edge=label)
            continue
        for endpoint in (edge["from"], edge["to"]):
            if endpoint not in uses.get(flow_id, set()):
                add("edge_entity_not_used", "warn",
                    f"{flow_id} has a passage through {endpoint} but does not "
                    "USE it",
                    nodes_involved=[endpoint], edge=label)
    for flow_id, flow in sorted(flows.items()):
        if not uses.get(flow_id):
            add("flow_without_entities", "warn",
                f"{flow_id} does not USE any entity",
                nodes_involved=[flow_id], refs=flow.get("refs") or [])
        if not any((edge.get("flow_id") or "") == flow_id for edge in passages):
            add("flow_without_path", "info",
                f"{flow_id} has no flow/transform passage",
                nodes_involved=[flow_id], refs=flow.get("refs") or [])
        if not flow.get("file_path") and not flow.get("refs"):
            add("incomplete_flow", "info",
                f"{flow_id} has neither file_path nor refs",
                nodes_involved=[flow_id])

    entrypoints = {nid: node for nid, node in nodes.items()
                   if node["kind"] == "entrypoint"}

    def reaches_trigger(start: str) -> bool:
        stack, seen = [start], set()
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            if triggers.get(current):
                return True
            stack.extend(relates.get(current, ()))
        return False

    for entrypoint_id, entrypoint in sorted(entrypoints.items()):
        if not reaches_trigger(entrypoint_id):
            add("entrypoint_without_flow", "warn",
                f"{entrypoint_id} does not TRIGGER a flow, directly or "
                "through related entrypoints",
                nodes_involved=[entrypoint_id], refs=entrypoint.get("refs") or [])
        if not entrypoint.get("file_path") and not entrypoint.get("refs"):
            add("incomplete_entrypoint", "info",
                f"{entrypoint_id} has neither file_path nor refs",
                nodes_involved=[entrypoint_id])
    for edge in (item for item in edges if item["kind"] == "trigger"):
        if edge["to"] not in flows:
            add("unknown_trigger_flow", "error",
                f"{edge['from']} triggers unknown flow {edge['to']}",
                nodes_involved=[edge["from"], edge["to"]], edge=edge)
    for flow_id, flow in sorted(flows.items()):
        if not any(flow_id in destinations for destinations in triggers.values()):
            add("flow_without_trigger", "info",
                f"{flow_id} is not triggered by any EntryPoint",
                nodes_involved=[flow_id], refs=flow.get("refs") or [])

    rule_targets = {edge["to"] for edge in edges if edge["kind"] == "rule"}
    for rule_id, rule in sorted(rules.items()):
        if not str(rule.get("description") or "").strip():
            add("rule_without_description", "warn",
                f"{rule_id} has no description explaining the rule",
                nodes_involved=[rule_id], refs=rule.get("refs") or [])
        if rule_id not in rule_targets:
            add("orphan_rule", "warn",
                f"{rule_id} is not attached to any Flow or EntryPoint",
                nodes_involved=[rule_id], refs=rule.get("refs") or [])
        if not rule.get("file_path") and not rule.get("refs"):
            add("incomplete_rule", "info",
                f"{rule_id} has neither file_path nor refs",
                nodes_involved=[rule_id])

    for edge in (item for item in edges if item["kind"] == "delivers-to"):
        flow_id = edge.get("flow_id") or ""
        label = {"kind": edge["kind"], "from": edge["from"],
                 "to": edge["to"], "flow_id": flow_id}
        if not flow_id:
            add("delivery_without_flow", "error",
                f"delivers-to {edge['from']} -> {edge['to']} has no flow_id",
                nodes_involved=[edge["from"], edge["to"]], edge=label,
                refs=edge.get("refs") or [])
            continue
        if flow_id not in flows:
            add("unknown_flow", "error",
                f"delivers-to {edge['from']} -> {edge['to']} references "
                f"unknown flow {flow_id}",
                nodes_involved=[edge["from"], edge["to"]], edge=label)
            continue
        if edge["from"] not in uses.get(flow_id, set()):
            add("edge_entity_not_used", "warn",
                f"{flow_id} delivers {edge['from']} to {edge['to']} but does "
                "not USE it", nodes_involved=[edge["from"]], edge=label)

    external_targets = {edge["to"] for edge in edges
                        if edge["kind"] in ("calls", "delivers-to")}
    for external_id, external in sorted(externals.items()):
        if external_id not in external_targets:
            add("orphan_external", "warn",
                f"{external_id} is not called by any Flow or delivered to",
                nodes_involved=[external_id], refs=external.get("refs") or [])
        if not external.get("file_path") and not external.get("refs"):
            add("incomplete_external", "info",
                f"{external_id} has neither file_path nor refs",
                nodes_involved=[external_id])

    for edge in (item for item in edges if item["kind"] == "trigger"):
        src_cluster = cluster_of(nodes.get(edge["from"], {}), nodes)
        dst_cluster = cluster_of(nodes.get(edge["to"], {}), nodes)
        if src_cluster and dst_cluster and src_cluster != dst_cluster:
            add("cluster_mismatch", "warn",
                f"{edge['from']} is in cluster {src_cluster!r} but its flow "
                f"{edge['to']} is in {dst_cluster!r}",
                nodes_involved=[edge["from"], edge["to"]],
                edge={"kind": edge["kind"], "from": edge["from"], "to": edge["to"]})

    # 2. contract completeness
    for node_id, node in sorted(contracts.items()):
        missing = []
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

    # 4. data shape
    for node_id, node in sorted(contracts.items()):
        if degree[node_id] == 0:
            add("isolated_contract", "warn",
                f"{node_id} is not connected to any other node",
                nodes_involved=[node_id], refs=node.get("refs") or [])
            continue
        upstream = [edge for edge in in_by_node[node_id]
                    if edge["kind"] in DATA_EDGE_KINDS]
        downstream = [edge for edge in out_by_node[node_id]
                      if edge["kind"] in DATA_EDGE_KINDS]
        if node["kind"] == "input" and not downstream:
            add("unconsumed_input", "warn",
                f"{node_id} has no downstream flow/transform edge",
                nodes_involved=[node_id], refs=node.get("refs") or [])
        if node["kind"] == "storage" and not upstream:
            add("unproduced_storage", "warn",
                f"{node_id} has no upstream flow/transform edge",
                nodes_involved=[node_id], refs=node.get("refs") or [])
        if node["kind"] == "entry" and upstream and not downstream:
            add("entry_dead_end", "info",
                f"{node_id} receives data but has no downstream edge",
                nodes_involved=[node_id], refs=node.get("refs") or [])

    # 5. transformations must be documented
    for edge in sorted(edges, key=lambda item: (item["from"], item["to"])):
        if edge["kind"] != "transform":
            continue
        if edge.get("note") or edge.get("refs") or edge.get("file_path"):
            continue
        add("transform_without_note", "warn",
            f"{edge['from']} -> {edge['to']} claims a transformation but has "
            "no note, file or ref explaining it",
            nodes_involved=[edge["from"], edge["to"]],
            edge={"kind": edge["kind"], "from": edge["from"], "to": edge["to"]})

    # 6. contract diffs along flow edges (data passes without changing shape)
    seen_pairs = set()
    for edge in sorted(edges, key=lambda item: (item["from"], item["to"])):
        if edge["kind"] != "flow":
            continue
        pair = (edge["from"], edge["to"])
        if pair in seen_pairs:
            # the same passage may occur in several flows; report it once
            continue
        seen_pairs.add(pair)
        src_fields = fields_by_owner.get(edge["from"], {})
        dst_fields = fields_by_owner.get(edge["to"], {})
        if not src_fields or not dst_fields:
            continue
        dst_kind = nodes.get(edge["to"], {}).get("kind")
        severity = "warn" if dst_kind == "entry" else "info"
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

    # 7. optional filesystem checks (one finding per missing path)
    if check_files:
        seen_paths = {}
        for node_id, node in sorted(list(contracts.items()) + list(fields.items())
                                    + list(flows.items())):
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
            "flows": len(flows),
            "rules": len(rules),
            "externals": len(externals),
            "fields": len(fields),
            "edges": len(edges),
            "clusters": sorted({cluster for cluster in
                                (cluster_of(node, nodes) for node in nodes.values())
                                if cluster}),
            "findings": {key: by_severity.get(key, 0) for key in ("error", "warn", "info")},
            "by_code": dict(sorted(by_code.items())),
        },
        "findings": findings,
    }


def format_report_text(report: dict) -> str:
    summary = report["summary"]
    lines = [
        f"Logistic map: {summary['contracts']} contracts, "
        f"{summary.get('flows', 0)} flows, {summary.get('rules', 0)} rules, "
        f"{summary.get('externals', 0)} external systems, "
        f"{summary['fields']} fields, "
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
        f"- Flows: **{summary.get('flows', 0)}**",
        f"- Rules: **{summary.get('rules', 0)}**",
        f"- External Systems: **{summary.get('externals', 0)}**",
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
        raise LogisticMapError(f"visualization template not found: {template_path}")
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

def write_meta(db: DB, version: int | None = None) -> None:
    ladybug_version = ""
    try:
        from importlib.metadata import version as package_version
        ladybug_version = package_version("ladybug")
    except Exception:
        pass
    meta = {
        "schema_version": SCHEMA_VERSION if version is None else version,
        "updated_at": now_iso(),
        "ladybug": ladybug_version,
        "tool": "logistic-map",
    }
    db.meta_path().write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")


def ensure_gitignore(db: DB) -> None:
    parent = db.path.parent
    gitignore = parent / ".gitignore"
    if gitignore.exists():
        return
    gitignore.write_text(
        "# LadybugDB database, venv and generated artifacts\n"
        "*\n"
        "!.gitignore\n"
        "!exports/\n"
        "!exports/**\n",
        encoding="utf-8")


def cmd_init(args) -> int:
    db = open_db(args, initialize=True)
    tables = db.tables()
    if db.meta_path().exists():
        existing = db_schema_version(db) or 1
    else:
        existing = SCHEMA_VERSION if not tables else 1
    if existing == SCHEMA_VERSION:
        for statement in DDL:
            db.run(statement)
        tables = db.tables()
    write_meta(db, version=existing)
    ensure_gitignore(db)
    tables = sorted(tables)
    version = existing
    payload = {"ok": True, "db": str(db.path), "schema_version": version,
               "tables": tables}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"initialized {db.path} (schema v{version})")
        print("tables: " + ", ".join(tables))
        if version != SCHEMA_VERSION:
            print(f"notice: this database still uses schema v{version}; "
                  "run `migrate` to convert it to "
                  f"v{SCHEMA_VERSION}")
        print(f"next: {db.path.parent}/exports/ for git-friendly snapshots")
    return 0


def _table_columns(db: DB, table: str) -> set:
    if table not in db.tables():
        return set()
    try:
        return {row[1] for row in db.run(f"CALL table_info('{table}') RETURN *")[1]}
    except Exception:
        return set()


def _copy_data_edges(db: DB, table: str, kind: str) -> int:
    _, rows = db.run(
        f"MATCH (a)-[r:{table}]->(b) "
        "RETURN a.id, b.id, r.refs, r.file_path, r.note")
    for src, dst, refs, file_path, note in rows:
        props = {"flow_id": ""}
        if refs:
            props["refs"] = refs
        if file_path:
            props["file_path"] = file_path
        if note:
            props["note"] = note
        merge_edge(db, kind, src, dst, props)
    return len(rows)


def _ensure_data_rel(db: DB, table: str, legacy: list, ddl: str,
                     kind: str) -> dict:
    """Create the v3 rel table and preserve rows as unassigned passages."""
    tables = db.tables()
    changed = False
    copied = 0
    if table not in tables:
        db.run(ddl)
        changed = True
    for old in legacy:
        if old in db.tables():
            copied += _copy_data_edges(db, old, kind)
            db.run(f"DROP TABLE {old}")
            changed = True
    if "flow_id" not in _table_columns(db, table):
        old = f"{table}_V2"
        if old not in db.tables():
            db.run(f"ALTER TABLE {table} RENAME TO {old}")
        db.run(ddl)
        copied += _copy_data_edges(db, old, kind)
        db.run(f"DROP TABLE {old}")
        changed = True
    return {"changed": changed, "copied": copied}


def migrate_db(db: DB) -> dict:
    """Convert a v1-v7 database to v8 in place (idempotent)."""
    before = db_schema_version(db)
    actions = []
    moved = 0
    if before == SCHEMA_VERSION:
        for statement in DDL:
            db.run(statement)
        write_meta(db)
        return {"before": before, "after": SCHEMA_VERSION, "actions": actions,
                "compose_to_flow": 0, "rebuilt": {}}

    # The v2 FLOW rel table collides with the new Flow node table in the
    # case-insensitive binding. Move it aside before creating USES.
    tables = db.tables()
    if "FLOW" in tables:
        if "FLOW_V2" in tables:
            raise LogisticMapError(
                "stale FLOW_V2 table found; inspect the database before migrating")
        db.run("ALTER TABLE FLOW RENAME TO FLOW_V2")
        actions.append("moved legacy FLOW table aside")

    for statement in DDL:
        db.run(statement)
    if before < 8 and "External" not in tables:
        actions.append("added External System and CALLS/DELIVERS_TO tables")

    for table in ("Input", "Entry", "Storage", "Flow", "EntryPoint"):
        if table not in db.tables():
            continue
        if "cluster" not in _table_columns(db, table):
            db.run(f"ALTER TABLE {table} ADD cluster STRING DEFAULT ''")
            actions.append(f"added {table}.cluster")

    columns = _table_columns(db, "Entry")
    for legacy in ("entry_point", "entry_type"):
        if legacy not in columns:
            continue
        db.run(f"ALTER TABLE Entry DROP {legacy}")
        actions.append(f"dropped Entry.{legacy}")

    input_columns = _table_columns(db, "Input")
    for legacy in ("entry_point", "channel"):
        if legacy in input_columns:
            db.run(f"ALTER TABLE Input DROP {legacy}")
            actions.append(f"dropped Input.{legacy}")

    rebuilt = {}
    if before <= 2:
        for table, ddl, kind, legacy in (
                ("FLOWS", FLOW_REL_DDL, "flow", ["FLOW_V2"]),
                ("TRANSFORM", TRANSFORM_REL_DDL, "transform", [])):
            result = _ensure_data_rel(db, table, legacy, ddl, kind)
            if result["changed"]:
                rebuilt[table] = result["copied"]
                actions.append(
                    f"prepared {table} with flow_id "
                    f"({result['copied']} unassigned passage(s) preserved)")

    if before <= 1:
        _, rows = db.run(
            "MATCH (a)-[r:COMPOSE]->(b) "
            "RETURN a.id, b.id, r.refs, r.file_path, r.note")
        for src, dst, refs, file_path, note in rows:
            props = {"flow_id": ""}
            if refs:
                props["refs"] = refs
            if file_path:
                props["file_path"] = file_path
            if note:
                props["note"] = note
            merge_edge(db, "flow", src, dst, props)
            moved += 1
        if moved:
            # In v1 COMPOSE meant "pipeline"; keep the path as unassigned
            # passages so report can ask which flow they belong to.
            db.run("MATCH (a)-[r:COMPOSE]->(b) DELETE r")
            actions.append(f"converted {moved} compose edge(s) to flow passages")

    write_meta(db)
    return {"before": before, "after": SCHEMA_VERSION, "actions": actions,
            "compose_to_flow": moved, "rebuilt": rebuilt}


LEGACY_EXPORT_CODE = r"""
import json, sys
sys.path.insert(0, sys.argv[2])
import logistic_map as lm
db = lm.KuzuDB(sys.argv[1])
try:
    nodes, edges = lm.load_graph(db)
finally:
    db.close()
payload = {
    "schema_version": lm.SCHEMA_VERSION,
    "nodes": sorted(nodes.values(), key=lambda item: (item["kind"], item["id"])),
    "edges": edges,
}
print(json.dumps(payload))
"""


def read_legacy_kuzu_payload(source: Path) -> dict:
    """Read a Kuzu database through a subprocess.

    Importing kuzu and ladybug in the same process can clash in their native
    type registration, so the legacy engine is loaded in a child process.
    """
    meta_path = source.parent / "meta.json"
    if meta_path.exists():
        try:
            version = int(json.loads(meta_path.read_text(encoding="utf-8"))
                          .get("schema_version", 0))
        except (json.JSONDecodeError, OSError, TypeError, ValueError):
            version = 0
        if version and version != SCHEMA_VERSION:
            raise LogisticMapError(
                f"{source} uses schema v{version}; convert it to "
                f"v{SCHEMA_VERSION} with the previous release of the CLI, "
                "then run `migrate-db` again")
    proc = subprocess.run(
        [sys.executable, "-c", LEGACY_EXPORT_CODE, str(source),
         str(Path(__file__).resolve().parent)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        raise LogisticMapError(
            "could not read the legacy Kuzu database: " + (detail or "unknown error"))
    return json.loads(proc.stdout)


def read_migration_payload(source: Path) -> dict:
    if source.suffix == ".json":
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise LogisticMapError(f"invalid JSON in {source}: {exc}") from exc
        if not isinstance(payload, dict):
            raise LogisticMapError("snapshot must be a JSON object")
        return payload
    return read_legacy_kuzu_payload(source)


def import_graph_payload(db: DB, payload: dict) -> dict:
    """Rebuild a graph from an exported payload without upsert heuristics."""
    nodes = payload.get("nodes", [])
    edges = payload.get("edges", [])
    db.run("BEGIN TRANSACTION")
    try:
        for item in nodes:
            kind = item.get("kind")
            if kind not in LABELS:
                raise LogisticMapError(
                    f"invalid node kind in migration: {kind!r}")
            if not item.get("id"):
                raise LogisticMapError("every migrated node needs an id")
            props = dict(item)
            props["refs"] = props.get("refs") or []
            if kind == "field" and not props.get("path") and "#" in item["id"]:
                props["path"] = item["id"].split("#", 1)[1]
            merge_node(db, kind, item["id"], props)
        for edge in edges:
            kind = edge.get("kind")
            if kind not in RELS:
                raise LogisticMapError(
                    f"invalid edge kind in migration: {kind!r}")
            props = {"refs": edge.get("refs") or []}
            for key in ("flow_id", "description", "file_path", "note"):
                value = edge.get(key)
                if value not in (None, ""):
                    props[key] = value
            merge_edge(db, kind, edge["from"], edge["to"], props)
        db.run("COMMIT")
    except Exception:
        try:
            db.run("ROLLBACK")
        except Exception:
            pass
        raise
    return {"nodes": len(nodes), "edges": len(edges)}


def cmd_migrate_db(args) -> int:
    source = Path(args.source).expanduser()
    target = Path(args.to or args.db).expanduser()
    if source == target:
        raise LogisticMapError("--from and --to must be different databases")
    if not source.exists():
        raise LogisticMapError(f"source database not found: {source}")
    if target.exists():
        if not args.force:
            raise LogisticMapError(
                f"{target} already exists; pass --force to replace it")
        state_path = branch_state_path(target)
        if state_path.exists():
            for name in read_branch_state(target).get("branches", {}):
                remove_db(branch_db_path(target, name))
            state_path.unlink()
        remove_db(target)
    payload = read_migration_payload(source)
    db = DB(target)
    try:
        for statement in DDL:
            db.run(statement)
        write_meta(db)
        ensure_gitignore(db)
        imported = import_graph_payload(db, payload)
        nodes, edges = load_graph(db)
        if len(nodes) < imported["nodes"]:
            raise LogisticMapError(
                f"migration wrote {len(nodes)} of {imported['nodes']} nodes")
        counts = {"nodes": len(nodes), "edges": len(edges)}
    finally:
        db.close()
    dropped = []
    state_path = branch_state_path(source)
    if state_path.exists():
        try:
            state = read_branch_state(source)
            dropped = sorted(state.get("branches", {}))
        except Exception:
            dropped = []
    result = {"ok": True, "from": str(source), "to": str(target),
              "nodes": counts["nodes"], "edges": counts["edges"],
              "dropped_branches": dropped}
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"migrated {source} -> {target} "
              f"({counts['nodes']} nodes, {counts['edges']} edges)")
        if dropped:
            print("branches were not migrated: " + ", ".join(dropped))
            print("recreate them from the new master with `branch create`")
    return 0


def cmd_migrate(args) -> int:
    db = open_db(args, initialize=True, allow_old=True)
    result = migrate_db(db)
    legacy_flows = [db.path.parent / name
                    for name in ("flows.lbdb", "flows.kuzu")
                    if (db.path.parent / name).exists()]
    result["legacy_flow_db"] = str(legacy_flows[0]) if legacy_flows else ""
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        if result["before"] == SCHEMA_VERSION and not result["actions"]:
            print(f"already at schema v{SCHEMA_VERSION}")
        else:
            print(f"migrated {db.path}: schema v{result['before'] or '?'} -> "
                  f"v{result['after']}")
            for action in result["actions"]:
                print(f"  {action}")
            if result["compose_to_flow"] or result["rebuilt"]:
                print("note: flows are now authored in the main graph; run "
                      "`report` for passages without a flow")
        if result["legacy_flow_db"]:
            print(f"note: {legacy_flow_db} is no longer used; flows live in "
                  "the main graph")
    return 0


def cmd_doctor(args) -> int:
    info = {"python": sys.version.split()[0], "executable": sys.executable}
    try:
        from importlib.metadata import version
        info["ladybug"] = version("ladybug")
    except Exception:
        info["ladybug"] = ""
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
        print(f"ladybug: {info['ladybug'] or 'MISSING'}")
        print(f"db:      {info['db']} ({'found' if info['exists'] else 'missing'})")
        print(f"git:     {info['git_repo'] or '-'} @ {info['git_commit'] or '-'}")
        if info.get("tables"):
            print("tables:  " + ", ".join(info["tables"]))
        if info.get("nodes") is not None:
            print(f"graph:   {info['nodes']} nodes, {info['edges']} edges")
    return 0 if info["ladybug"] else 1


def cmd_add_node(args) -> int:
    if args.kind not in CONTRACT_KINDS:
        raise LogisticMapError("kind must be input, entry or storage")
    attrs = {}
    for key in ("location", "store_type", "stage"):
        if key not in NODE_PROPS[args.kind]:
            continue
        value = getattr(args, key)
        if value is not None:
            attrs[key] = value
    db = open_db(args)
    result = upsert_contract(
        db, args.kind, node_id=args.id, name=args.name,
        description=args.description, attrs=attrs,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        repo=args.repo, commit_sha=args.commit, file_path=args.file,
        cluster=args.cluster)
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


def cmd_add_flow(args) -> int:
    db = open_db(args)
    result = upsert_flow(
        db, node_id=args.id, name=args.name, description=args.description,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        repo=args.repo, commit_sha=args.commit, file_path=args.file,
        cluster=args.cluster)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{'created' if result['created'] else 'updated'} {result['id']}")
    return 0


def cmd_add_entrypoint(args) -> int:
    db = open_db(args)
    result = upsert_entrypoint(
        db, node_id=args.id, name=args.name, description=args.description,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        repo=args.repo, commit_sha=args.commit, file_path=args.file,
        cluster=args.cluster)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{'created' if result['created'] else 'updated'} {result['id']}")
    return 0


def cmd_add_rule(args) -> int:
    db = open_db(args)
    result = upsert_rule(
        db, node_id=args.id, name=args.name, description=args.description,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        repo=args.repo, commit_sha=args.commit, file_path=args.file,
        cluster=args.cluster)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{'created' if result['created'] else 'updated'} {result['id']}")
    return 0


def cmd_add_external(args) -> int:
    db = open_db(args)
    result = upsert_external(
        db, node_id=args.id, name=args.name, description=args.description,
        provider=args.provider, protocol=args.protocol, location=args.location,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        repo=args.repo, commit_sha=args.commit, file_path=args.file,
        cluster=args.cluster)
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
        file_path=args.file, note=args.note, flow_id=args.flow,
        description=args.description)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        label = "created" if result["created"] else "updated"
        suffix = f" in {args.flow}" if args.flow else ""
        print(f"{label} {args.src} -[{args.kind}]-> {args.dst}{suffix}")
    return 0


def cmd_remove_node(args) -> int:
    db = open_db(args)
    result = remove_node(db, args.id)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        extras = []
        if result["removed_fields"]:
            extras.append(f"+{result['removed_fields']} fields")
        if result.get("removed_edges"):
            extras.append(f"+{result['removed_edges']} edges")
        extra = f" ({', '.join(extras)})" if extras else ""
        print(f"removed {result['id']}{extra}")
    return 0


def cmd_remove_edge(args) -> int:
    db = open_db(args)
    result = remove_edge(db, args.kind, args.src, args.dst, flow_id=args.flow,
                         description=args.description)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        suffix = f" in {args.flow}" if args.flow else ""
        print(f"removed {args.src} -[{args.kind}]-> {args.dst}{suffix}")
    return 0


def cmd_apply(args) -> int:
    path = Path(args.file)
    if not path.exists():
        raise LogisticMapError(f"file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise LogisticMapError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise LogisticMapError("apply expects a JSON object with nodes/fields/edges")

    db = open_db(args)
    counts = {"nodes": 0, "flows": 0, "entrypoints": 0, "rules": 0,
              "externals": 0, "fields": 0, "edges": 0}
    db.run("BEGIN TRANSACTION")
    try:
        node_items = payload.get("nodes", [])
        # contracts and flows first: field owners and passages need them
        for item in node_items:
            kind = item.get("kind")
            if kind == "field":
                continue
            if kind == "flow":
                upsert_flow(
                    db, node_id=item.get("id"), name=item.get("name"),
                    description=item.get("description"), refs=item.get("refs"),
                    repo=item.get("repo"),
                    commit_sha=item.get("commit_sha") or item.get("commit"),
                    file_path=item.get("file_path") or item.get("file"),
                    cluster=item.get("cluster"))
                counts["flows"] += 1
                continue
            if kind == "entrypoint":
                upsert_entrypoint(
                    db, node_id=item.get("id"), name=item.get("name"),
                    description=item.get("description"), refs=item.get("refs"),
                    repo=item.get("repo"),
                    commit_sha=item.get("commit_sha") or item.get("commit"),
                    file_path=item.get("file_path") or item.get("file"),
                    cluster=item.get("cluster"))
                counts["entrypoints"] += 1
                continue
            if kind == "rule":
                upsert_rule(
                    db, node_id=item.get("id"), name=item.get("name"),
                    description=item.get("description"), refs=item.get("refs"),
                    repo=item.get("repo"),
                    commit_sha=item.get("commit_sha") or item.get("commit"),
                    file_path=item.get("file_path") or item.get("file"),
                    cluster=item.get("cluster"))
                counts["rules"] += 1
                continue
            if kind == "external":
                upsert_external(
                    db, node_id=item.get("id"), name=item.get("name"),
                    description=item.get("description"),
                    provider=item.get("provider"), protocol=item.get("protocol"),
                    location=item.get("location"), refs=item.get("refs"),
                    repo=item.get("repo"),
                    commit_sha=item.get("commit_sha") or item.get("commit"),
                    file_path=item.get("file_path") or item.get("file"),
                    cluster=item.get("cluster"))
                counts["externals"] += 1
                continue
            if kind not in CONTRACT_KINDS:
                raise LogisticMapError(f"invalid node kind in apply: {kind!r}")
            attrs = {key: item.get(key) for key in
                     ("location", "store_type", "stage")
                     if key in NODE_PROPS[kind] and item.get(key) is not None}
            upsert_contract(
                db, kind, node_id=item.get("id"), name=item.get("name"),
                description=item.get("description"), attrs=attrs,
                refs=item.get("refs"), repo=item.get("repo"),
                commit_sha=item.get("commit_sha") or item.get("commit"),
                file_path=item.get("file_path") or item.get("file"),
                cluster=item.get("cluster"))
            counts["nodes"] += 1
        for item in node_items:
            if item.get("kind") != "field":
                continue
            field_id = item.get("id") or ""
            owner = item.get("owner") or owner_of(field_id)
            path = item.get("path") or (
                field_id.split("#", 1)[1] if "#" in field_id else "")
            if not owner or not path:
                raise LogisticMapError("field nodes need owner and path")
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
                raise LogisticMapError("each field needs owner and path")
            upsert_field(
                db, item["owner"], item["path"], name=item.get("name"),
                field_type=item.get("type"), description=item.get("description"),
                required=item.get("required") if "required" in item else None,
                refs=item.get("refs"), repo=item.get("repo"),
                commit_sha=item.get("commit_sha") or item.get("commit"),
                file_path=item.get("file_path") or item.get("file"))
            counts["fields"] += 1
        # USE first: delivers-to validates that the entry belongs to its flow.
        edge_items = sorted(
            payload.get("edges", []),
            key=lambda item: 0 if item.get("kind") == "use" else 1)
        for item in edge_items:
            if not item.get("kind") or not item.get("from") or not item.get("to"):
                raise LogisticMapError("each edge needs kind, from and to")
            upsert_edge(
                db, item["kind"], item["from"], item["to"],
                refs=item.get("refs"), file_path=item.get("file_path"),
                note=item.get("note"), flow_id=item.get("flow_id"),
                description=item.get("description"),
                allow_unassigned=True)
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
        print(f"applied {counts['nodes']} nodes, {counts['flows']} flows, "
              f"{counts['entrypoints']} entrypoints, {counts['rules']} rules, "
              f"{counts['externals']} external systems, {counts['fields']} fields, "
              f"{counts['edges']} edges")
    return 0


def cmd_list(args) -> int:
    db = open_db(args)
    if args.kind and args.kind not in ALL_KINDS:
        raise LogisticMapError(f"kind must be one of {ALL_KINDS}")
    nodes, _ = load_graph(db)
    selected = []
    for node_id, node in nodes.items():
        if args.kind and node["kind"] != args.kind:
            continue
        if args.owner and owner_of(node_id) != args.owner:
            continue
        if args.cluster and cluster_of(node, nodes) != args.cluster:
            continue
        if args.search:
            haystack = " ".join(str(node.get(key, "")) for key in
                                ("id", "name", "path", "location", "stage",
                                 "cluster", "description"))
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
        elif node["kind"] == "entry":
            detail = node.get("stage") or "-"
        elif node["kind"] in ("flow", "entrypoint", "rule", "external"):
            detail = node.get("description") or "-"
        else:
            detail = "-"
        suffix = f"  [{node.get('file_path')}]" if node.get("file_path") else ""
        cluster = cluster_of(node, nodes)
        suffix += f"  cluster:{cluster}" if cluster else ""
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
        raise LogisticMapError(
            f"query is read-only; {match.group(1).upper()} is not allowed")


def cmd_query(args) -> int:
    assert_read_only(args.cypher)
    db = open_db(args)
    params = {}
    for item in args.param or []:
        if "=" not in item:
            raise LogisticMapError(f"invalid --param {item!r}; use key=json")
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


# --------------------------------------------------------------------------
# markdown docs
# --------------------------------------------------------------------------

def _md_cell(value) -> str:
    text = " ".join(str(value).split())
    return text.replace("|", "\\|")


def _md_code(value) -> str:
    text = str(value)
    if "`" in text:
        return f"`` {text} ``"
    return f"`{text}`"


def _count(count: int, singular: str, plural: str | None = None) -> str:
    return f"{count} {singular if count == 1 else (plural or singular + 's')}"


def _doc_slug(prefix: str, name: str, used: set) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", str(name)).strip("-") or "unnamed"
    base = f"{prefix}/{safe}" if prefix else safe
    slug, index = base, 2
    while slug in used:
        slug = f"{base}-{index}"
        index += 1
    used.add(slug)
    return slug


def _entity_link(contract_id: str, slug_of: dict, from_dir: str = "entities") -> str:
    slug = slug_of.get(contract_id)
    if not slug:
        return _md_code(contract_id)
    prefix = "../" if from_dir == "entities" else "../entities/"
    return f"[{_md_code(contract_id)}]({prefix}{slug}.md)"


def _kind_summary(node: dict) -> str:
    kind = node["kind"]
    if kind == "entry":
        bits = [node.get("stage")]
    elif kind == "storage":
        bits = [node.get("store_type"), node.get("location")]
    else:
        bits = []
    return " · ".join(_md_code(bit) for bit in bits if bit)


def _relation_table(rows, slug_of, from_dir: str) -> list:
    if not rows:
        return ["_None._", ""]
    lines = ["| Edge | Entity | Flow | Note |", "|---|---|---|---|"]
    for edge in rows:
        other = edge["_other"]
        note = _md_cell(edge.get("note") or "")
        flow_id = edge.get("flow_id") or ""
        flow = _md_code(flow_id) if flow_id else "—"
        lines.append(
            f"| {_md_code(edge['kind'])} | {_entity_link(other, slug_of, from_dir)} "
            f"| {flow} | {note} |")
    lines.append("")
    return lines


def _entity_markdown(node: dict, fields: list, edges: list, flows: list,
                     slug_of: dict, flow_slug_of: dict, from_dir: str = "entities",
                     deliveries=None, external_slug_of=None) -> str:
    node_id = node["id"]
    title = node.get("name") or node_id.split(":", 1)[-1]
    lines = [f"# {_md_code(node_id)}", "",
             f"**{LABELS[node['kind']]}** — {title}", ""]
    if node.get("description"):
        lines += [_md_cell(node["description"]), ""]

    attributes = []
    for key, label in (("stage", "stage"), ("location", "location"),
                       ("store_type", "store type"), ("cluster", "cluster")):
        if node.get(key):
            attributes.append((label, _md_code(node[key])))
    for key, label in (("repo", "repo"), ("commit_sha", "commit"),
                       ("file_path", "file")):
        if node.get(key):
            attributes.append((label, _md_code(node[key])))
    if attributes:
        lines += ["| Attribute | Value |", "|---|---|"]
        lines += [f"| {label} | {value} |" for label, value in attributes]
        lines.append("")

    lines += [f"## Fields ({len(fields)})", ""]
    if fields:
        lines += ["| Field | Type | Required | Source | References |",
                  "|---|---|---|---|---|"]
        for field in sorted(fields, key=lambda item: item.get("path", "")):
            refs = ", ".join(_md_code(ref) for ref in (field.get("refs") or []))
            lines.append(
                f"| {_md_code(field.get('path', ''))} "
                f"| {_md_code(field.get('type') or '—')} "
                f"| {'yes' if field.get('required') else ''} "
                f"| {_md_cell(field.get('source') or '')} | {refs} |")
        lines.append("")
    else:
        lines += ["No fields registered. That means the contract was not mapped "
                  "yet, not that the payload is empty.", ""]

    incoming = [dict(edge, _other=edge["from"]) for edge in edges
                if edge["to"] == node_id and edge["kind"] in DATA_EDGE_KINDS]
    outgoing = [dict(edge, _other=edge["to"]) for edge in edges
                if edge["from"] == node_id and edge["kind"] in DATA_EDGE_KINDS]
    lines += ["## Data entering", ""]
    lines += _relation_table(incoming, slug_of, from_dir)
    lines += ["## Data leaving", ""]
    lines += _relation_table(outgoing, slug_of, from_dir)

    whole_of = [dict(edge, _other=edge["from"]) for edge in edges
                if edge["to"] == node_id and edge["kind"] == "compose"]
    composed_of = [dict(edge, _other=edge["to"]) for edge in edges
                   if edge["from"] == node_id and edge["kind"] == "compose"]
    lines += ["## Composition", ""]
    if whole_of:
        lines += ["Part of: " + ", ".join(
            _entity_link(edge["_other"], slug_of, from_dir) for edge in whole_of), ""]
    if composed_of:
        lines += ["Composed of: " + ", ".join(
            _entity_link(edge["_other"], slug_of, from_dir) for edge in composed_of), ""]
    if not whole_of and not composed_of:
        lines += ["_No composition registered._", ""]

    if node.get("refs"):
        lines += ["## Evidence", "",
                  "References: " + ", ".join(_md_code(ref) for ref in node["refs"]), ""]

    if flows:
        lines += ["## Flows", ""]
        for flow in flows:
            flow_slug = flow_slug_of.get(flow["id"])
            label = flow.get("name") or flow["id"]
            target = f"../../flows/{flow_slug}.md" if flow_slug else None
            if target:
                lines.append(f"- [{_md_cell(label)}]({target})")
            else:
                lines.append(f"- {_md_cell(label)}")
        lines.append("")

    if deliveries:
        lines += ["## Delivers to", ""]
        for item in deliveries:
            external = item["external"]
            slug = (external_slug_of or {}).get(external["id"])
            label = _md_cell(external.get("name") or external["id"])
            link = (f"[{label}](../../externals/{slug}.md)" if slug
                    else _md_code(external["id"]))
            flow = item.get("flow")
            suffix = (f" — via {_md_code(flow)}" if flow else "")
            lines.append(f"- {link}{suffix}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _flow_markdown(flow_payload: dict, slug_of: dict, triggered_by=None,
                   entrypoint_slug_of=None, rules=None, rule_slug_of=None,
                   externals=None, external_slug_of=None) -> str:
    flow = flow_payload["flow"]
    steps = sorted(flow_payload["steps"],
                   key=lambda step: (step.get("span") != "flow",
                                     step.get("contract_id") or step.get("id") or ""))
    feeds = sorted(flow_payload["feeds"],
                   key=lambda feed: (feed["from"], feed["to"],
                                     feed.get("kind") or ""))
    by_id = {step["id"]: step for step in steps}
    incoming, outgoing = defaultdict(list), defaultdict(list)
    for feed in feeds:
        incoming[feed["to"]].append(feed)
        outgoing[feed["from"]].append(feed)
    for items in incoming.values():
        items.sort(key=lambda feed: feed["from"])
    for items in outgoing.values():
        items.sort(key=lambda feed: feed["to"])
    joins = sorted(((step, incoming[step["id"]]) for step in steps
                    if len(incoming.get(step["id"], [])) > 1),
                   key=lambda item: item[0].get("contract_id") or item[0]["id"])

    lines = [f"# {_md_cell(flow.get('name') or flow['id'])}", "",
             _md_code(flow["id"]), ""]
    if flow.get("description"):
        lines += [_md_cell(flow["description"]), ""]
    if flow.get("cluster"):
        lines += [f"**Cluster** {_md_code(flow['cluster'])}", ""]
    lines += [f"**{_count(len(steps), 'entity', 'entities')} · "
              f"{_count(len(feeds), 'passage')} · "
              f"{_count(len(joins), 'join')}**", ""]

    if triggered_by:
        links = []
        for entrypoint in triggered_by:
            slug = (entrypoint_slug_of or {}).get(entrypoint["id"])
            label = _md_cell(entrypoint.get("name") or entrypoint["id"])
            links.append(f"[{label}](../entrypoints/{slug}.md)" if slug
                         else _md_code(entrypoint["id"]))
        lines += ["## Triggered by", "", *[f"- {link}" for link in links], ""]

    if rules:
        lines += ["## Rules", ""]
        for rule in rules:
            slug = (rule_slug_of or {}).get(rule["id"])
            label = _md_cell(rule.get("name") or rule["id"])
            target = f"[{label}](../rules/{slug}.md)" if slug else _md_code(rule["id"])
            lines.append(f"- {target} — {_md_cell(rule.get('description') or '')}")
        lines.append("")

    if externals:
        lines += ["## External systems", ""]
        for item in externals:
            external = item["external"]
            slug = (external_slug_of or {}).get(external["id"])
            label = _md_cell(external.get("name") or external["id"])
            link = (f"[{label}](../externals/{slug}.md)" if slug
                    else _md_code(external["id"]))
            lines.append(f"- {link} — {_md_code(item.get('via') or '')}")
        lines.append("")

    lines += ["## Path", ""]
    roots = [step for step in steps if not incoming.get(step["id"])]
    if not roots and steps:
        roots = [steps[0]]
    printed, stack = set(), set()

    def walk(step_id: str, depth: int) -> None:
        if step_id in stack:
            lines.append(f"{'  ' * depth}- … (cycle)")
            return
        if step_id in printed:
            lines.append(f"{'  ' * depth}- … (already shown)")
            return
        printed.add(step_id)
        stack.add(step_id)
        step = by_id[step_id]
        label = _entity_link(step["contract_id"], slug_of, "flows")
        role = step.get("role") or ""
        joins_here = len(incoming.get(step_id, []))
        suffix = f" · join {joins_here}" if joins_here > 1 else ""
        lines.append(f"{'  ' * depth}- {label} — {role}{suffix}")
        for feed in outgoing.get(step_id, []):
            target = by_id.get(feed["to"])
            if target:
                walk(feed["to"], depth + 1)
        stack.discard(step_id)

    if roots:
        for root in sorted(roots, key=lambda item: item["contract_id"]):
            walk(root["id"], 0)
    else:
        lines.append("_No entities in this flow yet; use `flow use`._")
    lines.append("")

    if feeds:
        lines += ["## Passages", "", "| From | Kind | To | Note |",
                  "|---|---|---|---|"]
        for feed in feeds:
            source = by_id.get(feed["from"], {})
            target = by_id.get(feed["to"], {})
            lines.append(
                f"| {_entity_link(source.get('contract_id', feed['from']), slug_of, 'flows')} "
                f"| {_md_code(feed.get('kind') or '')} "
                f"| {_entity_link(target.get('contract_id', feed['to']), slug_of, 'flows')} "
                f"| {_md_cell(feed.get('note') or '')} |")
        lines.append("")

    if joins:
        lines += ["## Joins", "", "| Step | Inputs |", "|---|---|"]
        for step, items in joins:
            sources = ", ".join(
                by_id.get(feed["from"], {}).get("contract_id", feed["from"])
                for feed in items)
            lines.append(
                f"| {_entity_link(step['contract_id'], slug_of, 'flows')} "
                f"| {_md_cell(sources)} |")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _docs_readme(title: str, contracts: list, fields: list, edges: list,
                 slug_of: dict, flows: list, flow_slug_of: dict,
                 entrypoints: list, entrypoint_slug_of: dict,
                 rules: list, rule_slug_of: dict,
                 externals: list, external_slug_of: dict) -> str:
    counts = Counter(node["kind"] for node in contracts)
    lines = [f"# {title}", "",
             "> Generated by `logistic_map.py export-md`. Do not edit these "
             "files by hand; re-run the command instead.", "",
              f"**{_count(len(contracts), 'entity', 'entities')} · "
              f"{_count(len(entrypoints), 'entrypoint')} · "
              f"{_count(len(flows), 'flow')} · "
              f"{_count(len(rules), 'rule')} · "
              f"{_count(len(externals), 'external system')} · "
             f"{_count(len(fields), 'field')}** · schema v{SCHEMA_VERSION}", ""]

    lines += ["## Reading these docs", "",
              "- Profiles: `Flow --USE--> entity` lists which flows use an "
              "entity. A `flow`/`transform` passage carries a `flow_id` and "
              "belongs to exactly one flow.",
              "- `flow` means the data passes to the next entity without changing "
              "shape; `transform` means the shape changes and the note records "
              "the mapping; `compose` is whole → part, never execution order.",
              "- A missing field means it was not mapped, not that the payload "
              "lacks it. Check the referenced file before concluding anything.",
              "- A `rule` node records a limit or condition that a flow or "
              "entrypoint follows; it is not a data entity and has no passage.",
              "- File references are evidence, not a call graph.", ""]

    lines += ["## Relations", "",
              "| Edge | Meaning |", "|---|---|",
              "| `contains` | entity → field |",
              "| `use` | Flow → entity: the flow uses this entity |",
              "| `trigger` | EntryPoint → Flow: the activation starts the flow |",
              "| `related` | EntryPoint → EntryPoint: directional activation relationship |",
              "| `rule` | Flow/EntryPoint → Rule: a limit or condition the activation follows |",
              "| `calls` | Flow → External System: the flow calls an external service |",
              "| `delivers-to` | Entry → External System: payload delivered in a flow |",
              "| `compose` | whole → part (composition) |",
              "| `flow` | data passes unchanged (inside one flow) |",
              "| `transform` | data changes shape; the note records the mapping |", ""]

    cluster_counts = defaultdict(lambda: {"entities": 0, "entrypoints": 0,
                                          "rules": 0, "externals": 0, "flows": 0})
    for node in contracts:
        cluster = str(node.get("cluster") or "").strip()
        if cluster:
            cluster_counts[cluster]["entities"] += 1
    for node in entrypoints:
        cluster = str(node.get("cluster") or "").strip()
        if cluster:
            cluster_counts[cluster]["entrypoints"] += 1
    for node in rules:
        cluster = str(node.get("cluster") or "").strip()
        if cluster:
            cluster_counts[cluster]["rules"] += 1
    for node in externals:
        cluster = str(node.get("cluster") or "").strip()
        if cluster:
            cluster_counts[cluster]["externals"] += 1
    for flow_payload in flows:
        cluster = str(flow_payload["flow"].get("cluster") or "").strip()
        if cluster:
            cluster_counts[cluster]["flows"] += 1
    if cluster_counts:
        lines += ["## Clusters", "",
                  "| Cluster | Entities | EntryPoints | Rules | External Systems | Flows |",
                  "|---|---:|---:|---:|---:|---:|"]
        for cluster, counts in sorted(cluster_counts.items()):
            lines.append(
                f"| {_md_code(cluster)} | {counts['entities']} "
                f"| {counts['entrypoints']} | {counts['rules']} "
                f"| {counts['externals']} | {counts['flows']} |")
        lines.append("")

    for kind in CONTRACT_KINDS:
        group = sorted((node for node in contracts if node["kind"] == kind),
                       key=lambda node: node["id"])
        lines += [f"## {PLURALS[kind]} ({len(group)})", ""]
        if not group:
            lines += ["_None._", ""]
            continue
        for node in group:
            summary = _kind_summary(node)
            description = _md_cell(node.get("description") or "")
            extra = f" — {description}" if description else ""
            detail = f" · {summary}" if summary else ""
            lines.append(
                f"- [{_md_code(node['id'])}](entities/{slug_of[node['id']]}.md)"
                f"{detail}{extra}")
        lines.append("")

    lines += [f"## EntryPoints ({len(entrypoints)})", ""]
    if not entrypoints:
        lines += ["_None._", ""]
    else:
        for node in entrypoints:
            lines.append(f"- [{_md_cell(node.get('name') or node['id'])}]"
                         f"(entrypoints/{entrypoint_slug_of[node['id']]}.md)")
        lines.append("")

    lines += [f"## Rules ({len(rules)})", ""]
    if not rules:
        lines += ["_None._", ""]
    else:
        for node in rules:
            description = _md_cell(node.get("description") or "")
            lines.append(f"- [{_md_cell(node.get('name') or node['id'])}]"
                         f"(rules/{rule_slug_of[node['id']]}.md)"
                         + (f" — {description}" if description else ""))
        lines.append("")

    lines += [f"## External Systems ({len(externals)})", ""]
    if not externals:
        lines += ["_None._", ""]
    else:
        for node in externals:
            description = _md_cell(node.get("description") or "")
            lines.append(f"- [{_md_cell(node.get('name') or node['id'])}]"
                         f"(externals/{external_slug_of[node['id']]}.md)"
                         + (f" — {description}" if description else ""))
        lines.append("")

    lines += [f"## Flows ({len(flows)})", ""]
    if not flows:
        lines += ["_No flows registered. Run `add-flow` and `flow use` and "
                  "`export-md` again._", ""]
    else:
        for flow_payload in flows:
            flow = flow_payload["flow"]
            label = flow.get("name") or flow["id"]
            lines.append(
                f"- [{_md_cell(label)}](flows/{flow_slug_of[flow['id']]}.md) — "
                f"`{flow['id']}` · {_count(len(flow_payload['steps']), 'step')} · "
                f"{_count(len(flow_payload['feeds']), 'feed')}")
        lines.append("")

    lines += ["## Regenerate", "", "```sh",
              'python scripts/logistic_map.py export-md --out docs/logistic-map',
              "```", ""]
    return "\n".join(lines).rstrip() + "\n"


def write_markdown_docs(db: DB, out_dir: Path, title: str, flows: list) -> dict:
    nodes, edges = load_graph(db)
    edges.sort(key=lambda edge: (edge["kind"], edge["from"], edge["to"]))
    contracts = sorted((node for node in nodes.values()
                        if node["kind"] in CONTRACT_KINDS),
                       key=lambda node: node["id"])
    fields = [node for node in nodes.values() if node["kind"] == "field"]
    entrypoints = sorted((node for node in nodes.values()
                          if node["kind"] == "entrypoint"), key=lambda n: n["id"])
    rules = sorted((node for node in nodes.values()
                    if node["kind"] == "rule"), key=lambda n: n["id"])
    externals = sorted((node for node in nodes.values()
                        if node["kind"] == "external"), key=lambda n: n["id"])
    fields_by_owner = defaultdict(list)
    for field in fields:
        fields_by_owner[owner_of(field["id"])].append(field)

    slug_of, used_entities = {}, set()
    for node in contracts:
        slug_of[node["id"]] = _doc_slug(
            node["kind"], node["id"].split(":", 1)[-1], used_entities)
    flow_slug_of, used_flows = {}, set()
    for flow_payload in flows:
        flow_id = flow_payload["flow"]["id"]
        flow_slug_of[flow_id] = _doc_slug(
            "", flow_id.split(":", 1)[-1], used_flows)
    entrypoint_slug_of, used_entrypoints = {}, set()
    for node in entrypoints:
        entrypoint_slug_of[node["id"]] = _doc_slug(
            "", node["id"].split(":", 1)[-1], used_entrypoints)
    rule_slug_of, used_rules = {}, set()
    for node in rules:
        rule_slug_of[node["id"]] = _doc_slug(
            "", node["id"].split(":", 1)[-1], used_rules)
    external_slug_of, used_externals = {}, set()
    for node in externals:
        external_slug_of[node["id"]] = _doc_slug(
            "", node["id"].split(":", 1)[-1], used_externals)

    flows_by_contract = defaultdict(list)
    flow_by_id = {flow_payload["flow"]["id"]: flow_payload["flow"]
                  for flow_payload in flows}
    entrypoints_by_id = {node["id"]: node for node in entrypoints}
    rules_by_id = {node["id"]: node for node in rules}
    rules_by_flow = defaultdict(list)
    rules_by_entrypoint = defaultdict(list)
    for edge in edges:
        if edge["kind"] != "rule":
            continue
        rule = rules_by_id.get(edge["to"])
        if not rule:
            continue
        if edge["from"] in flow_by_id:
            rules_by_flow[edge["from"]].append(rule)
        elif edge["from"] in entrypoints_by_id:
            rules_by_entrypoint[edge["from"]].append(rule)
    triggered_by = defaultdict(list)
    for edge in edges:
        if edge["kind"] == "trigger" and edge["from"] in entrypoints_by_id:
            triggered_by[edge["to"]].append(entrypoints_by_id[edge["from"]])
    externals_by_id = {node["id"]: node for node in externals}
    externals_by_flow = defaultdict(list)
    deliveries_by_entry = defaultdict(list)
    for edge in edges:
        if edge["kind"] == "calls":
            external = externals_by_id.get(edge["to"])
            if external and edge["from"] in flow_by_id:
                externals_by_flow[edge["from"]].append(
                    {"external": external, "via": "calls"})
        elif edge["kind"] == "delivers-to":
            external = externals_by_id.get(edge["to"])
            flow_id = edge.get("flow_id") or ""
            if not external:
                continue
            if flow_id in flow_by_id:
                externals_by_flow[flow_id].append(
                    {"external": external, "via": "delivers-to"})
            if edge["from"] in nodes and nodes[edge["from"]]["kind"] == "entry":
                deliveries_by_entry[edge["from"]].append(
                    {"external": external, "flow": flow_id})
    uses = flow_uses(nodes, edges)
    for contract_id in nodes:
        if nodes[contract_id]["kind"] not in CONTRACT_KINDS:
            continue
        related = {flow_id for flow_id, entities in uses.items()
                   if contract_id in entities}
        for edge in edges:
            if (edge["kind"] in DATA_EDGE_KINDS
                    and edge.get("flow_id")
                    and contract_id in (edge["from"], edge["to"])):
                related.add(edge["flow_id"])
        flows_by_contract[contract_id] = [
            flow_by_id[flow_id] for flow_id in sorted(related)
            if flow_id in flow_by_id]

    out_dir = Path(out_dir)
    (out_dir / "entities").mkdir(parents=True, exist_ok=True)
    (out_dir / "flows").mkdir(parents=True, exist_ok=True)
    (out_dir / "entrypoints").mkdir(parents=True, exist_ok=True)
    (out_dir / "rules").mkdir(parents=True, exist_ok=True)
    (out_dir / "externals").mkdir(parents=True, exist_ok=True)
    for pattern in ("entities/**/*.md", "flows/**/*.md",
                    "entrypoints/**/*.md", "rules/**/*.md",
                    "externals/**/*.md"):
        for stale in out_dir.glob(pattern):
            stale.unlink()

    written = 0
    for node in contracts:
        path = out_dir / "entities" / (slug_of[node["id"]] + ".md")
        path.parent.mkdir(parents=True, exist_ok=True)
        related = [edge for edge in edges
                   if edge["kind"] in DATA_EDGE_KINDS + ("compose",)
                   and node["id"] in (edge["from"], edge["to"])]
        path.write_text(
            _entity_markdown(node, fields_by_owner.get(node["id"], []), related,
                             flows_by_contract.get(node["id"], []), slug_of,
                             flow_slug_of,
                             deliveries=deliveries_by_entry.get(node["id"], []),
                             external_slug_of=external_slug_of),
            encoding="utf-8")
        written += 1

    for flow_payload in flows:
        flow_id = flow_payload["flow"]["id"]
        path = out_dir / "flows" / (flow_slug_of[flow_id] + ".md")
        path.write_text(
            _flow_markdown(flow_payload, slug_of, triggered_by.get(flow_id, []),
                           entrypoint_slug_of, rules_by_flow.get(flow_id, []),
                           rule_slug_of, externals_by_flow.get(flow_id, []),
                           external_slug_of), encoding="utf-8")
        written += 1

    for node in entrypoints:
        triggered = sorted({edge["to"] for edge in edges
                            if edge["kind"] == "trigger" and edge["from"] == node["id"]})
        lines = [f"# {node.get('name') or node['id']}", "",
                 f"`{node['id']}`", "",
                 node.get("description") or "", ""]
        if node.get("cluster"):
            lines += [f"**Cluster** `{node['cluster']}`", ""]
        lines += ["## Triggered flows", ""]
        lines.extend(f"- `{flow_id}`" for flow_id in triggered)
        if not triggered:
            lines.append("_None._")
        related_out = [edge for edge in edges
                       if edge["kind"] == "related" and edge["from"] == node["id"]]
        related_in = [edge for edge in edges
                      if edge["kind"] == "related" and edge["to"] == node["id"]]
        if related_out or related_in:
            lines += ["", "## Related entrypoints"]
            for edge in related_out:
                target = entrypoints_by_id.get(edge["to"], {})
                slug = entrypoint_slug_of.get(edge["to"])
                label = _md_cell(target.get("name") or edge["to"])
                link = f"[{label}](../entrypoints/{slug}.md)" if slug else _md_code(edge["to"])
                lines.append(f"- → {link} — {_md_cell(edge.get('description') or '')}")
            for edge in related_in:
                source = entrypoints_by_id.get(edge["from"], {})
                slug = entrypoint_slug_of.get(edge["from"])
                label = _md_cell(source.get("name") or edge["from"])
                link = f"[{label}](../entrypoints/{slug}.md)" if slug else _md_code(edge["from"])
                lines.append(f"- ← {link} — {_md_cell(edge.get('description') or '')}")
        attached_rules = rules_by_entrypoint.get(node["id"], [])
        if attached_rules:
            lines += ["", "## Rules"]
            for rule in attached_rules:
                slug = rule_slug_of.get(rule["id"])
                label = _md_cell(rule.get("name") or rule["id"])
                target = f"[{label}](../rules/{slug}.md)" if slug else _md_code(rule["id"])
                lines.append(f"- {target} — {_md_cell(rule.get('description') or '')}")
        if node.get("refs"):
            lines += ["", "## References", ""]
            lines.extend(f"- `{ref}`" for ref in node["refs"])
        path = out_dir / "entrypoints" / (entrypoint_slug_of[node["id"]] + ".md")
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        written += 1

    for rule in rules:
        attached_to = []
        for edge in edges:
            if edge["kind"] != "rule" or edge["to"] != rule["id"]:
                continue
            if edge["from"] in flow_by_id:
                slug = flow_slug_of.get(edge["from"])
                flow = flow_by_id[edge["from"]]
                label = _md_cell(flow.get("name") or edge["from"])
                attached_to.append(f"[{label}](../flows/{slug}.md)" if slug
                                   else _md_code(edge["from"]))
            elif edge["from"] in entrypoints_by_id:
                slug = entrypoint_slug_of.get(edge["from"])
                entrypoint = entrypoints_by_id[edge["from"]]
                label = _md_cell(entrypoint.get("name") or edge["from"])
                attached_to.append(
                    f"[{label}](../entrypoints/{slug}.md)" if slug
                    else _md_code(edge["from"]))
        lines = [f"# {rule.get('name') or rule['id']}", "",
                 _md_code(rule["id"]), ""]
        if rule.get("description"):
            lines += [_md_cell(rule["description"]), ""]
        if rule.get("cluster"):
            lines += [f"**Cluster** {_md_code(rule['cluster'])}", ""]
        lines += ["## Attached to", ""]
        lines += attached_to or ["_No flow or entrypoint yet._"]
        lines.append("")
        if rule.get("refs"):
            lines += ["## References", ""]
            lines.extend(f"- `{ref}`" for ref in rule["refs"])
            lines.append("")
        path = out_dir / "rules" / (rule_slug_of[rule["id"]] + ".md")
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        written += 1

    for external in externals:
        attached = []
        for edge in edges:
            if edge["kind"] == "calls" and edge["to"] == external["id"]:
                slug = flow_slug_of.get(edge["from"])
                flow = flow_by_id.get(edge["from"], {})
                label = _md_cell(flow.get("name") or edge["from"])
                link = f"[{label}](../flows/{slug}.md)" if slug else _md_code(edge["from"])
                attached.append(f"- {link} — `calls`")
            elif edge["kind"] == "delivers-to" and edge["to"] == external["id"]:
                entry_slug = slug_of.get(edge["from"])
                entry = nodes.get(edge["from"], {})
                entry_label = _md_cell(entry.get("name") or edge["from"])
                entry_link = (f"[{entry_label}](../entities/{entry_slug}.md)"
                              if entry_slug else _md_code(edge["from"]))
                flow_id = edge.get("flow_id") or ""
                flow_slug = flow_slug_of.get(flow_id)
                flow = flow_by_id.get(flow_id, {})
                flow_label = _md_cell(flow.get("name") or flow_id)
                flow_link = (f"[{flow_label}](../flows/{flow_slug}.md)"
                             if flow_slug else _md_code(flow_id))
                attached.append(
                    f"- {entry_link} — `delivers-to` via {flow_link}")
        lines = [f"# {external.get('name') or external['id']}", "",
                 _md_code(external["id"]), ""]
        if external.get("description"):
            lines += [_md_cell(external["description"]), ""]
        attributes = []
        for key, label in (("provider", "provider"), ("protocol", "protocol"),
                           ("location", "location"), ("cluster", "cluster"),
                           ("repo", "repo"), ("commit_sha", "commit"),
                           ("file_path", "file")):
            if external.get(key):
                attributes.append(
                    f"| {label} | {_md_code(external[key])} |")
        if attributes:
            lines += ["| Attribute | Value |", "|---|---|", *attributes, ""]
        lines += ["## Used by", ""]
        lines += attached or ["_No flow or entry uses this external system yet._"]
        lines.append("")
        if external.get("refs"):
            lines += ["## References", ""]
            lines.extend(f"- `{ref}`" for ref in external["refs"])
            lines.append("")
        path = out_dir / "externals" / (external_slug_of[external["id"]] + ".md")
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        written += 1

    (out_dir / "README.md").write_text(
        _docs_readme(title, contracts, fields, edges, slug_of, flows, flow_slug_of,
                     entrypoints, entrypoint_slug_of, rules, rule_slug_of,
                     externals, external_slug_of),
        encoding="utf-8")
    written += 1
    return {"out": str(out_dir), "entities": len(contracts),
            "flows": len(flows), "entrypoints": len(entrypoints),
            "rules": len(rules), "externals": len(externals),
            "files": written}


def cmd_export_md(args) -> int:
    db = open_db(args)
    flows = render_flows(db)
    result = write_markdown_docs(db, Path(args.out), args.title, flows)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"wrote {result['files']} markdown files to {result['out']} "
              f"({result['entities']} entities, {result['flows']} flows, "
              f"{result['rules']} rules, {result['externals']} external systems)")
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
    entrypoints = sum(node["kind"] == "entrypoint" for node in nodes.values())
    rules = sum(node["kind"] == "rule" for node in nodes.values())
    externals = sum(node["kind"] == "external" for node in nodes.values())
    stores = Counter(node.get("store_type") or "?" for node in contracts
                     if node["kind"] == "storage")
    per_contract = Counter(owner_of(field_id) for field_id in nodes if "#" in field_id)
    clusters = Counter(cluster for cluster in
                       (cluster_of(node, nodes) for node in nodes.values()) if cluster)
    payload = {
        "nodes": len(nodes), "edges": len(edges),
        "kinds": dict(kinds), "edge_kinds": dict(edge_kinds),
        "entrypoints": entrypoints, "store_types": dict(stores),
        "rules": rules, "externals": externals,
        "clusters": dict(clusters.most_common()),
        "fields_per_contract": dict(per_contract.most_common(10)),
    }
    if args.json:
        print(json.dumps(payload, indent=2))
        return 0
    print(f"nodes:  {len(nodes)} ({', '.join(f'{k}={v}' for k, v in sorted(kinds.items()))})")
    print(f"edges:  {len(edges)} ({', '.join(f'{k}={v}' for k, v in sorted(edge_kinds.items()))})")
    if entrypoints:
        print(f"entrypoints:    {entrypoints}")
    if rules:
        print(f"rules:          {rules}")
    if externals:
        print(f"externals:      {externals}")
    if clusters:
        print("clusters:       " + ", ".join(f"{k}={v}" for k, v in clusters.most_common()))
    if stores:
        print("storage types:  " + ", ".join(f"{k}={v}" for k, v in stores.most_common()))
    if per_contract:
        print("fields per contract:")
        for owner, count in per_contract.most_common(10):
            print(f"  {owner:<42} {count}")
    return 0


# --------------------------------------------------------------------------
# flows: nodes + USE + passages in the main graph
# --------------------------------------------------------------------------

def flow_uses(nodes, edges) -> dict:
    uses = defaultdict(set)
    for edge in edges:
        if edge["kind"] == "use" and edge["from"] in nodes and edge["to"] in nodes:
            uses[edge["from"]].add(edge["to"])
    return uses


def _flow_role(incoming, outgoing, node_id: str) -> str:
    has_in = bool(incoming.get(node_id))
    has_out = bool(outgoing.get(node_id))
    if has_in and has_out:
        return "readwrite"
    if has_in:
        return "write"
    if has_out:
        return "source"
    return "artifact"


def _flow_steps(nodes, flow_id: str, entities: list,
                incoming: dict, outgoing: dict) -> list:
    contract_keys = ("kind", "description", "location",
                     "store_type", "stage", "repo",
                     "commit_sha", "file_path", "refs")
    steps = []
    for seq, contract_id in enumerate(entities):
        node = nodes[contract_id]
        steps.append({
            "id": f"{flow_id}/{contract_id}",
            "flow_id": flow_id,
            "contract_id": contract_id,
            "contract_kind": node["kind"],
            "name": node.get("name") or contract_id,
            "role": _flow_role(incoming, outgoing, contract_id),
            "span": "flow",
            "seq": seq,
            "repo": node.get("repo", ""),
            "commit_sha": node.get("commit_sha", ""),
            "file_path": node.get("file_path", ""),
            "refs": node.get("refs") or [],
            "contract": {key: node[key] for key in contract_keys
                         if node.get(key) not in (None, "", [])},
        })
    return steps


def build_flow_payload(nodes, edges, flow_id: str):
    flow = nodes.get(flow_id)
    if not flow or flow["kind"] != "flow":
        return None
    contract_ids = {node_id for node_id, node in nodes.items()
                    if node["kind"] in CONTRACT_KINDS}
    used = {entity for entity in flow_uses(nodes, edges).get(flow_id, set())
            if entity in contract_ids}
    passages = [edge for edge in edges
                if edge["kind"] in DATA_EDGE_KINDS
                and (edge.get("flow_id") or "") == flow_id
                and edge["from"] in contract_ids and edge["to"] in contract_ids]
    passages = sorted(passages, key=lambda edge: (edge["from"], edge["to"], edge["kind"]))
    endpoints = {edge["from"] for edge in passages} | {edge["to"] for edge in passages}
    entities = sorted(used | endpoints)

    incoming, outgoing = defaultdict(list), defaultdict(list)
    for edge in passages:
        incoming[edge["to"]].append(edge)
        outgoing[edge["from"]].append(edge)

    steps = _flow_steps(nodes, flow_id, entities, incoming, outgoing)
    feeds = [{
        "from": f"{flow_id}/{edge['from']}",
        "to": f"{flow_id}/{edge['to']}",
        "kind": edge["kind"],
        "note": edge.get("note") or "",
        "flow_id": flow_id,
    } for edge in passages]
    externals = []
    for edge in edges:
        if edge["kind"] == "calls" and edge["from"] == flow_id:
            via = "calls"
        elif (edge["kind"] == "delivers-to"
              and (edge.get("flow_id") or "") == flow_id):
            via = "delivers-to"
        else:
            continue
        external = nodes.get(edge["to"])
        if external and external["kind"] == "external":
            externals.append({
                "id": external["id"],
                "name": external.get("name") or "",
                "via": via,
                "flow_id": flow_id,
            })
    return {"flow": flow, "steps": steps, "feeds": feeds,
            "externals": externals}


def build_flow_payloads(nodes, edges) -> list:
    flow_ids = sorted(node_id for node_id, node in nodes.items()
                      if node["kind"] == "flow")
    return [payload for payload in
            (build_flow_payload(nodes, edges, flow_id) for flow_id in flow_ids)
            if payload]


def render_flows(db: DB) -> list:
    nodes, edges = load_graph(db)
    return build_flow_payloads(nodes, edges)


def _flow_joins(payload: dict) -> list:
    by_id = {step["id"]: step for step in payload["steps"]}
    incoming = defaultdict(list)
    for feed in payload["feeds"]:
        incoming[feed["to"]].append(feed)
    joins = []
    for step_id, items in incoming.items():
        if len(items) <= 1:
            continue
        step = by_id.get(step_id)
        if not step:
            continue
        joins.append({
            "step": step["contract_id"],
            "sources": sorted(by_id[feed["from"]]["contract_id"]
                              for feed in items if feed["from"] in by_id),
        })
    return sorted(joins, key=lambda item: item["step"])


def _flow_text_tree(payload: dict) -> list:
    steps = payload["steps"]
    by_id = {step["id"]: step for step in steps}
    incoming, outgoing = defaultdict(list), defaultdict(list)
    for feed in payload["feeds"]:
        incoming[feed["to"]].append(feed)
        outgoing[feed["from"]].append(feed)
    roots = [step for step in steps if not incoming.get(step["id"])]
    if not roots:
        roots = [steps[0]] if steps else []
    lines, printed, stack = [], set(), set()

    def walk(step_id: str, depth: int) -> None:
        if step_id in stack:
            lines.append("  " * depth + "... (cycle)")
            return
        if step_id in printed:
            lines.append("  " * depth + "... (already shown)")
            return
        printed.add(step_id)
        stack.add(step_id)
        step = by_id[step_id]
        label = f"{step['contract_id']} [{step.get('role', '')}]"
        joins = len(incoming.get(step_id, []))
        if joins > 1:
            label += f"  (join: {joins})"
        lines.append("  " * depth + label)
        for feed in sorted(outgoing.get(step_id, []), key=lambda item: item["to"]):
            if feed["to"] in by_id:
                walk(feed["to"], depth + 1)
        stack.discard(step_id)

    for root in sorted(roots, key=lambda step: step["contract_id"]):
        walk(root["id"], 0)
    return lines


def cmd_flow_use(args) -> int:
    db = open_db(args)
    if not get_node(db, "flow", args.id):
        raise LogisticMapError(f"flow {args.id!r} does not exist; run add-flow first")
    result = upsert_edge(
        db, "use", args.id, args.entity,
        refs=parse_refs(args.ref) if args.ref is not None else None,
        file_path=args.file, note=args.note)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{'added' if result['created'] else 'updated'} "
              f"{args.id} USES {args.entity}")
    return 0


def cmd_flow_unuse(args) -> int:
    db = open_db(args)
    result = remove_edge(db, "use", args.id, args.entity)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"removed {args.id} USES {args.entity}")
    return 0


def cmd_flow_list(args) -> int:
    db = open_db(args)
    nodes, edges = load_graph(db)
    uses = flow_uses(nodes, edges)
    passage_count = Counter(edge.get("flow_id") or "" for edge in edges
                            if edge["kind"] in DATA_EDGE_KINDS)
    rows = []
    for flow_id in sorted(node_id for node_id, node in nodes.items()
                          if node["kind"] == "flow"):
        flow = nodes[flow_id]
        rows.append({
            "id": flow_id,
            "name": flow.get("name", ""),
            "description": flow.get("description", ""),
            "entities": len(uses.get(flow_id, set())),
            "passages": passage_count.get(flow_id, 0),
        })
    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return 0
    if not rows:
        print("no flows; run add-flow --id flow:<slug> --name <name>")
        return 0
    for row in rows:
        print(f"{row['id']:<34} {row['name'][:28]:<28} "
              f"entities={row['entities']} passages={row['passages']}")
    print(f"\n{len(rows)} flow(s)")
    return 0


def cmd_flow_show(args) -> int:
    db = open_db(args)
    nodes, edges = load_graph(db)
    payload = build_flow_payload(nodes, edges, args.id)
    if not payload:
        raise LogisticMapError(f"flow {args.id!r} does not exist")
    joins = _flow_joins(payload)
    flow = payload["flow"]
    triggers = sorted(edge["from"] for edge in edges
                      if edge["kind"] == "trigger" and edge["to"] == flow["id"])
    if args.json:
        print(json.dumps({
            "flow": flow,
            "triggered_by": triggers,
            "entities": [step["contract_id"] for step in payload["steps"]],
            "steps": payload["steps"],
            "feeds": payload["feeds"],
            "externals": payload.get("externals", []),
            "joins": joins,
        }, indent=2, ensure_ascii=False))
        return 0

    print(f"Flow {flow['id']} — {flow.get('name', '')}")
    if flow.get("description"):
        print(flow["description"])
    print(f"entities: {len(payload['steps'])}  "
          f"passages: {len(payload['feeds'])}  joins: {len(joins)}")
    if triggers:
        print("triggered by: " + ", ".join(triggers))
    print()
    print("ENTITIES")
    if not payload["steps"]:
        print("  (none; use `flow use --id ... --entity ...`)")
    for step in payload["steps"]:
        print(f"  {step['contract_id']} [{step.get('role', '')}]")
    print()
    print("PASSAGES")
    if not payload["feeds"]:
        print("  (none; add-edge --kind flow|transform --flow ...)")
    for feed in payload["feeds"]:
        note = f"  # {feed['note']}" if feed.get("note") else ""
        print(f"  {feed['from'].split('/', 1)[-1]} "
              f"-[{feed['kind']}]-> {feed['to'].split('/', 1)[-1]}{note}")
    print()
    if payload.get("externals"):
        print()
        print("EXTERNAL SYSTEMS")
        for item in payload["externals"]:
            print(f"  {item['id']} [{item['via']}]")
    print()
    print("PATH")
    tree = _flow_text_tree(payload)
    if tree:
        for line in tree:
            print("  " + line)
    else:
        print("  (none; add entities with `flow use` and passages with "
              "`add-edge --flow`)")
    if joins:
        print()
        print("JOINS")
        for join in joins:
            print(f"  {join['step']} <- {', '.join(join['sources'])}")
    return 0


def cmd_flow_remove(args) -> int:
    db = open_db(args)
    if not get_node(db, "flow", args.id):
        raise LogisticMapError(f"flow {args.id!r} does not exist")
    result = remove_node(db, args.id)
    result["ok"] = True
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"removed {args.id} ({result.get('removed_edges', 0)} passages)")
    return 0


def cmd_branch_create(args) -> int:
    master = Path(args.db).expanduser()
    if not master.exists():
        raise LogisticMapError(
            f"no logistic-map database at {master}; run `init` first")
    state = read_branch_state(master)
    refresh_stale_branches(master, state)
    name = branch_name(args.name)
    if name in state["branches"]:
        raise LogisticMapError(
            f"branch {name!r} already exists; drop it before reusing the name")
    target = branch_db_path(master, name)
    if target.exists():
        raise LogisticMapError(
            f"{target} already exists but is not recorded as a branch")
    fingerprint = master_fingerprint(master)
    copy_db(master, target)
    state["branches"][name] = {
        "status": BRANCH_OPEN,
        "base_revision": state.get("revision", 0),
        "base_fingerprint": fingerprint,
        "created_at": now_iso(),
    }
    state["active"] = name
    write_branch_state(master, state)
    result = {"ok": True, "branch": name, "status": BRANCH_OPEN,
              "base_revision": state["branches"][name]["base_revision"]}
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"created branch {name} from master r{result['base_revision']}; "
              f"now active")
    return 0


def cmd_branch_use(args) -> int:
    master = Path(args.db).expanduser()
    if not master.exists():
        raise LogisticMapError(
            f"no logistic-map database at {master}; run `init` first")
    state = read_branch_state(master)
    if refresh_stale_branches(master, state):
        write_branch_state(master, state)
    name = str(args.name or "").strip()
    if name == BRANCH_MASTER:
        state["active"] = BRANCH_MASTER
        write_branch_state(master, state)
        print("switched to master" if not args.json
              else json.dumps({"ok": True, "active": BRANCH_MASTER}, indent=2))
        return 0
    branch = state["branches"].get(name)
    if not branch:
        raise LogisticMapError(f"branch {name!r} does not exist")
    if branch.get("status") != BRANCH_OPEN:
        reason = branch.get("stale_reason") or f"status is {branch.get('status')}"
        raise LogisticMapError(f"branch {name!r} cannot be used: {reason}")
    state["active"] = name
    write_branch_state(master, state)
    if args.json:
        print(json.dumps({"ok": True, "active": name}, indent=2))
    else:
        print(f"switched to branch {name}")
    return 0


def cmd_branch_list(args) -> int:
    master = Path(args.db).expanduser()
    state = read_branch_state(master)
    if refresh_stale_branches(master, state):
        write_branch_state(master, state)
    if args.json:
        print(json.dumps({
            "ok": True,
            "revision": state.get("revision", 0),
            "active": state.get("active", BRANCH_MASTER),
            "branches": _branch_rows(state),
        }, indent=2))
    else:
        _print_branch_list(state)
    return 0


def cmd_branch_status(args) -> int:
    master = Path(args.db).expanduser()
    state = read_branch_state(master)
    if refresh_stale_branches(master, state):
        write_branch_state(master, state)
    active = state.get("active", BRANCH_MASTER)
    counts = {"nodes": 0, "edges": 0}
    try:
        db_path = resolve_active_db(master)
    except LogisticMapError:
        db_path = None
    if db_path and db_path.exists():
        db = DB(db_path)
        try:
            nodes, edges = load_graph(db)
            counts = {"nodes": len(nodes), "edges": len(edges)}
        finally:
            db.close()
    payload = {"ok": True, "revision": state.get("revision", 0),
               "active": active, "counts": counts,
               "branches": _branch_rows(state)}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"revision: {payload['revision']}")
        print(f"active:   {active}")
        print(f"graph:    {counts['nodes']} nodes, {counts['edges']} edges")
        rows = [row for row in payload["branches"] if row["name"] == active]
        for row in rows:
            print(f"branch:   {row['name']} ({row['status']}, "
                  f"base r{row['base_revision']})")
    return 0


def cmd_branch_apply(args) -> int:
    master = Path(args.db).expanduser()
    if not master.exists():
        raise LogisticMapError(
            f"no logistic-map database at {master}; run `init` first")
    state = read_branch_state(master)
    refresh_stale_branches(master, state)
    name = str(args.name or "").strip()
    branch = state["branches"].get(name)
    if not branch:
        raise LogisticMapError(f"branch {name!r} does not exist")
    if branch.get("status") == BRANCH_APPLIED:
        raise LogisticMapError(f"branch {name!r} was already applied")
    if branch.get("status") != BRANCH_OPEN:
        reason = branch.get("stale_reason") or f"status is {branch.get('status')}"
        raise LogisticMapError(f"branch {name!r} cannot be applied: {reason}")
    target = branch_db_path(master, name)
    if not target.exists():
        raise LogisticMapError(
            f"branch {name!r} is missing its database at {target}")
    current = master_fingerprint(master)
    if branch.get("base_fingerprint") != current:
        branch["status"] = BRANCH_STALE
        branch["stale_reason"] = (
            "stale: master is no longer at the revision this branch started from")
        write_branch_state(master, state)
        raise LogisticMapError(
            f"branch {name!r} is stale: master changed after it was created; "
            "create a new branch from the current master")

    backup = master.parent / f".{master.name}.applying-{os.getpid()}"
    if backup.exists():
        remove_db(backup)
    os.replace(master, backup)
    try:
        copy_db(target, master)
    except Exception:
        if master.exists():
            remove_db(master)
        os.replace(backup, master)
        raise
    remove_db(backup)
    remove_db(target)

    revision = int(state.get("revision", 0)) + 1
    state["revision"] = revision
    branch["status"] = BRANCH_APPLIED
    branch["applied_at"] = now_iso()
    branch["applied_revision"] = revision
    stale = []
    for other_name, other in state["branches"].items():
        if other_name == name or other.get("status") != BRANCH_OPEN:
            continue
        other["status"] = BRANCH_STALE
        other["stale_reason"] = (
            f"stale: master advanced to r{revision} when {name} was applied")
        stale.append(other_name)
    state["active"] = BRANCH_MASTER
    write_branch_state(master, state)
    payload = {"ok": True, "branch": name, "status": BRANCH_APPLIED,
               "revision": revision, "stale": sorted(stale)}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"applied {name} to master (revision {revision})")
        if stale:
            print(f"stale branches: {', '.join(sorted(stale))}")
    return 0


def cmd_branch_drop(args) -> int:
    master = Path(args.db).expanduser()
    state = read_branch_state(master)
    name = str(args.name or "").strip()
    branch = state["branches"].get(name)
    if not branch:
        raise LogisticMapError(f"branch {name!r} does not exist")
    target = branch_db_path(master, name)
    if target.exists():
        remove_db(target)
    del state["branches"][name]
    if state.get("active") == name:
        state["active"] = BRANCH_MASTER
    write_branch_state(master, state)
    payload = {"ok": True, "branch": name, "removed": True}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"dropped branch {name}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS,
                        help=f"LadybugDB database path (default: {DEFAULT_DB})")
    common.add_argument("--json", action="store_true", default=argparse.SUPPRESS,
                        help="machine-readable output")

    parser = argparse.ArgumentParser(
        prog="logistic_map.py",
        description="LadybugDB-backed logistic map for application data flow.")
    # Global forms are accepted before the subcommand; the per-command
    # duplicates in `common` accept the same options after it.
    parser.add_argument("--db", default=None,
                        help=f"LadybugDB database path (default: {DEFAULT_DB})")
    parser.add_argument("--json", action="store_true", default=None,
                        help="machine-readable output")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", parents=[common], help="create the graph schema")
    init.set_defaults(func=cmd_init)

    migrate = sub.add_parser(
        "migrate", parents=[common],
        help="convert a schema v1-v7 database to v8 (external systems)")
    migrate.set_defaults(func=cmd_migrate)

    migrate_db_cmd = sub.add_parser(
        "migrate-db", parents=[common],
        help="convert a legacy Kuzu database or JSON snapshot to LadybugDB")
    migrate_db_cmd.add_argument(
        "--from", dest="source", required=True,
        help="legacy .kuzu database or JSON export to read")
    migrate_db_cmd.add_argument(
        "--to", help="target LadybugDB path (default: --db)")
    migrate_db_cmd.add_argument(
        "--force", action="store_true", help="replace the target if it exists")
    migrate_db_cmd.set_defaults(func=cmd_migrate_db)

    doctor = sub.add_parser("doctor", parents=[common], help="check environment and database")
    doctor.set_defaults(func=cmd_doctor)

    add_node = sub.add_parser("add-node", parents=[common],
                              help="create or update a contract node")
    add_node.add_argument("--kind", required=True, choices=CONTRACT_KINDS)
    add_node.add_argument("--id", help="explicit id, e.g. input:orders.created")
    add_node.add_argument("--name")
    add_node.add_argument("--description")
    add_node.add_argument("--location", help="storage location, e.g. postgres://orders")
    add_node.add_argument("--store-type", dest="store_type",
                          help="storage type: sql, redis, object, file, cache")
    add_node.add_argument("--stage", help="entry stage, e.g. normalized, enriched, response")
    add_node.add_argument("--cluster",
                          help="grouping label, e.g. orders-service or checkout-ui")
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

    add_flow = sub.add_parser("add-flow", parents=[common],
                              help="create or update a flow (a named set of entities)")
    add_flow.add_argument("--id", help="explicit id, e.g. flow:create-order")
    add_flow.add_argument("--name")
    add_flow.add_argument("--description")
    add_flow.add_argument("--cluster",
                          help="grouping label, e.g. orders-service or checkout-ui")
    add_flow.add_argument("--repo")
    add_flow.add_argument("--commit", dest="commit")
    add_flow.add_argument("--file", dest="file")
    add_flow.add_argument("--ref", action="append",
                          help="reference file:line (repeatable)")
    add_flow.set_defaults(func=cmd_add_flow)

    add_entrypoint = sub.add_parser(
        "add-entrypoint", parents=[common],
        help="create or update an entrypoint that triggers flows")
    add_entrypoint.add_argument("--id", help="explicit id, e.g. entrypoint:orders-created")
    add_entrypoint.add_argument("--name")
    add_entrypoint.add_argument("--description", required=True,
                                help="how this entrypoint is activated")
    add_entrypoint.add_argument("--cluster",
                                help="grouping label, e.g. orders-service or checkout-ui")
    add_entrypoint.add_argument("--repo")
    add_entrypoint.add_argument("--commit", dest="commit")
    add_entrypoint.add_argument("--file", dest="file")
    add_entrypoint.add_argument("--ref", action="append",
                                help="reference file:line (repeatable)")
    add_entrypoint.set_defaults(func=cmd_add_entrypoint)

    add_rule = sub.add_parser(
        "add-rule", parents=[common],
        help="create or update a rule attached to a flow or entrypoint")
    add_rule.add_argument("--id", help="explicit id, e.g. rule:search-limit")
    add_rule.add_argument("--name")
    add_rule.add_argument("--description", required=True,
                          help="what the rule requires or limits")
    add_rule.add_argument("--cluster",
                          help="grouping label, e.g. orders-service or checkout-ui")
    add_rule.add_argument("--repo")
    add_rule.add_argument("--commit", dest="commit")
    add_rule.add_argument("--file", dest="file")
    add_rule.add_argument("--ref", action="append",
                          help="reference file:line (repeatable)")
    add_rule.set_defaults(func=cmd_add_rule)

    add_external = sub.add_parser(
        "add-external", parents=[common],
        help="create or update an External System node")
    add_external.add_argument("--id", help="explicit id, e.g. external:sms-provider")
    add_external.add_argument("--name")
    add_external.add_argument("--description")
    add_external.add_argument("--provider", help="vendor, e.g. Twilio")
    add_external.add_argument("--protocol", help="HTTP, gRPC, SMTP, ...")
    add_external.add_argument("--location", help="base URL or address")
    add_external.add_argument("--cluster",
                              help="grouping label, e.g. notification-service")
    add_external.add_argument("--repo")
    add_external.add_argument("--commit", dest="commit")
    add_external.add_argument("--file", dest="file")
    add_external.add_argument("--ref", action="append",
                              help="reference file:line (repeatable)")
    add_external.set_defaults(func=cmd_add_external)

    add_edge = sub.add_parser("add-edge", parents=[common],
                              help="create or update a relationship")
    add_edge.add_argument("--kind", required=True, choices=tuple(RELS))
    add_edge.add_argument("--from", dest="src", required=True)
    add_edge.add_argument("--to", dest="dst", required=True)
    add_edge.add_argument("--flow", help="flow id for flow/transform/delivers-to edges")
    add_edge.add_argument("--description",
                          help="what connects the entrypoints (related edges)")
    add_edge.add_argument("--ref", action="append", help="reference file:line (repeatable)")
    add_edge.add_argument("--file", dest="file")
    add_edge.add_argument("--note", help="free-form note")
    add_edge.set_defaults(func=cmd_add_edge)

    remove_node = sub.add_parser("remove-node", parents=[common],
                                 help="delete a node and its owned fields")
    remove_node.add_argument("--id", required=True)
    remove_node.set_defaults(func=cmd_remove_node)

    remove_edge = sub.add_parser("remove-edge", parents=[common], help="delete an edge")
    remove_edge.add_argument("--kind", required=True, choices=tuple(RELS))
    remove_edge.add_argument("--from", dest="src", required=True)
    remove_edge.add_argument("--to", dest="dst", required=True)
    remove_edge.add_argument("--flow", help="flow id for flow/transform/delivers-to edges")
    remove_edge.add_argument("--description",
                             help="identifies a related edge between entrypoints")
    remove_edge.set_defaults(func=cmd_remove_edge)

    apply = sub.add_parser("apply", parents=[common],
                           help="apply a JSON batch of nodes, fields and edges")
    apply.add_argument("--file", required=True)
    apply.set_defaults(func=cmd_apply)

    list_cmd = sub.add_parser("list", parents=[common], help="list nodes")
    list_cmd.add_argument("--kind", choices=ALL_KINDS)
    list_cmd.add_argument("--owner", help="filter fields by contract id")
    list_cmd.add_argument("--cluster", help="filter by cluster label")
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
    export.add_argument("--out", default=".logistic-map/exports/graph.json")
    export.set_defaults(func=cmd_export)

    export_md = sub.add_parser(
        "export-md", parents=[common],
        help="write Markdown docs for entities and flows (agent-friendly)")
    export_md.add_argument("--out", default="docs/logistic-map",
                           help="managed output directory")
    export_md.add_argument("--title", default="Logistic map")
    export_md.set_defaults(func=cmd_export_md)

    render = sub.add_parser("render", parents=[common], help="write a standalone HTML graph")
    render.add_argument("--out")
    render.add_argument("--title", default="Logistic map")
    render.set_defaults(func=cmd_render)

    stats = sub.add_parser("stats", parents=[common], help="print graph counters")
    stats.set_defaults(func=cmd_stats)

    flow = sub.add_parser("flow", help="inspect and edit authored flows")
    flow_sub = flow.add_subparsers(dest="flow_command", required=True)

    flow_use = flow_sub.add_parser(
        "use", parents=[common],
        help="mark an entity as used by a flow (Flow -USES-> entity)")
    flow_use.add_argument("--id", required=True, help="flow id")
    flow_use.add_argument("--entity", required=True, help="contract id")
    flow_use.add_argument("--note")
    flow_use.add_argument("--file", dest="file")
    flow_use.add_argument("--ref", action="append",
                          help="reference file:line (repeatable)")
    flow_use.set_defaults(func=cmd_flow_use)

    flow_unuse = flow_sub.add_parser(
        "unuse", parents=[common],
        help="remove an entity from a flow")
    flow_unuse.add_argument("--id", required=True, help="flow id")
    flow_unuse.add_argument("--entity", required=True, help="contract id")
    flow_unuse.set_defaults(func=cmd_flow_unuse)

    flow_list = flow_sub.add_parser("list", parents=[common],
                                    help="list flows")
    flow_list.set_defaults(func=cmd_flow_list)

    flow_show = flow_sub.add_parser("show", parents=[common],
                                    help="show one flow: entities, passages, path")
    flow_show.add_argument("id")
    flow_show.set_defaults(func=cmd_flow_show)

    flow_remove = flow_sub.add_parser("remove", parents=[common],
                                      help="delete a flow and its passages")
    flow_remove.add_argument("--id", required=True)
    flow_remove.set_defaults(func=cmd_flow_remove)

    branch = sub.add_parser(
        "branch", help="work on isolated copies of the graph and apply them")
    branch_sub = branch.add_subparsers(dest="branch_command", required=True)

    branch_create = branch_sub.add_parser(
        "create", parents=[common],
        help="copy master into a new branch and switch to it")
    branch_create.add_argument("name")
    branch_create.set_defaults(func=cmd_branch_create)

    branch_use = branch_sub.add_parser(
        "use", parents=[common],
        help="switch to a branch, or back to master")
    branch_use.add_argument("name")
    branch_use.set_defaults(func=cmd_branch_use)

    branch_list = branch_sub.add_parser(
        "list", parents=[common], help="list branches and their status")
    branch_list.set_defaults(func=cmd_branch_list)

    branch_status = branch_sub.add_parser(
        "status", parents=[common], help="show the active branch")
    branch_status.set_defaults(func=cmd_branch_status)

    branch_apply = branch_sub.add_parser(
        "apply", parents=[common],
        help="copy a branch onto master (no merge)")
    branch_apply.add_argument("name")
    branch_apply.set_defaults(func=cmd_branch_apply)

    branch_drop = branch_sub.add_parser(
        "drop", parents=[common], help="discard a branch")
    branch_drop.add_argument("name")
    branch_drop.set_defaults(func=cmd_branch_drop)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "db", None) is None:
        args.db = DEFAULT_DB
    if getattr(args, "json", None) is None:
        args.json = False
    master_commands = {"init", "migrate", "migrate-db", "doctor", "branch"}
    if args.command not in master_commands:
        args.db = str(resolve_active_db(Path(args.db).expanduser()))
    try:
        return args.func(args)
    except LogisticMapError as exc:
        if getattr(args, "json", False):
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # surface LadybugDB errors without a traceback
        if getattr(args, "json", False):
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
