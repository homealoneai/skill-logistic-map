"""Read-only repository tools plus the skill CLI, exposed to the extractor."""

from __future__ import annotations

import fnmatch
import json
import os
import re
from pathlib import Path

from inspect_ai.tool import Tool, ToolError, tool

from .config import SKILL_DIR
from .workspace import MapSession

MAX_READ_BYTES = 200_000
MAX_OUTPUT_CHARS = 20_000
MAX_LIST_ENTRIES = 600

SKIP_DIRS = {
    ".git",
    ".venv",
    ".work",
    "node_modules",
    "__pycache__",
    "dist",
    "build",
    "target",
    ".gradle",
    ".idea",
    ".logistic-map",
}

ALLOWED_SKILL_PATHS = ("SKILL.md", "README.md", "references", "examples", "scripts")

BLOCKED_CLI_COMMANDS = {
    "export": "the harness exports the snapshot itself",
    "export-md": "the harness writes documentation from the exported graph",
    "render": "the harness renders the map itself",
    "apply": "apply is not available; use add-node/add-field/add-edge",
}


def _skill_allowed(target: Path) -> bool:
    for entry in ALLOWED_SKILL_PATHS:
        base = (SKILL_DIR / entry).resolve()
        if target == base or base in target.parents:
            return True
    return False


def _resolve(session: MapSession, path: str) -> Path:
    raw = (path or ".").strip()
    if raw in ("skill", "skill/"):
        return SKILL_DIR.resolve()
    if raw.startswith("skill/"):
        target = (SKILL_DIR / raw[len("skill/") :]).resolve()
        if not _skill_allowed(target):
            raise ToolError(
                "skill/ paths are limited to SKILL.md, README.md, references/, "
                "examples/ and scripts/"
            )
        return target

    target = (session.case_dir / raw).resolve()
    base = session.case_dir.resolve()
    if target != base and base not in target.parents:
        raise ToolError(f"path escapes the case directory: {path}")
    return target


def _label(session: MapSession, target: Path) -> str:
    try:
        return str(target.relative_to(session.case_dir))
    except ValueError:
        try:
            return "skill/" + str(target.relative_to(SKILL_DIR.resolve()))
        except ValueError:
            return str(target)


def _iter_files(root: Path):
    if root.is_file():
        yield root
        return
    for directory, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(name for name in dirnames if name not in SKIP_DIRS)
        for name in sorted(filenames):
            if name.startswith(".") and name != ".env.example":
                continue
            yield Path(directory) / name


def _truncate(text: str) -> str:
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    return (
        text[:MAX_OUTPUT_CHARS]
        + f"\n... truncated ({len(text) - MAX_OUTPUT_CHARS} characters omitted)"
    )


def make_tools(session: MapSession) -> list[Tool]:
    """Build tools bound to one case, so every sample gets its own database."""

    @tool
    def list_files():
        async def execute(path: str = ".", depth: int = 2) -> str:
            """List files and directories in the case repository.

            Args:
                path: Path relative to the case root, or `skill/` for the skill itself.
                depth: How many directory levels to show (0-4).
            """
            target = _resolve(session, path)
            if not target.exists():
                raise ToolError(f"not found: {path}")
            if target.is_file():
                return _label(session, target)

            depth = max(0, min(int(depth), 4))
            lines: list[str] = []

            def walk(directory: Path, prefix: str, level: int) -> None:
                if level > depth or len(lines) >= MAX_LIST_ENTRIES:
                    return
                entries = sorted(
                    directory.iterdir(), key=lambda item: (item.is_file(), item.name.lower())
                )
                for entry in entries:
                    if len(lines) >= MAX_LIST_ENTRIES:
                        return
                    if entry.name in SKIP_DIRS:
                        continue
                    if entry.name.startswith(".") and entry.name != ".env.example":
                        continue
                    suffix = "/" if entry.is_dir() else ""
                    lines.append(f"{prefix}{entry.name}{suffix}")
                    if entry.is_dir():
                        walk(entry, prefix + "  ", level + 1)

            walk(target, "", 1)
            header = f"{_label(session, target) or '.'} ({len(lines)} entries)"
            return _truncate(header + "\n" + "\n".join(lines))

        return execute

    @tool
    def read_file():
        async def execute(path: str, start_line: int = 1, max_lines: int = 200) -> str:
            """Read a text file from the case repository or the skill.

            Args:
                path: Path relative to the case root, or `skill/SKILL.md` for the skill.
                start_line: First line to return (1-based).
                max_lines: Maximum number of lines to return (1-400).
            """
            target = _resolve(session, path)
            if not target.is_file():
                raise ToolError(f"not a file: {path}")
            size = target.stat().st_size
            if size > MAX_READ_BYTES:
                raise ToolError(
                    f"{path} is {size} bytes; read a range with start_line/max_lines "
                    "or use search_files"
                )
            try:
                text = target.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                raise ToolError(f"cannot read {path}: {exc}") from exc

            lines = text.splitlines()
            start = max(1, int(start_line))
            count = max(1, min(int(max_lines), 400))
            end = min(len(lines), start - 1 + count)
            body = [f"{number:>5}  {lines[number - 1]}" for number in range(start, end + 1)]
            header = f"{_label(session, target)} lines {start}-{end} of {len(lines)}"
            return _truncate(header + "\n" + "\n".join(body))

        return execute

    @tool
    def search_files():
        async def execute(
            pattern: str,
            path: str = ".",
            glob: str | None = None,
            max_results: int = 60,
        ) -> str:
            """Search file contents with a regular expression.

            Args:
                pattern: Python regular expression to match against each line.
                path: File or directory relative to the case root.
                glob: Optional filename filter, for example `*.go` or `*.ts`.
                max_results: Maximum number of matching lines (1-200).
            """
            try:
                regex = re.compile(pattern)
            except re.error as exc:
                raise ToolError(f"invalid regular expression: {exc}") from exc
            target = _resolve(session, path)
            if not target.exists():
                raise ToolError(f"not found: {path}")

            limit = max(1, min(int(max_results), 200))
            hits: list[str] = []
            for file_path in _iter_files(target):
                if glob and not fnmatch.fnmatch(file_path.name, glob):
                    continue
                try:
                    content = file_path.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                for number, line in enumerate(content.splitlines(), start=1):
                    if regex.search(line):
                        hits.append(f"{_label(session, file_path)}:{number}: {line.strip()[:220]}")
                        if len(hits) >= limit:
                            return _truncate("\n".join(hits) + "\n... result limit reached")
            return _truncate("\n".join(hits) if hits else f"no matches for {pattern!r}")

        return execute

    @tool
    def run_logistic_map():
        async def execute(args: list[str], timeout: int = 120) -> str:
            """Run the logistic_map.py CLI against this case's database.

            Args:
                args: CLI arguments without `--db`, for example
                    ["add-node", "--kind", "input", "--id", "input:get-products",
                    "--file", "controller/controller.go", "--ref", "controller/controller.go:58"].
                timeout: Seconds before the command is aborted (max 240).
            """
            if not args:
                raise ToolError("args must not be empty")
            command = [str(item) for item in args]
            if any(
                item in ("--db", "--flow-db") or item.startswith(("--db=", "--flow-db="))
                for item in command
            ):
                raise ToolError("do not pass --db or --flow-db; the harness owns the database")
            command_name = command[0]
            if command_name in BLOCKED_CLI_COMMANDS:
                raise ToolError(
                    f"`{command_name}` is not available: {BLOCKED_CLI_COMMANDS[command_name]}"
                )
            text = session.run_cli(command, timeout=max(1, min(int(timeout), 240)))
            return _truncate(text)

        return execute

    @tool
    def apply_graph():
        async def execute(graph: dict, timeout: int = 120) -> str:
            """Apply a batch of contracts, fields and edges in one transaction.

            Prefer this over many `add-node`/`add-field`/`add-edge` calls. The
            payload uses the skill's `apply` format:

            {
              "nodes": [
                {"kind": "entrypoint", "id": "entrypoint:get-products",
                 "name": "GET /catalog/products", "description": "HTTP GET /catalog/products",
                 "cluster": "catalog-service",
                 "file_path": "controller/controller.go",
                 "refs": ["controller/controller.go:58"]},
                {"kind": "input", "id": "input:get-products",
                 "name": "GET /catalog/products", "cluster": "catalog-service",
                 "file_path": "controller/controller.go",
                 "refs": ["controller/controller.go:58"]},
                {"kind": "entry", "id": "entry:product-list",
                 "name": "product list", "stage": "response",
                 "cluster": "catalog-service",
                 "file_path": "controller/controller.go"},
                {"kind": "storage", "id": "storage:catalog",
                 "location": "mysql://catalog.products", "store_type": "sql",
                 "cluster": "catalog-service",
                 "file_path": "repository/repository.go"},
                {"kind": "flow", "id": "flow:list-products",
                 "name": "List products", "description": "HTTP read path",
                 "cluster": "catalog-service"}
              ],
              "fields": [
                {"owner": "input:get-products", "path": "tags", "type": "string",
                 "file_path": "controller/controller.go",
                 "refs": ["controller/controller.go:61"]}
              ],
              "edges": [
                {"kind": "trigger", "from": "entrypoint:get-products",
                 "to": "flow:list-products"},
                {"kind": "use", "from": "flow:list-products",
                 "to": "input:get-products"},
                {"kind": "use", "from": "flow:list-products",
                 "to": "entry:product-list"},
                {"kind": "use", "from": "flow:list-products",
                 "to": "storage:catalog"},
                {"kind": "transform", "flow_id": "flow:list-products",
                 "from": "input:get-products", "to": "entry:product-list",
                 "note": "database rows -> API response"},
                {"kind": "flow", "flow_id": "flow:list-products",
                 "from": "entry:product-list", "to": "storage:catalog"}
              ]
            }

            When an outer activation starts another entrypoint, add:
            {"kind": "related", "from": "entrypoint:screen",
             "to": "entrypoint:http", "description": "screen invokes the route",
             "refs": ["ui/screen.tsx:42"]}.

            For an enforced limit or condition, add a rule node and attach it:
            {"kind": "rule", "id": "rule:page-limit",
             "name": "Page limit", "cluster": "catalog-service",
             "description": "A page returns at most 10 products",
             "refs": ["controller/controller.go:58"]}
            and {"kind": "rule", "from": "flow:list-products",
             "to": "rule:page-limit"}.

            Args:
                graph: Object with optional `nodes`, `fields` and `edges` lists.
                timeout: Seconds before the command is aborted (max 240).
            """
            payload = graph
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError as exc:
                    raise ToolError(f"graph is not valid JSON: {exc}") from exc
            if not isinstance(payload, dict):
                raise ToolError("graph must be an object with nodes/fields/edges")
            for key in ("nodes", "fields", "edges"):
                value = payload.get(key)
                if value is not None and not isinstance(value, list):
                    raise ToolError(f"`{key}` must be a list")
            if not any(payload.get(key) for key in ("nodes", "fields", "edges")):
                raise ToolError("graph has no nodes, fields or edges")

            session.apply_count += 1
            path = session.root / f"apply-{session.apply_count}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            text = session.run_cli(
                ["apply", "--file", str(path), "--json"],
                timeout=max(1, min(int(timeout), 240)),
            )
            return _truncate(text)

        return execute

    return [list_files(), read_file(), search_files(), run_logistic_map(), apply_graph()]
