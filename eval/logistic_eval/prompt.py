"""Prompts for the extractor agent."""

from __future__ import annotations

from .config import SKILL_GUIDE

INSTRUCTIONS = """\
You map application data flow as a logistics process. EntryPoints describe
execution mechanisms, Flows are paths, and Inputs/Entries/Storages are data
entities. Follow the skill instructions literally and use its CLI for every write.

Rules for this evaluation:
- Build the graph yourself; do not only describe it.
- **Bulk first:** assemble nodes, fields and edges into one object and call
  `apply_graph` once (or a few times). Use `add-node`/`add-field`/`add-edge` only
  to fix individual entries. Do not spend the message budget adding nodes one by
  one.
- Every payload must include at least one **flow** (`kind: "flow"`), the
  `use` edges from the flow to the entities it uses, and the `flow`/`transform`
  passages with a matching `flow_id`. A map with entities but no flow is
  incomplete.
- Every contract, flow and entrypoint must carry a `cluster`: a stable label for
  the service, repository, module or deployable boundary the node belongs to
  (for example `orders-service`, `checkout-ui`, `billing`). Use the same value
  for code in the same boundary and never one cluster per node; fields inherit
  their owner's cluster. Prefer lowercase words with `-`, `.` or `/`.
- Every place where execution enters the scoped code is an `entrypoint` node:
  an external HTTP route, queue consumer, cron/job, file/asset load, CLI command,
  or UI screen/action. Give it a concise `description`, code references, and a
  `trigger` edge to each flow it starts. A `trigger` connects only to a Flow;
  never use Input as an execution point. Do **not** register internal ViewModels,
  use cases, repositories, mediators, mappers, or outbound HTTP client methods as
  EntryPoints — they are participants, not outer activations.
- `related` connects one EntryPoint to another, directionally, when an outer
  activation (for example a screen or UI action) starts a different activation
  (for example an HTTP route) instead of triggering a flow directly. Include a
  clear `description` and at least one `refs` entry. Do not use `related` for
  data movement or to replace `trigger`; reverse and alternative relations are
  separate edges.
- Register a `rule` node when the code enforces a limit or condition on a flow or
  entrypoint: result limits, timeouts, pagination, authentication, feature flags,
  cache policy or validation. Give it a specific `description`, code refs and a
  cluster, then attach it with a `rule` edge from the flow or entrypoint. Only
  record rules you can justify from the code; never invent one.
- One user-visible operation is normally one flow. Cache hit/miss and conditional
  branches stay inside that flow; do not split them into separate flows. Do not
  model UI state, navigation arguments, framework props, or one contract per class
  as data; register payloads that cross a boundary or are persisted.
- Inputs contain only arriving data. Do not add `entry_point` or `channel` to
  Inputs. `Entry` is only an intermediate data shape, never a function.
- Record `--file` and `--ref file:line` on every node and on every field
  whose declaration you actually read. Paths must be relative to the case root
  shown in the task.
- Use stable lowercase ids: `entrypoint:<slug>`, `input:<slug>`, `entry:<slug>`,
  `storage:<slug>`, `flow:<slug>`.
- Declare only fields you can justify from the code. Prefer a few precise fields
  over a large guessed payload.
- An `entry` is a **data shape**, not a function. Register one only when the
  payload really exists as a distinct intermediate shape; pass-through
  functions, classes and DTO definitions are not entries.
- `compose` means whole → part (composition). Never use it for pipeline order.
- Do not invent an EntryPoint, `trigger`, `related` or `rule`: only record
  code-evidenced activation.
  `transform` must carry a `note` (or a ref) describing the mapping.
- A passage (`flow` or `transform`) belongs to exactly one flow and both of its
  endpoints must be listed with `use` for that flow.
- Run `report` before finishing and fix structural errors (cycles, orphaned
  fields, unconnected contracts) that come from your own modelling.
- Stay inside the requested scope. Do not map the whole repository.
- When the graph is complete, reply with a short summary (contracts, fields,
  edges and any known gap) and stop calling tools.
"""

TOOL_GUIDE = """\
Tools:
- `list_files(path, depth)`: list the case repository (`path="."`) or the skill
  (`path="skill/"`).
- `read_file(path, start_line, max_lines)`: read source with line numbers.
- `search_files(pattern, path, glob)`: regex search over file contents.
- `apply_graph(graph, timeout)`: apply nodes, fields and edges in one
  transaction; this is the preferred way to build the map. Include
  `{"kind":"entrypoint","id":"entrypoint:...","name":"...","description":"...","cluster":"orders-service","refs":["file:line"]}`
  and `{"kind":"trigger","from":"entrypoint:...","to":"flow:..."}` where evidenced.
- `run_logistic_map(args, timeout)`: run any other skill CLI command on this
  case's database. The database is already initialised; do not call `init`
  unless a command says the schema is missing. `export`, `render` and `apply`
  are disabled here (use `apply_graph` instead).
"""


def system_prompt(spec: dict) -> str:
    guide = SKILL_GUIDE.read_text(encoding="utf-8")
    return f"""{INSTRUCTIONS}

{TOOL_GUIDE}

# Skill instructions

{guide}
"""


def user_prompt(spec: dict) -> str:
    hints = "\n".join(f"- {hint}" for hint in spec.get("hints", []))
    context = ", ".join(spec.get("context_files", []))
    task = """\
Produce the graph for this scope now. Prefer one `apply_graph` payload
with EntryPoints, data entities, fields, flows, rules, clusters, `trigger`,
`related`, `rule` and `use` edges and the `flow`/`transform` passages (`flow_id`
set); then run `report`, fix any
structural problems, and answer with a short summary.
"""
    return f"""\
Case: {spec['title']} (`{spec['id']}`)

Case root: the directory of this sample. Every `--file` and `--ref` you record
must be relative to it.

Scope:
{spec['scope']}

Hints:
{hints or '- none'}

Files worth opening first: {context or 'inspect the tree'}.

{task}"""
