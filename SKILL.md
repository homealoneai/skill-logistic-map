---
name: logistic-map
description: Build, query and audit a Kuzu graph of application data flow as a logistics process — EntryPoint activations, Flow paths, Input, Entry, Storage and Field contracts, joins, and the paths data takes. Use when asked to map data movement between services, queues, endpoints, jobs and storages; to find contract mismatches between producer and consumer; to trace a field; or to query, report on, or render the graph.
---

# Logistic map

Data flow as a logistics process: **Input** is the cargo at the door, **Entry** is
an intermediate form the cargo takes inside the application, **Storage** is the
warehouse, and **Field** trees are the cargo manifest. Every node is a **data
entity**, never a function or class.

A repository-local graph database (Kuzu) records **which data exists, how it
changes, and which fields each entity carries**. The point is contract
visibility: a field that the producer sends but the consumer never reads, or a
field whose type disagrees between two contracts, becomes a query result instead
of a production surprise.

All writes go through `scripts/logistic_map.py`. The graph is disposable: rebuild it
from the JSON snapshot or re-run the mapping, it is not a source of truth by
itself.

## Setup

Run from the repository root. `SKILL_DIR` is the directory that contains this
`SKILL.md`; a skill loader usually reports it, and otherwise set it to where the
skill was cloned.

```sh
SKILL_DIR=/path/to/logistic-map   # directory containing SKILL.md
PY=.logistic-map/.venv/bin/python

# one-time: venv + kuzu + schema (idempotent)
bash "$SKILL_DIR/scripts/bootstrap.sh"

# later sessions: just confirm the database
"$PY" "$SKILL_DIR/scripts/logistic_map.py" doctor
```

The database lives at `.logistic-map/graph.kuzu`; the venv at `.logistic-map/.venv`.
Override with `--db PATH` or `LOGISTIC_MAP_DB`. Kuzu is installed with pip inside
the project venv, so nothing global changes. Nothing here depends on a specific
agent harness: the CLI is plain Python and `render` produces a static HTML file.

## Model

```
Input ──contains──▶ Field ◀──contains── Storage
  │                  ▲
  │ flow/transform   │ contains (nested)
  ▼                  │
Entry ──contains─────┘

EntryPoint ──trigger──▶ Flow ──use──▶ Input|Entry|Storage
EntryPoint ──related──▶ EntryPoint  (directional activation relation)
Flow|EntryPoint ──rule──▶ Rule       (limit or condition, no data)
whole ──compose──▶ part              (composition, not execution order)
source ──flow {flow_id}──▶ next      (data passes unchanged)
source ──transform {flow_id}──▶ result  (data changes shape)
```

| Node | Meaning | Kind-specific attributes |
|---|---|---|
| `EntryPoint` | Code-level activation relevant to the map: route, consumer, cron job, file reader, UI screen/action | `description`, `cluster`, `file_path`, `refs` |
| `Input` | Data that enters the application: queue message, HTTP payload, file contents, or gRPC request | `cluster` |
| `Entry` | An **intermediate data entity**: validated/normalized/enriched payload, DTO, view model | `stage`, `cluster` |
| `Storage` | Persisted data: database, Redis, object store, file (never an external service) | `location`, `store_type`, `cluster` |
| `Flow` | A named path that groups the entities it uses and their passages | `name`, `description`, `cluster` |
| `Rule` | A limit or condition followed by a flow or entrypoint | `description`, `cluster` |
| `Field` | One field of one entity; can contain nested fields | `path`, `type`, `required`, `source` |

| Edge | From → To | Meaning |
|---|---|---|
| `contains` | entity → field, field → field | field membership and nesting |
| `use` | Flow → entity | this flow uses this entity |
| `trigger` | EntryPoint → Flow | this code-level entrypoint starts this flow |
| `related` | EntryPoint → EntryPoint | directional activation relationship with `description` and `refs` |
| `rule` | Flow/EntryPoint → Rule | the activation follows this limit or condition |
| `compose` | entity → entity | **whole → part**: this entity is composed of that one |
| `transform` | entity → entity, with `flow_id` | the data changes shape; `note` records the mapping |
| `flow` | entity → entity, with `flow_id` | data passes without changing shape |

`compose` never means "then". Execution order belongs to a **Flow**, which you
register explicitly; `flow_id` on every passage identifies that flow. The same
data pair may participate in several flows, so a passage is identified by
`(kind, from, to, flow)`.

`related` links activation points without carrying data. It is directional and
identified by `(from, to, description)`, so a screen → HTTP relation and the
HTTP → screen relation are different rows, and the same pair can have several
relations with different descriptions. Use it when an outer activation starts a
different entrypoint instead of a flow directly.

`rule` records behaviour that the activation follows, without creating a
Markdown document for it: a result limit, timeout, pagination, authentication
requirement, feature flag, cache policy or validation. A rule is a node with a
required `description`, and it may be attached to one or several flows and
entrypoints. It is not a data entity and never appears in a passage or `Impact`.

Every node and edge carries `repo`, `commit_sha`, `file_path` and `refs`
(`file:line` strings). Those references are how the report and a human get back
to the code. Two distinct entities may point at the same file.

`cluster` is a single free-form label used to group nodes by the logic that
matters for the map: a service, repository, module, deployable unit or bounded
context. Use one stable value per boundary (`orders-service`, `checkout-ui`,
`billing`) and set it on contracts, flows and entrypoints; a field inherits its
owner's cluster. `report` warns when an entrypoint and the flow it triggers are
in different clusters, and `render` exposes a cluster dropdown that filters the
whole graph.

IDs: entrypoints are `entrypoint:<slug>`, entities are `input:<slug>`,
`entry:<slug>`, `storage:<slug>`, flows are `flow:<slug>`; fields are
`<owner-id>#<dotted.path>` (generated by the CLI). Use
stable slugs, not display names.

## Workflow

1. Identify code-level activation mechanisms (HTTP routes, queue consumers,
   cron/jobs, file readers) separately from the data they receive. Record an
   `EntryPoint` only when code evidence exists; `Entry` remains a data shape.
2. **Register entrypoints and data contracts** with `add-entrypoint` and
   `add-node`; use `--location` (storage), `--stage` (entry), `--cluster`,
   `--file` and `--ref` as appropriate.
3. **Register fields** with `add-field` and dotted paths (`customer.id`). Parent
   fields are created automatically as `inferred`; declare them explicitly when
   their type is known.
4. **Register the flow** with `add-flow` (set its `--cluster`), connect each
   activator using
   `add-edge --kind trigger --from entrypoint:<slug> --to flow:<slug>`, then
   `flow use --id <flow> --entity
   <id>` for every entity on the path (including entities that only pass data
   through). If an outer activation such as a screen starts another entrypoint
   instead of a flow, link them with `add-edge --kind related` and a
   `--description`.
5. **Link** entities with `add-edge --kind flow --flow <flow>` when data passes
   unchanged, `--kind transform --flow <flow>` when the payload changes shape
   (always with a `--note`), and `--kind compose` only when one entity is
   literally made of others.
6. **Register rules** that the code enforces with `add-rule` and attach each one
   with `add-edge --kind rule` to the Flow or EntryPoint it constrains. Rules
   replace ad-hoc documentation for limits, timeouts, pagination, auth, feature
   flags, cache policy and validation.
7. **Audit** with `report`; treat warnings as questions for the code, not as
   facts. Fix the graph when the code says the report is wrong.
8. **Publish**: `export` for a git-friendly snapshot, `export-md` for Markdown
   docs a coding agent can read, and `render` for a standalone HTML graph. The
    HTML has lanes for **EntryPoints**, **Flows** and **Rules**; selecting an
    EntryPoint follows `related` entrypoints and highlights the flows they reach,
    and the **cluster** dropdown filters the graph to one service, repository or
    other grouping label. With an entity selected, turn on **Impact** to
    highlight its downstream data path.

```sh
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-node --kind input \
  --id input:orders.created --name orders.created \
  --cluster orders-service \
  --file services/orders/src/consumer.py \
  --ref services/orders/src/consumer.py:24 --ref infra/queues.yaml:12

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-entrypoint \
  --id entrypoint:orders-created --name "orders.created consumer" \
  --description "Consumes the orders.created queue message" \
  --cluster orders-service \
  --file services/orders/src/consumer.py \
  --ref services/orders/src/consumer.py:24

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-field \
  --owner input:orders.created --path customer.email --type string \
  --file services/orders/src/consumer.py --ref services/orders/src/consumer.py:30

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-node --kind entry \
  --id entry:order-validated --name order-validated --stage validated \
  --file services/orders/src/validate.py \
  --ref services/orders/src/validate.py:40

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-flow \
  --id flow:create-order --name "Create order" \
  --description "Receive, validate and persist an order" \
  --file services/orders/src/validate.py

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind trigger \
  --from entrypoint:orders-created --to flow:create-order

"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow use \
  --id flow:create-order --entity input:orders.created
"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow use \
  --id flow:create-order --entity entry:order-validated

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind transform \
  --flow flow:create-order \
  --from input:orders.created --to entry:order-validated \
  --note "email lowercased; total: string -> number" \
  --ref services/orders/src/validate.py:40

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-node --kind storage \
  --id storage:orders --location postgres://orders.orders --store-type sql \
  --file services/orders/src/repository.py

"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow use \
  --id flow:create-order --entity storage:orders
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind flow \
  --flow flow:create-order \
  --from entry:order-validated --to storage:orders \
  --file services/orders/src/repository.py

"$PY" "$SKILL_DIR/scripts/logistic_map.py" report
"$PY" "$SKILL_DIR/scripts/logistic_map.py" render
```

For a full bootstrap of a known flow, write one JSON file and apply it in a
transaction: `apply --file flow.json`. The format matches `export`, so a
snapshot can be edited and re-applied. See `examples/orders-flow.json`.

## EntryPoints and flows

An EntryPoint is the outer activation of one or more flows: an external HTTP
route, queue consumer, cron/job, file/asset load, CLI command, or UI screen/action.
It starts a Flow with `trigger` and may reach another EntryPoint through a
directional `related`; it never connects to data entities. Internal collaborators
(ViewModels, use cases, repositories, mediators, mappers, outbound HTTP clients)
are participants, not EntryPoints.

A flow is explicit, not derived. It has a description and a set of entities it
uses; its path is the chain of `flow`/`transform` passages carrying its
`flow_id`. `compose` is containment and never part of a path.

```sh
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-flow \
  --id flow:create-order --name "Create order"
"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow use \
  --id flow:create-order --entity input:orders.created
"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow list
"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow show flow:create-order
```

A flow may use a subset of the graph; an entity may participate in several
flows. A passage with several incoming feeds is a join. Different entities may
share `file_path`/`refs` — that is evidence, not identity. Details in
`references/flows.md`.

### Rules on a flow

```sh
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-rule \
  --id rule:search-limit --name "Search result limit" \
  --description "The search flow returns at most 10 products" \
  --cluster catalog-service --ref controller/controller.go:58
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind rule \
  --from flow:search-products --to rule:search-limit \
  --ref controller/controller.go:58
```

## Branches

Use a branch when the graph change must be reviewed or applied as one unit:

```sh
"$PY" "$SKILL_DIR/scripts/logistic_map.py" branch create checkout-rules
# ... edits run against the active branch ...
"$PY" "$SKILL_DIR/scripts/logistic_map.py" branch apply checkout-rules
```

- Every branch starts from `master`, never from another branch.
- `branch apply` replaces `master` with the branch; there is no merge.
- Applying a branch marks every other open branch `stale`.
- Editing `master` while branches are open also marks them `stale`.
- Stale or applied branches cannot be used or applied; create a new branch from
  the current master instead.
- `branch list`, `branch status` and `branch drop` inspect or discard branches.

## Rules

- **CLI is the only writer.** `query` is guarded read-only; never edit the Kuzu
  directory by hand. `export` + `apply` is the supported bulk path.
- **Always record `--file`, `--ref`, `--repo`/`--commit` when known.** Without
  references the report is much less useful; `--check-files` verifies them.
- **Use `--cluster` for the grouping that matters.** Give contracts, flows and
  entrypoints one stable label for their service, repository, module or
  deployable boundary (`--cluster orders-service`). Do not create one cluster per
  node; fields inherit their owner's cluster. `report` warns when an entrypoint
  and the flow it triggers belong to different clusters.
- **Rules live in the graph.** Record an enforced limit or condition with
  `add-rule` and attach it with `add-edge --kind rule` instead of writing a
  separate Markdown note. A rule needs a `--description` and code evidence, and
  may constrain more than one flow or entrypoint.
- **A field belongs to one contract.** The same logical path in two contracts is
  two `Field` nodes, compared by `path` during `report` — do not merge them into
  one node to "fix" a conflict.
- **`compose` is containment, `flow` is passage, `transform` is change.**
  `compose` never expresses execution order; a pipeline is a chain of
  `flow`/`transform` passages inside a **Flow**. Every `transform` needs a
  `--note` or a `--ref` explaining the mapping; `report` warns when it has
  neither.
- **Every passage belongs to a flow.** `add-edge --kind flow|transform` requires
  `--flow`; both endpoints must be listed with `flow use`. A passage is
  identified by `(kind, from, to, flow)`, so the same pair can appear in several
  flows.
- **`Entry` is data, not code.** Do not register a function, handler or class as
  an `Entry`; register the payload shape it produces. Even if the content does
  not change, two positions on the path are **two distinct entities** and may
  point at the same file.
- **`EntryPoint` is the outer activation, not any method that calls something.**
  Register the place where execution enters the mapped scope: an external route,
  queue consumer, scheduled job, file/asset load, CLI command, or UI
  screen/action. Do not register internal ViewModels, use cases, repositories,
  mediators, mappers or outbound HTTP client methods as EntryPoints; they are
  participants inside the flow. One user-visible operation is normally one flow;
  cache hit/miss and conditional branches stay inside it instead of becoming
  separate flows. An outer activation that starts another entrypoint is linked
  with a directional `related` edge carrying `--description` and `--ref`; never
  use `related` for data movement.
- **Do not model framework glue as data.** UI state objects, navigation
  arguments, React props, or a class just because it exists are not payload
  contracts. Register the data that crosses a boundary or is persisted.
- **Run `report` after every mapping session.** Use `--fail-on warn` in CI or
  before a large review; default exit code is 0.
- **Keep the snapshot.** `export --out .logistic-map/exports/graph.json` and commit
  it when the team wants the map in review; the database itself stays ignored.

## Reference files

Read only what the task needs:

| File | Use it for |
|---|---|
| `references/schema.md` | Exact tables, properties, id rules, edge matrix, invariants |
| `references/cli.md` | Every command, flag, output shape and exit code |
| `references/modeling.md` | How to decide what is a node/field/edge; multi-service and shared-field cases |
| `references/report-rules.md` | Every finding code, its severity and how to act on it |
| `references/flows.md` | Authored flows: USE, passages, path, joins, commands |

## Failure modes

| Symptom | Fix |
|---|---|
| `kuzu is not installed` | `bash "$SKILL_DIR/scripts/bootstrap.sh"` |
| `no logistic-map database` | `"$PY" .../logistic_map.py init` |
| `uses vN of the schema` | `"$PY" .../logistic_map.py migrate` (old passages become unassigned; add a flow and re-add them) |
| database lock / IO error | another process holds the DB; close it and retry |
| report floods `missing_field` | `flow` means the shape should be equal; narrow the graph or use `transform` with a note when it really changes |
| `field ... has no contains edge` | delete/re-add the field; the owner link is missing |
