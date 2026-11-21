# Graph schema v7

Kuzu property graph. Seven node tables, eight relationship tables, all
directional. `init` creates them with `IF NOT EXISTS`, so it is safe to re-run.

Contracts are **data entities**: `Input` is data that arrives, `Entry` is an
intermediate entity inside the application, `Storage` is persisted data. A
`Flow` is an explicit named path that groups the entities it uses. `EntryPoint`
is a code-level activation mechanism: it connects to Flow with `trigger` and to
another EntryPoint with the directional `related` relation. `Rule` describes a
limit or condition that a Flow or EntryPoint follows.

## Node tables

Common properties on every node:

| Property | Type | Meaning |
|---|---|---|
| `id` | STRING (primary key) | Stable identifier; see ID rules |
| `name` | STRING | Human label (entry point name, table name, flow name) |
| `description` | STRING | Short explanation of the contract or flow |
| `cluster` | STRING | Grouping label on contracts, flows and entrypoints; `Field` inherits it |
| `repo` | STRING | Repository URL or path that owns the code |
| `commit_sha` | STRING | Commit the mapping was taken from |
| `file_path` | STRING | Primary file that implements the contract |
| `refs` | STRING[] | Reference file:line strings |

| Table | Extra properties |
|---|---|
| `Input` | — |
| `Entry` | `stage` |
| `Storage` | `location`, `store_type` |
| `Flow` | — |
| `EntryPoint` | — |
| `Rule` | — |
| `Field` | `path`, `type`, `required` (BOOLEAN), `source` (`declared` / `inferred`) |

`cluster` is a single free-form string. Use one stable value per boundary and
apply it to contracts, flows and entrypoints; a field has no own cluster and
inherits the one from its owner. Example values: `orders-service`,
`checkout-ui`, `github.com/acme/billing` or `module:users`.

`stage` examples: `validated`, `normalized`, `enriched`, `response`, `view`.
`store_type` examples: `sql`, `nosql`, `redis`, `object`, `file`, `cache`.

`Field.source = inferred` means the node was created automatically as an
ancestor of a dotted path (`customer` for `customer.id`) and its type is still
unknown. Declaring it with `add-field` turns it into `declared`.

## Relationship tables

All relationship rows carry `refs STRING[]` and `file_path STRING`; `compose`,
`transform`, `flow` and `use` rows also carry `note STRING`.

| Table | Allowed endpoints | Meaning |
|---|---|---|
| `CONTAINS` | `Input→Field`, `Storage→Field`, `Entry→Field`, `Field→Field` | Field membership and nesting |
| `COMPOSE` | every ordered pair of `Input`, `Entry`, `Storage` (9 pairs) | **Whole → part**: the source entity is composed of the target entity |
| `TRANSFORM` | every ordered pair of `Input`, `Entry`, `Storage` (9 pairs) | The data changes shape; `note` records the mapping |
| `FLOWS` | every ordered pair of `Input`, `Entry`, `Storage` (9 pairs) | The data passes to the next entity without changing shape |
| `USES` | `Flow→Input`, `Flow→Entry`, `Flow→Storage` | The flow uses this entity |
| `TRIGGER` | `EntryPoint→Flow` | The entrypoint starts the flow |
| `RELATED` | `EntryPoint→EntryPoint` | Directional activation relationship; carries `description` |
| `RULE_REL` | `Flow→Rule`, `EntryPoint→Rule` | The activation follows this limit or condition (exposed as the `rule` edge) |

`TRANSFORM` and `FLOWS` rows carry `flow_id STRING`. A passage is identified by
`(kind, from, to, flow_id)`, so the same pair can participate in several flows.
`COMPOSE` is containment, never execution order.

`RELATED` rows require `description STRING` and at least one `refs` entry. The
relation is identified by `(from, to, description)`, so the reverse direction
and alternative descriptions between the same pair are independent rows.
`RELATED` never carries data and is not part of `Impact`.

`RULE` attaches a `Rule` to a flow or entrypoint. A rule has a required
`description` and may be attached to several flows and entrypoints; it carries
no data and is not part of `Impact`. Kuzu binds table names case-insensitively,
so the physical relation is `RULE_REL` while the model and CLI call it `rule`.

`contains` edges between two fields must stay inside one contract: both ids must
share the same owner prefix.

## ID rules

```
   entrypoint:<slug>   input:<slug>   entry:<slug>   storage:<slug>   flow:<slug>
   rule:<slug>
<owner-id>#<path>                                           # fields
```

- The prefix decides the node table; `input:orders.created` and
  `entry:orders.created` are different nodes.
- Slugs are stable, lowercase, and contain no whitespace or `#`
  (`orders.created`, `validate-order`, `orders-db`, `create-order`).
- Field ids are generated: owner id + `#` + dotted path
  (`input:orders.created#customer.id`). Never invent field ids by hand.
- `path` segments use dots; array segments may keep brackets (`items[].sku`),
  but a plain `items.sku` is preferred and simpler to diff.
- Renaming an id is a new node. Prefer correcting attributes over renaming.

## Invariants

Enforced by the CLI:

- A field owner must exist and must be a contract node.
- Adding a field creates the `contains` chain owner → first segment → … → leaf.
- `compose` / `transform` / `flow` endpoints must be contract nodes; self edges
  are rejected.
- `flow` and `transform` passages require an existing `--flow`.
- `use` goes from an existing `Flow` to a contract node.
- `trigger` goes only from an existing `EntryPoint` to an existing `Flow`.
- `related` goes only from an existing `EntryPoint` to another `EntryPoint`;
  `description` and at least one `refs` entry are required.
- `rule` goes only from an existing `Flow` or `EntryPoint` to an existing
  `Rule`; `Rule` requires a description.
- `contains` cannot cross contract boundaries.
- Every node referenced by an edge must exist.

Checked by `report` (not enforced at write time, because partial maps are
legitimate while modelling):

- every passage has a `flow_id` and that flow exists;
- both endpoints of a passage are listed with `use` for that flow;
- a flow uses at least one entity and has at least one passage;
- no cycles in `compose` (containment) or inside a flow;
- every field has a `contains` edge;
- EntryPoints trigger a flow directly or reach one through `related`, and have
  code evidence; flows without triggers are reported; storages have their
  location;
- an entrypoint and the flow it triggers belong to the same cluster when both
  have one (`cluster_mismatch`);
- a rule is attached to a flow or entrypoint (`orphan_rule`), has a description
  and has code evidence;
- `transform` passages carry a note, file or ref explaining the mapping;
- fields declared by hand have a type;
- paths that exist on one side of a `flow` passage but not the other
  are reported as missing;
- types that normalize differently on the two sides are reported as conflicts.

## Snapshot format

`export` writes:

```json
{
  "schema_version": 7,
  "generated_at": "2026-11-17T11:20:00+00:00",
  "repo": "github.com/acme/logistics",
  "commit": "a1b2c3d",
  "nodes": [
    {"kind": "input", "id": "input:orders.created", "name": "orders.created",
     "cluster": "orders-service", "...": "..."},
    {"kind": "entry", "id": "entry:order-validated", "name": "order-validated",
     "stage": "validated", "cluster": "orders-service", "...": "..."},
    {"kind": "flow", "id": "flow:create-order", "name": "Create order",
      "cluster": "orders-service",
      "description": "Receive, validate and persist an order"},
    {"kind": "entrypoint", "id": "entrypoint:create-order",
     "name": "POST /orders", "description": "HTTP request",
     "cluster": "orders-service"},
    {"kind": "entrypoint", "id": "entrypoint:orders-screen",
     "name": "Orders screen", "description": "UI activation",
     "cluster": "orders-ui"},
    {"kind": "rule", "id": "rule:order-limit",
     "name": "Order item limit", "cluster": "orders-service",
     "description": "An order may contain at most 20 items"},
    {"kind": "field", "id": "input:orders.created#total", "owner": "input:orders.created",
     "path": "total", "type": "string", "...": "..."}
  ],
  "edges": [
    {"kind": "use", "from": "flow:create-order", "to": "input:orders.created"},
    {"kind": "trigger", "from": "entrypoint:create-order", "to": "flow:create-order"},
    {"kind": "rule", "from": "flow:create-order", "to": "rule:order-limit"},
    {"kind": "related", "from": "entrypoint:orders-screen",
     "to": "entrypoint:create-order",
     "description": "the screen submits the order form",
     "refs": ["ui/orders.tsx:42"]},
    {"kind": "use", "from": "flow:create-order", "to": "entry:order-validated"},
    {"kind": "transform", "flow_id": "flow:create-order",
     "from": "input:orders.created", "to": "entry:order-validated",
     "note": "total: string -> number"},
    {"kind": "flow", "flow_id": "flow:create-order",
     "from": "entry:order-validated", "to": "storage:orders"}
  ]
}
```

Empty attributes are omitted; `refs` is always present. `apply` accepts the same
shape plus `fields` as an alternative list of `{owner, path, ...}` objects, and
processes entities and flows before fields and edges, so owners exist. Legacy
v1 attributes on `Entry` (`entry_point`, `entry_type`) are ignored by `apply`.
Passages without `flow_id` are accepted by `apply` for migration compatibility
and flagged by `report` as `edge_without_flow`.

## Schema versioning

`schema_version` is stored in `<db-parent>/meta.json` and in every snapshot.
`init` never alters existing tables. A database with an older version is refused
by normal commands; run `migrate` to convert it in place:

- v1/v2 → v3: `Entry.entry_point` / `Entry.entry_type` are dropped, `FLOWS` and
  `TRANSFORM` are rebuilt with `flow_id` (preserving every row as an
  **unassigned** passage), and `Flow`/`USES` tables are created. v1 `COMPOSE`
  rows (which meant "pipeline") likewise become unassigned passages. `compose`
  then means composition only.
- v3 → v4: `Input.entry_point` and `Input.channel` are dropped; no EntryPoint or
  TRIGGER is guessed from them. Create and verify those anew from code evidence.
- v4 → v5: add the `RELATED` table. Existing nodes, triggers and passages are
  preserved; no relation is invented.
- v5 → v6: add `cluster STRING` to `Input`, `Entry`, `Storage`, `Flow` and
  `EntryPoint` with an empty default. Existing nodes and edges are preserved;
  no cluster is guessed.
- v6 → v7: add the `Rule` node table and the `RULE_REL` relation. Existing
  nodes and edges are preserved; no rule is invented.
- The old `flows.kuzu` database (`Flow`, `Step`, `FEEDS`) is obsolete and no
  longer read or written. Use `flow use` and `add-edge --flow` to describe the
  paths explicitly; `report` lists passages that still need a flow.
