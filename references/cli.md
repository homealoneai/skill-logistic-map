# CLI reference

All commands: `python "$SKILL_DIR/scripts/logistic_map.py" <command>`, where
`SKILL_DIR` is the directory containing `SKILL.md`.
Global options `--db PATH` and `--json` are accepted before or after the
command. `--db` defaults to `$LOGISTIC_MAP_DB` or `.logistic-map/graph.kuzu`.
When a branch is active, `--db` still names the master database; graph commands
are routed to the branch automatically (see `branch`).

Exit codes: `0` success (and report under its threshold), `1` report threshold
crossed, `2` usage or runtime error. With `--json`, failures print
`{"ok": false, "error": "..."}` and successes print a JSON object.

## setup

| Command | What it does |
|---|---|
| `init` | Create the node and relationship tables, write `meta.json` and `.logistic-map/.gitignore`. Idempotent; it never rewrites the schema version of an older database. |
| `migrate` | Convert v1-v6 databases in place. v3 `Input.entry_point`/`channel` are dropped without guessing EntryPoints or triggers; v4 adds `RELATED` without inventing relations; v5 adds an empty `cluster` column; v6 adds the `Rule` node and `RULE_REL` relation. Idempotent. |
| `doctor` | Print Python, Kuzu version, DB path, tables, node/edge counts, detected git repo/commit. Exits 1 only when Kuzu is missing. |

`scripts/bootstrap.sh` creates `.logistic-map/.venv`, installs `kuzu>=0.11,<0.12`
and runs `init`. Use `--no-init` to skip initialization.

## writing

### add-node

```
add-node --kind input|entry|storage [--id ID] [--name NAME] [--description TEXT]
         [--stage TEXT]
         [--location TEXT] [--store-type TEXT] [--cluster LABEL]
         [--repo URL] [--commit SHA] [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates a contract node (upsert). `--id` is optional; without it the
id is `<kind>:<slugified name>`. Updates preserve attributes you do not pass.
`--repo` and `--commit` default to the current git origin and `HEAD`.
`--cluster` is a free-form grouping label (service, repository, module or
deployable boundary), for example `orders-service` or `checkout-ui`.

`--stage` only applies to `entry`; `--location` and `--store-type` only to
`storage`. `Input` is data only; activation belongs to an EntryPoint.

```
add-node --kind storage --id storage:orders-db --name orders-db \
  --location postgres://orders.orders --store-type sql \
  --cluster orders-service \
  --file services/orders/src/repository.py
```

### add-field

```
add-field --owner CONTRACT_ID --path a.b.c [--name NAME] [--type TYPE]
          [--description TEXT] [--required]
          [--repo URL] [--commit SHA] [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates the field and every missing ancestor. `type` is free text
(`string`, `uuid`, `decimal(12,2)`, `array`, ...); `report` normalizes common
aliases when comparing. `--required` sets the flag to true; omit it to leave the
current value. Ancestors are created with `source=inferred`; declaring one
explicitly sets `source=declared`.

### add-flow

```
add-flow [--id flow:SLUG] [--name NAME] [--description TEXT] [--cluster LABEL]
         [--repo URL] [--commit SHA] [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates a `Flow` node. Without `--id`, the id is
`flow:<slugified name>`. The flow does not define order by itself: its entities
are attached with `flow use`, and its path is made of `flow`/`transform`
passages carrying its `flow_id`.

```sh
add-flow --id flow:create-order --name "Create order" \
  --description "Receive, validate and persist an order" \
  --cluster orders-service \
  --file services/orders/src/handler.py
```

### add-entrypoint

```
add-entrypoint [--id entrypoint:SLUG] [--name NAME] --description TEXT
               [--cluster LABEL]
               [--repo URL] [--commit SHA] [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates a code-level activation mechanism. It may trigger flows with
`trigger` and relate to other entrypoints with `related`, but connects to no data
entities. Record source evidence in `--file` and `--ref`. `--cluster` groups it
with the service or UI it belongs to.

```sh
add-entrypoint --id entrypoint:orders-created --name "orders.created consumer" \
  --description "Consumes orders.created messages" \
  --cluster orders-service --ref consumer.py:24
add-edge --kind trigger --from entrypoint:orders-created --to flow:create-order
```

```sh
add-entrypoint --id entrypoint:orders-screen --name "Orders screen" \
  --description "UI screen that submits the order form" \
  --cluster orders-ui --ref ui/orders.tsx:12
add-edge --kind related --from entrypoint:orders-screen \
  --to entrypoint:orders-http \
  --description "screen submits the order form" --ref ui/orders.tsx:42
```

### add-rule

```
add-rule [--id rule:SLUG] [--name NAME] --description TEXT
         [--cluster LABEL]
         [--repo URL] [--commit SHA] [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates a rule: a limit or condition followed by a flow or
entrypoint. `--description` is required and is the rule content. Attach it with
`add-edge --kind rule`; the same rule may constrain several flows and
entrypoints.

```sh
add-rule --id rule:search-limit --name "Search result limit" \
  --description "The search flow returns at most 10 products" \
  --cluster catalog-service --ref controller/controller.go:58
add-edge --kind rule --from flow:search-products --to rule:search-limit
```

### flow use / flow unuse

```
flow use --id FLOW --entity CONTRACT [--note TEXT] [--file PATH] [--ref ...]
flow unuse --id FLOW --entity CONTRACT
```

`flow use` creates the `Flow → entity` (`use`) edge; `flow unuse` removes it.
Every endpoint of a passage should be connected with `use`; `report` flags
passages whose endpoints are missing.

### add-edge

```
add-edge --kind contains|compose|transform|flow|use|trigger|related|rule --from ID --to ID
         [--flow FLOW] [--description TEXT] [--note TEXT]
         [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates one relationship (upsert). `contains` is normally created by
`add-field`; use it directly only to attach references to an existing
containment.

- `compose`: whole → part (composition);
- `transform`: the data changes shape; document it with `--note` and/or `--ref`;
- `flow`: the data passes unchanged;
- `use`: `Flow → entity`; `--from` is the flow and `--to` the entity.
- `trigger`: `EntryPoint → Flow`; the endpoints are validated strictly.
- `related`: `EntryPoint → EntryPoint`; requires `--description` and at least
  one `--ref`. It is identified by `(from, to, description)`, so both directions
  and several descriptions between the same pair coexist.
- `rule`: `Flow|EntryPoint → Rule`; the endpoints are validated strictly.

`flow` and `transform` **require `--flow`**. The same pair may appear in several
flows; the passage is identified by `(kind, from, to, flow)`.

A passage left **unassigned** by `migrate` (empty `flow_id`) is adopted when you
add it again with `--flow`; no duplicate is created. `remove-edge --kind
flow|transform` without `--flow` removes the unassigned passage.

```sh
add-edge --kind transform --flow flow:create-order \
  --from input:orders.created --to entry:order-validated \
  --note "total: string -> number; email lowercased" \
  --ref services/orders/src/validate.py:40
```

A pipeline is a chain of `flow`/`transform` passages inside a flow; never use
`compose` for execution order.

### apply

```
apply --file flow.json
```

Transactional batch upsert. Accepts the `export` shape:

```json
{
  "nodes": [
    {"kind": "input", "id": "input:orders.created", "name": "orders.created",
     "file": "services/orders/src/consumer.py", "refs": ["services/orders/src/consumer.py:24"]},
    {"kind": "flow", "id": "flow:create-order", "name": "Create order"},
    {"kind": "field", "owner": "input:orders.created", "path": "total", "type": "string"}
  ],
  "edges": [
    {"kind": "use", "from": "flow:create-order", "to": "input:orders.created"},
    {"kind": "transform", "flow_id": "flow:create-order",
     "from": "input:orders.created", "to": "entry:order-validated",
     "note": "total: string -> number"}
  ]
}
```

EntryPoints, entities and flows are inserted before fields, and all nodes before edges. A
failure rolls the whole batch back. `fields: [{owner, path, ...}]` is also
accepted. Legacy v1 `entry_point`/`entry_type` attributes on `entry` nodes are
ignored; Input activation fields are no longer accepted. Passages without `flow_id` are accepted and flagged by `report` as
unassigned.

### remove-node / remove-edge

```
remove-node --id input:orders.created
remove-edge --kind flow --from entry:order-validated --to storage:orders
remove-edge --kind related --from entrypoint:orders-screen \
  --to entrypoint:orders-http --description "screen submits the order form"
```

`remove-node` also deletes the fields owned by that contract. Edges are deleted
with the node. A `related` edge needs `--description` so the exact relation is
removed; the reverse direction is a separate edge.

## reading

### list

```
list [--kind entrypoint|input|entry|storage|flow|rule|field] [--owner CONTRACT_ID]
     [--cluster LABEL] [--search TEXT]
```

Prints one line per node: id, path/type, stage, location, flow or entrypoint
description, file and cluster. `--owner` filters fields by contract;
`--cluster` filters by cluster label (fields inherit their owner's cluster).

### query

```
query --cypher "MATCH ..." [--param key=json ...] [--max-rows 100]
```

Read-only guard: rejects `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH`, `DROP`,
`ALTER`, `COPY`, transaction statements and unsafe `CALL`s. `CALL show_tables`,
`table_info`, `db_version` and `current_setting` are allowed.

Parameters use JSON values:

```sh
--param id='"input:orders.created"'      # string needs JSON quotes
--param names='["a","b"]'                # list
```

Useful queries:

```cypher
-- every field of one contract
MATCH (c:Input {id: $id})-[:CONTAINS]->(f:Field)
RETURN f.path, f.type, f.required ORDER BY f.path

-- contracts that produce a given field path
MATCH (f:Field {path: 'customer.email'})<-[:CONTAINS]-(c)
RETURN c.id, c.kind

-- full path of one flow from an input to a storage
MATCH p = (i:Input {id: $id})-[r:FLOWS|TRANSFORM*1..5]->(s:Storage)
WHERE all(rel IN relationships(p) WHERE rel.flow_id = $flow)
RETURN s.id, length(p)

-- fan-out of an input inside a flow
MATCH (i:Input)-[r:FLOWS|TRANSFORM]->(x)
WHERE r.flow_id = $flow
RETURN i.id, count(x) AS downstream ORDER BY downstream DESC

-- entities used by a flow
MATCH (f:Flow {id: $flow})-[:USES]->(n)
RETURN n.id, n.kind ORDER BY n.id
```

### report

```
report [--format text|md|json] [--fail-on none|error|warn|info] [--check-files]
```

Runs every rule in `report-rules.md`. `--fail-on error` exits 1 only when there
is at least one error; `--fail-on warn` also fails on warnings. `--check-files`
verifies `file_path` and the path part of each ref against the current working
directory. See `report-rules.md` for codes and severities.

### stats

```
stats [--json]
```

Counts nodes by kind, edges by kind, entrypoints, storage types and the ten
contracts with the most fields.

### export

```
export [--out PATH]        # default: .logistic-map/exports/graph.json
```

Writes the snapshot described in `schema.md`. Commit it when the team wants the
map reviewed with code changes; the Kuzu directory stays ignored.

### export-md

```
export-md [--out DIR] [--title TEXT]
```

Writes Markdown documentation aimed at coding agents. Default output is
`docs/logistic-map`:

```
README.md                    relation semantics, entity index, flow index
entities/<kind>/<slug>.md    attributes, fields, relations, composition, evidence, flows
flows/<slug>.md              path tree, feeds, context, joins
```

Text is deterministic for the same graph. The output directory is treated as
managed: `entities/**/*.md` and `flows/**/*.md` are rewritten and stale pages are
removed. Flows come from the `Flow` nodes in the main graph; the index links each
one and its entities.

```sh
python scripts/logistic_map.py export-md \
  --out docs/logistic-map --title "Orders map"
```

### render

```
render [--out PATH] [--title TEXT]   # default: .logistic-map/graph.html
```

Writes a standalone HTML file (no network, no CDN) with a single graph view.

Nodes sit in horizontal lanes: **EntryPoints** above **Flows**, with **Rules**
beside both in a column spanning their two lanes; then Input, Entry and one lane
per `store_type`. Fields are hidden by default; enable the **Fields** chip. Click a
node for attributes, references, fields, flows and connections, or click an
**edge** for its `note`, `refs`, file and endpoints.

Flows are regular nodes in the same view: `use` edges link each flow to the
entities it uses, and the flow panel lists its entities and `flow`/`transform`
passages. Selecting a flow highlights that set and dims everything else; click
empty space or an entity to clear it. `use` and `compose` are drawn but excluded
from **Impact**, which follows data movement only.

Search, zoom, pan, drag, and PNG export are built in.

With a contract selected, enable **Impact** to dim everything outside its
downstream reach. The reach follows `flow` and `transform` edges (not `compose`
or `contains`); the panel lists the affected contracts, how many fields they
hold, and the shortest paths to terminal contracts. Path entries are clickable,
so you can walk the flow. Selecting a field keeps the impact of its owner. A
terminal contract (nothing downstream) is not dimmed. An example rendering is in
`assets/preview.png`.

## JSON output

Mutation commands print `{"ok": true, "id": "...", "created": true|false}` (or
`from`/`to`/`kind` for edges). `list --json` prints the node objects.
`report --format json` prints the full report:

```json
{
  "summary": {"contracts": 6, "fields": 30, "edges": 35,
              "findings": {"error": 0, "warn": 3, "info": 6},
              "by_code": {"type_conflict": 1}},
  "findings": [
    {"code": "type_conflict", "severity": "warn", "message": "...",
     "nodes": ["input:orders.created", "entry:order-validated"],
     "edge": {"kind": "flow", "from": "input:orders.created",
              "to": "entry:order-validated"},
     "refs": ["services/orders/src/consumer.py:24"],
     "data": {"field": "total", "from_type": "string", "to_type": "number"}}
  ]
}
```


## flow

Flows are `Flow` nodes in the main graph, connected to their entities by `use`
edges. A flow's path is the set of `flow`/`transform` passages carrying its
`flow_id`; `compose` is ignored.

| Command | What it does |
|---|---|
| `add-flow [--id FLOW] [--name NAME] [--description TEXT]` | Create or update a flow node |
| `flow use --id FLOW --entity ID` | Add the `Flow → entity` relation |
| `flow unuse --id FLOW --entity ID` | Remove the relation |
| `flow list [--json]` | List flows with entity/passage counts |
| `flow show ID [--json]` | Print entities, passages, path and joins |
| `flow remove --id FLOW` | Delete the flow and its passages (entities stay) |

See `flows.md` for the model, roles, joins and report rules.


## branch

Branches are isolated local copies of the graph, used to prepare a set of
changes and apply them to `master` in one go. There is **no merge**: a branch
either applies as a whole or is rejected because the master moved.

```sh
branch create NAME
branch use NAME            # or: branch use master
branch list [--json]
branch status [--json]
branch apply NAME
branch drop NAME
```

| Command | What it does |
|---|---|
| `branch create NAME` | Copy `master` to a new branch, record its base revision and fingerprint, and switch to it |
| `branch use NAME` | Switch to an open branch; `master` switches back to the main line |
| `branch list` | Revision, active branch and every branch with status `open`, `applied` or `stale` |
| `branch status` | Active branch, master revision and node/edge counts |
| `branch apply NAME` | Replace `master` with the branch and bump the master revision |
| `branch drop NAME` | Delete the branch and its database |

Rules:

- Every branch starts from `master`; a branch can never start from another
  branch.
- Only open branches can be used or applied.
- `apply` verifies that the branch still matches the master it started from.
- Applying a branch marks every other open branch `stale`.
- Editing `master` directly while branches are open also marks them `stale`, so
  applying an old branch cannot drop newer master edits.
- Names must match `[a-z0-9][a-z0-9._-]*` (at most 60 characters); `master` is
  reserved.
- State lives in `<db>.branches.json`; branch databases are `<db>.<name>` and are
  local, untracked files. `--db` still points at the master path in every command.
