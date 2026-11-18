# CLI reference

All commands: `python "$SKILL_DIR/scripts/logistic_map.py" <command>`, where
`SKILL_DIR` is the directory containing `SKILL.md`.
Global options `--db PATH` and `--json` are accepted before or after the
command. `--db` defaults to `$LOGISTIC_MAP_DB` or `.logistic-map/graph.kuzu`.

Exit codes: `0` success (and report under its threshold), `1` report threshold
crossed, `2` usage or runtime error. With `--json`, failures print
`{"ok": false, "error": "..."}` and successes print a JSON object.

## setup

| Command | What it does |
|---|---|
| `init` | Create the four node tables and three relationship tables, write `meta.json` and `.logistic-map/.gitignore`. Idempotent. |
| `doctor` | Print Python, Kuzu version, DB path, tables, node/edge counts, detected git repo/commit. Exits 1 only when Kuzu is missing. |

`scripts/bootstrap.sh` creates `.logistic-map/.venv`, installs `kuzu>=0.11,<0.12`
and runs `init`. Use `--no-init` to skip initialization.

## writing

### add-node

```
add-node --kind input|entry|storage [--id ID] [--name NAME] [--description TEXT]
         [--entry-point TEXT] [--channel TEXT] [--entry-type TEXT]
         [--location TEXT] [--store-type TEXT]
         [--repo URL] [--commit SHA] [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates a contract node (upsert). `--id` is optional; without it the
id is `<kind>:<slugified name>`. Updates preserve attributes you do not pass.
`--repo` and `--commit` default to the current git origin and `HEAD`.

```
add-node --kind storage --id storage:orders-db --name orders-db \
  --location postgres://orders.orders --store-type sql \
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

### add-edge

```
add-edge --kind contains|compose|transform --from ID --to ID
         [--note TEXT] [--file PATH] [--ref PATH:LINE ...]
```

Creates or updates one relationship (upsert). `contains` is normally created by
`add-field`; use it directly only to attach references to an existing
containment. `compose`/`transform` connect two contract nodes.

```
add-edge --kind transform --from input:orders.created --to entry:validate-order \
  --note "total: string -> number; email lowercased" \
  --ref services/orders/src/validate.py:40
```

### apply

```
apply --file flow.json
```

Transactional batch upsert. Accepts the `export` shape:

```json
{
  "nodes": [
    {"kind": "input", "id": "input:orders.created", "name": "orders.created",
     "entry_point": "queue:orders.created", "channel": "queue",
     "file": "services/orders/src/consumer.py", "refs": ["services/orders/src/consumer.py:24"]},
    {"kind": "field", "owner": "input:orders.created", "path": "total", "type": "string"}
  ],
  "edges": [
    {"kind": "compose", "from": "input:orders.created", "to": "entry:validate-order"}
  ]
}
```

Contract nodes are inserted before fields regardless of list order. A failure
rolls the whole batch back. `fields: [{owner, path, ...}]` is also accepted.

### remove-node / remove-edge

```
remove-node --id input:orders.created
remove-edge --kind compose --from input:orders.created --to entry:validate-order
```

`remove-node` also deletes the fields owned by that contract. Edges are deleted
with the node.

## reading

### list

```
list [--kind input|entry|storage|field] [--owner CONTRACT_ID] [--search TEXT]
```

Prints one line per node: id, path/type or entry point/location, and file.
`--owner` filters fields by contract.

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

-- full pipeline from an input to a storage (up to 5 compose hops)
MATCH p = (i:Input {id: $id})-[:COMPOSE*1..5]->(s:Storage)
RETURN s.id, length(p)

-- fan-out of an input
MATCH (i:Input)-[r:COMPOSE]->(x)
RETURN i.id, count(x) AS downstream ORDER BY downstream DESC
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

Counts nodes by kind, edges by kind, input channels, storage types and the ten
contracts with the most fields.

### export

```
export [--out PATH]        # default: .logistic-map/exports/graph.json
```

Writes the snapshot described in `schema.md`. Commit it when the team wants the
map reviewed with code changes; the Kuzu directory stays ignored.

### render

```
render [--out PATH] [--title TEXT] [--flow-db PATH]   # default: .logistic-map/graph.html
```

Writes a standalone HTML file (no network, no CDN) with two tabs.

**Contracts** is a force-directed graph. Fields are hidden by default; enable the
**Fields** chip. Click a node for attributes, references, fields and
connections, or click an **edge** for its `note`, `refs`, file and endpoints.

**Flows** embeds every flow from the flow database (`--flow-db`, default
`<db-parent>/flows.kuzu`). Pick a flow in the selector; dashed steps are
`span=context`, pink badges are joins, and a step panel links back to its
contract in the Contracts tab.

Search, zoom, pan, drag, and PNG export are built in.

With a contract selected, enable **Impact** to dim everything outside its
downstream reach. The reach follows `compose` and `transform` edges (not
`contains`); the panel lists the affected contracts, how many fields they hold,
and the shortest paths to terminal contracts. Path entries are clickable, so you
can walk the flow. Selecting a field keeps the impact of its owner. A terminal
contract (nothing downstream) is not dimmed. An example rendering is in
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
     "nodes": ["input:orders.created", "entry:validate-order"],
     "edge": {"kind": "compose", "from": "...", "to": "..."},
     "refs": ["services/orders/src/consumer.py:24"],
     "data": {"field": "total", "from_type": "string", "to_type": "number"}}
  ]
}
```


## flow

The flow graph lives in a separate Kuzu database (`<db-parent>/flows.kuzu`,
`--flow-db`, or `LOGISTIC_MAP_FLOW_DB`). It is derived from the contract graph and is
never a second source of truth.

| Command | What it does |
|---|---|
| `flow build --from ID [--name NAME] [--id FLOW] [--force]` | Derive one flow, materialize steps and feeds, rebuild if the id exists |
| `flow list` | List flows with step/context/feed counts |
| `flow show ID [--json]` | Print the tree, context inputs and joins |
| `flow sync` | Rebuild every flow from the current contract graph |
| `flow remove --id FLOW` | Delete a flow and its steps |

See `flows.md` for the schema, roles, `span=flow|context` and limitations.
