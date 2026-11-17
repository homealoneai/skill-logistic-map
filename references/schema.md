# Graph schema v1

Kuzu property graph. Four node tables, three relationship tables, all
directional. `init` creates them with `IF NOT EXISTS`, so it is safe to re-run.

## Node tables

Common properties on every node:

| Property | Type | Meaning |
|---|---|---|
| `id` | STRING (primary key) | Stable identifier; see ID rules |
| `name` | STRING | Human label (entry point name, table name) |
| `description` | STRING | Short explanation of the contract |
| `repo` | STRING | Repository URL or path that owns the code |
| `commit_sha` | STRING | Commit the mapping was taken from |
| `file_path` | STRING | Primary file that implements the contract |
| `refs` | STRING[] | Reference file:line strings |

| Table | Extra properties |
|---|---|
| `Input` | `entry_point`, `channel` |
| `Entry` | `entry_point`, `entry_type` |
| `Storage` | `location`, `store_type` |
| `Field` | `path`, `type`, `required` (BOOLEAN), `source` (`declared` / `inferred`) |

`channel` examples: `queue`, `http`, `grpc`, `file`, `cron`, `websocket`.
`entry_type` examples: `function`, `handler`, `worker`, `job`, `lambda`.
`store_type` examples: `sql`, `nosql`, `redis`, `object`, `file`, `cache`.

`Field.source = inferred` means the node was created automatically as an
ancestor of a dotted path (`customer` for `customer.id`) and its type is still
unknown. Declaring it with `add-field` turns it into `declared`.

## Relationship tables

All relationship rows carry `refs STRING[]` and `file_path STRING`; `compose`
and `transform` rows also carry `note STRING`.

| Table | Allowed endpoints | Meaning |
|---|---|---|
| `CONTAINS` | `Input→Field`, `Storage→Field`, `Entry→Field`, `Field→Field` | Contract membership and field nesting |
| `COMPOSE` | every ordered pair of `Input`, `Entry`, `Storage` (9 pairs) | Structural pipeline edge |
| `TRANSFORM` | every ordered pair of `Input`, `Entry`, `Storage` (9 pairs) | Data transformation edge |

Both `COMPOSE` and `TRANSFORM` accept all nine ordered pairs (`Input→Input`,
`Storage→Entry`, ...). The CLI does not restrict direction; `report` detects
compose cycles and checks that each input has at most one outgoing compose
edge.

`contains` edges between two fields must stay inside one contract: both ids must
share the same owner prefix.

## ID rules

```
input:<slug>        entry:<slug>        storage:<slug>        # contracts
<owner-id>#<path>                                            # fields
```

- The prefix decides the node table; `input:orders.created` and
  `entry:orders.created` are different nodes.
- Slugs are stable, lowercase, and contain no whitespace or `#`
  (`orders.created`, `validate-order`, `orders-db`).
- Field ids are generated: owner id + `#` + dotted path
  (`input:orders.created#customer.id`). Never invent field ids by hand.
- `path` segments use dots; array segments may keep brackets (`items[].sku`),
  but a plain `items.sku` is preferred and simpler to diff.
- Renaming an id is a new node. Prefer correcting attributes over renaming.

## Invariants

Enforced by the CLI:

- A field owner must exist and must be a contract node.
- Adding a field creates the `contains` chain owner → first segment → … → leaf.
- `compose` / `transform` endpoints must be contract nodes; self edges are
  rejected.
- `contains` cannot cross contract boundaries.
- Every node referenced by an edge must exist.

Checked by `report` (not enforced at write time, because partial maps are
legitimate while modelling):

- each `Input` has at most one outgoing `compose` edge;
- no cycles in `compose`;
- every field has a `contains` edge;
- contracts declared as inputs/entries/storages have their entry point/location;
- fields declared by hand have a type;
- paths that exist on one side of a `compose`/`transform` edge but not the other
  are reported as missing;
- types that normalize differently on the two sides are reported as conflicts.

## Snapshot format

`export` writes:

```json
{
  "schema_version": 1,
  "generated_at": "2026-11-17T11:20:00+00:00",
  "repo": "github.com/acme/logistics",
  "commit": "a1b2c3d",
  "nodes": [
    {"kind": "input", "id": "input:orders.created", "name": "orders.created", "...": "..."},
    {"kind": "field", "id": "input:orders.created#total", "owner": "input:orders.created",
     "path": "total", "type": "string", "...": "..."}
  ],
  "edges": [
    {"kind": "compose", "from": "input:orders.created", "to": "entry:validate-order"}
  ]
}
```

Empty attributes are omitted; `refs` is always present. `apply` accepts the same
shape plus `fields` as an alternative list of `{owner, path, ...}` objects, and
processes contracts before fields, so field owners always exist.

## Schema versioning

`schema_version` is stored in `<db-parent>/meta.json` and in every snapshot.
`init` never alters existing tables. If a future schema changes, export the
graph, initialize a new database at a new path, and `apply` the snapshot with
the matching CLI version.

The derived flow graph is a separate database with its own `Flow`, `Step` and
`FEEDS` tables; it is not part of this contract schema. See `flows.md`.
