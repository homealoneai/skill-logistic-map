# Modeling a data flow

The graph answers "where does this field come from, what changes it, and where
does it end up". Model **data entities and how they change**, not classes, call
graphs or architecture boxes. A good graph is small enough to read and precise
enough that a `missing_field` warning points at real code.

## 1. Find the entry points

An `EntryPoint` is the code-level mechanism that starts a Flow. An `Input` is
the data contract arriving through that mechanism. Keep these separate:

| Activation mechanism | EntryPoint example |
|---|---|
| Message consumer | `orders.created consumer` |
| HTTP route | `POST /orders` |
| gRPC method | `Orders/Create` |
| Cron / scheduled job | `nightly reconciliation` |
| File drop / watcher | `orders import watcher` |
| CLI | `import-orders command` |
| UI screen / action | `User list screen load` |

Rules of thumb:

- One `EntryPoint` per independently identifiable activation mechanism; connect
  it to each Flow it starts with `trigger`. Do not infer triggers.
- When an outer activation starts another entrypoint instead of a flow directly,
  link them with a directional `related` edge: `description` explains the
  connection and `refs` confirms it. Reverse and alternative relations are
  separate edges. A screen that invokes an HTTP route is a valid entrypoint even
  though it does not itself receive the HTTP payload.
- One `Input` per **data contract**, not per handler. If two
  services consume `orders.created` with different field expectations, create
  two inputs with distinct slugs (`input:orders.created.billing`).
- If the same queue feeds five identical workers, model one input; capacity is
  not a contract.
- An HTTP response returned to an external caller is an input contract of the
  caller, not a storage. Model the boundary once, on the side you control.
- `Input` has no activation attributes. Do not use `Entry` for handlers or
  execution points; `Entry` is only an intermediate data shape.

### Clusters

`cluster` groups nodes by the boundary that matters for the analysis: a service,
repository, module, deployable app or bounded context. It is one string, set on
contracts, flows and entrypoints; fields inherit it from their owner.

- Use one label per boundary, not one per node: `orders-service`,
  `checkout-ui`, `billing`.
- The same label should cover code that is deployed and owned together.
- An entrypoint and the flow it triggers normally share a cluster; `report`
  warns with `cluster_mismatch` when they do not.
- Cross-cluster data movement is normal and is expressed by `flow`/`transform`
  passages, not by changing the cluster.
- `render` exposes a cluster dropdown, so consistent labels directly improve
  navigation.

## 2. Find the intermediate entities

An `Entry` is a **data shape that exists between ingress and persistence**:
validated payload, normalized order, enriched customer, response DTO, view
model. It is not a function, handler, worker or class.

- Do not model every function. If a function only passes data through, do not
  add a `transform`; the data still gets its own entity for the new position and
  can share the same `file_path`/`refs`.
- Do model an entity when it adds, removes, renames or retypes fields; those are
  the shapes that create contract drift.
- A worker that consumes and writes can produce one intermediate entity; split
  it when it produces two clearly separate shapes used by different consumers.
- `stage` is an optional label for where the shape sits (`validated`,
  `normalized`, `enriched`, `response`); `file_path`/`refs` point at the code
  that builds it.

## 3. Find the storage

A `Storage` is any place **you** persist data: SQL/NoSQL table, Redis namespace,
object-store prefix, file path, search index. Never use `Storage` for a service
you call over the network; an external response is an `Input` if it enters your
scope, or an `Entry` only when you are modelling the data shape you keep from it.

`location` should be specific enough to find the data:

- `postgres://orders.orders` (database + table)
- `redis://customers:6379/tier`
- `s3://acme-events/orders/dt=*/`
- `file:///var/lib/orders/current.jsonl`

## 4. Model fields

Fields are **per entity**. The same logical field in producer and consumer is
two `Field` nodes; `report` compares them by `path` along `flow` edges. This is
deliberate: merging them would hide exactly the conflicts the graph exists to
find.

- Use the payload's own path: `customer.id`, `items.sku`, `items[].quantity`.
- Declare the fields that matter for the contract, then stop. A 200-field payload
  does not need 200 nodes on day one; add the ones involved in mismatches.
- Declare parent objects explicitly when their type is known (`customer`,
  `object`); otherwise the CLI creates them as `inferred` with no type and the
  report reminds you.
- `required` means the contract requires the field, not that the database column
  is NOT NULL. Leave it off when unknown.
- Types are free text; `report` normalizes `int/integer/int64`, `str/string`,
  `bool/boolean`, `float/decimal/number`, `dict/object`, `list/array`,
  `datetime/timestamp`.

## 5. Choose compose, flow or transform

| Situation | Edge |
|---|---|
| A request envelope has headers and a body; a document has parts | `compose` (whole → part) |
| A storage record is an aggregate made of documented sections | `compose` |
| A queue is consumed and the payload is stored as-is | `flow` |
| A stored row is read into a job without reshaping | `flow` |
| A step parses `total` from string to decimal, renames `id` to `order_id` | `transform` (add a `--note` with the mapping) |
| An entity is enriched with fields from another source | `transform` |

`compose` is **containment**: it says one entity is made of another, never what
runs next. `flow` says data moves between shapes that stay equivalent. `transform`
says the shape changed, and must carry a `note` or `ref` explaining how.

A path is a **Flow** node plus `use` relations for every entity on it. Register
the flow first, list its entities, then add `flow`/`transform` passages carrying
its `flow_id`:

```sh
add-flow --id flow:create-order --name "Create order" \
  --description "Receive, validate and persist an order"
flow use --id flow:create-order --entity input:orders.created
flow use --id flow:create-order --entity entry:order-validated
add-edge --kind transform --flow flow:create-order \
  --from input:orders.created --to entry:order-validated \
  --note "total: string -> number"
```

Connect the code-level activator separately:

```sh
add-entrypoint --id entrypoint:orders-http --name "POST /orders" \
  --description "Receives order creation requests" --ref handler.py:18
add-edge --kind trigger --from entrypoint:orders-http --to flow:create-order
```

If a screen is the outer point of entry and starts the HTTP activation, model
both and connect them directionally:

```sh
add-entrypoint --id entrypoint:orders-screen --name "Orders screen" \
  --description "Submits the order form" --ref ui/orders.tsx:12
add-edge --kind related --from entrypoint:orders-screen \
  --to entrypoint:orders-http \
  --description "screen submits the order form" --ref ui/orders.tsx:42
```

The screen needs no `trigger` of its own; the report accepts an entrypoint that
reaches a flow through `related`. The reverse relation, if evidenced, is a
separate edge with its own description.

An entity may belong to several flows; a passage is identified by
`(kind, from, to, flow)`. When the data does not change but the position on the
path does, register a **new entity** and connect it with `flow`; pointing both at
the same file is fine and expected.

## 6. Record references

Every node and edge takes:

```sh
--repo github.com/acme/logistics --commit a1b2c3d \
--file services/orders/src/consumer.py \
--ref services/orders/src/consumer.py:24 --ref infra/queues.yaml:12
```

- `--repo` / `--commit` default to the current git origin and `HEAD`; pass them
  explicitly when mapping another repository or an old revision.
- Prefer line numbers that point at the field declaration or the mapping line,
  not just the file.
- Run `report --check-files` after a refactor to find stale references.
- `refs` accept comma-separated values as well as repeated `--ref`.

## 7. Multi-service and multi-repo graphs

One database can hold several repositories: every node carries `repo`, so
`MATCH (n) WHERE n.repo = '...' RETURN ...` gives a per-repo view. Keep the DB in
the repository that owns the flow you are auditing (usually the consumer or the
shared contract repo) and record the producer repo on its nodes.

Shared contract schemas (Avro/Protobuf/OpenAPI) are good `Input` roots: register
the schema fields once as the input contract, then compare each consumer against
it.

## 8. Incremental mapping

Do not try to map everything at once. A productive sequence:

1. Pick one entry point and follow it to its storage.
2. Register contracts with `file`/`refs`, no fields yet.
3. Register only the fields named in the ticket or the error.
4. Run `report`; let the findings tell you which contract to open next.
5. Export the snapshot when the flow is stable.

## 9. Record rules

A `Rule` holds behaviour that the flow or entrypoint must follow, so it does not
need a separate document: result limits, timeouts, pagination, authentication,
feature flags, cache policy and validation. Attach it with `rule`:

```sh
add-rule --id rule:search-limit --name "Search result limit" \
  --description "The search flow returns at most 10 products" \
  --cluster catalog-service --ref controller/controller.go:58
add-edge --kind rule --from flow:search-products --to rule:search-limit
```

- Register only rules the code actually enforces or documents; do not turn every
  comment into a rule.
- A rule may attach to several flows and entrypoints.
- Keep the rule description specific and testable ("at most 10 results"), not a
  vague label ("pagination").
- Rules carry no data and never appear in a passage or `Impact`.

## Anti-patterns

| Anti-pattern | Why it hurts |
|---|---|
| One `Input` for everything the service receives | Field diffs become meaningless; split by contract |
| `Entry` per class or handler | An entity is a data shape, not code; this recreates the call graph |
| `compose` used as "then" | Composition is whole/part; execution order belongs to Flows |
| `related` used to move data | It only connects activation points; data belongs to Flow passages |
| A rule written as a separate Markdown note | The behaviour is not queryable or rendered; use a `Rule` node |
| A `transform` with no note or reference | Nobody can audit the mapping; the report warns |
| `Storage` for an external API | The graph no longer distinguishes your data from someone else's |
| Reusing a `Field` node across contracts to avoid duplicates | Hides type conflicts and missing fields |
| Guessing field types | The report reports guesses as facts; leave the type empty instead |
| Editing the database files directly | Bypasses edge creation and invariants; use `export`/`apply` |
