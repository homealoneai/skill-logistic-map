# Modeling a data flow

The graph answers "where does this field come from, what changes it, and where
does it end up". Model **contracts and transitions**, not classes, call graphs or
architecture boxes. A good graph is small enough to read and precise enough that
a `missing_field` warning points at real code.

## 1. Find the entry points

An `Input` is where data crosses into the application:

| Entry point | `entry_point` example | `channel` |
|---|---|---|
| Message consumer | `queue:orders.created` | `queue` |
| HTTP route | `POST /orders` | `http` |
| gRPC method | `Orders/Create` | `grpc` |
| Cron / scheduled job | `cron:0 3 * * *` | `cron` |
| File drop / watcher | `s3://landing/orders/` | `file` |
| CLI | `bin/import-orders` | `cli` |

Rules of thumb:

- One `Input` per **contract at an entry point**, not per handler. If two
  services consume `orders.created` with different field expectations, create
  two inputs with distinct slugs (`input:orders.created.billing`).
- If the same queue feeds five identical workers, model one input; capacity is
  not a contract.
- An HTTP response returned to an external caller is an input contract of the
  caller, not a storage. Model the boundary once, on the side you control.

## 2. Find the transformations

An `Entry` is a place where **the payload changes shape**: validation,
normalization, enrichment, mapping, aggregation.

- Do not model every function. If a function only passes data through, it is not
  an `Entry`.
- Do model a step when it adds, removes, renames or retypes fields; those are the
  steps that create contract drift.
- A worker that both consumes and writes can be one `Entry`; split it when it
  runs two clearly separate transformations used by different pipelines.
- `entry_point` should identify the code: `validate_order`, `OrderHandler.handle`,
  `worker:enrich-customer`.

## 3. Find the storage

A `Storage` is any place **you** persist data: SQL/NoSQL table, Redis namespace,
object-store prefix, file path, search index. Never use `Storage` for a service
you call over the network; that boundary is an `Input` (response) or an `Entry`
(outbound call), depending on what you are mapping.

`location` should be specific enough to find the data:

- `postgres://orders.orders` (database + table)
- `redis://customers:6379/tier`
- `s3://acme-events/orders/dt=*/`
- `file:///var/lib/orders/current.jsonl`

## 4. Model fields

Fields are **per contract**. The same logical field in producer and consumer is
two `Field` nodes; `report` compares them by `path`. This is deliberate: merging
them would hide exactly the conflicts the graph exists to find.

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

## 5. Choose compose vs transform

| Situation | Edge |
|---|---|
| Queue is consumed by a worker; worker writes a table | `compose` |
| One transformation feeds the next transformation | `compose` |
| A step parses `total` from string to decimal, renames `id` to `order_id` | `transform` (add a `--note` with the mapping) |
| Fan-out from one input to several consumers | `transform` to each extra consumer |
| A storage is read back by a job | `compose` from storage to that entry |

`compose` is the skeleton; `transform` explains where and how data changes. When
both exist between the same pair, the report diffs fields once, on the compose
edge, and skips the duplicate transform diff.

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

## Anti-patterns

| Anti-pattern | Why it hurts |
|---|---|
| One `Input` for everything the service receives | Field diffs become meaningless; split by contract |
| `Entry` per class | Noise; only payload changes are interesting |
| `Storage` for an external API | The graph no longer distinguishes your data from someone else's |
| Reusing a `Field` node across contracts to avoid duplicates | Hides type conflicts and missing fields |
| Guessing field types | The report reports guesses as facts; leave the type empty instead |
| Editing the Kuzu directory directly | Bypasses edge creation and invariants; use `export`/`apply` |
