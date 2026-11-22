# Flows

A flow is an **explicit node** in the main graph. It groups the data entities
that participate in one path and gives that path a name and a description. Flows
are authored, not derived: there is no second database, no `Step` and no `FEEDS`
table anymore.

## Model

```text
flow:create-order ──USE──▶ input:orders.created
flow:create-order ──USE──▶ entry:order-validated
flow:create-order ──USE──▶ storage:orders
entrypoint:orders-created ──TRIGGER──▶ flow:create-order
entrypoint:orders-screen ──RELATED──▶ entrypoint:orders-created

input:orders.created ──TRANSFORM {flow_id: flow:create-order}──▶ entry:order-validated
entry:order-validated ──FLOW {flow_id: flow:create-order}──▶ storage:orders
```

| Element | Meaning |
|---|---|
| `Flow` node | One named path: `id`, `name`, `description`, `cluster`, refs |
| `EntryPoint` | A code-evidenced activation mechanism, distinct from data |
| `TRIGGER` | `EntryPoint → Flow`: this activation starts the flow |
| `RELATED` | `EntryPoint → EntryPoint`: directional activation relationship |
| `RULE` | `Flow/EntryPoint → Rule`: a limit or condition the activation follows |
| `CALLS` | `Flow → External`: the flow calls an external system |
| `DELIVERS_TO` | `Entry → External` with `flow_id`: payload delivered in that flow |
| `USE` | `Flow → entity`: this flow uses this entity |
| `FLOW` passage | The data passes unchanged; carries `flow_id` |
| `TRANSFORM` passage | The data changes shape; carries `flow_id` and a note |

`USE` answers "which entities does this flow use?". The passages with the same
`flow_id` answer "in what order, and where does the data change?".
An EntryPoint answers "what starts this flow?". Its `trigger` never connects to
an Input, Entry, or Storage. `RELATED` connects activation points to each other:
it is directional, requires `description` and `refs`, and is identified by
`(from, to, description)`, so a screen → HTTP relation and an HTTP → screen
relation are independent.

`cluster` groups the flow with its service or repository. Set it with
`add-flow --cluster`. An entrypoint and the flow it triggers normally share the
cluster; `report` warns when they differ.

`Rule` records a limit or condition the flow follows; attach it with
`add-edge --kind rule`. A rule is not a step and carries no data.

An External System represents a service outside the mapped repositories. A Flow
calls it with `calls`; an Entry delivers a payload to it with `delivers-to` and
a required `flow_id`:

```sh
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-external \
  --id external:sms-gateway --name "SMS gateway" --provider Twilio \
  --protocol HTTP --cluster notification-service
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind calls \
  --from flow:send-sms --to external:sms-gateway
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind delivers-to \
  --flow flow:send-sms --from entry:sms-request --to external:sms-gateway
```

The same Entry may deliver to the same External System in several flows; the
delivery is identified by `(from, to, flow_id)`. External Systems are not data
entities, never appear in a passage and are excluded from `Impact`.

A passage is identified by `(kind, from, to, flow_id)`: the same pair may appear
in several flows without one overwriting the other. Two entities may have the
same fields and the same `file_path`/`refs`; they are still distinct positions
on the path.

## Commands

```sh
PY=.logistic-map/.venv/bin/python
SKILL_DIR=/path/to/logistic-map

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-flow \
  --id flow:create-order --name "Create order" \
  --description "Receive, validate and persist an order" \
  --cluster orders-service
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-entrypoint \
  --id entrypoint:orders-created --name "orders.created consumer" \
  --description "Consumes the orders.created queue" --ref consumer.py:24
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind trigger \
  --from entrypoint:orders-created --to flow:create-order
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-entrypoint \
  --id entrypoint:orders-screen --name "Orders screen" \
  --description "Screen that submits the order form" --ref ui/orders.tsx:12
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind related \
  --from entrypoint:orders-screen --to entrypoint:orders-created \
  --description "screen submits the order form" --ref ui/orders.tsx:42
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-rule \
  --id rule:order-item-limit --name "Order item limit" \
  --description "An order may contain at most 20 items" \
  --cluster orders-service --ref services/orders/src/validate.py:18
"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind rule \
  --from flow:create-order --to rule:order-item-limit

"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow use \
  --id flow:create-order --entity input:orders.created
"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow use \
  --id flow:create-order --entity entry:order-validated
"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow use \
  --id flow:create-order --entity storage:orders

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind transform \
  --flow flow:create-order \
  --from input:orders.created --to entry:order-validated \
  --note "total: string -> number"

"$PY" "$SKILL_DIR/scripts/logistic_map.py" add-edge --kind flow \
  --flow flow:create-order \
  --from entry:order-validated --to storage:orders

"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow list
"$PY" "$SKILL_DIR/scripts/logistic_map.py" flow show flow:create-order
```

`flow show` prints the entities with their roles, the passages, the path tree
and the joins. `--json` returns `flow`, `entities`, `steps`, `feeds` and `joins`.
`flow remove --id ...` deletes the flow and its passages, but not the entities.

`flow unuse --id ... --entity ...` removes an entity from a flow.
`remove-edge --kind flow|transform --flow ...` removes a specific passage.

## Roles and joins

A step's role is derived from its passages inside the flow:

| Role | Meaning |
|---|---|
| `source` | has outgoing passages and no incoming one |
| `write` | has incoming passages and no outgoing one |
| `readwrite` | has both |
| `artifact` | is used by the flow but has no passage |

A step with more than one incoming passage is a **join**. `flow show` lists it
under `JOINS`; the HTML shows the passages, but does not badge joins.

## Invariants checked by `report`

- every `flow`/`transform` passage has a `flow_id` (`edge_without_flow`);
- the referenced flow exists (`unknown_flow`);
- both endpoints of a passage are listed with `use` for that flow
  (`edge_entity_not_used`);
- a flow uses at least one entity (`flow_without_entities`);
- a flow has at least one passage (`flow_without_path`);
- a flow has no cycles (`flow_cycle`).
- EntryPoints have source evidence and trigger a flow directly or reach one
  through `related`; untriggered flows are reported as informational findings;
- `related` edges carry a description and refs and connect only EntryPoints.
- Rules are attached to a Flow or EntryPoint and carry a description and refs.
- `delivers-to` edges carry a `flow_id` and their Entry is USEd by that flow;
  external systems with neither `calls` nor `delivers-to` are reported.

## Reading a flow

```text
Flow flow:create-order — Create order
Receive, validate and persist an order
entities: 3  passages: 2  joins: 0

ENTITIES
  entry:order-validated [readwrite]
  input:orders.created [source]
  storage:orders [write]

PASSAGES
  entry:order-validated -[flow]-> storage:orders
  input:orders.created -[transform]-> entry:order-validated  # total: string -> number

PATH
  input:orders.created [source]
    entry:order-validated [readwrite]
      storage:orders [write]
```

## Rendering

`render` draws EntryPoints above flows, with rules and External Systems beside
both in columns that span their two lanes. The view tabs switch between **All**,
**Flows** and **External Systems**. Selecting an EntryPoint
follows outgoing `related` edges and highlights the flows those entrypoints
trigger, including their entities, `use` edges and passages. The cluster
dropdown filters every lane to nodes with the selected cluster. Selecting a flow
highlights its entities, `use` edges, `rule` edges, `calls`/`delivers-to`
edges and `flow`/`transform` passages, and dims everything else; selecting an entity lists the flows that use
it. Impact follows `flow`/`transform` passages only, so `use`, `compose`,
`trigger`, `related`, `rule`, `calls` and `delivers-to` never appear in an
impact path.

`export-md` writes one Markdown page per flow with the path, passages, joins and
links to the entity pages. Entity pages list the flows that use them. A
`## Clusters` table in the index summarizes the extracted boundaries.
