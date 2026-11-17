# Derived flow graphs

The contract graph (`Input`, `Entry`, `Storage`, `Field`) is the source of truth:
it holds fields, references and the rules behind `report`. The flow graph is a
second, **derived** graph for one question only: *which path does data take, and
what are the joins on that path?*

It exists because a stored procedure is not a simple edge. A procedure can read
three tables and write two; a linear path through the contract graph hides all
but one input. In the flow graph that procedure is a **step with three incoming
feeds**, and the join is explicit.

Nothing in the contract graph changes. The flow graph lives in its own Kuzu
database (default `.dataflow/flows.kuzu`) and is rebuilt from the contract graph
with `flow build` / `flow sync`.

## Schema

```cypher
CREATE NODE TABLE Flow(
  id STRING, name STRING, description STRING, start_contract STRING,
  repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
  created_at STRING, PRIMARY KEY(id));

CREATE NODE TABLE Step(
  id STRING, flow_id STRING, contract_id STRING, contract_kind STRING,
  name STRING, role STRING, span STRING, seq INT64,
  repo STRING, commit_sha STRING, file_path STRING, refs STRING[],
  PRIMARY KEY(id));

CREATE REL TABLE HAS_STEP(FROM Flow TO Step);
CREATE REL TABLE FEEDS(FROM Step TO Step, kind STRING, note STRING);
```

| Concept | Meaning |
|---|---|
| `Flow` | One named path, anchored at a start contract |
| `Step` | An occurrence of a contract inside that flow; `contract_id` points back to the contract graph |
| `FEEDS` | Data moves from one step to another (`kind` is the original `compose`/`transform`, or `compose+transform` when both connect the same pair; notes are joined) |
| `span=flow` | The step is reachable from the flow start |
| `span=context` | The step is an input of a reachable operation, pulled in to expose a join |
| `role` | `source`, `read`, `transform`, `write`, `readwrite` or `artifact` |

A procedure that reads `orders` and `customers` and writes `shipments` becomes:

```
storage:orders ───┐
                  ├──▶ entry:sp_ship_order ───▶ storage:shipments
storage:customers ┘          (join: 2)
```

`customers` is `span=context`: it is not affected by `orders`, but it is part of
the join, so the tree is not allowed to pretend `orders` is the only source.

## Build algorithm

`flow build --from <contract>`:

1. Walks forward from the start over `compose` and `transform` edges (cycles are
   followed once).
2. Creates a `Step` for every reachable contract (`span=flow`, BFS `seq`).
3. Creates `FEEDS` for every data edge whose two ends are in the flow. A
   `compose` + `transform` pair between the same nodes produces one feed.
4. For every reachable `Entry`, looks at its incoming data edges and creates a
   `span=context` step for each input that is outside the forward reach, plus a
   `FEEDS` into the operation.
5. Copies `repo`, `commit_sha`, `file_path` and `refs` from the contract nodes;
   nothing is interpreted.
6. Stores the flow id, name and start contract.

Context is **one hop**. If a context table is itself produced by another
procedure, that upstream chain is not pulled in; select that table and build a
separate flow if you need it.

## Commands

```sh
PY=.dataflow/.venv/bin/python
SKILL_DIR=/path/to/data-flow-map

# derive one flow (id defaults to flow:<slug of name or start>)
"$PY" "$SKILL_DIR/scripts/dataflow.py" flow build \
  --from storage:orders --name "Orders to notifications"

# or pick the id explicitly
"$PY" "$SKILL_DIR/scripts/dataflow.py" flow build \
  --from storage:orders --id flow:orders --name "Orders to notifications"

"$PY" "$SKILL_DIR/scripts/dataflow.py" flow list
"$PY" "$SKILL_DIR/scripts/dataflow.py" flow show flow:orders-to-notifications
"$PY" "$SKILL_DIR/scripts/dataflow.py" flow show flow:orders-to-notifications --json

# after changing the contract graph, rebuild every flow
"$PY" "$SKILL_DIR/scripts/dataflow.py" flow sync

"$PY" "$SKILL_DIR/scripts/dataflow.py" flow remove --id flow:orders-to-notifications
```

Flow database location:

- default: `<db-parent>/flows.kuzu` (normally `.dataflow/flows.kuzu`);
- `--flow-db PATH` per command;
- `DATAFLOW_FLOW_DB` environment variable.

`flow build` is idempotent: rebuilding an id deletes its previous flow and steps
first. Rebuilding an id that belongs to a different start contract is refused
unless `--force` is passed.

## Reading the tree

```
Flow flow:orders-to-notifications — Orders to notifications
start: storage:orders
steps: 5 (+1 context)  feeds: 5  joins: 1

TREE
storage:orders [source]
  entry:sp_ship_order [transform]  (join: 2)
    storage:shipments [readwrite]
      entry:sp_notify [transform]
        storage:notifications [write]

CONTEXT
  storage:customers [read] -> entry:sp_ship_order

JOINS
  entry:sp_ship_order <- storage:orders, storage:customers
```

- `TREE` follows `FEEDS` from the start and prints branches; it stops on a cycle
  with `... (cycle)`.
- `CONTEXT` lists the steps pulled in as join inputs and where they feed.
- `JOINS` lists every step with more than one incoming data feed.

`--json` returns the complete `flow`, `steps`, `feeds` and `joins` arrays, which
is the shape a renderer or another tool should consume.

## Limitations

- **Derived, not authored.** A manual step, condition or order is lost on the
  next `flow build`/`flow sync`. Change the contract graph instead.
- **No call graph.** `Entry → Entry` edges are still treated as data flow. If a
  procedure calls another just for control, model that as a separate edge kind in
  a future schema (planned `call`), not as `compose`.
- **Context is one hop.** Deep join inputs need their own flow or a future
  recursive context option.
- **No field-level lineage.** Steps carry contract ids, not columns. Column
  lineage needs `Field → Field` mapping and is out of scope here.
- **Rebuild cost.** `flow sync` rebuilds every flow; fine for hundreds of
  contracts, not intended for huge graphs.
- **One feed per pair.** A `compose` and a `transform` between the same two
  nodes are merged into a single `FEEDS` row with `kind=compose+transform` and
  the notes joined; they are not two separate paths.
- **No HTML yet.** `flow show` is the current reader; a flow view in `render`
  is the next step.
