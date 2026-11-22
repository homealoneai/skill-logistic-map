# Report rules

`report` is a set of deterministic checks over the graph. It does not read code;
it compares contracts you registered. Every finding carries `refs`, so the next
step is always to open the referenced file and decide whether the graph or the
code is wrong.

## Severities

| Severity | Meaning | Action |
|---|---|---|
| `error` | The graph is structurally invalid (cycle, orphaned field) | Fix the graph before trusting any other finding |
| `warn` | A contract is probably broken or incomplete | Inspect the referenced code; add the missing field, correct the type, or register the missing edge |
| `info` | Completeness or awareness, not necessarily a defect | Use it to decide what to model next; do not try to zero it |

`--fail-on` maps to these levels: `error` fails only on errors, `warn` fails on
errors and warnings, `info` fails on anything. The default `none` always exits 0.
`report --format json` is the machine-readable form; `--format md` is for a PR
comment.

## Codes

### Structural

| Code | Severity | Trigger | Fix |
|---|---|---|---|
| `compose_cycle` | error | A cycle in `COMPOSE` (containment) edges | Remove the edge that closes the loop; composition is a DAG |
| `orphan_field` | error | A `Field` has no `contains` edge at all | Re-add the field with `add-field`, or `remove-node` it |
| `unknown_flow` | error | A `flow`/`transform` passage or `delivers-to` edge references a `flow_id` with no `Flow` node | Create the flow with `add-flow`, or remove the edge |
| `flow_cycle` | warn | A cycle among the passages of one flow | Break the loop; a flow is a path, not a cycle |
| `unknown_trigger_flow` | error | A `trigger` points at an unknown Flow | Create the flow first; `trigger` is EntryPoint → Flow only |

### Flow structure

| Code | Severity | Trigger | Fix |
|---|---|---|---|
| `edge_without_flow` | warn | A `flow`/`transform` passage has no `flow_id` (legacy or unassigned) | Re-add the passage with `add-edge --flow`, or delete it |
| `delivery_without_flow` | error | A `delivers-to` edge has no `flow_id` | Re-add it with `add-edge --kind delivers-to --flow ...` |
| `edge_entity_not_used` | warn | A passage endpoint is not connected to that flow with `use` | `flow use --id FLOW --entity ENDPOINT` |
| `flow_without_entities` | warn | A `Flow` does not `use` any entity | Add its entities with `flow use` |
| `flow_without_path` | info | A `Flow` has no `flow`/`transform` passage | Add the passages with `add-edge --flow` |
| `incomplete_flow` | info | A `Flow` has neither `file_path` nor `refs` | Record where the flow is described in code |
| `entrypoint_without_flow` | warn | An EntryPoint neither triggers a Flow nor reaches one through `related` | Add only code-evidenced `trigger`/`related` edges |
| `orphan_rule` | warn | A Rule is not attached to any Flow or EntryPoint | Add a `rule` edge from the activation it constrains |
| `rule_without_description` | warn | A Rule has no description | Describe the enforced limit or condition |
| `incomplete_rule` | info | A Rule has no `file_path` or `refs` | Record the code evidence |
| `orphan_external` | warn | An External System is neither called nor delivered to | Add a `calls` or `delivers-to` edge, or remove the node |
| `incomplete_external` | info | An External System has no `file_path` or `refs` | Record the code evidence |
| `flow_without_trigger` | info | A Flow is not triggered by an EntryPoint | Record its activation mechanism when known |
| `cluster_mismatch` | warn | An EntryPoint and the Flow it triggers have different non-empty clusters | Set the same `--cluster` on both, or fix the wrong grouping |
| `incomplete_entrypoint` | info | An EntryPoint has no `file_path` or `refs` | Add code evidence |

### Data shape

| Code | Severity | Trigger | Fix |
|---|---|---|---|
| `isolated_contract` | warn | Contract has zero edges | Connect it, or delete it if it was a probe |
| `unconsumed_input` | warn | `Input` has no outgoing `flow`/`transform` | Register the next entity, mark the input as external/unused in its description, or remove it |
| `unproduced_storage` | warn | `Storage` has no incoming `flow`/`transform` | Register the entity that is written there |
| `entry_dead_end` | info | `Entry` receives data but has no outgoing edge | Add the storage/next entity, or delete the entry if it is a leaf by design |
| `transform_without_note` | warn | A `transform` edge has no `note`, `file_path` or `refs` | Record the mapping (`--note "total: string -> number"`) or the code reference |

### Completeness

| Code | Severity | Trigger | Fix |
|---|---|---|---|
| `incomplete_contract` | info | `Storage` without `location`, or a contract with neither `file_path` nor `refs` | Fill the attribute |
| `field_without_type` | info | A declared field has an empty `type` | Declare the type; leave it empty only while deliberately unknown |
| `inferred_fields` | info | One aggregate finding: N parent fields were auto-created from dotted paths | Review their types when convenient; no action needed if the parent is an object you do not model |
| `missing_file` | warn | With `--check-files`: `file_path` or the path part of a ref does not exist relative to the current directory | Update the path/ref, or run from the repository root |

### Entity diffs along flow passages

`flow` means the shape does **not** change, so both ends should expose the same
paths. `transform` passages are deliberately **not** diffed: changing the shape
is what they are for.

| Code | Severity | Trigger |
|---|---|---|
| `missing_field` | `warn` on `flow` into an `Entry`; `info` otherwise | A path exists on the source side but not on the target side |
| `type_conflict` | same rule | A path exists on both sides with non-empty types that normalize differently |

Details:

- **Comparison is by `path`**, not by field id. `customer.email` in
  `input:orders.created` is compared with `customer.email` in
  `entry:order-validated`.
- **Nested paths are pruned**: if `customer` is missing, its children
  `customer.id` / `customer.email` are not reported separately.
- **Empty types never conflict.** A missing type is `field_without_type`, not a
  conflict.
- **Type normalization** collapses aliases (`int/integer/int64` → `integer`,
  `str/string/varchar` → `string`, `float/decimal/number` → `number`,
  `dict/object/map` → `object`, `list/array` → `array`,
  `datetime/timestamp` → `timestamp`). `decimal(12,2)` and `uuid` stay distinct.
- **Storage targets are informational.** Writing a projection of a contract into
  a table is normal; a missing field there is a prompt to check the projection,
  not automatically a bug.
- **If the shape changes, use `transform`** with a note; then no field diff is
  attempted.

## Why a finding may be wrong

The report only knows the graph. Before changing code:

- **The field may be intentionally dropped.** If so, the edge is a
  `transform`, not a `flow`; record the mapping in the note and the diff
  disappears.
- **The target may be partial by design.** Storage projections are the common
  case; that is why they are `info`.
- **The producer may be wrong.** A field present only in the consumer usually
  means the consumer expects something nobody sends; register the producer's
  actual entity and the finding flips.
- **The entity may be shared.** If several services share one schema, model the
  schema as the `Input` and compare consumers against it, instead of comparing
  consumers with each other.

## Triage order

1. `error` findings first — the graph is not trustworthy until they are fixed.
2. `warn` `missing_field`/`type_conflict` on `flow` passages into `Entry`: these
   are the producer/consumer gaps where the shape was supposed to stay equal.
3. `warn` `edge_without_flow` and `edge_entity_not_used`: the flow is incomplete.
4. `warn` `transform_without_note`: an undocumented shape change cannot be
   audited.
5. `warn` shape findings (`isolated_contract`, `unconsumed_input`,
   `unproduced_storage`, `flow_cycle`, `flow_without_entities`): missing parts of
   the map.
6. `info` findings: use them to decide what to register next.

Do not silence a finding by deleting the field or the edge. Either the code
supports the finding (fix the contract) or the model is wrong (fix the node,
attribute, or edge).
