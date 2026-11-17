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
| `compose_cycle` | error | A cycle in `COMPOSE` edges | Remove the edge that closes the loop or split the contract; pipelines are DAGs |
| `orphan_field` | error | A `Field` has no `contains` edge at all | Re-add the field with `add-field`, or `remove-node` it |

### Pipeline shape

| Code | Severity | Trigger | Fix |
|---|---|---|---|
| `isolated_contract` | warn | Contract has zero edges | Connect it, or delete it if it was a probe |
| `unconsumed_input` | warn | `Input` has no outgoing `compose`/`transform` | Register the consumer, or mark the input as external/unused in its description |
| `unproduced_storage` | warn | `Storage` has no incoming edge | Register the writer |
| `input_multiple_compose` | warn | `Input` has more than one outgoing `compose` | Keep one structural consumer; fan-out uses `transform` |
| `entry_dead_end` | info | `Entry` receives data but has no outgoing edge | Add the storage/next entry, or delete the entry if it is a leaf by design |

### Completeness

| Code | Severity | Trigger | Fix |
|---|---|---|---|
| `incomplete_contract` | info | `Input`/`Entry` without `entry_point`, `Storage` without `location`, or a contract with neither `file_path` nor `refs` | Fill the attribute |
| `field_without_type` | info | A declared field has an empty `type` | Declare the type; leave it empty only while deliberately unknown |
| `inferred_fields` | info | One aggregate finding: N parent fields were auto-created from dotted paths | Review their types when convenient; no action needed if the parent is an object you do not model |
| `missing_file` | warn | With `--check-files`: `file_path` or the path part of a ref does not exist relative to the current directory | Update the path/ref, or run from the repository root |

### Contract diffs

These run for every `compose` and `transform` edge whose two endpoints both have
fields.

| Code | Severity | Trigger |
|---|---|---|
| `missing_field` | `warn` on `compose` into an `Entry`/`Input`; `info` on `compose` into a `Storage` or on `transform` | A path exists on the source side but not on the target side |
| `type_conflict` | same rule | A path exists on both sides with non-empty types that normalize differently |

Details:

- **Comparison is by `path`**, not by field id. `customer.email` in
  `input:orders.created` is compared with `customer.email` in
  `entry:validate-order`.
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
- **Compose wins over transform.** When both connect the same pair, only the
  compose edge is diffed; the transform edge is not reported twice.

## Why a finding may be wrong

The report only knows the graph. Before changing code:

- **The field may be intentionally dropped.** If so, say it in the contract
  description or the edge `note`; the graph does not currently have a
  "deliberate drop" flag, so the info finding remains. That is acceptable.
- **The target may be partial by design.** Storage projections are the common
  case; that is why they are `info`.
- **The producer may be wrong.** A field present only in the consumer usually
  means the consumer expects something nobody sends; register the producer's
  actual contract and the finding flips.
- **The contract may be shared.** If several services share one schema, model the
  schema as the `Input` and compare consumers against it, instead of comparing
  consumers with each other.

## Triage order

1. `error` findings first — the graph is not trustworthy until they are fixed.
2. `warn` `missing_field`/`type_conflict` on `compose` edges into `Entry`: these
   are the producer/consumer gaps.
3. `warn` shape findings (`isolated_contract`, `unconsumed_input`,
   `unproduced_storage`, `input_multiple_compose`): missing parts of the map.
4. `info` findings: use them to decide what to register next.

Do not silence a finding by deleting the field or the edge. Either the code
supports the finding (fix the contract) or the model is wrong (fix the node,
attribute, or edge).
