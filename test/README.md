# Tests

Deterministic tests for `scripts/logistic_map.py`. They use only the standard
library test runner plus Kuzu: no model, gateway or Docker involved.

## Setup and run

```sh
make setup     # create .logistic-map/.venv (Python 3.11+ preferred)
make test
```

`make test` prints one test per CLI behaviour plus one test per sample graph.
Each test creates a temporary database; the repository is never modified.

## Sample graphs

`test/sample/*.json` are the graphs produced by the evaluation runs and copied
here as fixtures. `manifest.json` records their counts and the skill report
output that the tests compare against. The fixtures are generated from
the current extractor: `EntryPoint` nodes, data entities, one or more `Flow`
nodes, `trigger`/`related`/`use` relations, cluster labels and passages with
`flow_id`. The tests assert
the recorded findings exactly, so a CLI change that alters them is caught.

```sh
make samples-list              # ids and sizes
make sample SAMPLE=go          # import, render and open the HTML
make sample-no-open SAMPLE=web # render only (CI-friendly)
make index                     # build every sample and open the index
```

`make index` writes `test/.build/index.html`, a static page linking to every
sample graph; `make sample` also refreshes that index after rendering.

The generated database and `graph.html` live under `test/.build/<sample>/`,
which is ignored by Git. References inside the samples are relative to
`eval/code-sample/<case>/`, so the importer and the tests run the CLI from that
directory when `--check-files` matters.

To regenerate the fixtures after an eval run:

```sh
make samples
```

This picks the newest **schema-v7** snapshot per case from `eval/logs/*.eval`,
validates it by applying it to a temporary database, and rewrites `test/sample/`
and the manifest. It never reports success by mixing a current manifest with
stale v3 graphs, so rerun the evals after a schema change. Refs whose file or line does not resolve are dropped while copying
(the eval scorer tolerates a small ratio; these fixtures do not). It requires the
eval environment (`./eval/bootstrap.sh`) because it reads Inspect logs.

## What is covered

`test_cli.py` checks the CLI contract:

- schema creation and empty stats
- v1-v6 → v7 migration (unassigned passages, dropped Input activation fields, no invented triggers/relations/rules, empty clusters)
- EntryPoint validation: `TRIGGER` only EntryPoint → Flow; directional `RELATED` only EntryPoint → EntryPoint
- node, field and edge properties after an export round trip, including entrypoints, flows, rules, `cluster`, `trigger`, `related`, `rule` and `use`
- two entities in one flow and the same pair in two flows
- inferred parent fields and explicit parent promotion
- type alias normalisation and type conflicts
- `missing_field` severity for entries versus storage projections
- compose cycle detection, `transform_without_note` and flow structure findings
- `--check-files` finding missing references
- transactional batch `apply` (rollback on an invalid edge)
- node/edge removal, flow removal without deleting entities
- read-only query guard and invalid input rejection
- authored flows: USE membership, passages, path and joins (compose is not followed)
- Markdown documentation export (entities, entrypoints, flows, index, stale-page cleanup)
- standalone HTML rendering with EntryPoint/Flow/Rule lanes, `related` focus, cluster filter and rule panels
- linear branches: isolation from master, apply, stale propagation and no branch-of-branch

`test_samples.py` checks every fixture:

- counts match the manifest (nodes, entities, flows, fields, edges)
- ids, kinds, edge endpoints and field containment are consistent
- every flow is triggered and every entrypoint reaches a flow, directly or via `related`
- every node has a cluster and the manifest cluster count matches
- every rule has a description, evidence and an attachment
- every `file_path`/`file:line` reference exists and is within the file
- `apply` → `export` preserves nodes and edges
- `report --check-files` matches the recorded summary exactly and has no errors
- `stats` matches the manifest
- the HTML embeds the graph

## Layout

```
test/
├── run_tests.py          # unittest discovery
├── sample/               # graphs exported from eval + manifest.json
├── scripts/
│   ├── extract_samples.py  # eval logs -> test/sample
│   └── open_sample.py      # import a sample and open the HTML
└── tests/
    ├── helpers.py        # temp database and CLI helpers
    ├── test_cli.py
    └── test_samples.py
```
