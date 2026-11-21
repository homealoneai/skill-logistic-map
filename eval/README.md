# Evaluation

This directory validates the `logistic-map` skill against real code: a model
reads a repository slice, uses the skill CLI to build the contract graph, and a
second model grades the result. The extractor and the judge are independent
models served by a [LiteLLM](https://docs.litellm.ai/) gateway.

The harness uses [Inspect AI](https://inspect.aisi.org.uk/) for the agent
runtime, dataset, scoring, logs and viewer. It does not reimplement an eval
framework.

## Setup

Requirements: Python 3.11+ and network access to the LiteLLM gateway. Docker is
not needed: the tools only read files and run the skill CLI against a throwaway
Kuzu database.

```sh
cd eval
cp .env.example .env     # fill in URL, key and the two model ids
./bootstrap.sh           # creates eval/.venv
```

`.env` is ignored by Git. The provider wiring is:

| Variable | Used by |
|---|---|
| `LITELLM_BASE_URL` | OpenAI-compatible gateway URL (include `/v1` if required) |
| `LITELLM_API_KEY` | gateway key |
| `EXTRACTOR_MODEL` | model id used by the ReAct extractor |
| `JUDGE_MODEL` | model id bound to the Inspect `grader` role |
| `JUDGE_TEMPERATURE` | optional; unset for models that reject `temperature` |

Model ids are passed through as `openai-api/litellm/<model>`, so any id the
gateway exposes works (`anthropic/...`, `openai/...`, a deployment alias, ...).
The extractor must support tool calling.

## Run

```sh
./run.sh                        # all cases
./run.sh --cases go,node        # selected cases
./run.sh --limit 1              # quick smoke test
.venv/bin/inspect view --log-dir logs   # browse transcripts and scores
```

The process exits with `1` when any sample scores below the thresholds
(`--min-structural 0.75`, `--min-judge 0.60`), which makes it usable in CI.

Every run also writes a report next to the logs:

```
eval/reports/report-<timestamp>.md
eval/reports/report-<timestamp>.json
eval/reports/latest.md
eval/reports/latest.json
```

The Markdown report is the human summary: result, per-case scores, failed
structural checks, judge reasoning/problems, graph size, `report` findings and
token usage. The JSON is the machine-readable version. `--no-report` disables
it and `--report-dir` changes the location; `reports/` is ignored by Git.

## What is scored

Each sample produces two scores:

1. **`structural_scorer`** — deterministic checks over the exported JSON:
   graph is not empty, ids are well formed and unique, every edge resolves,
   every field has an owner, every flow has `use` and passages, every passage
   carries a declared `flow_id`, per-kind minimums are reached (including at
   least one flow and EntryPoint), `TRIGGER` edges resolve only EntryPoint →
   Flow, EntryPoints have code evidence and reach a flow directly or through
   `RELATED`, related edges connect only EntryPoints and carry description/refs,
   every contract, flow and EntryPoint carries a stable cluster label, a
   triggered flow shares its entrypoint cluster, rules resolve from a Flow or
   EntryPoint and carry a description and evidence, Inputs contain no execution
   metadata, expected activation names are present, referenced files are
   expected, at least one `file:line` reference exists and at most 20% of
   references are missing on disk, and `report` has no structural errors.
2. **`judge_scorer`** — an LLM judge receives the map digest, the skill report,
   the case rubric and excerpts of the code files the map references. It scores
   *coverage*, *accuracy*, *reference quality* and *clustering*, explains the
   problems and returns one overall score. It never sees the harness
   expectations, only the code.

The structural score is intentionally mechanical; the judge is intentionally
separate. A map that reaches the expected nodes but misdescribes the flow should
pass one and fail the other.

## Regenerate committed samples

After changing the skill or schema, rerun all cases; do not retrofit old
snapshots. The extractor accepts schema-v7 logs only:

```sh
./run.sh --cases go,java,node,android,web,otel --max-messages 100
cd ..
make samples
make test
make sample SAMPLE=go
```

`make samples` copies the newest v7 snapshot for each case into `test/sample/`,
validates paths and references, and rebuilds `manifest.json` from the eval runs.
Inspect the run report and manifest before committing. The final command opens
the regenerated Go HTML for visual review.

## Isolation

- Every sample gets a temporary Kuzu database under `eval/.work/`; it is deleted
  after scoring, so the sample repositories are never modified.
- The extractor can only read inside the case directory (plus `SKILL.md`,
  `references/`, `examples/` and `scripts/` under `skill/`).
- The extractor builds the map in bulk through `apply_graph` (the skill's
  `apply` path); individual `add-*` commands remain available for fixes.
- `export`, `render` and raw `apply` are blocked in the agent tool; the harness
  exports the snapshot itself and records `report --check-files`.
- `logs/` contains Inspect transcripts and is ignored by Git.

## Cases

One JSON file per slice in `eval/cases/`:

```json
{
  "id": "go",
  "title": "Go catalog API",
  "path": "code-sample/go",
  "scope": "what the extractor must map and what it must ignore",
  "hints": ["where to start reading"],
  "context_files": ["openapi.yml", "controller/controller.go"],
  "expect": {
    "entry_points": ["/catalog/products"],
    "min_inputs": 3,
    "min_entries": 2,
    "min_storages": 1,
    "min_fields": 6,
    "min_edges": 3,
    "files": ["controller/controller.go"]
  },
  "rubric": "case-specific judging guidance"
}
```

Add a case by dropping a new JSON file and a sample directory under
`eval/code-sample/`. Expectations should stay coarse (minimums and the entry
points that define the scope), otherwise the deterministic score becomes a
golden-file test of one particular mapping.

Current cases: Android users feature (`android`), Go catalog API (`go`), Java
orders API (`java`), Node checkout API (`node`), web character viewer data
(`web`) and the OpenTelemetry demo checkout journey (`otel`). The `otel` sample
keeps only the storefront, checkout, cart, product catalog, currency, payment,
shipping and email code; documentation and observability stacks were removed
from the fixture.

## Layout

```
eval/
├── bootstrap.sh          # venv with Python 3.11+
├── run.sh                # entry point
├── run_eval.py           # Inspect eval call, summary and exit code
├── requirements.txt
├── .env.example          # LiteLLM configuration template
├── cases/*.json          # scoped samples and expectations
├── code-sample/          # the repositories being mapped
└── logistic_eval/
    ├── config.py         # paths, env and model naming
    ├── specs.py          # case loading
    ├── workspace.py      # per-sample database and CLI
    ├── tools.py          # read/search tools + run_logistic_map
    ├── prompt.py         # extractor system/user prompts
    ├── task.py           # Inspect task and solver
    ├── report.py         # Markdown/JSON run report
    └── scorers.py        # structural checks and LLM judge
```
