# data-flow-map

A portable agent skill that keeps a [Kuzu](https://kuzudb.com/) graph of how data
moves through an application, then answers the questions a contract review
usually misses:

- Which input does this field come from, and what changes it on the way?
- Which fields does the producer send that the consumer never reads?
- Where does a type disagree between two contracts?
- What does an input affect downstream, and by which paths?

It records **Input** contracts (queues, HTTP routes, files, cron jobs), **Entry**
transformation points, **Storage** locations and the **Field** tree of each
contract. A CLI writes the graph and produces reports; `render` turns it into a
standalone interactive HTML map with an **Impact** mode.

Everything runs on plain Python plus a static HTML file: no server, no cloud
service, no harness-specific runtime.

![Selected contract impact](assets/preview.png)

## Requirements

- Python 3.8 or newer
- Bash (macOS or Linux)
- No global installs: `scripts/bootstrap.sh` creates a project-local venv and
  installs Kuzu (`>=0.11,<0.12`) with pip.

## Install

A skill is a directory containing `SKILL.md` plus optional resources. Clone this
repository wherever your harness looks for skills:

```sh
mkdir -p .agents/skills
git clone git@github.com:homealoneai/skill-logistic-map.git .agents/skills/data-flow-map
```

The directory must be named `data-flow-map`, matching the `name` in `SKILL.md`.

Common skill directories:

| Scope | Directory |
|---|---|
| Shared, project | `.agents/skills/` |
| Shared, user | `~/.agents/skills/` |
| Claude Code, project | `.claude/skills/` |
| Claude Code, user | `~/.claude/skills/` |

Other harnesses use their own directory; the requirement is that the harness
discovers `SKILL.md` inside a directory named `data-flow-map`. Keeping the
checkout elsewhere and symlinking it works too:

```sh
git clone git@github.com:homealoneai/skill-logistic-map.git ~/src/data-flow-map
mkdir -p ~/.agents/skills
ln -s ~/src/data-flow-map ~/.agents/skills/data-flow-map
```

Any harness that follows the common skill layout (YAML frontmatter in
`SKILL.md`, plus `scripts/`, `references/` and `assets/`) can load it as-is.

### Use without a harness

The CLI has no agent dependency:

```sh
python3 -m venv .dataflow/.venv
.dataflow/.venv/bin/pip install "kuzu>=0.11,<0.12"
.dataflow/.venv/bin/python /path/to/data-flow-map/scripts/dataflow.py init
```

## Quick start

Run from the root of the repository being mapped:

```sh
SKILL_DIR=/path/to/data-flow-map
PY=.dataflow/.venv/bin/python

bash "$SKILL_DIR/scripts/bootstrap.sh"

"$PY" "$SKILL_DIR/scripts/dataflow.py" add-node --kind input \
  --id input:orders.created --entry-point queue:orders.created --channel queue \
  --file services/orders/src/consumer.py \
  --ref services/orders/src/consumer.py:24

"$PY" "$SKILL_DIR/scripts/dataflow.py" add-field \
  --owner input:orders.created --path customer.email --type string

"$PY" "$SKILL_DIR/scripts/dataflow.py" add-node --kind entry \
  --id entry:validate-order --entry-point validate_order --entry-type function \
  --file services/orders/src/validate.py

"$PY" "$SKILL_DIR/scripts/dataflow.py" add-edge --kind compose \
  --from input:orders.created --to entry:validate-order

"$PY" "$SKILL_DIR/scripts/dataflow.py" report
"$PY" "$SKILL_DIR/scripts/dataflow.py" render
```

A batch seed is available at `examples/orders-flow.json`:

```sh
"$PY" "$SKILL_DIR/scripts/dataflow.py" apply --file "$SKILL_DIR/examples/orders-flow.json"
```

## Commands

| Command | Purpose |
|---|---|
| `init` | Create the graph schema |
| `doctor` | Check Python/Kuzu/database/git |
| `add-node` / `add-field` / `add-edge` | Register contracts, fields and relationships |
| `apply --file` | Transactional batch upsert (same shape as `export`) |
| `remove-node` / `remove-edge` | Remove nodes (with owned fields) or edges |
| `list` / `query` | Inspect nodes, run read-only Cypher |
| `report` | Missing fields, type conflicts, cycles, orphans, shape problems |
| `export` | JSON snapshot for review and re-apply |
| `render` | Standalone interactive HTML map |
| `stats` | Counters by kind and channel |
| `flow build` / `flow list` / `flow show` / `flow sync` | Derive and inspect flow graphs with explicit joins |

## Flows

A procedure with several inputs is a join, and a linear path through the contract
graph hides that. `flow build` derives a second, read-only graph of steps and
feeds from the contract graph:

```sh
"$PY" "$SKILL_DIR/scripts/dataflow.py" flow build \
  --from storage:orders --name "Orders to notifications"
"$PY" "$SKILL_DIR/scripts/dataflow.py" flow show flow:orders-to-notifications
```

A step with multiple incoming `FEEDS` is a join; inputs outside the forward
reach are kept as `context`, so the tree does not pretend one table is the only
source. The contract graph is never modified; rerun `flow sync` after changes.

`SKILL.md` is the agent-facing guide. Detailed references:

- `references/schema.md` — tables, properties, IDs and invariants
- `references/cli.md` — every command, flag and output shape
- `references/modeling.md` — what should become a node, field or edge
- `references/report-rules.md` — finding codes, severities and fixes
- `references/flows.md` — derived flow graph: steps, joins, context and limits

## HTML map

`render` writes `.dataflow/graph.html` (or `--out PATH`). It is self-contained:
no CDN, no server. Click nodes or edges for details, enable **Fields** to show
the field tree, and enable **Impact** with a contract selected to dim everything
outside its downstream reach and list affected contracts and paths.

## Layout

```
SKILL.md               agent instructions
README.md              repository and install guide
references/            schema, CLI, modeling, report and flow rules
scripts/dataflow.py    CLI: schema, writes, queries, reports, export, render
scripts/bootstrap.sh   venv + Kuzu + init
assets/graph.html      self-contained visualization template
assets/preview.png     example rendering
examples/              seed graphs
```

## License

MIT — see [LICENSE](LICENSE).
