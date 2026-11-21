"""Inspect AI task: skill-guided extraction with deterministic and judge scoring."""

from __future__ import annotations

import shutil

from inspect_ai import Task, task
from inspect_ai.agent import react
from inspect_ai.dataset import MemoryDataset, Sample
from inspect_ai.solver import Generate, TaskState, solver

from .prompt import system_prompt, user_prompt
from .scorers import judge_scorer, structural_scorer
from .specs import load_specs
from .tools import make_tools
from .workspace import MapSession


@solver
def extract_with_skill():
    async def solve(state: TaskState, generate: Generate) -> TaskState:
        spec = state.metadata["spec"]
        session = MapSession(spec)
        state.metadata["case_dir"] = str(session.case_dir)
        state.metadata["work_dir"] = str(session.root)

        state.metadata["init_log"] = session.init()
        agent = react(
            tools=make_tools(session),
            prompt=system_prompt(spec),
            submit=False,
            attempts=1,
        )
        try:
            state = await agent(state)
        except Exception as exc:  # keep the sample scorable when the agent dies
            state.metadata["agent_error"] = f"{type(exc).__name__}: {exc}"

        snapshot, export_log = session.export()
        state.metadata["snapshot"] = snapshot
        state.metadata["export_log"] = export_log
        state.metadata["report"] = session.report()
        state.metadata["cli_calls"] = session.call_count
        return state

    return solve


async def cleanup_case(state: TaskState) -> None:
    work_dir = state.metadata.get("work_dir")
    if work_dir:
        shutil.rmtree(work_dir, ignore_errors=True)


@task
def logistic_extraction(cases: str = "", max_messages: int = 90) -> Task:
    specs = load_specs(cases)
    samples = [
        Sample(
            id=spec["id"],
            input=user_prompt(spec),
            target="",
            metadata={"spec": spec},
        )
        for spec in specs
    ]
    return Task(
        dataset=MemoryDataset(samples, name="logistic-map-cases"),
        solver=[extract_with_skill()],
        scorer=[structural_scorer(), judge_scorer()],
        cleanup=cleanup_case,
        message_limit=max_messages,
        metadata={"skill": "logistic-map", "cases": [spec["id"] for spec in specs]},
    )
