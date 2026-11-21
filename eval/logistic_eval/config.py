"""Paths and settings shared by the logistic-map eval harness."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

EVAL_DIR = Path(__file__).resolve().parent.parent
REPO_DIR = EVAL_DIR.parent
SKILL_DIR = REPO_DIR
SKILL_CLI = SKILL_DIR / "scripts" / "logistic_map.py"
SKILL_GUIDE = SKILL_DIR / "SKILL.md"
CASES_DIR = EVAL_DIR / "cases"
CODE_DIR = EVAL_DIR / "code-sample"
WORK_DIR = EVAL_DIR / ".work"
LOGS_DIR = EVAL_DIR / "logs"

REQUIRED_ENV = ("LITELLM_BASE_URL", "LITELLM_API_KEY", "EXTRACTOR_MODEL", "JUDGE_MODEL")


@dataclass(frozen=True)
class Settings:
    base_url: str
    api_key: str
    extractor_model: str
    judge_model: str

    @property
    def extractor(self) -> str:
        return inspect_model(self.extractor_model)

    @property
    def judge(self) -> str:
        return inspect_model(self.judge_model)


def inspect_model(name: str) -> str:
    """Turn a LiteLLM model id into an Inspect ``openai-api`` model spec."""
    name = name.strip()
    if name.startswith("openai-api/"):
        return name
    return f"openai-api/litellm/{name}"


def load_settings(require_models: bool = True) -> Settings:
    load_dotenv(EVAL_DIR / ".env")
    missing = [key for key in REQUIRED_ENV if not os.environ.get(key)]
    if missing:
        raise SystemExit(
            "missing environment variables: "
            + ", ".join(missing)
            + f"\ncopy {EVAL_DIR / '.env.example'} to {EVAL_DIR / '.env'} and fill it in"
        )
    if require_models:
        return Settings(
            base_url=os.environ["LITELLM_BASE_URL"],
            api_key=os.environ["LITELLM_API_KEY"],
            extractor_model=os.environ["EXTRACTOR_MODEL"],
            judge_model=os.environ["JUDGE_MODEL"],
        )
    return Settings(  # type: ignore[arg-type]
        base_url=os.environ.get("LITELLM_BASE_URL", ""),
        api_key=os.environ.get("LITELLM_API_KEY", ""),
        extractor_model=os.environ.get("EXTRACTOR_MODEL", ""),
        judge_model=os.environ.get("JUDGE_MODEL", ""),
    )
