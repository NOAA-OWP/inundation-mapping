"""Small standard-library workflow runner with validation gates and resume support.

Each step runs a subprocess (`command`) or an in-process callable (`action`). A step passes only when the
command exits 0 (or the action returns True) and its `validator`, if any, returns True. The first failed
step stops the workflow. Results are saved to `<log_dir>/<name>_state.json` after every step, so a later
run can `resume` (skip steps that passed) or restart `from_step`.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from typing import Callable


_ENV_ASSIGNMENT = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")


@dataclass
class Step:
    name: str
    command: list[str] | None = None
    action: Callable[[], bool] | None = None
    validator: Callable[[], bool] | None = None
    enabled: bool = True
    skip_reason: str = ""
    description: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("A workflow step must have a name")
        if (self.command is None) == (self.action is None):
            raise ValueError("A workflow step must define exactly one of command or action")
        if self.command is not None and not self.command:
            raise ValueError(f"Step {self.name!r} has an empty command")


@dataclass
class StepResult:
    name: str
    status: str
    return_code: int | None
    validated: bool | None
    duration_sec: float
    message: str


class WorkflowRunner:
    """Run named subprocess or in-process steps, stopping at the first failed gate."""

    def __init__(self, name: str, log_dir: Path, resume: bool = False, from_step: str | None = None) -> None:
        if not name:
            raise ValueError("A workflow must have a name")
        self.name = name
        self.log_dir = Path(log_dir)
        self.resume = resume
        self.from_step = from_step
        self.state_path = self.log_dir / f"{name}_state.json"
        self.report_path = self.log_dir / f"{name}_report.md"
        self.steps: list[Step] = []
        self.results: list[StepResult] = []
        self._previous_results = self._load_previous_results() if resume or from_step else {}

    def _load_previous_results(self) -> dict[str, StepResult]:
        if not self.state_path.exists():
            return {}
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
            return {item["name"]: StepResult(**item) for item in state.get("steps", [])}
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise RuntimeError(f"Unable to read workflow resume state at {self.state_path}: {exc}") from exc

    def add_steps(self, steps: list[Step]) -> None:
        existing_names = {step.name for step in self.steps}
        new_names = [step.name for step in steps]
        if len(new_names) != len(set(new_names)) or existing_names.intersection(new_names):
            raise ValueError("Workflow step names must be unique")
        self.steps.extend(steps)

    def _start_index(self) -> int:
        if self.from_step is None:
            return 0
        names = [step.name for step in self.steps]
        if self.from_step not in names:
            raise ValueError(f"Unknown --from_step value: {self.from_step}")
        return names.index(self.from_step)

    def _skip_result(self, index: int, step: Step) -> StepResult | None:
        """Return the result to record instead of running the step, or None when the step must run."""
        if not step.enabled:
            return StepResult(step.name, "skipped", None, None, 0.0, step.skip_reason or "Disabled")
        previous = self._previous_results.get(step.name)
        passed_before = previous is not None and previous.status == "passed"
        if self.from_step is not None:
            if index >= self._start_index():
                return None
            if passed_before:
                message = f"Passed in an earlier run; not rerun (before --from_step {self.from_step})"
                return StepResult(
                    step.name,
                    "passed",
                    previous.return_code,
                    previous.validated,
                    previous.duration_sec,
                    message,
                )
            return StepResult(step.name, "skipped", None, None, 0.0, f"Before --from_step {self.from_step}")
        if self.resume and passed_before:
            return StepResult(
                step.name,
                "passed",
                previous.return_code,
                previous.validated,
                previous.duration_sec,
                "Already passed; skipped on resume",
            )
        return None

    def will_run(self, name: str) -> bool:
        """True when the named step would be executed (not skipped) by `run()`."""
        for index, step in enumerate(self.steps):
            if step.name == name:
                return self._skip_result(index, step) is None
        return False

    def list_steps(self) -> str:
        rows = ["Steps:"]
        for index, step in enumerate(self.steps):
            skipped = self._skip_result(index, step)
            status = "will run" if skipped is None else f"{skipped.status}: {skipped.message}"
            rows.append(f"{index + 1}. {step.name} [{status}]")
            if step.description:
                rows.append(f"   {step.description}")
            if step.enabled and step.command:
                rows.append(f"   {shlex.join(step.command)}")
        return "\n".join(rows)

    def _save_state(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "workflow": self.name,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "steps": [asdict(result) for result in self.results],
        }
        self.state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _record(self, result: StepResult) -> None:
        self.results.append(result)
        self._save_state()

    def _execute_step(self, step: Step) -> StepResult:
        started = time.monotonic()
        return_code: int | None = None
        validated: bool | None = None

        try:
            if step.command is not None:
                return_code = subprocess.run(step.command, check=False).returncode
                if return_code != 0:
                    return StepResult(
                        step.name,
                        "failed",
                        return_code,
                        None,
                        time.monotonic() - started,
                        f"Command exited with status {return_code}",
                    )
            elif step.action is not None and not step.action():
                return StepResult(
                    step.name, "failed", None, False, time.monotonic() - started, "Action returned False"
                )

            if step.validator is not None:
                validated = bool(step.validator())
                if not validated:
                    return StepResult(
                        step.name,
                        "failed",
                        return_code,
                        False,
                        time.monotonic() - started,
                        "Validation failed",
                    )

            return StepResult(
                step.name,
                "passed",
                return_code,
                validated,
                time.monotonic() - started,
                "Completed successfully",
            )
        except Exception as exc:  # Keep the failure in the workflow state and report.
            return StepResult(
                step.name,
                "failed",
                return_code,
                validated,
                time.monotonic() - started,
                f"{type(exc).__name__}: {exc}",
            )

    def run(self) -> bool:
        """Run the steps in order; True only if every enabled step passed (or had already passed)."""
        self._start_index()  # validates from_step before anything runs
        self.results = []

        for index, step in enumerate(self.steps):
            skipped = self._skip_result(index, step)
            if skipped is not None:
                self._record(skipped)
                continue

            result = self._execute_step(step)
            self._record(result)
            if result.status == "failed":
                self.write_report()
                return False

        self.write_report()
        return True

    def write_report(self, extra_sections: dict[str, str] | None = None) -> Path:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            f"# Workflow report: {self.name}",
            "",
            "| Step | Status | Exit code | Validated | Seconds | Message |",
            "|---|---|---:|---|---:|---|",
        ]
        for result in self.results:
            code = "" if result.return_code is None else str(result.return_code)
            validated = "" if result.validated is None else str(result.validated)
            message = result.message.replace("|", "\\|").replace("\n", " ")
            lines.append(
                f"| {result.name} | {result.status} | {code} | {validated} | "
                f"{result.duration_sec:.2f} | {message} |"
            )
        for heading, content in (extra_sections or {}).items():
            lines.extend(["", f"## {heading}", "", content])
        self.report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return self.report_path


def python_command(script: Path, *args: str) -> list[str]:
    """Build the standard subprocess command for a Python script."""
    return [sys.executable, str(script), *args]


def _parse_env_value(raw_value: str) -> str:
    value = raw_value.strip()
    if value[:1] in {"'", '"'}:
        end = value.find(value[0], 1)
        if end != -1:
            return value[1:end]
    return re.split(r"\s+#", value, maxsplit=1)[0].strip()


def read_env_file(path: Path | str) -> dict[str, str]:
    """Read `KEY=value` lines (optionally prefixed by `export`) without executing or changing the file.

    Used for src/bash_variables.env, the workflows params file and the AWS credentials file. `${name}`
    references are expanded from the file itself and from the environment (for example `${inputsDir}`).
    """
    path = Path(path)
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = _ENV_ASSIGNMENT.match(line)
        if match:
            values[match.group(1)] = _parse_env_value(match.group(2))
    for _ in range(3):  # allow values that reference other values
        substitutions = {**os.environ, **values}
        values = {key: Template(value).safe_substitute(substitutions) for key, value in values.items()}
    return values
