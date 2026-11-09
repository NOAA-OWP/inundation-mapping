"""Validate the per-state road and bridge GeoParquet outputs of `data/osm/pull_osm.py`.

Sample usage (standalone, against a manual run):
    python /foss_fim/workflows/validators/validate_pull_osm.py \
        -o /data/inputs/osm/geofabric_states/20261108 -s 'delaware' --report_dir /outputs/temp
"""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path

from workflows.validators.validator_base import (
    CheckResult,
    ValidationResult,
    check_log_markers,
    check_parquet_schema,
    check_ratio_vs_baseline,
    newest_log,
    read_log,
    run_validator_cli,
)


# Thresholds (first guesses; tune after real runs).
MIN_ROAD_ROWS = 100
MIN_BRIDGE_ROWS = 1
BASELINE_ROW_RATIO = (0.8, 1.5)
ROAD_COLUMNS = ("osmid", "highway", "geometry")
BRIDGE_COLUMNS = ("osmid", "bridge_type", "geometry")
STATE_CRS = {"alaska": 3338, "american-oceania": 4326}
DEFAULT_STATE_CRS = 5070

# Log patterns written by pull_osm.py (a host test checks they still exist in its source).
LOG_GLOB = "pull_osm_*.log"
BRIDGES_LOG_TAG = "[bridges]"
BRIDGES_SKIP_WORD = "skipping"
FAILURE_MARKERS = ("❌",)

PULL_SCRIPT = Path(__file__).resolve().parents[2] / "data" / "osm" / "pull_osm.py"


def _supported_states() -> list[str]:
    """Read the source state's literal list without importing its geospatial dependencies."""
    tree = ast.parse(PULL_SCRIPT.read_text(encoding="utf-8"))
    constants: dict[str, str] = {}
    state_list: list[object] | None = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if target.id == "ALL_STATES" and isinstance(node.value, ast.List):
                        state_list = node.value.elts
                    else:
                        try:
                            value = ast.literal_eval(node.value)
                        except (ValueError, TypeError):
                            continue
                        if isinstance(value, str):
                            constants[target.id] = value

    if state_list is None:
        raise RuntimeError(f"Unable to read ALL_STATES from {PULL_SCRIPT}")
    resolved: list[str] = []
    for item in state_list:
        if isinstance(item, ast.Constant) and isinstance(item.value, str):
            resolved.append(item.value)
        elif isinstance(item, ast.Name) and item.id in constants:
            resolved.append(constants[item.id])
        else:
            raise RuntimeError(f"Unable to resolve state in ALL_STATES from {PULL_SCRIPT}")
    return resolved


def _requested_states(states: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(states, str):
        selected = states.split()
    else:
        selected = [str(state) for state in states]
    return selected or _supported_states()


def _state_crs(state: str) -> int:
    return STATE_CRS.get(state, DEFAULT_STATE_CRS)


def _parquet_rows(path: Path) -> int | None:
    try:
        import pyarrow.parquet as pq

        return pq.read_metadata(path).num_rows
    except Exception:
        return None


def _bridge_skip_logged(log_content: str, state: str) -> bool:
    """True when pull_osm.py logged that it wrote no bridges parquet for this state on purpose."""
    pattern = rf"{re.escape(BRIDGES_LOG_TAG)}\s+{re.escape(state)}:\s+.*{BRIDGES_SKIP_WORD}"
    return re.search(pattern, log_content) is not None


def run_checks(
    output_dir: Path | str,
    states: str | list[str] | tuple[str, ...] = "",
    baseline_roads_dir: Path | str | None = None,
    baseline_bridges_dir: Path | str | None = None,
) -> ValidationResult:
    output_dir = Path(output_dir)
    roads_dir = output_dir / "states_parquet" / "roads"
    bridges_dir = output_dir / "states_parquet" / "bridges"
    baseline_roads = Path(baseline_roads_dir) if baseline_roads_dir else None
    baseline_bridges = Path(baseline_bridges_dir) if baseline_bridges_dir else None
    selected_states = _requested_states(states)
    log_path = newest_log(output_dir, LOG_GLOB)
    log_content = read_log(log_path)
    checks: list[CheckResult] = []

    for state in selected_states:
        roads_path = roads_dir / f"{state}_roads.parquet"
        bridges_path = bridges_dir / f"{state}_bridges.parquet"
        if not roads_path.is_file():
            checks.append(CheckResult(f"Roads output: {state}", False, f"Missing {roads_path}"))
        else:
            checks.extend(
                CheckResult(f"{state} roads: {check.name}", check.passed, check.message, check.is_warning)
                for check in check_parquet_schema(
                    roads_path,
                    required_columns=ROAD_COLUMNS,
                    min_rows=MIN_ROAD_ROWS,
                    expected_crs=_state_crs(state),
                )
            )

        if not bridges_path.is_file():
            if _bridge_skip_logged(log_content, state):
                checks.append(
                    CheckResult(
                        f"Bridges output: {state}",
                        False,
                        "No bridge parquet was produced; the pull log records an expected skip",
                        is_warning=True,
                    )
                )
            else:
                checks.append(CheckResult(f"Bridges output: {state}", False, f"Missing {bridges_path}"))
        else:
            checks.extend(
                CheckResult(f"{state} bridges: {check.name}", check.passed, check.message, check.is_warning)
                for check in check_parquet_schema(
                    bridges_path,
                    required_columns=BRIDGE_COLUMNS,
                    min_rows=MIN_BRIDGE_ROWS,
                    expected_crs=_state_crs(state),
                )
            )

        for label, output_path, baseline_dir in (
            ("roads", roads_path, baseline_roads),
            ("bridges", bridges_path, baseline_bridges),
        ):
            if not output_path.is_file():
                continue
            baseline_path = baseline_dir / output_path.name if baseline_dir else None
            if baseline_path is None or not baseline_path.is_file():
                checks.append(
                    CheckResult(
                        f"{state} {label} baseline",
                        False,
                        "Baseline parquet is unavailable; row-count comparison was not made",
                        is_warning=True,
                    )
                )
                continue
            current_rows = _parquet_rows(output_path)
            baseline_rows = _parquet_rows(baseline_path)
            if current_rows is None or baseline_rows is None:
                checks.append(
                    CheckResult(
                        f"{state} {label} baseline",
                        False,
                        "Could not read current and baseline row counts",
                        is_warning=True,
                    )
                )
                continue
            checks.append(
                check_ratio_vs_baseline(
                    current_rows, baseline_rows, *BASELINE_ROW_RATIO, name=f"{state} {label} baseline ratio"
                )
            )

    checks.append(check_log_markers(log_path, FAILURE_MARKERS, name="Pull failure markers"))
    return ValidationResult("validate_pull_osm", str(output_dir), checks)


def validate_pull_osm(
    output_dir: Path | str,
    states: str | list[str] | tuple[str, ...] = "",
    baseline_roads_dir: Path | str | None = None,
    baseline_bridges_dir: Path | str | None = None,
    report_dir: Path | None = None,
) -> bool:
    result = run_checks(output_dir, states, baseline_roads_dir, baseline_bridges_dir)
    if report_dir is not None:
        result.write_report(report_dir)
    return result.passed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate per-state OSM roads and bridges outputs (exit 0 = pass)."
    )
    parser.add_argument("-o", "--output_dir", required=True, help="The -o root folder given to pull_osm.py")
    parser.add_argument(
        "-s", "--states", default="", help="Space-delimited state slugs; defaults to all states"
    )
    parser.add_argument("--baseline_roads_dir", type=Path, help="Current osm_geofabric_states_roads_dir")
    parser.add_argument("--baseline_bridges_dir", type=Path, help="Current osm_geofabric_states_bridges_dir")
    parser.add_argument("--report_dir", type=Path, default=Path("."), help="Folder for the JSON report")
    args = parser.parse_args()
    result = run_checks(args.output_dir, args.states, args.baseline_roads_dir, args.baseline_bridges_dir)
    return run_validator_cli(result, args.report_dir)


if __name__ == "__main__":
    raise SystemExit(main())
