"""Shared context, defaults, command-line options, pull step and report for the OSM input workflows.

Output paths mirror the versioned layout in src/bash_variables.env (for example `<output_root>/roads/<v>/`).
bash_variables.env is only read here, to find the current pre-clip, DEM and baseline versions. No workflow
code writes to it; the final report lists suggested lines for the developer to apply by hand.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from string import Template

from workflows.shared.regions import (
    EXTRA_ALASKA_DEM_ENV_VAR,
    REGIONS,
    bridge_diff_variable_for_region,
    dem_directories_for_region,
    get_region,
)
from workflows.shared.s3_copy import build_s3_copy_step, copy_step_name, preflight_fim_s3
from workflows.shared.workflow_runner import Step, WorkflowRunner, python_command, read_env_file
from workflows.validators.validate_pull_osm import validate_pull_osm
from workflows.validators.validator_base import (
    count_huc_files,
    expected_preclip_hucs,
    format_list,
    split_hucs,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASH_VARIABLES_FILE = PROJECT_ROOT / "src" / "bash_variables.env"
WORKFLOWS_PARAMS_TEMPLATE = PROJECT_ROOT / "config" / "workflows_params.template.env"
DEFAULT_AWS_CREDS_FILE = Path("/data/config/aws_credentials.env")
PULL_OSM_SCRIPT = PROJECT_ROOT / "data" / "osm" / "pull_osm.py"
PRE_CLIP_SCRIPT = "/foss_fim/data/wbd/generate_pre_clip_fim_huc8.py"
ALL_REGIONS = tuple(REGIONS)
LIDAR_REGIONS = ("conus", "alaska")

# Step names shared by the step builders, the report and the guide.
PULL_STEP = "osm.pull"
ROADS_STEP = "osm.roads_per_huc"
BRIDGES_STEP = "osm.bridges_per_huc"
LIDAR_STEP = "bridges.lidar"  # bridges.lidar.<region> when --run_lidar is set
DEM_DIFF_STEP = "bridges.dem_diff"  # bridges.dem_diff.<region>


def load_bash_variables(path: Path | str | None = None) -> dict[str, str]:
    """Read src/bash_variables.env (read-only)."""
    return read_env_file(path or BASH_VARIABLES_FILE)


def load_workflow_params(path: Path | str | None = None) -> dict[str, str]:
    """Template values (FIM S3 settings are `notset`), overridden by a working params file when given."""
    params = read_env_file(WORKFLOWS_PARAMS_TEMPLATE)
    if path is not None:
        if not Path(path).is_file():
            raise FileNotFoundError(f"Workflows params file not found: {path}")
        params.update(read_env_file(path))
    return params


def setup_workflow_logging(log_dir: Path, prefix: str) -> None:
    """Log to the screen and to `<log_dir>/<prefix>_<YYYYmmdd-HHMM>.log` (FIM shared logger if available)."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    try:
        import src.utils.shared_functions as sf
    except Exception:  # e.g. a host without the FIM geospatial libraries; logging must never stop a run
        log_path = Path(log_dir) / f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}.log"
        handlers = [logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")]
        logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=handlers, force=True)
        return
    sf.setup_file_logger(str(log_dir), prefix)


def _expand(value: str | None, variables: dict[str, str]) -> Path | None:
    if not value:
        return None
    return Path(Template(value).safe_substitute({**os.environ, **variables}))


def add_run_control_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("run control")
    exclusive = group.add_mutually_exclusive_group()
    exclusive.add_argument(
        "--resume", action="store_true", help="Skip the steps that passed in the previous run"
    )
    exclusive.add_argument("--from_step", help="Restart at this step (step names come from --list_steps)")
    group.add_argument(
        "--list_steps", action="store_true", help="Print the planned steps and commands, then exit"
    )
    group.add_argument(
        "--log_dir",
        type=Path,
        help="Folder for the log, state, report and validator JSON "
        "(default: ${outputsDir}/workflow_logs/osm_<version>)",
    )


def add_bridge_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("bridges: lidar and DEM diffs")
    lidar = group.add_mutually_exclusive_group()
    lidar.add_argument(
        "--run_lidar",
        action="store_true",
        help="Run make_rasters_using_lidar.py for CONUS and Alaska (about 48 hours for all HUCs)",
    )
    lidar.add_argument(
        "--existing_lidar_dir",
        type=Path,
        help="Lidar root to reuse when --run_lidar is not set; it must contain lidar_processing/ "
        "(default: the folder above osm_bridges_modified_dir)",
    )
    group.add_argument(
        "--dem_diff_regions",
        nargs="+",
        default=list(ALL_REGIONS),
        help=f"Regions for make_dem_dif_for_bridges.py, space separated (default: {' '.join(ALL_REGIONS)})",
    )
    group.add_argument(
        "-jl", "--lidar_jobs", type=int, default=15, help="-jl for the lidar script (default 15)"
    )
    group.add_argument(
        "-jr", "--raster_jobs", type=int, default=10, help="-jr for the lidar script (default 10)"
    )
    group.add_argument(
        "-jd", "--dem_jobs", type=int, default=10, help="-j for the DEM-diff script (default 10)"
    )


def add_s3_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("FIM S3 copy (preview only unless --upload_to_fim_s3)")
    group.add_argument(
        "--upload_to_fim_s3", action="store_true", help="Upload each validated output to FIM S3"
    )
    group.add_argument(
        "-wp",
        "--workflows_params_file",
        type=Path,
        help="Working copy of config/workflows_params.template.env; its values override the template",
    )
    group.add_argument(
        "-a",
        "--aws_creds_file",
        type=Path,
        default=DEFAULT_AWS_CREDS_FILE,
        help=f"AWS credentials file, read only for uploads (default: {DEFAULT_AWS_CREDS_FILE.as_posix()})",
    )
    group.add_argument("--s3_overwrite", action="store_true", help="Allow uploads into S3 folders that exist")


@dataclass
class OsmContext:
    """Version, folders and options shared by every OSM step builder."""

    version: str
    output_root: Path | str | None = None
    preclip_dir: Path | str | None = None
    states: str = ""
    hucs: str = ""
    number_jobs: int = 4
    dem_jobs: int = 10
    lidar_jobs: int = 15
    raster_jobs: int = 10
    run_lidar: bool = False
    existing_lidar_dir: Path | str | None = None
    dem_diff_regions: tuple[str, ...] | str = ALL_REGIONS
    log_dir: Path | str | None = None
    bash_variables: dict[str, str] | None = None
    workflow_params: dict[str, str] | None = None
    aws_creds_file: Path | str | None = None
    upload_to_fim_s3: bool = False
    s3_overwrite: bool = False
    s3_copy_summaries: list[str] = field(default_factory=list)
    s3_copy_targets: dict[str, list[Path]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.version = str(self.version or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9._-]+", self.version):
            raise ValueError(
                f"The version (-v) must be a simple folder name such as 20261108, not {self.version!r}"
            )
        # Copies, so callers' dictionaries are never changed.
        self.bash_variables = dict(
            load_bash_variables() if self.bash_variables is None else self.bash_variables
        )
        self.workflow_params = dict(self.workflow_params or {})
        extra_alaska_dirs = self.workflow_params.get(EXTRA_ALASKA_DEM_ENV_VAR, "").strip()
        if extra_alaska_dirs:
            self.bash_variables[EXTRA_ALASKA_DEM_ENV_VAR] = extra_alaska_dirs
        self.states = " ".join(self.states.split())
        self.hucs = " ".join(self.hucs.split())

        if self.output_root is None:
            inputs_dir = os.getenv("inputsDir")
            if not inputs_dir:
                raise ValueError("Set inputsDir or pass an output root (-o)")
            self.output_root = Path(inputs_dir) / "osm"
        self.output_root = Path(self.output_root)
        if self.preclip_dir is None:
            self.preclip_dir = _expand(self.bash_variables.get("pre_clip_huc_dir"), self.bash_variables)
        else:
            self.preclip_dir = Path(self.preclip_dir)
        if self.log_dir is None:
            outputs_dir = os.getenv("outputsDir")
            log_root = Path(outputs_dir) if outputs_dir else self.output_root
            self.log_dir = log_root / "workflow_logs" / f"osm_{self.version}"
        self.log_dir = Path(self.log_dir)

        if min(self.number_jobs, self.dem_jobs, self.lidar_jobs, self.raster_jobs) < 1:
            raise ValueError("Job counts (-j, -jd, -jl, -jr) must be at least 1")
        regions = (
            self.dem_diff_regions.split() if isinstance(self.dem_diff_regions, str) else self.dem_diff_regions
        )
        self.dem_diff_regions = tuple(dict.fromkeys(get_region(region).name for region in regions))
        if self.existing_lidar_dir is not None:
            self.existing_lidar_dir = Path(self.existing_lidar_dir)
        if self.aws_creds_file is not None:
            self.aws_creds_file = Path(self.aws_creds_file)

    # ----- new outputs (same layout as bash_variables.env) -----
    @property
    def geofabric_dir(self) -> Path:
        return self.output_root / "geofabric_states" / self.version

    @property
    def pull_roads_dir(self) -> Path:
        return self.geofabric_dir / "states_parquet" / "roads"

    @property
    def pull_bridges_dir(self) -> Path:
        return self.geofabric_dir / "states_parquet" / "bridges"

    @property
    def roads_dir(self) -> Path:
        return self.output_root / "roads" / self.version

    @property
    def bridges_dir(self) -> Path:
        return self.output_root / "bridges" / "bridge_lines" / self.version

    @property
    def lidar_root(self) -> Path:
        """New lidar folder with --run_lidar, otherwise the existing one that DEM diffs reuse."""
        if self.run_lidar:
            return self.output_root / "bridges" / "lidar_data" / self.version
        if self.existing_lidar_dir is not None:
            return Path(self.existing_lidar_dir)
        modified_dir = _expand(self.bash_variables.get("osm_bridges_modified_dir"), self.bash_variables)
        if modified_dir is not None:
            return modified_dir.parent
        return self.output_root / "bridges" / "lidar_data" / self.version

    @property
    def lidar_processing_dir(self) -> Path:
        return self.lidar_root / "lidar_processing"

    @property
    def modified_bridges_dir(self) -> Path:
        return self.lidar_root / "modified_osm_bridges"

    def dem_diff_dir(self, region: str) -> Path:
        return self.output_root / "bridges" / "DEM_Diffs" / self.version / get_region(region).name

    @property
    def s3_local_root(self) -> Path:
        """Local folder that maps to FIM_S3_ROOT_INPUTS_PATH (the parent of the output root)."""
        return self.output_root.parent

    # ----- current versions from bash_variables.env (validator baselines) -----
    @property
    def baseline_roads_dir(self) -> Path | None:
        return _expand(self.bash_variables.get("osm_geofabric_states_roads_dir"), self.bash_variables)

    @property
    def baseline_bridges_dir(self) -> Path | None:
        return _expand(self.bash_variables.get("osm_geofabric_states_bridges_dir"), self.bash_variables)

    @property
    def baseline_roads_per_huc_dir(self) -> Path | None:
        return _expand(self.bash_variables.get("osm_roads_per_huc_dir"), self.bash_variables)

    @property
    def baseline_bridges_per_huc_dir(self) -> Path | None:
        """bridges/bridge_lines/<v> for the lidar version in osm_bridges_modified_dir."""
        modified_dir = _expand(self.bash_variables.get("osm_bridges_modified_dir"), self.bash_variables)
        if modified_dir is None or len(modified_dir.parents) < 3:
            return None
        return modified_dir.parents[2] / "bridge_lines" / modified_dir.parent.name


def context_from_args(args: argparse.Namespace) -> OsmContext:
    """Build an OsmContext from the options added by the add_*_arguments helpers."""
    values = vars(args)
    kwargs = {
        "version": args.version,
        "output_root": args.output_root,
        "preclip_dir": args.preclip_dir,
        "states": args.states,
        "hucs": args.lst_hucs,
        "number_jobs": args.number_jobs,
        "log_dir": args.log_dir,
        "workflow_params": load_workflow_params(args.workflows_params_file),
        "aws_creds_file": args.aws_creds_file,
        "upload_to_fim_s3": args.upload_to_fim_s3,
        "s3_overwrite": args.s3_overwrite,
    }
    for name in ("dem_jobs", "lidar_jobs", "raster_jobs", "run_lidar", "existing_lidar_dir"):
        if name in values:
            kwargs[name] = values[name]
    if "dem_diff_regions" in values:
        kwargs["dem_diff_regions"] = tuple(
            region for value in args.dem_diff_regions for region in value.split()
        )
    return OsmContext(**kwargs)


def build_copy_step(ctx: OsmContext, name: str, paths: list[Path]) -> Step:
    """FIM S3 copy step (preview unless ctx.upload_to_fim_s3) for the outputs that step `name` validated."""
    ctx.s3_copy_targets[copy_step_name(name)] = [Path(path) for path in paths]
    return build_s3_copy_step(
        name,
        paths,
        ctx.s3_local_root,
        ctx.workflow_params,
        ctx.aws_creds_file,
        upload=ctx.upload_to_fim_s3,
        overwrite=ctx.s3_overwrite,
        summary=ctx.s3_copy_summaries,
        manifest_dir=ctx.log_dir,
    )


def build_pull_steps(ctx: OsmContext) -> list[Step]:
    """pull_osm.py for the requested states (all regions at once), gated by validate_pull_osm."""
    args = ["-o", str(ctx.geofabric_dir)]
    if ctx.states:
        args += ["-s", ctx.states]

    def validator() -> bool:
        return validate_pull_osm(
            ctx.geofabric_dir,
            states=ctx.states,
            baseline_roads_dir=ctx.baseline_roads_dir,
            baseline_bridges_dir=ctx.baseline_bridges_dir,
            report_dir=ctx.log_dir,
        )

    return [
        Step(name=PULL_STEP, command=python_command(PULL_OSM_SCRIPT, *args), validator=validator),
        build_copy_step(ctx, PULL_STEP, [ctx.geofabric_dir]),
    ]


def _preclip_checks(ctx: OsmContext) -> tuple[list[str], list[str]]:
    preclip_dir = Path(ctx.preclip_dir) if ctx.preclip_dir is not None else None
    if preclip_dir is None or not preclip_dir.is_dir():
        return [f"The pre-clip folder is missing: {ctx.preclip_dir}"], []
    selected = split_hucs(ctx.hucs)
    hucs = expected_preclip_hucs(preclip_dir, selected)
    if not hucs:
        return [f"No HUC8 folders were found in the pre-clip folder {preclip_dir.as_posix()}"], []

    errors: list[str] = []
    warnings: list[str] = []
    missing_folders = [huc for huc in hucs if not (preclip_dir / huc).is_dir()]
    if missing_folders:
        errors.append(f"HUC(s) given with -lh are not in the pre-clip folder: {format_list(missing_folders)}")
    missing_wbd = [
        huc for huc in hucs if (preclip_dir / huc).is_dir() and not (preclip_dir / huc / "wbd.gpkg").is_file()
    ]
    if missing_wbd:
        message = (
            f"Pre-clip HUC folder(s) without wbd.gpkg (the scripts skip them): {format_list(missing_wbd)}"
        )
        if selected:
            errors.append(message)
        else:
            warnings.append(message)
    return errors, warnings


def preflight_checks(
    ctx: OsmContext,
    *,
    pull: bool = False,
    roads: bool = False,
    bridges: bool = False,
    allow_existing_outputs: bool = False,
) -> tuple[list[str], list[str]]:
    """Fail-fast checks for the parts that will run, before any long step. Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []
    targets = []
    if pull:
        targets.append(ctx.geofabric_dir)
    if roads:
        targets.append(ctx.roads_dir)
    if bridges:
        targets.append(ctx.bridges_dir)
        if ctx.run_lidar:
            targets.append(ctx.lidar_root)
        targets.extend(ctx.dem_diff_dir(region) for region in ctx.dem_diff_regions)
    existing = [path.as_posix() for path in targets if path.exists()]
    if existing and not allow_existing_outputs:
        errors.append(
            "Output folder(s) for this version already exist; use --resume or --from_step to reuse them, "
            f"or choose a new -v: {', '.join(existing)}"
        )

    if roads or bridges:
        preclip_errors, preclip_warnings = _preclip_checks(ctx)
        errors.extend(preclip_errors)
        warnings.extend(preclip_warnings)
    if bridges:
        if not ctx.run_lidar and not ctx.lidar_processing_dir.is_dir():
            errors.append(
                f"The existing lidar folder is missing: {ctx.lidar_processing_dir.as_posix()} "
                "(set --existing_lidar_dir or use --run_lidar)"
            )
        for region in ctx.dem_diff_regions:
            directories = dem_directories_for_region(region, ctx.bash_variables)
            missing = [directory.as_posix() for directory in directories if not directory.is_dir()]
            if missing:
                errors.append(f"DEM folder(s) for the {region} DEM diffs are missing: {', '.join(missing)}")
    return errors, warnings


def s3_upload_preflight(ctx: OsmContext, runner: WorkflowRunner) -> list[str]:
    """With --upload_to_fim_s3, check FIM S3 for the copy steps that will run (not ones that passed)."""
    if not ctx.upload_to_fim_s3:
        return []
    paths = [
        path for name, targets in ctx.s3_copy_targets.items() if runner.will_run(name) for path in targets
    ]
    return preflight_fim_s3(
        paths, ctx.s3_local_root, ctx.workflow_params, ctx.aws_creds_file, overwrite=ctx.s3_overwrite
    )


# ----- final report -----
def _bash_value(path: Path) -> str:
    inputs_dir = os.getenv("inputsDir")
    if inputs_dir:
        try:
            return "${inputsDir}/" + Path(path).relative_to(Path(inputs_dir)).as_posix()
        except ValueError:
            pass
    return Path(path).as_posix()


def _passed_steps(runner: WorkflowRunner) -> set[str]:
    return {result.name for result in runner.results if result.status == "passed"}


def _lidar_ran(passed: set[str]) -> bool:
    return all(f"{LIDAR_STEP}.{region}" in passed for region in LIDAR_REGIONS)


def suggested_bash_variables(ctx: OsmContext, runner: WorkflowRunner) -> list[str]:
    """`export` lines for outputs that were produced and validated (never written to bash_variables.env)."""
    passed = _passed_steps(runner)
    lines = []
    if PULL_STEP in passed:
        if any(ctx.pull_roads_dir.glob("*_roads.parquet")):
            lines.append(f"export osm_geofabric_states_roads_dir={_bash_value(ctx.pull_roads_dir)}/")
        if any(ctx.pull_bridges_dir.glob("*_bridges.parquet")):
            lines.append(f"export osm_geofabric_states_bridges_dir={_bash_value(ctx.pull_bridges_dir)}/")
    if ROADS_STEP in passed and count_huc_files(ctx.roads_dir, "roads"):
        lines.append(f"export osm_roads_per_huc_dir={_bash_value(ctx.roads_dir)}/")
    if BRIDGES_STEP in passed:
        if _lidar_ran(passed) and any(ctx.modified_bridges_dir.glob("huc_*_osm_bridges_modified.parquet")):
            lines.append(f"export osm_bridges_modified_dir={_bash_value(ctx.modified_bridges_dir)}/")
        for region in ctx.dem_diff_regions:
            vrt_path = ctx.dem_diff_dir(region) / "bridge_elev_diff.vrt"
            if f"{DEM_DIFF_STEP}.{region}" in passed and vrt_path.is_file():
                lines.append(f"export {bridge_diff_variable_for_region(region)}={_bash_value(vrt_path)}")
    return lines


def validation_summaries(log_dir: Path) -> str:
    """One line per validator JSON report, plus every failed or warning check."""
    reports = sorted(Path(log_dir).glob("validate_*.json"))
    if not reports:
        return "No validator reports were written."
    lines = []
    for path in reports:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            lines.append(f"- `{path.name}`: unreadable ({exc})")
            continue
        checks = payload.get("checks", [])
        failed = [check for check in checks if not check.get("passed") and not check.get("is_warning")]
        warnings = [check for check in checks if not check.get("passed") and check.get("is_warning")]
        status = "PASS" if payload.get("passed") else "FAIL"
        lines.append(
            f"- **{status}** `{payload.get('validator')}` for `{payload.get('target')}`: "
            f"{len(checks) - len(failed) - len(warnings)} passed, {len(failed)} failed, "
            f"{len(warnings)} warning(s) (`{path.name}`)"
        )
        lines.extend(f"  - FAIL {check.get('name')}: {check.get('message')}" for check in failed)
        lines.extend(f"  - WARNING {check.get('name')}: {check.get('message')}" for check in warnings)
    return "\n".join(lines)


def manual_next_steps(ctx: OsmContext, runner: WorkflowRunner) -> str:
    passed = _passed_steps(runner)
    flags = []
    if ROADS_STEP in passed:
        flags.append("--osm_roads")
    if BRIDGES_STEP in passed and _lidar_ran(passed):
        flags.append("--osm_bridges")
    lines = [
        f"1. Review this report and the validator JSON files in `{Path(ctx.log_dir).as_posix()}`.",
        "2. Apply the suggested lines to `src/bash_variables.env` yourself. The workflow never edits it, "
        "and pre-clip reads the new paths from it.",
    ]
    if flags:
        preclip_dir = Path(ctx.preclip_dir).as_posix() if ctx.preclip_dir else "<current pre_clip_huc_dir>"
        lines.append(
            "3. When every input workflow you need has finished, run pre-clip by hand, for example:\n\n"
            "```bash\n"
            f"python {PRE_CLIP_SCRIPT} \\\n"
            "    -u /data/inputs/huc_lists/full_huc_list.lst \\\n"
            "    -n /data/inputs/pre_clip_huc8/<new_version>/ \\\n"
            f"    --copy_from_dir {preclip_dir}/ \\\n"
            f"    {' '.join(flags)}\n"
            "```"
        )
    else:
        lines.append("3. Nothing from this run needs a pre-clip refresh yet.")
    return "\n".join(lines)


def report_sections(
    ctx: OsmContext,
    runner: WorkflowRunner,
    preflight_errors: list[str] | None = None,
    preflight_warnings: list[str] | None = None,
) -> dict[str, str]:
    """Markdown sections appended to the runner's step table."""
    sections: dict[str, str] = {}
    if preflight_errors is not None or preflight_warnings:
        lines = [f"- ERROR: {error}" for error in preflight_errors or []]
        lines += [f"- WARNING: {warning}" for warning in preflight_warnings or []]
        sections["Pre-flight checks"] = "\n".join(lines) if lines else "Passed."
    sections["Validation summaries"] = validation_summaries(ctx.log_dir)
    sections["FIM S3 copies"] = (
        "\n".join(f"- {line}" for line in ctx.s3_copy_summaries) or "No FIM S3 copy step ran."
    )
    suggestions = suggested_bash_variables(ctx, runner)
    sections["Suggested src/bash_variables.env edits"] = (
        "Not applied. Review them, then edit `src/bash_variables.env` by hand:\n\n```bash\n"
        + "\n".join(suggestions)
        + "\n```"
        if suggestions
        else "None: no new output passed validation in this run."
    )
    lidar_placeholder = next((step for step in runner.steps if step.name == LIDAR_STEP), None)
    if lidar_placeholder is not None and not lidar_placeholder.enabled:
        sections["Lidar was not run"] = (
            f"DEM diffs used the existing lidar folder `{ctx.lidar_processing_dir.as_posix()}`. "
            "`osm_bridges_modified_dir` is unchanged, so pre-clip `--osm_bridges` would keep transferring "
            "the older modified bridges. Run again with `--run_lidar` to refresh them."
        )
    sections["Manual next steps"] = manual_next_steps(ctx, runner)
    return sections


def run_workflow_cli(
    runner: WorkflowRunner,
    ctx: OsmContext,
    args: argparse.Namespace,
    log_prefix: str,
    *,
    pull: bool = False,
    roads: bool = False,
    bridges: bool = False,
) -> int:
    """Shared main() body: --list_steps, logging, pre-flight checks, the run and the final report."""
    if args.list_steps:
        print(runner.list_steps())
        return 0

    setup_workflow_logging(ctx.log_dir, log_prefix)
    errors, warnings = preflight_checks(
        ctx,
        pull=pull,
        roads=roads,
        bridges=bridges,
        allow_existing_outputs=args.resume or args.from_step is not None,
    )
    if not errors:
        errors = s3_upload_preflight(ctx, runner)
    for warning in warnings:
        logging.warning(f"Pre-flight warning: {warning}")
    for error in errors:
        logging.error(f"Pre-flight error: {error}")
    if errors:
        report_path = runner.write_report(report_sections(ctx, runner, errors, warnings))
        logging.error(f"Pre-flight checks failed, so nothing was run. Report: {report_path}")
        return 1

    logging.info(runner.list_steps())
    success = runner.run()
    report_path = runner.write_report(report_sections(ctx, runner, [], warnings))
    logging.info(f"Workflow {runner.name} {'passed' if success else 'FAILED'}. Report: {report_path}")
    return 0 if success else 1
