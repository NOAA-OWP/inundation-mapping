"""OSM bridges sub-workflow: bridges per HUC -> optional lidar (CONUS, Alaska) -> DEM diffs per region.

Every data step is gated by its validator and followed by a FIM S3 copy step (preview by default). A parent
workflow composes `build_bridges_steps(ctx)`; a later DEM workflow can reuse the DEM-diff steps the same way.
On its own, this command defaults to the state bridge parquets currently set in bash_variables.env
(osm_geofabric_states_bridges_dir).

Sample usage (inside the FIM container):
    python /foss_fim/workflows/inputs/osm_bridges_workflow.py -v 20261108
    python /foss_fim/workflows/inputs/osm_bridges_workflow.py -v 20261108 --run_lidar
    python /foss_fim/workflows/inputs/osm_bridges_workflow.py -v 20261108 -lh '02040205' \
        --dem_diff_regions conus -o /outputs/dev_osm/osm
"""

from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from workflows.inputs.osm_common import (
    BRIDGES_STEP,
    DEM_DIFF_STEP,
    LIDAR_REGIONS,
    LIDAR_STEP,
    PROJECT_ROOT,
    OsmContext,
    add_bridge_arguments,
    add_run_control_arguments,
    add_s3_arguments,
    build_copy_step,
    context_from_args,
    run_workflow_cli,
)
from workflows.shared.regions import dem_directories_for_region, get_region, region_for_huc
from workflows.shared.workflow_runner import Step, WorkflowRunner, python_command
from workflows.validators.validate_make_dem_dif_for_bridges import validate_make_dem_dif_for_bridges
from workflows.validators.validate_make_osm_bridges_per_huc import validate_make_osm_bridges_per_huc
from workflows.validators.validate_make_rasters_using_lidar import validate_make_rasters_using_lidar
from workflows.validators.validator_base import HUC8_PATTERN, CheckResult, ValidationResult, split_hucs


BRIDGES_SCRIPT = PROJECT_ROOT / "data" / "bridges" / "make_osm_bridges_per_huc.py"
LIDAR_SCRIPT = PROJECT_ROOT / "data" / "bridges" / "make_rasters_using_lidar.py"
DEM_DIFF_SCRIPT = PROJECT_ROOT / "data" / "bridges" / "make_dem_dif_for_bridges.py"


def bridge_hucs_for_region(bridges_dir: Path | str, region: str, selected_hucs: str = "") -> list[str]:
    """HUC8s that have a bridges_<HUC8>.parquet file in `region`, limited to `selected_hucs` when given."""
    region_name = get_region(region).name
    selected = set(split_hucs(selected_hucs))
    hucs = []
    for path in sorted(Path(bridges_dir).glob("bridges_*.parquet")):
        huc = path.stem.removeprefix("bridges_")  # bridges_<HUC8>_bad.parquet does not match HUC8_PATTERN
        if not HUC8_PATTERN.match(huc) or region_for_huc(huc).name != region_name:
            continue
        if not selected or huc in selected:
            hucs.append(huc)
    return hucs


def _lidar_step(ctx: OsmContext, bridges_dir: Path, region: str) -> Step:
    """make_rasters_using_lidar.py for one region. Its -lh list is built when the step runs."""
    region_name = get_region(region).name

    def run_lidar() -> bool:
        # DEM diffs need this folder even when a region has no bridges to process.
        ctx.lidar_processing_dir.mkdir(parents=True, exist_ok=True)
        hucs = bridge_hucs_for_region(bridges_dir, region_name, ctx.hucs)
        if not hucs:
            logging.info(f"No {region_name} bridge files in {bridges_dir}, so lidar is not needed for it")
            return True
        command = python_command(
            LIDAR_SCRIPT,
            "-i",
            str(bridges_dir),
            "-o",
            str(ctx.lidar_root),
            "-lh",
            " ".join(hucs),
            "-jl",
            str(ctx.lidar_jobs),
            "-jr",
            str(ctx.raster_jobs),
        )
        logging.info(f"Running {LIDAR_SCRIPT.name} for {len(hucs)} {region_name} HUC(s)")
        return_code = subprocess.run(command, check=False).returncode
        if return_code != 0:
            logging.error(f"{LIDAR_SCRIPT.name} for {region_name} exited with status {return_code}")
        return return_code == 0

    def validate_lidar() -> bool:
        hucs = bridge_hucs_for_region(bridges_dir, region_name, ctx.hucs)
        if not hucs:
            check = CheckResult(
                "Region HUCs", False, f"No {region_name} bridge files; lidar was not needed", True
            )
            target = f"{region_name}:{ctx.lidar_root}"
            ValidationResult("validate_make_rasters_using_lidar", target, [check]).write_report(ctx.log_dir)
            return True
        return validate_make_rasters_using_lidar(
            ctx.lidar_root, hucs=hucs, label=region_name, report_dir=ctx.log_dir
        )

    description = (
        f"{LIDAR_SCRIPT.name} -i {bridges_dir} -o {ctx.lidar_root} "
        f"-lh <{region_name} HUCs with bridge files, listed when the step runs> "
        f"-jl {ctx.lidar_jobs} -jr {ctx.raster_jobs}"
    )
    return Step(
        name=f"{LIDAR_STEP}.{region_name}",
        action=run_lidar,
        validator=validate_lidar,
        description=description,
    )


def _dem_diff_steps(ctx: OsmContext, bridges_dir: Path, region: str) -> list[Step]:
    """make_dem_dif_for_bridges.py for one region, gated by validate_make_dem_dif_for_bridges."""
    region_name = get_region(region).name
    output_dir = ctx.dem_diff_dir(region_name)
    dem_dirs = [str(directory) for directory in dem_directories_for_region(region_name, ctx.bash_variables)]
    args = ["-d", *dem_dirs, "-l", str(ctx.lidar_processing_dir), "-i", str(bridges_dir)]
    args += ["-o", str(output_dir), "-j", str(ctx.dem_jobs)]

    def validator() -> bool:
        return validate_make_dem_dif_for_bridges(output_dir, region_name, report_dir=ctx.log_dir)

    name = f"{DEM_DIFF_STEP}.{region_name}"
    return [
        Step(name=name, command=python_command(DEM_DIFF_SCRIPT, *args), validator=validator),
        build_copy_step(ctx, name, [output_dir]),
    ]


def build_bridges_steps(ctx: OsmContext, bridges_parquet_dir: Path | str | None = None) -> list[Step]:
    """Bridges per HUC, then lidar for CONUS and Alaska (only with --run_lidar), then DEM diffs per region.

    `bridges_parquet_dir` defaults to this run's pull output (`ctx.pull_bridges_dir`).
    """
    if ctx.preclip_dir is None:
        raise ValueError("No pre-clip folder: pass -p or set pre_clip_huc_dir in bash_variables.env")
    source_dir = Path(bridges_parquet_dir) if bridges_parquet_dir is not None else ctx.pull_bridges_dir
    args = ["-b", str(source_dir), "-p", str(ctx.preclip_dir), "-o", str(ctx.bridges_dir)]
    args += ["-j", str(ctx.number_jobs)]
    if ctx.hucs:
        args += ["-lh", ctx.hucs]

    def validate_bridges() -> bool:
        return validate_make_osm_bridges_per_huc(
            ctx.bridges_dir,
            ctx.preclip_dir,
            hucs=ctx.hucs,
            states_limited=bool(ctx.states),
            baseline_dir=ctx.baseline_bridges_per_huc_dir,
            report_dir=ctx.log_dir,
        )

    steps = [
        Step(name=BRIDGES_STEP, command=python_command(BRIDGES_SCRIPT, *args), validator=validate_bridges),
        build_copy_step(ctx, BRIDGES_STEP, [ctx.bridges_dir]),
    ]
    if ctx.run_lidar:
        steps.extend(_lidar_step(ctx, ctx.bridges_dir, region) for region in LIDAR_REGIONS)
        steps.append(build_copy_step(ctx, LIDAR_STEP, [ctx.lidar_root]))
    else:
        steps.append(
            Step(
                name=LIDAR_STEP,
                command=python_command(LIDAR_SCRIPT),
                enabled=False,
                skip_reason=f"--run_lidar not set; DEM diffs reuse {ctx.lidar_processing_dir.as_posix()}",
            )
        )
    for region in ctx.dem_diff_regions:
        steps.extend(_dem_diff_steps(ctx, ctx.bridges_dir, region))
    return steps


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the OSM bridges sub-workflow: bridges per HUC, optional lidar, DEM diffs per region."
    )
    parser.add_argument(
        "-v", "--version", required=True, help="Version folder name for the outputs, e.g. 20261108"
    )
    parser.add_argument(
        "-o",
        "--output_root",
        type=Path,
        help="OSM output root (default: ${inputsDir}/osm); change it for dev tests",
    )
    parser.add_argument(
        "-b",
        "--bridges_parquet_dir",
        type=Path,
        help="State bridge parquets (default: osm_geofabric_states_bridges_dir)",
    )
    parser.add_argument("-p", "--preclip_dir", type=Path, help="Pre-clip folder (default: pre_clip_huc_dir)")
    parser.add_argument(
        "-s", "--states", default="", help="States the bridge parquets were limited to, if any"
    )
    parser.add_argument("-lh", "--lst_hucs", default="", help="Space-delimited HUC8 subset (default: all)")
    parser.add_argument(
        "-j", "--number_jobs", type=int, default=4, help="-j for the bridges script (default 4)"
    )
    add_bridge_arguments(parser)
    add_run_control_arguments(parser)
    add_s3_arguments(parser)
    return parser


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    try:
        ctx = context_from_args(args)
        source_dir = args.bridges_parquet_dir or ctx.baseline_bridges_dir
        if source_dir is None:
            raise ValueError("Pass -b: osm_geofabric_states_bridges_dir is not set in bash_variables.env")
        runner = WorkflowRunner("osm_bridges", ctx.log_dir, resume=args.resume, from_step=args.from_step)
        runner.add_steps(build_bridges_steps(ctx, bridges_parquet_dir=source_dir))
        runner.list_steps()  # validates --from_step
    except (OSError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    return run_workflow_cli(runner, ctx, args, "osm_bridges_workflow", bridges=True)


if __name__ == "__main__":
    raise SystemExit(main())
