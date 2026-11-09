"""OSM roads sub-workflow: make_osm_roads_per_huc.py -> validate -> FIM S3 copy (preview by default).

A parent workflow composes `build_roads_steps(ctx)`. On its own, this command defaults to the state road
parquets currently set in bash_variables.env (osm_geofabric_states_roads_dir), so the per-HUC roads can be
rebuilt for a new pre-clip without pulling OSM again.

Sample usage (inside the FIM container):
    python /foss_fim/workflows/inputs/osm_roads_workflow.py -v 20261108
    python /foss_fim/workflows/inputs/osm_roads_workflow.py -v 20261108 -lh '02040205' -o /outputs/dev_osm/osm
"""

from __future__ import annotations

import argparse
from pathlib import Path

from workflows.inputs.osm_common import (
    PROJECT_ROOT,
    ROADS_STEP,
    OsmContext,
    add_run_control_arguments,
    add_s3_arguments,
    build_copy_step,
    context_from_args,
    run_workflow_cli,
)
from workflows.shared.workflow_runner import Step, WorkflowRunner, python_command
from workflows.validators.validate_make_osm_roads_per_huc import validate_make_osm_roads_per_huc


ROADS_SCRIPT = PROJECT_ROOT / "data" / "roads" / "make_osm_roads_per_huc.py"


def build_roads_steps(ctx: OsmContext, roads_parquet_dir: Path | str | None = None) -> list[Step]:
    """make_osm_roads_per_huc.py (all regions at once), gated by validate_make_osm_roads_per_huc.

    `roads_parquet_dir` defaults to this run's pull output (`ctx.pull_roads_dir`).
    """
    if ctx.preclip_dir is None:
        raise ValueError("No pre-clip folder: pass -p or set pre_clip_huc_dir in bash_variables.env")
    source_dir = Path(roads_parquet_dir) if roads_parquet_dir is not None else ctx.pull_roads_dir
    args = ["-r", str(source_dir), "-p", str(ctx.preclip_dir), "-o", str(ctx.roads_dir)]
    args += ["-j", str(ctx.number_jobs)]
    if ctx.hucs:
        args += ["-lh", ctx.hucs]

    def validator() -> bool:
        return validate_make_osm_roads_per_huc(
            ctx.roads_dir,
            ctx.preclip_dir,
            hucs=ctx.hucs,
            states_limited=bool(ctx.states),
            baseline_dir=ctx.baseline_roads_per_huc_dir,
            report_dir=ctx.log_dir,
        )

    return [
        Step(name=ROADS_STEP, command=python_command(ROADS_SCRIPT, *args), validator=validator),
        build_copy_step(ctx, ROADS_STEP, [ctx.roads_dir]),
    ]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run make_osm_roads_per_huc.py, validate its output, "
        "then preview or upload the FIM S3 copy."
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
        "-r",
        "--roads_parquet_dir",
        type=Path,
        help="State road parquets (default: osm_geofabric_states_roads_dir)",
    )
    parser.add_argument("-p", "--preclip_dir", type=Path, help="Pre-clip folder (default: pre_clip_huc_dir)")
    parser.add_argument("-s", "--states", default="", help="States the road parquets were limited to, if any")
    parser.add_argument("-lh", "--lst_hucs", default="", help="Space-delimited HUC8 subset (default: all)")
    parser.add_argument(
        "-j", "--number_jobs", type=int, default=4, help="-j for the roads script (default 4)"
    )
    add_run_control_arguments(parser)
    add_s3_arguments(parser)
    return parser


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    try:
        ctx = context_from_args(args)
        source_dir = args.roads_parquet_dir or ctx.baseline_roads_dir
        if source_dir is None:
            raise ValueError("Pass -r: osm_geofabric_states_roads_dir is not set in bash_variables.env")
        runner = WorkflowRunner("osm_roads", ctx.log_dir, resume=args.resume, from_step=args.from_step)
        runner.add_steps(build_roads_steps(ctx, roads_parquet_dir=source_dir))
        runner.list_steps()  # validates --from_step
    except (OSError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    return run_workflow_cli(runner, ctx, args, "osm_roads_workflow", roads=True)


if __name__ == "__main__":
    raise SystemExit(main())
