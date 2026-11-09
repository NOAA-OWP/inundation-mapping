"""Parent OSM input workflow: pull_osm.py -> roads per HUC -> bridges per HUC -> optional lidar -> DEM diffs.

Every data step is gated by its validator and followed by a FIM S3 copy step (preview unless
--upload_to_fim_s3). Pre-flight checks run before any long step. The final report lists suggested
src/bash_variables.env lines and the manual pre-clip run; the workflow never edits bash_variables.env.
See workflows/workflow_automation.md for the full guide.

Sample usage (inside the FIM container):
    python /foss_fim/workflows/inputs/osm_workflow.py -v 20261108
    python /foss_fim/workflows/inputs/osm_workflow.py -v 20261108 -s 'delaware' -lh '02040205' \
        --dem_diff_regions conus -o /outputs/dev_osm/osm
"""

from __future__ import annotations

import argparse
from pathlib import Path

from workflows.inputs.osm_bridges_workflow import build_bridges_steps
from workflows.inputs.osm_common import (
    OsmContext,
    add_bridge_arguments,
    add_run_control_arguments,
    add_s3_arguments,
    build_pull_steps,
    context_from_args,
    preflight_checks,
    report_sections,
    run_workflow_cli,
)
from workflows.inputs.osm_roads_workflow import build_roads_steps
from workflows.shared.workflow_runner import Step, WorkflowRunner


__all__ = ["build_workflow_steps", "preflight_checks", "report_sections", "main"]


def build_workflow_steps(ctx: OsmContext, skip_roads: bool = False, skip_bridges: bool = False) -> list[Step]:
    """Pull, then the roads and bridges sub-workflows that were not skipped."""
    steps = build_pull_steps(ctx)
    if not skip_roads:
        steps.extend(build_roads_steps(ctx))
    if not skip_bridges:
        steps.extend(build_bridges_steps(ctx))
    return steps


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pull OSM roads and bridges, build the per-HUC and DEM-diff inputs, validate every step, "
        "and preview or upload the FIM S3 copies. See workflows/workflow_automation.md."
    )
    parser.add_argument(
        "-v", "--version", required=True, help="Version folder name for all outputs, e.g. 20261108"
    )
    parser.add_argument(
        "-o",
        "--output_root",
        type=Path,
        help="OSM output root (default: ${inputsDir}/osm); change it for dev tests",
    )
    parser.add_argument(
        "-p",
        "--preclip_dir",
        type=Path,
        help="Pre-clip folder for the per-HUC scripts (default: pre_clip_huc_dir)",
    )
    parser.add_argument(
        "-s",
        "--states",
        default="",
        help="Space-delimited Geofabrik state slugs, e.g. 'delaware' (default: all)",
    )
    parser.add_argument("-lh", "--lst_hucs", default="", help="Space-delimited HUC8 subset (default: all)")
    parser.add_argument("--skip_roads", action="store_true", help="Skip the roads sub-workflow")
    parser.add_argument(
        "--skip_bridges",
        action="store_true",
        help="Skip the bridges sub-workflow (bridges, lidar, DEM diffs)",
    )
    parser.add_argument(
        "-j",
        "--number_jobs",
        type=int,
        default=4,
        help="-j for the per-HUC roads and bridges scripts (default 4)",
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
        runner = WorkflowRunner(
            f"osm_{ctx.version}", ctx.log_dir, resume=args.resume, from_step=args.from_step
        )
        runner.add_steps(build_workflow_steps(ctx, args.skip_roads, args.skip_bridges))
        runner.list_steps()  # validates --from_step
    except (OSError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    return run_workflow_cli(
        runner, ctx, args, "osm_workflow", pull=True, roads=not args.skip_roads, bridges=not args.skip_bridges
    )


if __name__ == "__main__":
    raise SystemExit(main())
