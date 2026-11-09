"""Validate the per-HUC bridge GeoParquet outputs of `data/bridges/make_osm_bridges_per_huc.py`.

Sample usage (standalone, against a manual run):
    python /foss_fim/workflows/validators/validate_make_osm_bridges_per_huc.py \
        -o /data/inputs/osm/bridges/bridge_lines/20261108 -p /data/inputs/pre_clip_huc8/20261006 \
        --report_dir /outputs/temp
"""

from __future__ import annotations

import argparse
from pathlib import Path

from workflows.shared.regions import region_for_huc
from workflows.validators.validator_base import (
    CheckResult,
    ValidationResult,
    check_log_markers,
    check_per_huc_outputs,
    newest_log,
    run_validator_cli,
)


# Thresholds (first guesses; tune after real runs).
MIN_HUC_COVERAGE = 0.98
BASELINE_FILE_RATIO = 0.95
REQUIRED_COLUMNS = ("osmid", "huc8", "geometry")

# Log and file patterns written by make_osm_bridges_per_huc.py (a host test checks they still exist).
LOG_GLOB = "make_osm_bridges_*.log"
NO_BRIDGES_MARKER = "No bridges within HUC"
NO_OVERLAP_MARKER = "No state parquets overlap HUC"
MISSING_WBD_MARKER = "Missing wbd.gpkg"  # such HUCs simply count as missing
BAD_OUTPUT_GLOB = "bridges_*_bad.parquet"
FAILURE_MARKERS = ("❌",)


def run_checks(
    output_dir: Path | str,
    preclip_dir: Path | str,
    hucs: str | list[str] | tuple[str, ...] = "",
    states_limited: bool = False,
    baseline_dir: Path | str | None = None,
) -> ValidationResult:
    output_dir = Path(output_dir)
    log_path = newest_log(output_dir, LOG_GLOB)
    checks = check_per_huc_outputs(
        output_dir,
        Path(preclip_dir),
        hucs,
        states_limited,
        Path(baseline_dir) if baseline_dir else None,
        file_prefix="bridges",
        log_path=log_path,
        empty_marker=NO_BRIDGES_MARKER,
        overlap_marker=NO_OVERLAP_MARKER,
        required_columns=REQUIRED_COLUMNS,
        crs_for_huc=lambda huc: region_for_huc(huc).crs,
        min_coverage=MIN_HUC_COVERAGE,
        baseline_file_ratio=BASELINE_FILE_RATIO,
    )

    bad_files = sorted(path.name for path in output_dir.glob(BAD_OUTPUT_GLOB))
    checks.append(
        CheckResult(
            "Failed bridge outputs",
            not bad_files,
            (
                f"Failed bridge parquet(s) found: {', '.join(bad_files)}"
                if bad_files
                else "No _bad.parquet files"
            ),
        )
    )
    checks.append(check_log_markers(log_path, FAILURE_MARKERS, name="Bridges failure markers"))
    return ValidationResult("validate_make_osm_bridges_per_huc", str(output_dir), checks)


def validate_make_osm_bridges_per_huc(
    output_dir: Path | str,
    preclip_dir: Path | str,
    hucs: str | list[str] | tuple[str, ...] = "",
    states_limited: bool = False,
    baseline_dir: Path | str | None = None,
    report_dir: Path | None = None,
) -> bool:
    result = run_checks(output_dir, preclip_dir, hucs, states_limited, baseline_dir)
    if report_dir is not None:
        result.write_report(report_dir)
    return result.passed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate per-HUC OSM bridge GeoParquet outputs (exit 0 = pass)."
    )
    parser.add_argument("-o", "--output_dir", required=True, help="Folder with bridges_<HUC8>.parquet files")
    parser.add_argument("-p", "--preclip_dir", required=True, help="Pre-clip folder used for the run")
    parser.add_argument("-lh", "--lst_hucs", default="", help="Space-delimited HUC8 subset used for the run")
    parser.add_argument("-s", "--states", default="", help="States used for the run, if limited")
    parser.add_argument("--baseline_dir", type=Path, help="Current bridge_lines/<version> folder")
    parser.add_argument("--report_dir", type=Path, default=Path("."), help="Folder for the JSON report")
    args = parser.parse_args()
    result = run_checks(
        args.output_dir,
        args.preclip_dir,
        args.lst_hucs,
        states_limited=bool(args.states.strip()),
        baseline_dir=args.baseline_dir,
    )
    return run_validator_cli(result, args.report_dir)


if __name__ == "__main__":
    raise SystemExit(main())
