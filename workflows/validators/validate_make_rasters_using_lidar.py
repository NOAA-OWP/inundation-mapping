"""Validate the outputs of `data/bridges/make_rasters_using_lidar.py` for one lidar run (one region).

Under its -o root the script writes:
    lidar_processing/<HUC8>/lidar_osm_rasters/<osmid>.tif
    lidar_processing/<HUC8>/bridge_elevation_filter_summary.csv   (HUCs where lidar was processed)
    modified_osm_bridges/huc_<HUC8>_osm_bridges_modified.parquet  (every HUC that finished)
    all_bridge_elevation_filter_summary.csv                       (only when more than one HUC was requested)
    make_rasters_using_lidar_<YYYYmmdd-HHMM>.log

Sample usage (standalone, against a manual run):
    python /foss_fim/workflows/validators/validate_make_rasters_using_lidar.py \
        -o /data/inputs/osm/bridges/lidar_data/20261108 -lh '19020101 19020102' --label alaska \
        --report_dir /outputs/temp
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from workflows.validators.validator_base import (
    HUC8_PATTERN,
    CheckResult,
    ValidationResult,
    newest_log,
    read_log,
    run_validator_cli,
    split_hucs,
)


# Thresholds (first guesses; tune after real runs).
MIN_LIDAR_HUC_FRACTION = 0.5
MIN_MODIFIED_HUC_FRACTION = 0.95

# Folder, file and log names written by make_rasters_using_lidar.py (a host test checks they still exist).
LIDAR_PROCESSING_DIR = "lidar_processing"
MODIFIED_BRIDGES_DIR = "modified_osm_bridges"
RASTER_SUBDIR = "lidar_osm_rasters"
MODIFIED_FILE_SUFFIX = "_osm_bridges_modified.parquet"
SUMMARY_CSV = "all_bridge_elevation_filter_summary.csv"
PER_HUC_SUMMARY_CSV = "bridge_elevation_filter_summary.csv"
LOG_GLOB = "make_rasters_using_lidar_*.log"
LOG_ERROR_MARKERS = ("❌", "ERROR")


def _expected_hucs(output_dir: Path, hucs: str | list[str] | tuple[str, ...]) -> list[str]:
    selected = split_hucs(hucs)
    if selected:
        return selected
    lidar_dir = output_dir / LIDAR_PROCESSING_DIR
    if not lidar_dir.is_dir():
        return []
    return sorted(
        path.name for path in lidar_dir.iterdir() if path.is_dir() and HUC8_PATTERN.match(path.name)
    )


def _csv_has_header(path: Path) -> bool:
    try:
        with path.open(newline="", encoding="utf-8-sig") as file:
            return next(csv.reader(file), None) is not None
    except (OSError, UnicodeError, csv.Error):
        return False


def _check_summary_csv(output_dir: Path, expected_hucs: list[str], lidar_hucs: list[str]) -> CheckResult:
    name = "Elevation summary CSV"
    if not lidar_hucs:
        return CheckResult(name, False, "No HUC produced lidar rasters, so no summary CSV is expected", True)
    if len(expected_hucs) > 1:
        summary_path = output_dir / SUMMARY_CSV
        readable = summary_path.is_file() and _csv_has_header(summary_path)
        return CheckResult(
            name, readable, f"{summary_path} is {'readable' if readable else 'missing or unreadable'}"
        )
    # The script only writes the combined CSV when more than one HUC was requested.
    summary_path = output_dir / LIDAR_PROCESSING_DIR / expected_hucs[0] / PER_HUC_SUMMARY_CSV
    readable = summary_path.is_file() and _csv_has_header(summary_path)
    return CheckResult(
        name,
        readable,
        f"Single-HUC run: {summary_path} is {'readable' if readable else 'missing or unreadable'}",
    )


def run_checks(
    output_dir: Path | str,
    hucs: str | list[str] | tuple[str, ...] = "",
    label: str = "",
    min_lidar_huc_fraction: float = MIN_LIDAR_HUC_FRACTION,
    min_modified_huc_fraction: float = MIN_MODIFIED_HUC_FRACTION,
) -> ValidationResult:
    output_dir = Path(output_dir)
    expected_hucs = _expected_hucs(output_dir, hucs)
    lidar_dir = output_dir / LIDAR_PROCESSING_DIR
    modified_dir = output_dir / MODIFIED_BRIDGES_DIR
    checks: list[CheckResult] = [
        CheckResult("Lidar processing directory", lidar_dir.is_dir(), str(lidar_dir)),
        CheckResult("Modified bridges directory", modified_dir.is_dir(), str(modified_dir)),
    ]

    lidar_hucs = [huc for huc in expected_hucs if any((lidar_dir / huc / RASTER_SUBDIR).glob("*.tif"))]
    if not expected_hucs:
        checks.append(
            CheckResult("Expected HUCs", False, "No bridge HUCs were given or found for lidar validation")
        )
    else:
        modified_hucs = [
            huc for huc in expected_hucs if (modified_dir / f"huc_{huc}{MODIFIED_FILE_SUFFIX}").is_file()
        ]
        lidar_fraction = len(lidar_hucs) / len(expected_hucs)
        modified_fraction = len(modified_hucs) / len(expected_hucs)
        checks.extend(
            [
                CheckResult(
                    "HUCs with lidar rasters",
                    lidar_fraction >= min_lidar_huc_fraction,
                    f"{len(lidar_hucs)} of {len(expected_hucs)} HUCs have rasters ({lidar_fraction:.1%}); "
                    f"minimum {min_lidar_huc_fraction:.1%}",
                ),
                CheckResult(
                    "HUCs with modified bridges",
                    modified_fraction >= min_modified_huc_fraction,
                    f"{len(modified_hucs)} of {len(expected_hucs)} HUCs have modified parquets "
                    f"({modified_fraction:.1%}); minimum {min_modified_huc_fraction:.1%}",
                ),
                _check_summary_csv(output_dir, expected_hucs, lidar_hucs),
            ]
        )

    log_path = newest_log(output_dir, LOG_GLOB)
    if log_path is None:
        checks.append(CheckResult("Lidar log", False, "Master lidar log was not found", is_warning=True))
    else:
        log_content = read_log(log_path)
        errors = [marker for marker in LOG_ERROR_MARKERS if marker in log_content]
        checks.append(
            CheckResult(
                "Lidar log errors",
                not errors,
                (
                    f"Error marker(s) in {log_path.name}: {', '.join(errors)}"
                    if errors
                    else "No error markers found"
                ),
                is_warning=True,
            )
        )

    target = f"{label}:{output_dir}" if label else str(output_dir)
    return ValidationResult("validate_make_rasters_using_lidar", target, checks)


def validate_make_rasters_using_lidar(
    output_dir: Path | str,
    hucs: str | list[str] | tuple[str, ...] = "",
    label: str = "",
    report_dir: Path | None = None,
) -> bool:
    result = run_checks(output_dir, hucs, label=label)
    if report_dir is not None:
        result.write_report(report_dir)
    return result.passed


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate OSM bridge lidar outputs (exit 0 = pass).")
    parser.add_argument("-o", "--output_dir", required=True, help="Lidar output root (the script's -o)")
    parser.add_argument("-lh", "--lst_hucs", default="", help="Space-delimited HUC8 list given to the script")
    parser.add_argument("--label", default="", help="Optional run label, e.g. conus or alaska")
    parser.add_argument("--report_dir", type=Path, default=Path("."), help="Folder for the JSON report")
    args = parser.parse_args()
    result = run_checks(args.output_dir, args.lst_hucs, label=args.label)
    return run_validator_cli(result, args.report_dir)


if __name__ == "__main__":
    raise SystemExit(main())
