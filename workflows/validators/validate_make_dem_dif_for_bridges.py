"""Validate one region's output of `data/bridges/make_dem_dif_for_bridges.py`.

The script exits 0 even when it fails (its lines 231 and 255-260 swallow errors), so this validator also
reads the newest `DEM_diff_rasters-<timestamp>.log` for the failure text it writes.

Sample usage (standalone, against a manual run):
    python /foss_fim/workflows/validators/validate_make_dem_dif_for_bridges.py \
        -o /data/inputs/osm/bridges/DEM_Diffs/20261108/conus --region conus --report_dir /outputs/temp
"""

from __future__ import annotations

import argparse
from pathlib import Path

from workflows.shared.regions import REGIONS, get_region
from workflows.validators.validator_base import (
    CheckResult,
    ValidationResult,
    check_log_markers,
    newest_log,
    run_validator_cli,
)


# Regions whose DEM diffs may legitimately be all zero (little or no bridge lidar coverage).
ALLOW_ALL_ZERO_REGIONS = {"guam", "american_samoa"}
MAX_LISTED_RASTER_PROBLEMS = 20

# File and log patterns written by make_dem_dif_for_bridges.py (a host test checks they still exist).
VRT_NAME = "bridge_elev_diff.vrt"
DIFF_TIF_GLOB = "*_diff.tif"
LOG_GLOB = "DEM_diff_rasters-*.log"
FAILURE_MARKERS = ("*** An error occurred", "❌")


def _inspect_raster(path: Path, expected_crs: int, read_values: bool = False) -> tuple[bool, bool, str]:
    """Return (readable_and_correct_crs, has_valid_nonzero_value, detail)."""
    try:
        import rasterio

        has_nonzero = False
        with rasterio.open(path) as dataset:
            if dataset.count < 1 or dataset.width < 1 or dataset.height < 1:
                return False, False, "Raster has no bands or empty dimensions"
            actual_crs = dataset.crs.to_epsg() if dataset.crs else None
            if actual_crs != expected_crs:
                return False, False, f"Found CRS EPSG:{actual_crs}; expected EPSG:{expected_crs}"
            if read_values:
                for _, window in dataset.block_windows(1):
                    values = dataset.read(1, window=window, masked=True).compressed()
                    if values.size and (values != 0).any():
                        has_nonzero = True
                        break
        return True, has_nonzero, f"Readable raster in EPSG:{expected_crs}"
    except ImportError as exc:
        return False, False, f"rasterio is unavailable: {exc}"
    except Exception as exc:
        return False, False, f"Unable to read raster {path}: {exc}"


def run_checks(output_dir: Path | str, region: str) -> ValidationResult:
    output_dir = Path(output_dir)
    region_def = get_region(region)
    vrt_path = output_dir / VRT_NAME
    tif_paths = sorted(output_dir.glob(DIFF_TIF_GLOB))
    checks: list[CheckResult] = [
        CheckResult("DEM-diff output directory", output_dir.is_dir(), str(output_dir)),
        CheckResult("Diff GeoTIFFs", bool(tif_paths), f"Found {len(tif_paths)} diff tif(s)"),
    ]

    if not vrt_path.is_file():
        checks.append(CheckResult("Bridge elevation VRT", False, f"Missing {vrt_path}"))
    else:
        vrt_ok, _, vrt_detail = _inspect_raster(vrt_path, region_def.crs)
        checks.append(CheckResult("Bridge elevation VRT", vrt_ok, vrt_detail))

    raster_ok = True
    any_nonzero = False
    raster_details: list[str] = []
    for tif_path in tif_paths:
        is_valid, has_nonzero, detail = _inspect_raster(tif_path, region_def.crs, read_values=not any_nonzero)
        raster_ok = raster_ok and is_valid
        any_nonzero = any_nonzero or has_nonzero
        if not is_valid:
            raster_details.append(f"{tif_path.name}: {detail}")
    if raster_details:
        details = "; ".join(raster_details[:MAX_LISTED_RASTER_PROBLEMS])
        if len(raster_details) > MAX_LISTED_RASTER_PROBLEMS:
            details += f"; and {len(raster_details) - MAX_LISTED_RASTER_PROBLEMS} more"
    else:
        details = "All diff rasters are readable with the expected CRS"
    checks.append(CheckResult("Diff raster CRS and readability", raster_ok and bool(tif_paths), details))

    if region_def.name in ALLOW_ALL_ZERO_REGIONS:
        checks.append(
            CheckResult(
                "Nonzero DEM corrections", True, f"All-zero/nodata output is allowed for {region_def.name}"
            )
        )
    else:
        checks.append(
            CheckResult(
                "Nonzero DEM corrections",
                any_nonzero,
                (
                    "At least one valid nonzero correction value was found"
                    if any_nonzero
                    else "All diff rasters contain only zero or nodata values"
                ),
            )
        )

    log_path = newest_log(output_dir, LOG_GLOB)
    checks.append(check_log_markers(log_path, FAILURE_MARKERS, name="DEM-diff failure markers"))
    return ValidationResult("validate_make_dem_dif_for_bridges", f"{region_def.name}:{output_dir}", checks)


def validate_make_dem_dif_for_bridges(
    output_dir: Path | str, region: str, report_dir: Path | None = None
) -> bool:
    result = run_checks(output_dir, region)
    if report_dir is not None:
        result.write_report(report_dir)
    return result.passed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate a region's bridge DEM-diff output (exit 0 = pass)."
    )
    parser.add_argument("-o", "--output_dir", required=True, help="The script's -o folder for one region")
    parser.add_argument("--region", required=True, choices=tuple(REGIONS), help="Region of the output")
    parser.add_argument("--report_dir", type=Path, default=Path("."), help="Folder for the JSON report")
    args = parser.parse_args()
    result = run_checks(args.output_dir, args.region)
    return run_validator_cli(result, args.report_dir)


if __name__ == "__main__":
    raise SystemExit(main())
