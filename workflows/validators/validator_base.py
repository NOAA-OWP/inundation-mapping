"""Shared validation results and checks; geospatial dependencies load only when needed."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence


HUC8_PATTERN = re.compile(r"^\d{8}$")
MAX_REPORT_NAME_LENGTH = 120


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    is_warning: bool = False


@dataclass
class ValidationResult:
    validator: str
    target: str
    checks: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(check.passed or check.is_warning for check in self.checks)

    def write_report(self, report_dir: Path) -> Path:
        report_dir = Path(report_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        safe_validator = _safe_name(self.validator)
        safe_target = _safe_name(self.target)
        report_path = report_dir / f"{safe_validator}_{safe_target}.json"
        payload = {
            "validator": self.validator,
            "target": self.target,
            "passed": self.passed,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "checks": [asdict(check) for check in self.checks],
        }
        report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return report_path


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_.-") or "target"
    if len(safe) <= MAX_REPORT_NAME_LENGTH:
        return safe
    # Keep report file names well under file system limits while staying unique per target.
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]
    return f"{safe[: MAX_REPORT_NAME_LENGTH - 11]}_{digest}"


def format_list(items: Sequence[str], limit: int = 20) -> str:
    """Comma-separated items, trimmed to `limit` entries with a count of the rest."""
    shown = ", ".join(items[:limit])
    return shown if len(items) <= limit else f"{shown} and {len(items) - limit} more"


def split_hucs(hucs: str | Iterable[str] | None) -> list[str]:
    """Normalize a space-delimited string or iterable of HUC8 values into a sorted, unique list."""
    if not hucs:
        return []
    values = hucs.split() if isinstance(hucs, str) else [str(huc) for huc in hucs]
    return sorted(set(values))


def expected_preclip_hucs(preclip_dir: Path, selected_hucs: str | Iterable[str] | None = None) -> list[str]:
    """Expected HUCs are the selected -lh HUCs, or every HUC8 folder in the pre-clip directory."""
    selected = split_hucs(selected_hucs)
    if selected:
        return selected
    preclip_dir = Path(preclip_dir)
    if not preclip_dir.is_dir():
        return []
    return sorted(
        path.name for path in preclip_dir.iterdir() if path.is_dir() and HUC8_PATTERN.match(path.name)
    )


def check_preclip_inputs(
    preclip_dir: Path, selected_hucs: str | Iterable[str] | None = None
) -> list[CheckResult]:
    """Fail when the pre-clip folder, or a HUC folder requested with -lh, does not exist."""
    preclip_dir = Path(preclip_dir)
    if not preclip_dir.is_dir():
        return [CheckResult("Pre-clip directory", False, f"Directory not found: {preclip_dir}")]
    missing = [huc for huc in split_hucs(selected_hucs) if not (preclip_dir / huc).is_dir()]
    if missing:
        return [CheckResult("Selected pre-clip HUCs", False, f"Missing HUC folders: {format_list(missing)}")]
    return []


def count_huc_files(folder: Path | None, prefix: str) -> int:
    """Count `<prefix>_<HUC8>.parquet` files, ignoring renamed failures such as `_bad.parquet`."""
    if folder is None or not Path(folder).is_dir():
        return 0
    pattern = re.compile(rf"^{re.escape(prefix)}_\d{{8}}\.parquet$")
    return sum(1 for path in Path(folder).iterdir() if path.is_file() and pattern.match(path.name))


def check_files_exist(paths: Iterable[Path], name: str = "Required files") -> CheckResult:
    missing = [str(path) for path in paths if not Path(path).is_file()]
    if missing:
        return CheckResult(name, False, f"Missing files: {', '.join(missing)}")
    return CheckResult(name, True, "All required files exist")


LOG_SIDE_FILE_SUFFIXES = ("-errors.log", "-warnings.log")


def newest_log(folder: Path, pattern: str) -> Path | None:
    """Return the newest main log, ignoring the -errors/-warnings side files the FIM loggers create."""
    candidates = [
        path
        for path in Path(folder).glob(pattern)
        if path.is_file() and not path.name.endswith(LOG_SIDE_FILE_SUFFIXES)
    ]
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name), default=None)


def read_log(log_path: Path | None) -> str:
    if log_path is None or not Path(log_path).is_file():
        return ""
    return Path(log_path).read_text(encoding="utf-8", errors="replace")


def check_log_markers(
    log_path: Path | None, markers: Sequence[str], name: str = "Log markers"
) -> CheckResult:
    if not markers:
        return CheckResult(name, True, "No failure markers configured")
    if log_path is None or not Path(log_path).is_file():
        return CheckResult(name, False, "Expected log file was not found")

    content = read_log(log_path)
    found = [marker for marker in markers if marker in content]
    if found:
        return CheckResult(name, False, f"Failure marker(s) in {Path(log_path).name}: {', '.join(found)}")
    return CheckResult(name, True, f"No failure markers in {Path(log_path).name}")


def check_huc_coverage(
    expected_hucs: Iterable[str],
    output_dir: Path,
    file_glob: str,
    log_path: Path | None,
    states_limited: bool = False,
    min_coverage: float = 0.98,
    empty_marker: str = "No roads within HUC",
    overlap_marker: str = "No state parquets overlap HUC",
) -> list[CheckResult]:
    expected = sorted(set(str(huc) for huc in expected_hucs))
    log_content = read_log(log_path)

    covered: list[str] = []
    missing: list[str] = []
    warnings: list[str] = []
    excluded = 0
    output_dir = Path(output_dir)

    for huc in expected:
        if "{huc}" in file_glob:
            output_pattern = file_glob.format(huc=huc)
        elif "*" in file_glob:
            output_pattern = file_glob.replace("*", huc)
        else:
            output_pattern = file_glob
        has_output = any(output_dir.glob(output_pattern))
        if has_output or f"{empty_marker} {huc}" in log_content:
            covered.append(huc)
            continue

        overlap_log_marker = f"{overlap_marker} {huc}"
        if states_limited and overlap_log_marker in log_content:
            excluded += 1
            warnings.append(f"HUC {huc} excluded because selected states do not overlap it")
            continue
        missing.append(huc)

    denominator = len(expected) - excluded
    if denominator == 0:
        details = "No expected HUCs were left to validate"
        if excluded:
            details += f" after excluding {excluded} HUC(s) that the selected states do not overlap"
        results = [CheckResult("HUC coverage", False, details)]
    else:
        coverage = len(covered) / denominator
        details = (
            f"Covered {len(covered)} of {denominator} expected HUCs ({coverage:.1%}); "
            f"minimum {min_coverage:.1%}"
        )
        if missing:
            details += f". Missing: {format_list(missing, limit=50)}"
        results = [CheckResult("HUC coverage", coverage >= min_coverage, details)]
    if warnings:
        results.append(CheckResult("Subset overlap exclusions", False, "; ".join(warnings), is_warning=True))
    return results


def summarize_item_checks(
    item_checks: dict[str, list[CheckResult]], item_label: str, max_listed: int = 20
) -> list[CheckResult]:
    """Collapse per-item checks (for example one set per HUC file) into one result per check name."""
    by_name: dict[str, list[tuple[str, CheckResult]]] = {}
    for item, checks in item_checks.items():
        for check in checks:
            by_name.setdefault(check.name, []).append((item, check))

    summary = []
    for name, entries in by_name.items():
        failures = [(item, check) for item, check in entries if not check.passed]
        if not failures:
            summary.append(CheckResult(f"{item_label}: {name}", True, f"All {len(entries)} file(s) passed"))
            continue
        listed = "; ".join(f"{item}: {check.message}" for item, check in failures[:max_listed])
        if len(failures) > max_listed:
            listed += f"; and {len(failures) - max_listed} more"
        summary.append(
            CheckResult(
                f"{item_label}: {name}",
                False,
                f"{len(failures)} of {len(entries)} file(s) failed: {listed}",
                is_warning=all(check.is_warning for _, check in failures),
            )
        )
    return summary


def check_file_count_vs_baseline(
    output_dir: Path, baseline_dir: Path | None, file_prefix: str, min_ratio: float
) -> CheckResult:
    """Compare `<prefix>_<HUC8>.parquet` counts against the version currently in bash_variables.env."""
    if baseline_dir is None or not Path(baseline_dir).is_dir():
        return CheckResult(
            "Baseline file count",
            False,
            f"Baseline directory is unavailable ({baseline_dir}); comparison skipped",
            is_warning=True,
        )
    current = count_huc_files(output_dir, file_prefix)
    baseline = count_huc_files(baseline_dir, file_prefix)
    minimum = math.ceil(min_ratio * baseline)
    return CheckResult(
        "Baseline file count",
        current >= minimum,
        f"Found {current} {file_prefix} file(s); minimum is {minimum} "
        f"({min_ratio:.0%} of the {baseline} in {Path(baseline_dir).as_posix()})",
    )


def check_per_huc_outputs(
    output_dir: Path,
    preclip_dir: Path,
    hucs: str | Iterable[str] | None,
    states_limited: bool,
    baseline_dir: Path | None,
    *,
    file_prefix: str,
    log_path: Path | None,
    empty_marker: str,
    overlap_marker: str,
    required_columns: Sequence[str],
    crs_for_huc: Callable[[str], int],
    min_coverage: float,
    baseline_file_ratio: float,
) -> list[CheckResult]:
    """Shared rules for the per-HUC roads and bridges outputs (see the HUC coverage rule in the guide)."""
    output_dir = Path(output_dir)
    selected = split_hucs(hucs)
    expected = expected_preclip_hucs(preclip_dir, selected)
    log_content = read_log(log_path)

    checks = check_preclip_inputs(preclip_dir, selected)
    checks.extend(
        check_huc_coverage(
            expected,
            output_dir,
            f"{file_prefix}_{{huc}}.parquet",
            log_path,
            states_limited=states_limited,
            min_coverage=min_coverage,
            empty_marker=empty_marker,
            overlap_marker=overlap_marker,
        )
    )

    file_checks: dict[str, list[CheckResult]] = {}
    for huc in expected:
        output_path = output_dir / f"{file_prefix}_{huc}.parquet"
        if not output_path.is_file():
            continue
        file_checks[f"HUC {huc}"] = check_parquet_schema(
            output_path, required_columns=required_columns, expected_crs=crs_for_huc(huc)
        )
        if f"{empty_marker} {huc}" in log_content:
            checks.append(
                CheckResult(
                    f"HUC {huc} stale output",
                    False,
                    f"The newest log says '{empty_marker} {huc}', so {output_path.name} is left from an "
                    "earlier run",
                    is_warning=True,
                )
            )
    checks.extend(summarize_item_checks(file_checks, f"{file_prefix} files"))

    if not states_limited and not selected:
        checks.append(
            check_file_count_vs_baseline(output_dir, baseline_dir, file_prefix, baseline_file_ratio)
        )
    return checks


def _crs_identifier(crs: object) -> str | None:
    if crs is None:
        return "OGC:CRS84"
    if isinstance(crs, str):
        return crs.upper().replace(" ", "")
    if not isinstance(crs, dict):
        return None

    identifier = crs.get("id")
    if isinstance(identifier, dict) and identifier.get("authority") and identifier.get("code"):
        return f"{identifier['authority']}:{identifier['code']}".upper()
    name = crs.get("name")
    if isinstance(name, str):
        match = re.search(r"EPSG(?::|::)(\d+)$", name, re.IGNORECASE)
        if match:
            return f"EPSG:{match.group(1)}"
    return None


def _normalize_expected_crs(expected_crs: int | str) -> str:
    value = str(expected_crs).upper().replace(" ", "")
    if value.isdigit():
        return f"EPSG:{value}"
    return value


def check_parquet_schema(
    parquet_path: Path,
    required_columns: Iterable[str] = (),
    min_rows: int = 0,
    expected_crs: int | str | None = None,
) -> list[CheckResult]:
    """Check parquet columns, row minimum and optional GeoParquet CRS using lazily imported pyarrow."""
    parquet_path = Path(parquet_path)
    if not parquet_path.is_file():
        return [CheckResult("Parquet exists", False, f"File not found: {parquet_path}")]

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        return [CheckResult("Parquet inspection", False, f"pyarrow is unavailable: {exc}")]

    try:
        metadata = pq.read_metadata(parquet_path)
        schema = metadata.schema.to_arrow_schema()
        columns = set(schema.names)
        missing_columns = sorted(set(required_columns) - columns)
        results = [
            CheckResult(
                "Required columns",
                not missing_columns,
                (
                    "All required columns are present"
                    if not missing_columns
                    else f"Missing columns: {', '.join(missing_columns)}"
                ),
            ),
            CheckResult(
                "Minimum rows",
                metadata.num_rows >= min_rows,
                f"Found {metadata.num_rows} rows; minimum is {min_rows}",
            ),
        ]
        if expected_crs is not None:
            raw_geo = (schema.metadata or {}).get(b"geo")
            geo = json.loads(raw_geo.decode("utf-8")) if raw_geo else {}
            primary_column = geo.get("primary_column")
            geo_column = geo.get("columns", {}).get(primary_column, {})
            actual_crs = _crs_identifier(geo_column.get("crs"))
            expected = _normalize_expected_crs(expected_crs)
            results.append(
                CheckResult(
                    "GeoParquet CRS",
                    actual_crs == expected,
                    f"Found {actual_crs or 'unknown CRS'}; expected {expected}",
                )
            )
        return results
    except Exception as exc:
        return [CheckResult("Parquet inspection", False, f"Unable to inspect {parquet_path}: {exc}")]


def check_ratio_vs_baseline(
    current: int | float,
    baseline: int | float,
    min_ratio: float,
    max_ratio: float,
    name: str = "Baseline ratio",
) -> CheckResult:
    if baseline <= 0:
        return CheckResult(name, current == 0, f"Baseline is {baseline}; current is {current}")
    ratio = current / baseline
    passed = min_ratio <= ratio <= max_ratio
    return CheckResult(
        name,
        passed,
        f"Current/baseline ratio is {ratio:.3f} ({current} vs {baseline}); accepted range is "
        f"{min_ratio:.2f} to {max_ratio:.2f}",
    )


def _print(text: str) -> None:
    # Log markers such as the red X can not be printed on every console encoding; never crash on them.
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    print(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def run_validator_cli(result: ValidationResult, report_dir: Path) -> int:
    """Print the checks, write the JSON report and return the exit code (0 = pass, 1 = fail)."""
    report_path = result.write_report(report_dir)
    _print(f"{'PASS' if result.passed else 'FAIL'}: {result.validator} for {result.target}")
    for check in result.checks:
        prefix = "WARNING" if check.is_warning else ("PASS" if check.passed else "FAIL")
        _print(f"{prefix}: {check.name}: {check.message}")
    _print(f"JSON report: {report_path}")
    return 0 if result.passed else 1
