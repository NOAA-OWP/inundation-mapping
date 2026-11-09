import os
import tempfile
import unittest
from pathlib import Path

from workflows.validators.validator_base import (
    CheckResult,
    ValidationResult,
    check_huc_coverage,
    check_log_markers,
    count_huc_files,
    expected_preclip_hucs,
    format_list,
    newest_log,
    summarize_item_checks,
)


class NewestLogTests(unittest.TestCase):
    def test_selects_latest_matching_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir)
            older, newer = folder / "run_older.log", folder / "run_newer.log"
            older.touch()
            newer.touch()
            os.utime(older, (100, 100))
            os.utime(newer, (200, 200))

            self.assertEqual(newest_log(folder, "run_*.log"), newer)
            self.assertIsNone(newest_log(folder, "missing_*.log"))

    def test_ignores_the_errors_and_warnings_side_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir)
            main_log = folder / "make_osm_roads_20261108-1200.log"
            side_logs = [
                folder / "make_osm_roads_20261108-1200-errors.log",
                folder / "make_osm_roads_20261108-1200-warnings.log",
            ]
            for path in [main_log, *side_logs]:
                path.touch()
            os.utime(main_log, (100, 100))
            for path in side_logs:
                os.utime(path, (300, 300))

            self.assertEqual(newest_log(folder, "make_osm_roads_*.log"), main_log)

    def test_older_failure_log_is_ignored_when_a_newer_log_is_clean(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir)
            older, newer = folder / "pull_osm_20261101-0900.log", folder / "pull_osm_20261108-0900.log"
            older.write_text("\u274c 1 state(s) failed: ['delaware']\n", encoding="utf-8")
            newer.write_text("\u2705 All states succeeded\n", encoding="utf-8")
            os.utime(older, (100, 100))
            os.utime(newer, (200, 200))

            self.assertTrue(check_log_markers(newest_log(folder, "pull_osm_*.log"), ("\u274c",)).passed)
            self.assertFalse(check_log_markers(older, ("\u274c",)).passed)
            self.assertFalse(check_log_markers(None, ("\u274c",)).passed)


class HucCoverageTests(unittest.TestCase):
    def make_output(self, root: Path, log_text: str) -> tuple[Path, Path]:
        output = root / "outputs"
        output.mkdir()
        log_path = output / "run.log"
        log_path.write_text(log_text, encoding="utf-8")
        return output, log_path

    def test_empty_huc_log_line_counts_as_covered(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output, log_path = self.make_output(Path(temp_dir), "[01010002] No roads within HUC 01010002\n")
            (output / "roads_01010001.parquet").touch()

            checks = check_huc_coverage(["01010001", "01010002"], output, "roads_{huc}.parquet", log_path)
            self.assertTrue(checks[0].passed)
            self.assertIn("Covered 2 of 2", checks[0].message)

    def test_state_subset_overlap_is_a_warning_but_a_full_run_counts_it_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output, log_path = self.make_output(
                Path(temp_dir), "No state parquets overlap HUC 01010001, skipping\n"
            )
            (output / "roads_01010002.parquet").touch()
            hucs = ["01010001", "01010002"]

            subset_checks = check_huc_coverage(
                hucs, output, "roads_{huc}.parquet", log_path, states_limited=True
            )
            self.assertTrue(subset_checks[0].passed)
            self.assertIn("Covered 1 of 1", subset_checks[0].message)
            self.assertTrue(subset_checks[1].is_warning)

            full_checks = check_huc_coverage(
                hucs, output, "roads_{huc}.parquet", log_path, states_limited=False
            )
            self.assertFalse(full_checks[0].passed)
            self.assertIn("Missing: 01010001", full_checks[0].message)

    def test_missing_wbd_huc_remains_uncovered(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output, log_path = self.make_output(Path(temp_dir), "Missing wbd.gpkg for 01010001, skipping\n")
            checks = check_huc_coverage(["01010001"], output, "roads_{huc}.parquet", log_path)
            self.assertFalse(checks[0].passed)

    def test_coverage_threshold_and_nothing_left_to_validate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output, log_path = self.make_output(Path(temp_dir), "")
            hucs = [f"010100{index:02d}" for index in range(1, 51)]
            for huc in hucs[1:]:
                (output / f"roads_{huc}.parquet").touch()

            self.assertTrue(
                check_huc_coverage(hucs, output, "roads_{huc}.parquet", log_path, min_coverage=0.98)[0].passed
            )
            self.assertFalse(
                check_huc_coverage(hucs, output, "roads_{huc}.parquet", log_path, min_coverage=0.99)[0].passed
            )
            self.assertFalse(check_huc_coverage([], output, "roads_{huc}.parquet", log_path)[0].passed)


class HelperTests(unittest.TestCase):
    def test_expected_hucs_are_the_selected_hucs_or_every_preclip_folder(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            preclip = Path(temp_dir)
            for name in ("01010001", "01010002", "01010003", "not_a_huc"):
                (preclip / name).mkdir()

            self.assertEqual(expected_preclip_hucs(preclip, "01010003 01010001"), ["01010001", "01010003"])
            self.assertEqual(expected_preclip_hucs(preclip), ["01010001", "01010002", "01010003"])
            self.assertEqual(expected_preclip_hucs(preclip / "missing"), [])

    def test_count_huc_files_ignores_bad_and_unrelated_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir)
            for name in (
                "bridges_01010001.parquet",
                "bridges_01010002_bad.parquet",
                "bridges_x.parquet",
                "b.log",
            ):
                (folder / name).touch()
            self.assertEqual(count_huc_files(folder, "bridges"), 1)
            self.assertEqual(count_huc_files(None, "bridges"), 0)

    def test_summarize_item_checks_collapses_per_file_results(self):
        item_checks = {
            "HUC 01010001": [
                CheckResult("Required columns", True, "ok"),
                CheckResult("GeoParquet CRS", True, "ok"),
            ],
            "HUC 01010002": [
                CheckResult("Required columns", True, "ok"),
                CheckResult("GeoParquet CRS", False, "bad"),
            ],
        }
        summary = {check.name: check for check in summarize_item_checks(item_checks, "roads files")}
        self.assertTrue(summary["roads files: Required columns"].passed)
        self.assertFalse(summary["roads files: GeoParquet CRS"].passed)
        self.assertIn(
            "1 of 2 file(s) failed: HUC 01010002: bad", summary["roads files: GeoParquet CRS"].message
        )

    def test_format_list_trims_long_lists(self):
        self.assertEqual(format_list(["a", "b"]), "a, b")
        self.assertEqual(format_list([str(i) for i in range(5)], limit=2), "0, 1 and 3 more")

    def test_warnings_never_fail_and_long_report_names_stay_short_and_unique(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            warning = CheckResult("baseline", False, "skipped", is_warning=True)
            self.assertTrue(ValidationResult("validate_x", "target", [warning]).passed)
            self.assertFalse(
                ValidationResult("validate_x", "target", [CheckResult("c", False, "bad")]).passed
            )

            path_a = ValidationResult("validate_x", "a" * 500, [warning]).write_report(Path(temp_dir))
            path_b = ValidationResult("validate_x", "a" * 499 + "b", [warning]).write_report(Path(temp_dir))
            self.assertLessEqual(len(path_a.name), 140)
            self.assertNotEqual(path_a, path_b)


if __name__ == "__main__":
    unittest.main()
