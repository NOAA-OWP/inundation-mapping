import tempfile
import unittest
from pathlib import Path

from workflows.tests.helpers import HAS_PYARROW, write_geoparquet
from workflows.validators.validate_make_osm_roads_per_huc import run_checks as run_roads_checks
from workflows.validators.validate_pull_osm import _bridge_skip_logged, _supported_states
from workflows.validators.validate_pull_osm import run_checks as run_pull_checks


ROAD_ROWS = {"osmid": [str(i) for i in range(100)], "highway": ["primary"] * 100, "geometry": [b"wkb"] * 100}
BRIDGE_ROWS = {"osmid": ["1"], "bridge_type": ["highway-primary"], "geometry": [b"wkb"]}


def make_preclip(root: Path, hucs: tuple[str, ...]) -> Path:
    preclip = root / "preclip"
    for huc in hucs:
        (preclip / huc).mkdir(parents=True)
        (preclip / huc / "wbd.gpkg").touch()
    return preclip


def write_log(folder: Path, name: str, text: str) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text(text, encoding="utf-8")
    return path


class PullValidatorHostTests(unittest.TestCase):
    def test_default_state_list_is_read_from_pull_osm_without_importing_it(self):
        states = _supported_states()
        self.assertIn("delaware", states)
        self.assertIn("american-oceania", states)
        self.assertEqual(len(states), len(set(states)))

    def test_bridge_skip_lines_are_matched_per_state(self):
        log = (
            "[bridges] delaware: no linear bridge geometries found, skipping\n"
            "[bridges] new-york: wrote 5 features\n"
        )
        self.assertTrue(_bridge_skip_logged(log, "delaware"))
        self.assertFalse(_bridge_skip_logged(log, "new-york"))
        self.assertFalse(
            _bridge_skip_logged("[bridges] new-york: all features were unwanted, skipping", "york")
        )

    def test_missing_outputs_and_failure_marker_fail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_log(root, "pull_osm_20261108-1200.log", "\u274c 1 state(s) failed: ['delaware']\n")
            result = run_pull_checks(root, states="delaware")
            self.assertFalse(result.passed)
            failed = {check.name for check in result.checks if not check.passed and not check.is_warning}
            self.assertIn("Roads output: delaware", failed)
            self.assertIn("Bridges output: delaware", failed)
            self.assertIn("Pull failure markers", failed)


class RoadsValidatorHostTests(unittest.TestCase):
    def test_subset_run_only_expects_the_selected_hucs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = make_preclip(root, ("01010001", "01010002", "01010003"))
            output = root / "roads"
            write_log(output, "make_osm_roads_20261108-1200.log", "[01010002] No roads within HUC 01010002\n")

            result = run_roads_checks(output, preclip, hucs="01010002", states_limited=True)
            self.assertTrue(result.passed, result.checks)
            coverage = next(check for check in result.checks if check.name == "HUC coverage")
            self.assertIn("Covered 1 of 1", coverage.message)
            self.assertFalse(any(check.name == "Baseline file count" for check in result.checks))

    def test_selected_huc_missing_from_preclip_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = make_preclip(root, ("01010001",))
            output = root / "roads"
            write_log(output, "make_osm_roads_20261108-1200.log", "done\n")
            result = run_roads_checks(output, preclip, hucs="09999999")
            self.assertFalse(result.passed)

    def test_failure_marker_in_the_newest_log_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = make_preclip(root, ("01010001",))
            output = root / "roads"
            write_log(
                output,
                "make_osm_roads_20261108-1200.log",
                "[01010001] No roads within HUC 01010001\n\u274c 1 failed: ['01010002']\n",
            )
            result = run_roads_checks(output, preclip)
            self.assertFalse(result.passed)

    def test_full_run_without_baseline_only_warns(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = make_preclip(root, ("01010001",))
            output = root / "roads"
            write_log(output, "make_osm_roads_20261108-1200.log", "[01010001] No roads within HUC 01010001\n")
            result = run_roads_checks(output, preclip, baseline_dir=root / "missing_baseline")
            self.assertTrue(result.passed)
            baseline = next(check for check in result.checks if check.name == "Baseline file count")
            self.assertTrue(baseline.is_warning)


@unittest.skipUnless(HAS_PYARROW, "pyarrow is required for the GeoParquet fixtures")
class GeoParquetValidatorTests(unittest.TestCase):
    def test_pull_accepts_a_logged_bridge_skip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_geoparquet(root / "states_parquet" / "roads" / "delaware_roads.parquet", ROAD_ROWS, 5070)
            write_log(
                root,
                "pull_osm_20261108-1200.log",
                "[bridges] delaware: no linear bridge geometries found, skipping\n",
            )

            result = run_pull_checks(root, states="delaware")
            self.assertTrue(result.passed, result.checks)
            self.assertTrue(
                any(check.is_warning and "Bridges output" in check.name for check in result.checks)
            )

    def test_pull_checks_crs_rows_and_baseline_ratio(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_geoparquet(root / "states_parquet" / "roads" / "alaska_roads.parquet", ROAD_ROWS, 5070)
            write_geoparquet(
                root / "states_parquet" / "bridges" / "alaska_bridges.parquet", BRIDGE_ROWS, 3338
            )
            write_log(root, "pull_osm_20261108-1200.log", "\u2705 All states succeeded\n")
            failed = {
                check.name for check in run_pull_checks(root, states="alaska").checks if not check.passed
            }
            self.assertIn("alaska roads: GeoParquet CRS", failed)

            write_geoparquet(root / "states_parquet" / "roads" / "alaska_roads.parquet", ROAD_ROWS, 3338)
            baseline = root / "baseline"
            write_geoparquet(
                baseline / "alaska_roads.parquet", {key: value * 2 for key, value in ROAD_ROWS.items()}, 3338
            )
            result = run_pull_checks(root, states="alaska", baseline_roads_dir=baseline)
            ratio = next(check for check in result.checks if check.name == "alaska roads baseline ratio")
            self.assertFalse(ratio.passed)
            self.assertFalse(result.passed)

    def test_roads_validator_checks_columns_and_crs_by_region(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = make_preclip(root, ("01010001", "19020101"))
            output = root / "roads"
            columns = {"osmid": ["1"], "huc8": ["01010001"], "osmid_catchid": ["1_000"], "geometry": [b"wkb"]}
            write_geoparquet(output / "roads_01010001.parquet", columns, 5070)
            write_geoparquet(output / "roads_19020101.parquet", columns, 3338)
            write_log(output, "make_osm_roads_20261108-1200.log", "\u2705 All HUC tasks succeeded\n")
            self.assertTrue(run_roads_checks(output, preclip).passed)

            write_geoparquet(output / "roads_19020101.parquet", columns, 5070)
            self.assertFalse(run_roads_checks(output, preclip).passed)

            del columns["osmid_catchid"]
            write_geoparquet(output / "roads_19020101.parquet", columns, 3338)
            self.assertFalse(run_roads_checks(output, preclip).passed)

    def test_roads_stale_file_is_a_warning(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = make_preclip(root, ("01010001",))
            output = root / "roads"
            columns = {"osmid": ["1"], "huc8": ["01010001"], "osmid_catchid": ["1_000"], "geometry": [b"wkb"]}
            write_geoparquet(output / "roads_01010001.parquet", columns, 5070)
            write_log(output, "make_osm_roads_20261108-1200.log", "[01010001] No roads within HUC 01010001\n")
            result = run_roads_checks(output, preclip)
            self.assertTrue(result.passed)
            self.assertTrue(any(check.is_warning and "stale" in check.name for check in result.checks))


if __name__ == "__main__":
    unittest.main()
