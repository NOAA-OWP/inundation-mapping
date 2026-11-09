"""The validators depend on exact log text, file names and options in the data scripts.

If one of these tests fails, a data script changed: update the matching validator constant (or the step
builder) in the same change, instead of letting a validator quietly misread a real run.
"""

import unittest
from pathlib import Path

from workflows.validators import validate_make_dem_dif_for_bridges as dem_diff
from workflows.validators import validate_make_osm_bridges_per_huc as bridges
from workflows.validators import validate_make_osm_roads_per_huc as roads
from workflows.validators import validate_make_rasters_using_lidar as lidar
from workflows.validators import validate_pull_osm as pull


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def read_source(*parts: str) -> str:
    return PROJECT_ROOT.joinpath(*parts).read_text(encoding="utf-8")


def log_prefix(glob: str) -> str:
    return glob.split("*", 1)[0]


class LogAndFilePatternTests(unittest.TestCase):
    def assert_in_source(self, source: str, patterns: list[str]) -> None:
        for pattern in patterns:
            with self.subTest(pattern=pattern):
                self.assertIn(pattern, source)

    def assert_options_exist(self, source: str, options: list[str]) -> None:
        for option in options:
            with self.subTest(option=option):
                self.assertTrue(f'"{option}"' in source or f"'{option}'" in source, option)

    def test_pull_osm(self):
        source = read_source("data", "osm", "pull_osm.py")
        patterns = [log_prefix(pull.LOG_GLOB), f"{pull.BRIDGES_LOG_TAG} {{state}}:", pull.BRIDGES_SKIP_WORD]
        patterns += [*pull.FAILURE_MARKERS, "state(s) failed", "_roads.parquet", "_bridges.parquet"]
        patterns += ["states_parquet", '"roads"', '"bridges"', "states_pbf", *pull.ROAD_COLUMNS[:2]]
        self.assert_in_source(source, patterns)
        self.assert_options_exist(source, ["-o", "-s"])

    def test_make_osm_roads_per_huc(self):
        source = read_source("data", "roads", "make_osm_roads_per_huc.py")
        patterns = [log_prefix(roads.LOG_GLOB), roads.NO_ROADS_MARKER, roads.NO_OVERLAP_MARKER]
        patterns += [
            roads.MISSING_WBD_MARKER,
            *roads.FAILURE_MARKERS,
            "roads_{huc8}.parquet",
            *roads.REQUIRED_COLUMNS,
        ]
        self.assert_in_source(source, patterns)
        self.assert_options_exist(source, ["-r", "-p", "-o", "-j", "-lh"])

    def test_make_osm_bridges_per_huc(self):
        source = read_source("data", "bridges", "make_osm_bridges_per_huc.py")
        patterns = [log_prefix(bridges.LOG_GLOB), bridges.NO_BRIDGES_MARKER, bridges.NO_OVERLAP_MARKER]
        patterns += [bridges.MISSING_WBD_MARKER, *bridges.FAILURE_MARKERS, "bridges_{huc8}.parquet", '"_bad"']
        patterns += list(bridges.REQUIRED_COLUMNS)
        self.assert_in_source(source, patterns)
        self.assert_options_exist(source, ["-b", "-p", "-o", "-j", "-lh"])

    def test_make_rasters_using_lidar(self):
        source = read_source("data", "bridges", "make_rasters_using_lidar.py")
        patterns = [log_prefix(lidar.LOG_GLOB), lidar.LIDAR_PROCESSING_DIR, lidar.MODIFIED_BRIDGES_DIR]
        patterns += [
            lidar.RASTER_SUBDIR,
            lidar.MODIFIED_FILE_SUFFIX,
            lidar.SUMMARY_CSV,
            lidar.PER_HUC_SUMMARY_CSV,
        ]
        # The combined summary is only written for multi-HUC runs; the validator relies on this rule.
        patterns += ["len(bridge_files) > 1", "lst_hucs.split(\" \")"]
        self.assert_in_source(source, patterns)
        self.assert_options_exist(source, ["-i", "-o", "-lh", "-jl", "-jr"])

    def test_make_dem_dif_for_bridges(self):
        source = read_source("data", "bridges", "make_dem_dif_for_bridges.py")
        patterns = [
            log_prefix(dem_diff.LOG_GLOB),
            dem_diff.VRT_NAME,
            "_diff{extension}",
            *dem_diff.FAILURE_MARKERS,
        ]
        patterns += ["nargs='+'", "'lidar_osm_rasters'", "bridges_{HUC}.parquet"]
        self.assert_in_source(source, patterns)
        self.assert_options_exist(source, ["-d", "-l", "-i", "-o", "-j"])


if __name__ == "__main__":
    unittest.main()
