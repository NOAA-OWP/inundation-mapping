import tempfile
import unittest
from pathlib import Path

from workflows.tests.helpers import HAS_PYARROW, HAS_RASTERIO, write_geoparquet, write_raster, write_vrt
from workflows.validators.validate_make_dem_dif_for_bridges import run_checks as run_dem_checks
from workflows.validators.validate_make_osm_bridges_per_huc import run_checks as run_bridges_checks
from workflows.validators.validate_make_rasters_using_lidar import run_checks as run_lidar_checks


def make_lidar_root(root: Path, hucs_with_rasters: tuple[str, ...], hucs_finished: tuple[str, ...]) -> Path:
    lidar_root = root / "lidar"
    for huc in hucs_with_rasters:
        raster_dir = lidar_root / "lidar_processing" / huc / "lidar_osm_rasters"
        raster_dir.mkdir(parents=True)
        (raster_dir / "123.tif").touch()
        (lidar_root / "lidar_processing" / huc / "bridge_elevation_filter_summary.csv").write_text(
            "osmid,kept\n123,1\n", encoding="utf-8"
        )
    (lidar_root / "lidar_processing").mkdir(parents=True, exist_ok=True)
    (lidar_root / "modified_osm_bridges").mkdir(parents=True, exist_ok=True)
    for huc in hucs_finished:
        (lidar_root / "modified_osm_bridges" / f"huc_{huc}_osm_bridges_modified.parquet").touch()
    return lidar_root


class BridgesValidatorTests(unittest.TestCase):
    def test_no_bridges_log_line_covers_huc_but_bad_parquet_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = root / "preclip"
            (preclip / "01010001").mkdir(parents=True)
            output = root / "bridges"
            output.mkdir()
            (output / "make_osm_bridges_20261108-1200.log").write_text(
                "[01010001] No bridges within HUC 01010001\n\u2705 All HUC tasks succeeded\n",
                encoding="utf-8",
            )
            self.assertTrue(run_bridges_checks(output, preclip).passed)

            (output / "bridges_01010001_bad.parquet").touch()
            result = run_bridges_checks(output, preclip)
            self.assertFalse(result.passed)
            bad = next(check for check in result.checks if check.name == "Failed bridge outputs")
            self.assertIn("bridges_01010001_bad.parquet", bad.message)


class LidarValidatorTests(unittest.TestCase):
    def test_fractions_summary_and_log_errors_as_warnings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lidar_root = make_lidar_root(Path(temp_dir), ("01010001",), ("01010001", "01010002"))
            (lidar_root / "all_bridge_elevation_filter_summary.csv").write_text(
                "HUC,osmid\n", encoding="utf-8"
            )
            (lidar_root / "make_rasters_using_lidar_20261108-1200.log").write_text(
                "\u274c 1 HUC(s) failed: ['01010003']\n", encoding="utf-8"
            )

            result = run_lidar_checks(lidar_root, ["01010001", "01010002"], label="conus")
            self.assertTrue(result.passed, result.checks)
            self.assertTrue(result.target.startswith("conus:"))
            self.assertTrue(any(check.is_warning and not check.passed for check in result.checks))

            (lidar_root / "modified_osm_bridges" / "huc_01010002_osm_bridges_modified.parquet").unlink()
            self.assertFalse(run_lidar_checks(lidar_root, ["01010001", "01010002"]).passed)

    def test_multi_huc_run_needs_the_combined_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lidar_root = make_lidar_root(Path(temp_dir), ("01010001", "01010002"), ("01010001", "01010002"))
            result = run_lidar_checks(lidar_root, "01010001 01010002")
            summary = next(check for check in result.checks if check.name == "Elevation summary CSV")
            self.assertFalse(summary.passed)
            self.assertFalse(result.passed)

    def test_single_huc_run_uses_its_own_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lidar_root = make_lidar_root(Path(temp_dir), ("19020101",), ("19020101",))
            self.assertTrue(run_lidar_checks(lidar_root, "19020101", label="alaska").passed)

            (lidar_root / "lidar_processing" / "19020101" / "bridge_elevation_filter_summary.csv").unlink()
            self.assertFalse(run_lidar_checks(lidar_root, "19020101", label="alaska").passed)

    def test_too_few_hucs_with_rasters_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            hucs = ("01010001", "01010002", "01010003")
            lidar_root = make_lidar_root(Path(temp_dir), ("01010001",), hucs)
            (lidar_root / "all_bridge_elevation_filter_summary.csv").write_text(
                "HUC,osmid\n", encoding="utf-8"
            )
            result = run_lidar_checks(lidar_root, hucs)
            rasters = next(check for check in result.checks if check.name == "HUCs with lidar rasters")
            self.assertFalse(rasters.passed)
            self.assertFalse(run_lidar_checks(Path(temp_dir) / "missing", "").passed)


@unittest.skipUnless(HAS_PYARROW, "pyarrow is required for the GeoParquet fixtures")
class BridgeGeoParquetTests(unittest.TestCase):
    def test_bridge_files_are_checked_for_columns_and_crs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preclip = root / "preclip"
            (preclip / "22010000").mkdir(parents=True)
            output = root / "bridges"
            columns = {"osmid": ["1"], "huc8": ["22010000"], "geometry": [b"wkb"]}
            write_geoparquet(output / "bridges_22010000.parquet", columns, 6637)
            (output / "make_osm_bridges_20261108-1200.log").write_text("done\n", encoding="utf-8")
            self.assertTrue(run_bridges_checks(output, preclip).passed)

            write_geoparquet(output / "bridges_22010000.parquet", columns, 4326)
            self.assertFalse(run_bridges_checks(output, preclip).passed)


@unittest.skipUnless(HAS_RASTERIO, "rasterio is required for the GeoTIFF fixtures")
class DemDiffValidatorTests(unittest.TestCase):
    def make_region(self, root: Path, region: str, crs: int, values: list[list[float]]) -> Path:
        output = root / region
        tif = output / "HUC8_01010001_dem_diff.tif"
        write_raster(tif, crs, values)
        write_vrt(output / "bridge_elev_diff.vrt", tif, crs)
        (output / "DEM_diff_rasters-2026_11_08-12_00_00.log").write_text(
            "TOTAL RUN TIME: 0:01:00\n", encoding="utf-8"
        )
        return output

    def test_conus_needs_nonzero_corrections_but_guam_may_be_all_zero(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            zeros = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
            conus = self.make_region(root, "conus", 5070, [[0, 0, 0], [0, 2.5, 0], [0, 0, -9999]])
            self.assertTrue(run_dem_checks(conus, "conus").passed)

            write_raster(conus / "HUC8_01010001_dem_diff.tif", 5070, zeros)
            self.assertFalse(run_dem_checks(conus, "conus").passed)

            guam = self.make_region(root, "guam", 6637, zeros)
            self.assertTrue(run_dem_checks(guam, "guam").passed)

    def test_wrong_crs_missing_vrt_and_swallowed_error_fail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            alaska = self.make_region(root, "alaska", 5070, [[1, 1, 1], [1, 1, 1], [1, 1, 1]])
            self.assertFalse(run_dem_checks(alaska, "alaska").passed)

            conus = self.make_region(root, "conus", 5070, [[1, 1, 1], [1, 1, 1], [1, 1, 1]])
            (conus / "DEM_diff_rasters-2026_11_08-12_00_00.log").write_text(
                "*** An error occurred while making dem diffs : Details: boom\n", encoding="utf-8"
            )
            self.assertFalse(run_dem_checks(conus, "conus").passed)

            (conus / "DEM_diff_rasters-2026_11_08-12_00_00.log").write_text("ok\n", encoding="utf-8")
            (conus / "bridge_elev_diff.vrt").unlink()
            self.assertFalse(run_dem_checks(conus, "conus").passed)


if __name__ == "__main__":
    unittest.main()
