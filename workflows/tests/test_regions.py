import unittest
from pathlib import Path

from workflows.shared.regions import (
    REGIONS,
    bridge_diff_variable_for_region,
    dem_directories_for_region,
    region_crs,
    region_for_huc,
)


class RegionTests(unittest.TestCase):
    def test_region_mapping_and_crs(self):
        self.assertEqual(region_for_huc("19010101").name, "alaska")
        self.assertEqual(region_for_huc("22010000").name, "guam")
        self.assertEqual(region_for_huc("22030001").name, "american_samoa")
        self.assertEqual(region_for_huc("01010002").name, "conus")
        self.assertEqual(
            {name: region_crs(name) for name in REGIONS},
            {"conus": 5070, "alaska": 3338, "guam": 6637, "american_samoa": 32702},
        )

    def test_dem_directories_include_alaska_extras(self):
        variables = {
            "input_DEM_Alaska": "C:/inputs/alaska/2026/dem.vrt",
            "OSM_DEM_DIFF_EXTRA_DEM_DIRS_ALASKA": "C:/inputs/ifsar/north C:/inputs/ifsar/coastal",
        }
        self.assertEqual(
            dem_directories_for_region("alaska", variables),
            [Path("C:/inputs/alaska/2026"), Path("C:/inputs/ifsar/north"), Path("C:/inputs/ifsar/coastal")],
        )

    def test_dem_file_uses_its_parent_folder(self):
        directories = dem_directories_for_region("conus", {"input_DEM": "/inputs/conus/hand.vrt"})
        self.assertEqual(directories, [Path("/inputs/conus")])

    def test_bridge_diff_variable_names(self):
        self.assertEqual(
            bridge_diff_variable_for_region("american_samoa"), "input_bridge_elev_diff_americansamoa"
        )


if __name__ == "__main__":
    unittest.main()
