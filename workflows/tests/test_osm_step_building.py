import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from workflows.inputs.osm_bridges_workflow import (
    BRIDGES_SCRIPT,
    DEM_DIFF_SCRIPT,
    LIDAR_SCRIPT,
    bridge_hucs_for_region,
    build_bridges_steps,
)
from workflows.inputs.osm_common import PULL_OSM_SCRIPT, OsmContext, build_pull_steps
from workflows.inputs.osm_roads_workflow import ROADS_SCRIPT, build_roads_steps


def make_context(root: Path, **overrides) -> OsmContext:
    options = {
        "output_root": root / "inputs" / "osm",
        "preclip_dir": root / "preclip",
        "log_dir": root / "logs",
        "dem_diff_regions": ("conus", "alaska"),
        "existing_lidar_dir": root / "existing_lidar",
        "bash_variables": {
            "input_DEM": "/inputs/conus/dem.vrt",
            "input_DEM_Alaska": "/inputs/alaska/dem.vrt",
            "OSM_DEM_DIFF_EXTRA_DEM_DIRS_ALASKA": "/inputs/ifsar/north",
        },
    }
    options.update(overrides)
    return OsmContext("20261108", **options)


def steps_by_name(steps):
    return {step.name: step for step in steps}


class PullAndRoadsStepTests(unittest.TestCase):
    def test_pull_command_passes_output_and_states(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir), states="delaware  michigan")
            pull, copy = build_pull_steps(ctx)
            self.assertEqual(pull.name, "osm.pull")
            self.assertEqual(
                pull.command[1:],
                [str(PULL_OSM_SCRIPT), "-o", str(ctx.geofabric_dir), "-s", "delaware michigan"],
            )
            self.assertIsNotNone(pull.validator)
            self.assertEqual(copy.name, "osm.pull.s3_copy")
            self.assertEqual(ctx.s3_copy_targets["osm.pull.s3_copy"], [ctx.geofabric_dir])

    def test_empty_states_and_hucs_are_left_off(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir))
            self.assertNotIn("-s", build_pull_steps(ctx)[0].command)
            self.assertNotIn("-lh", build_roads_steps(ctx)[0].command)
            self.assertNotIn("-lh", build_bridges_steps(ctx)[0].command)

    def test_roads_command_matches_the_script_arguments(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir), hucs="01010002 01010003", number_jobs=3)
            roads, copy = build_roads_steps(ctx)
            self.assertEqual(roads.name, "osm.roads_per_huc")
            self.assertEqual(roads.command[1], str(ROADS_SCRIPT))
            expected = ["-r", str(ctx.pull_roads_dir), "-p", str(ctx.preclip_dir), "-o", str(ctx.roads_dir)]
            self.assertEqual(roads.command[2:], expected + ["-j", "3", "-lh", "01010002 01010003"])
            self.assertEqual(copy.name, "osm.roads_per_huc.s3_copy")

    def test_standalone_roads_can_use_existing_state_parquets(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir))
            source = Path(temp_dir) / "existing_state_parquets"
            command = build_roads_steps(ctx, roads_parquet_dir=source)[0].command
            self.assertEqual(command[command.index("-r") + 1], str(source))

    def test_paths_mirror_the_bash_variables_layout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir), existing_lidar_dir=None)
            root = ctx.output_root
            self.assertEqual(
                ctx.pull_roads_dir, root / "geofabric_states" / "20261108" / "states_parquet" / "roads"
            )
            self.assertEqual(ctx.roads_dir, root / "roads" / "20261108")
            self.assertEqual(ctx.bridges_dir, root / "bridges" / "bridge_lines" / "20261108")
            self.assertEqual(
                ctx.dem_diff_dir("american_samoa"),
                root / "bridges" / "DEM_Diffs" / "20261108" / "american_samoa",
            )
            self.assertEqual(ctx.s3_local_root, root.parent)

    def test_context_rejects_bad_versions_regions_and_job_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for overrides in ({"number_jobs": 0}, {"dem_diff_regions": ("hawaii",)}):
                with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                    make_context(root, **overrides)
            with self.assertRaises(ValueError):
                OsmContext("../20261108", output_root=root, bash_variables={})
            ctx = make_context(root, dem_diff_regions="conus conus guam")
            self.assertEqual(ctx.dem_diff_regions, ("conus", "guam"))

    def test_baselines_come_from_bash_variables_without_changing_the_caller_dict(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            variables = {
                "osm_roads_per_huc_dir": "/data/inputs/osm/roads/20261006/",
                "osm_bridges_modified_dir": (
                    "/data/inputs/osm/bridges/lidar_data/20261006/modified_osm_bridges/"
                ),
            }
            ctx = make_context(
                Path(temp_dir),
                existing_lidar_dir=None,
                bash_variables=variables,
                workflow_params={"OSM_DEM_DIFF_EXTRA_DEM_DIRS_ALASKA": "/extra"},
            )
            self.assertEqual(ctx.baseline_roads_per_huc_dir, Path("/data/inputs/osm/roads/20261006"))
            self.assertEqual(
                ctx.baseline_bridges_per_huc_dir, Path("/data/inputs/osm/bridges/bridge_lines/20261006")
            )
            self.assertEqual(
                ctx.lidar_processing_dir,
                Path("/data/inputs/osm/bridges/lidar_data/20261006/lidar_processing"),
            )
            self.assertNotIn("OSM_DEM_DIFF_EXTRA_DEM_DIRS_ALASKA", variables)


class BridgesStepTests(unittest.TestCase):
    def test_bridges_and_dem_diff_commands_match_the_script_arguments(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ctx = make_context(root, hucs="01010001 19010101", number_jobs=3, dem_jobs=7)
            steps = build_bridges_steps(ctx, bridges_parquet_dir=root / "state_bridges")
            named = steps_by_name(steps)

            bridges = named["osm.bridges_per_huc"]
            self.assertEqual(bridges.command[1], str(BRIDGES_SCRIPT))
            expected = [
                "-b",
                str(root / "state_bridges"),
                "-p",
                str(ctx.preclip_dir),
                "-o",
                str(ctx.bridges_dir),
            ]
            self.assertEqual(bridges.command[2:], expected + ["-j", "3", "-lh", "01010001 19010101"])

            conus = named["bridges.dem_diff.conus"].command
            self.assertEqual(conus[1], str(DEM_DIFF_SCRIPT))
            self.assertEqual(
                conus[2:],
                [
                    "-d",
                    str(Path("/inputs/conus")),
                    "-l",
                    str(ctx.lidar_processing_dir),
                    "-i",
                    str(ctx.bridges_dir),
                ]
                + ["-o", str(ctx.dem_diff_dir("conus")), "-j", "7"],
            )
            alaska = named["bridges.dem_diff.alaska"].command
            self.assertEqual(
                alaska[2:6], ["-d", str(Path("/inputs/alaska")), str(Path("/inputs/ifsar/north")), "-l"]
            )

    def test_every_data_step_is_followed_by_its_copy_step(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir), run_lidar=True, existing_lidar_dir=None)
            names = [step.name for step in build_bridges_steps(ctx)]
            self.assertEqual(
                names,
                [
                    "osm.bridges_per_huc",
                    "osm.bridges_per_huc.s3_copy",
                    "bridges.lidar.conus",
                    "bridges.lidar.alaska",
                    "bridges.lidar.s3_copy",
                    "bridges.dem_diff.conus",
                    "bridges.dem_diff.conus.s3_copy",
                    "bridges.dem_diff.alaska",
                    "bridges.dem_diff.alaska.s3_copy",
                ],
            )

    def test_without_run_lidar_the_lidar_step_is_skipped_and_existing_lidar_is_used(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ctx = make_context(root)
            named = steps_by_name(build_bridges_steps(ctx))
            self.assertFalse(named["bridges.lidar"].enabled)
            self.assertIn("--run_lidar not set", named["bridges.lidar"].skip_reason)
            self.assertNotIn("bridges.lidar.conus", named)
            dem = named["bridges.dem_diff.alaska"].command
            self.assertEqual(dem[dem.index("-l") + 1], str(root / "existing_lidar" / "lidar_processing"))

    def test_lidar_hucs_are_found_per_region_when_the_step_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(
                Path(temp_dir), run_lidar=True, existing_lidar_dir=None, lidar_jobs=8, raster_jobs=9
            )
            named = steps_by_name(build_bridges_steps(ctx))
            ctx.bridges_dir.mkdir(parents=True)
            for name in ("01010001", "01010002", "19010101", "22010000", "01010003_bad"):
                (ctx.bridges_dir / f"bridges_{name}.parquet").touch()

            self.assertEqual(bridge_hucs_for_region(ctx.bridges_dir, "conus"), ["01010001", "01010002"])
            self.assertEqual(bridge_hucs_for_region(ctx.bridges_dir, "conus", "01010002"), ["01010002"])
            self.assertEqual(bridge_hucs_for_region(ctx.bridges_dir, "alaska"), ["19010101"])

            completed = type("Completed", (), {"returncode": 0})()
            with patch("workflows.inputs.osm_bridges_workflow.subprocess.run", return_value=completed) as run:
                self.assertTrue(named["bridges.lidar.conus"].action())
                conus = run.call_args.args[0]
                self.assertTrue(named["bridges.lidar.alaska"].action())
                alaska = run.call_args.args[0]

            self.assertEqual(conus[1], str(LIDAR_SCRIPT))
            self.assertEqual(conus[2:6], ["-i", str(ctx.bridges_dir), "-o", str(ctx.lidar_root)])
            self.assertEqual(conus[conus.index("-lh") + 1], "01010001 01010002")
            self.assertEqual(conus[-4:], ["-jl", "8", "-jr", "9"])
            self.assertEqual(alaska[alaska.index("-lh") + 1], "19010101")
            self.assertTrue(ctx.lidar_processing_dir.is_dir())

    def test_lidar_step_without_region_bridges_does_not_run_the_script(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir), run_lidar=True, existing_lidar_dir=None)
            named = steps_by_name(build_bridges_steps(ctx))
            ctx.bridges_dir.mkdir(parents=True)
            with patch("workflows.inputs.osm_bridges_workflow.subprocess.run") as run:
                self.assertTrue(named["bridges.lidar.alaska"].action())
                self.assertTrue(named["bridges.lidar.alaska"].validator())
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
