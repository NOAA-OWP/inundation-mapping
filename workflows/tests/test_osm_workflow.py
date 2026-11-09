import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from workflows.inputs.osm_common import OsmContext, report_sections, s3_upload_preflight
from workflows.inputs.osm_workflow import build_workflow_steps, preflight_checks
from workflows.shared.workflow_runner import StepResult, WorkflowRunner
from workflows.validators.validator_base import CheckResult, ValidationResult


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def make_context(root: Path, *, hucs: str = "01010001", create_dem: bool = True, **overrides) -> OsmContext:
    preclip = root / "preclip"
    for huc in ("01010001", "01010002"):
        (preclip / huc).mkdir(parents=True, exist_ok=True)
        (preclip / huc / "wbd.gpkg").touch()
    (root / "existing_lidar" / "lidar_processing").mkdir(parents=True, exist_ok=True)
    dem_dir = root / "dems" / "conus"
    if create_dem:
        dem_dir.mkdir(parents=True, exist_ok=True)
    options = {
        "output_root": root / "inputs" / "osm",
        "preclip_dir": preclip,
        "states": "delaware",
        "hucs": hucs,
        "existing_lidar_dir": None if overrides.get("run_lidar") else root / "existing_lidar",
        "dem_diff_regions": ("conus",),
        "log_dir": root / "logs",
        "bash_variables": {"input_DEM": str(dem_dir / "hand_seamless_3dep_dems.vrt")},
    }
    options.update(overrides)
    return OsmContext("20261108", **options)


def passed(*names: str) -> list[StepResult]:
    return [StepResult(name, "passed", 0, True, 0.1, "Completed successfully") for name in names]


class PreflightTests(unittest.TestCase):
    def test_passes_then_stops_on_existing_outputs_unless_resuming(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir))
            self.assertEqual(preflight_checks(ctx, pull=True, roads=True, bridges=True), ([], []))

            ctx.roads_dir.mkdir(parents=True)
            errors, _ = preflight_checks(ctx, pull=True, roads=True, bridges=True)
            self.assertTrue(any("already exist" in error for error in errors))
            errors, _ = preflight_checks(
                ctx, pull=True, roads=True, bridges=True, allow_existing_outputs=True
            )
            self.assertEqual(errors, [])
            self.assertEqual(preflight_checks(ctx, pull=True, bridges=True), ([], []))

    def test_preclip_rules_for_subsets_and_full_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ctx = make_context(root)
            (ctx.preclip_dir / "01010001" / "wbd.gpkg").unlink()
            errors, warnings = preflight_checks(ctx, roads=True)
            self.assertTrue(any("without wbd.gpkg" in error for error in errors))
            self.assertEqual(warnings, [])

            full_run = make_context(root, hucs="")
            (full_run.preclip_dir / "01010001" / "wbd.gpkg").unlink(missing_ok=True)
            errors, warnings = preflight_checks(full_run, roads=True)
            self.assertEqual(errors, [])
            self.assertTrue(any("without wbd.gpkg" in warning for warning in warnings))

            errors, _ = preflight_checks(make_context(root, hucs="09999999"), roads=True)
            self.assertTrue(any("not in the pre-clip folder" in error for error in errors))

            errors, _ = preflight_checks(make_context(root, preclip_dir=root / "missing"), roads=True)
            self.assertTrue(any("pre-clip folder is missing" in error for error in errors))
            self.assertEqual(
                preflight_checks(make_context(root, preclip_dir=root / "missing"), pull=True), ([], [])
            )

    def test_bridges_need_the_existing_lidar_and_dem_folders(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ctx = make_context(root)
            ctx.lidar_processing_dir.rmdir()
            errors, _ = preflight_checks(ctx, bridges=True)
            self.assertTrue(any("existing lidar folder is missing" in error for error in errors))
            self.assertEqual(preflight_checks(make_context(root, run_lidar=True), bridges=True), ([], []))

            errors, _ = preflight_checks(make_context(root / "no_dem", create_dem=False), bridges=True)
            self.assertTrue(
                any("DEM folder(s) for the conus DEM diffs are missing" in error for error in errors)
            )


class CompositionAndReportTests(unittest.TestCase):
    def test_composed_steps_respect_the_skip_flags(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir))
            names = [step.name for step in build_workflow_steps(ctx)]
            self.assertEqual(
                names[:4], ["osm.pull", "osm.pull.s3_copy", "osm.roads_per_huc", "osm.roads_per_huc.s3_copy"]
            )
            self.assertIn("bridges.dem_diff.conus", names)

            names = [step.name for step in build_workflow_steps(ctx, skip_bridges=True)]
            self.assertEqual(
                names, ["osm.pull", "osm.pull.s3_copy", "osm.roads_per_huc", "osm.roads_per_huc.s3_copy"]
            )
            names = [step.name for step in build_workflow_steps(ctx, skip_roads=True)]
            self.assertNotIn("osm.roads_per_huc", names)
            self.assertIn("osm.bridges_per_huc", names)
            names = [step.name for step in build_workflow_steps(ctx, skip_roads=True, skip_bridges=True)]
            self.assertEqual(names, ["osm.pull", "osm.pull.s3_copy"])

    def test_report_lists_produced_outputs_and_the_preclip_step_without_editing_bash_variables(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir), run_lidar=True)
            for folder, name in (
                (ctx.pull_roads_dir, "delaware_roads.parquet"),
                (ctx.pull_bridges_dir, "delaware_bridges.parquet"),
                (ctx.roads_dir, "roads_01010001.parquet"),
                (ctx.bridges_dir, "bridges_01010001.parquet"),
                (ctx.modified_bridges_dir, "huc_01010001_osm_bridges_modified.parquet"),
                (ctx.dem_diff_dir("conus"), "bridge_elev_diff.vrt"),
            ):
                folder.mkdir(parents=True, exist_ok=True)
                (folder / name).touch()
            runner = WorkflowRunner("osm_test", ctx.log_dir)
            runner.add_steps(build_workflow_steps(ctx))
            runner.results = passed(
                "osm.pull",
                "osm.roads_per_huc",
                "osm.bridges_per_huc",
                "bridges.lidar.conus",
                "bridges.lidar.alaska",
                "bridges.dem_diff.conus",
            )
            ValidationResult(
                "validate_sample", "target", [CheckResult("check", False, "late", True)]
            ).write_report(ctx.log_dir)

            bash_variables = PROJECT_ROOT / "src" / "bash_variables.env"
            before = hashlib.sha256(bash_variables.read_bytes()).hexdigest()
            report = runner.write_report(report_sections(ctx, runner)).read_text(encoding="utf-8")
            self.assertEqual(before, hashlib.sha256(bash_variables.read_bytes()).hexdigest())

            for expected in (
                "export osm_geofabric_states_roads_dir=",
                "export osm_geofabric_states_bridges_dir=",
                "export osm_roads_per_huc_dir=",
                "export osm_bridges_modified_dir=",
                "export input_bridge_elev_diff=",
                "WARNING check: late",
                "generate_pre_clip_fim_huc8.py",
                "--osm_roads --osm_bridges",
            ):
                self.assertIn(expected, report)
            self.assertNotIn("Lidar was not run", report)

    def test_report_without_lidar_warns_and_leaves_bridges_out_of_preclip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir))
            ctx.roads_dir.mkdir(parents=True)
            (ctx.roads_dir / "roads_01010001.parquet").touch()
            runner = WorkflowRunner("osm_test", ctx.log_dir)
            runner.add_steps(build_workflow_steps(ctx))
            runner.results = passed("osm.pull", "osm.roads_per_huc", "osm.bridges_per_huc")
            runner.results.append(
                StepResult("bridges.dem_diff.conus", "failed", 0, False, 0.1, "Validation failed")
            )

            sections = report_sections(ctx, runner)
            self.assertIn("older modified bridges", sections["Lidar was not run"])
            self.assertIn("--osm_roads", sections["Manual next steps"])
            self.assertNotIn("--osm_bridges", sections["Manual next steps"])
            suggestions = sections["Suggested src/bash_variables.env edits"]
            self.assertIn("osm_roads_per_huc_dir", suggestions)
            self.assertNotIn("input_bridge_elev_diff", suggestions)
            self.assertNotIn("osm_bridges_modified_dir", suggestions)

    def test_upload_preflight_only_checks_copy_steps_that_will_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ctx = make_context(Path(temp_dir), upload_to_fim_s3=True)
            ctx.log_dir.mkdir(parents=True)
            state = {"steps": [vars(result) for result in passed("osm.pull", "osm.pull.s3_copy")]}
            (ctx.log_dir / "osm_test_state.json").write_text(json.dumps(state), encoding="utf-8")
            runner = WorkflowRunner("osm_test", ctx.log_dir, resume=True)
            runner.add_steps(build_workflow_steps(ctx, skip_bridges=True))

            with patch("workflows.inputs.osm_common.preflight_fim_s3", return_value=[]) as preflight:
                self.assertEqual(s3_upload_preflight(ctx, runner), [])
            checked_paths = preflight.call_args.args[0]
            self.assertEqual(checked_paths, [ctx.roads_dir])

            ctx.upload_to_fim_s3 = False
            with patch("workflows.inputs.osm_common.preflight_fim_s3") as preflight:
                self.assertEqual(s3_upload_preflight(ctx, runner), [])
            preflight.assert_not_called()


if __name__ == "__main__":
    unittest.main()
