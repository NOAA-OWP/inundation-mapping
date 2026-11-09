import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GUIDE = PROJECT_ROOT / "workflows" / "workflow_automation.md"
OPTION_PATTERN = re.compile(r"(?<![\w-])(--?[A-Za-z][\w-]*)")
WORKFLOW_MODULES = [
    "workflows.inputs.osm_workflow",
    "workflows.inputs.osm_roads_workflow",
    "workflows.inputs.osm_bridges_workflow",
]
VALIDATOR_MODULES = [
    "workflows.validators.validate_pull_osm",
    "workflows.validators.validate_make_osm_roads_per_huc",
    "workflows.validators.validate_make_osm_bridges_per_huc",
    "workflows.validators.validate_make_rasters_using_lidar",
    "workflows.validators.validate_make_dem_dif_for_bridges",
]


def run_module(module: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", module, *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )


class CommandLineTests(unittest.TestCase):
    def test_every_command_line_prints_help(self):
        for module in WORKFLOW_MODULES + VALIDATOR_MODULES:
            with self.subTest(module=module):
                result = run_module(module, "--help")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage:", result.stdout)

    def test_workflow_command_lines_share_the_run_control_and_s3_options(self):
        for module in WORKFLOW_MODULES:
            help_text = run_module(module, "--help").stdout
            for option in (
                "--resume",
                "--from_step",
                "--list_steps",
                "--log_dir",
                "--upload_to_fim_s3",
                "-wp",
                "-a",
            ):
                with self.subTest(module=module, option=option):
                    self.assertIn(option, help_text)

    def test_list_steps_shows_the_composed_chain_without_running_anything(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            result = run_module(
                "workflows.inputs.osm_workflow",
                "-v",
                "20261108",
                "-o",
                str(root / "osm"),
                "--log_dir",
                str(root / "logs"),
                "--dem_diff_regions",
                "conus alaska",
                "--list_steps",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            for expected in (
                "1. osm.pull [will run]",
                "osm.pull.s3_copy",
                "osm.roads_per_huc",
                "osm.bridges_per_huc",
                "bridges.lidar [skipped: --run_lidar not set",
                "bridges.dem_diff.conus",
                "bridges.dem_diff.alaska.s3_copy",
                "10m_NorthAlaska",
            ):
                self.assertIn(expected, result.stdout)
            self.assertNotIn("american_samoa", result.stdout)
            self.assertFalse((root / "osm").exists())
            self.assertFalse((root / "logs").exists())

    def test_unknown_from_step_and_bad_region_are_usage_errors(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = ["-v", "20261108", "-o", str(Path(temp_dir) / "osm"), "--list_steps"]
            result = run_module("workflows.inputs.osm_workflow", *base, "--from_step", "nope")
            self.assertEqual(result.returncode, 2)
            self.assertIn("Unknown --from_step value: nope", result.stderr)

            result = run_module(
                "workflows.inputs.osm_bridges_workflow", *base, "--dem_diff_regions", "hawaii"
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("Unknown region: hawaii", result.stderr)

    def test_failed_preflight_exits_1_writes_the_report_and_runs_nothing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            result = run_module(
                "workflows.inputs.osm_workflow",
                "-v",
                "20261108",
                "-o",
                str(root / "osm"),
                "-p",
                str(root / "missing_preclip"),
                "--log_dir",
                str(root / "logs"),
                "--skip_bridges",
            )
            self.assertEqual(result.returncode, 1, result.stderr)
            report = (root / "logs" / "osm_20261108_report.md").read_text(encoding="utf-8")
            self.assertIn("## Pre-flight checks", report)
            self.assertIn("pre-clip folder is missing", report)
            self.assertFalse((root / "osm" / "geofabric_states").exists())

    def test_guide_examples_only_use_options_that_exist(self):
        text = GUIDE.read_text(encoding="utf-8")
        commands = re.findall(r"python /foss_fim/(workflows/\S+)\.py((?:[^\n]*\\\n)*[^\n]*)", text)
        self.assertGreater(len(commands), 10)
        defined: dict[str, set[str]] = {}
        for script, arguments in commands:
            module = script.replace("/", ".")
            if module not in defined:
                help_text = run_module(module, "--help").stdout
                defined[module] = set(OPTION_PATTERN.findall(help_text))
            for option in OPTION_PATTERN.findall(arguments.split("#", 1)[0]):
                with self.subTest(module=module, option=option):
                    self.assertIn(option, defined[module])

    def test_standalone_validator_exits_1_and_writes_json_for_a_bad_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "preclip" / "01010001").mkdir(parents=True)
            (root / "roads").mkdir()
            result = run_module(
                "workflows.validators.validate_make_osm_roads_per_huc",
                "-o",
                str(root / "roads"),
                "-p",
                str(root / "preclip"),
                "--report_dir",
                str(root / "reports"),
            )
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertIn("FAIL: HUC coverage", result.stdout)
            self.assertEqual(len(list((root / "reports").glob("validate_make_osm_roads_per_huc_*.json"))), 1)


if __name__ == "__main__":
    unittest.main()
