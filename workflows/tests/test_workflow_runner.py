import json
import sys
import tempfile
import unittest
from pathlib import Path

from workflows.shared.workflow_runner import Step, WorkflowRunner, read_env_file


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PASS = [sys.executable, "-c", "pass"]
FAIL = [sys.executable, "-c", "raise SystemExit(1)"]


def append_command(path: Path) -> list[str]:
    source = f"from pathlib import Path; Path({str(path)!r}).open('a').write('x')"
    return [sys.executable, "-c", source]


class WorkflowRunnerTests(unittest.TestCase):
    def test_runs_steps_and_writes_state_and_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            runner = WorkflowRunner("test", log_dir)
            runner.add_steps(
                [Step(name, command=PASS, validator=lambda: True) for name in ("one", "two", "three")]
            )

            self.assertTrue(runner.run())
            state = json.loads((log_dir / "test_state.json").read_text(encoding="utf-8"))
            self.assertEqual([item["status"] for item in state["steps"]], ["passed"] * 3)
            self.assertIn(
                "| one | passed | 0 | True |", (log_dir / "test_report.md").read_text(encoding="utf-8")
            )

    def test_validator_failure_stops_later_steps(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            marker = Path(temp_dir) / "third.txt"
            runner = WorkflowRunner("test", Path(temp_dir))
            runner.add_steps(
                [
                    Step("one", command=PASS, validator=lambda: True),
                    Step("two", command=PASS, validator=lambda: False),
                    Step("three", command=append_command(marker)),
                ]
            )

            self.assertFalse(runner.run())
            self.assertFalse(marker.exists())
            self.assertEqual([result.status for result in runner.results], ["passed", "failed"])
            self.assertEqual(runner.results[1].message, "Validation failed")

    def test_nonzero_command_stops_before_validator(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            called = []
            runner = WorkflowRunner("test", Path(temp_dir))
            runner.add_steps(
                [
                    Step(
                        "bad",
                        command=[sys.executable, "-c", "raise SystemExit(4)"],
                        validator=lambda: called.append(True) or True,
                    ),
                    Step("later", command=PASS),
                ]
            )

            self.assertFalse(runner.run())
            self.assertEqual(called, [])
            self.assertEqual(runner.results[0].return_code, 4)
            self.assertEqual(len(runner.results), 1)

    def test_actions_disabled_steps_and_exceptions(self):
        def broken_action():
            raise RuntimeError("boom")

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = WorkflowRunner("test", Path(temp_dir))
            runner.add_steps(
                [
                    Step("disabled", command=FAIL, enabled=False, skip_reason="not requested"),
                    Step("action", action=lambda: True),
                    Step("broken", action=broken_action),
                ]
            )

            self.assertFalse(runner.run())
            self.assertEqual([result.status for result in runner.results], ["skipped", "passed", "failed"])
            self.assertIn("RuntimeError: boom", runner.results[2].message)

    def test_resume_skips_passed_steps_and_retries_failed_step(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            first, last = log_dir / "first.txt", log_dir / "last.txt"
            initial = WorkflowRunner("test", log_dir)
            initial.add_steps(
                [
                    Step("first", command=append_command(first)),
                    Step("middle", command=FAIL),
                    Step("last", command=append_command(last)),
                ]
            )
            self.assertFalse(initial.run())

            resumed = WorkflowRunner("test", log_dir, resume=True)
            resumed.add_steps(
                [
                    Step("first", command=append_command(first)),
                    Step("middle", command=PASS),
                    Step("last", command=append_command(last)),
                ]
            )
            self.assertTrue(resumed.run())
            self.assertEqual(first.read_text(encoding="utf-8"), "x")
            self.assertEqual(last.read_text(encoding="utf-8"), "x")
            self.assertIn("skipped on resume", resumed.results[0].message)

    def test_from_step_reruns_named_and_later_steps_and_keeps_earlier_passes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            first, second = log_dir / "first.txt", log_dir / "second.txt"

            def steps():
                return [
                    Step("first", command=append_command(first)),
                    Step("second", command=append_command(second)),
                ]

            runner = WorkflowRunner("test", log_dir)
            runner.add_steps(steps())
            self.assertTrue(runner.run())

            rerun = WorkflowRunner("test", log_dir, from_step="second")
            rerun.add_steps(steps())
            self.assertTrue(rerun.run())
            self.assertEqual(first.read_text(encoding="utf-8"), "x")
            self.assertEqual(second.read_text(encoding="utf-8"), "xx")
            self.assertEqual(rerun.results[0].status, "passed")
            self.assertIn("before --from_step second", rerun.results[0].message)

    def test_will_run_and_list_steps_follow_resume_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            initial = WorkflowRunner("test", log_dir)
            initial.add_steps([Step("one", command=PASS), Step("two", command=FAIL)])
            self.assertFalse(initial.run())

            resumed = WorkflowRunner("test", log_dir, resume=True)
            resumed.add_steps(
                [
                    Step("one", command=PASS, description="first step"),
                    Step("two", command=PASS),
                    Step("off", command=PASS, enabled=False, skip_reason="flag not set"),
                ]
            )
            self.assertFalse(resumed.will_run("one"))
            self.assertTrue(resumed.will_run("two"))
            self.assertFalse(resumed.will_run("off"))
            self.assertFalse(resumed.will_run("missing"))
            listing = resumed.list_steps()
            self.assertIn("1. one [passed: Already passed; skipped on resume]", listing)
            self.assertIn("   first step", listing)
            self.assertIn("2. two [will run]", listing)
            self.assertIn("3. off [skipped: flag not set]", listing)

    def test_unknown_from_step_and_duplicate_names_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = WorkflowRunner("test", Path(temp_dir), from_step="missing")
            runner.add_steps([Step("one", command=PASS)])
            with self.assertRaises(ValueError):
                runner.run()
            with self.assertRaises(ValueError):
                runner.add_steps([Step("one", command=PASS)])
            with self.assertRaises(ValueError):
                Step("neither")


class ReadEnvFileTests(unittest.TestCase):
    def test_reads_exports_quotes_comments_and_references(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / "vars.env"
            env_file.write_text(
                "# a comment\n"
                "export                 root_dir=/data/inputs\n"
                "export child_dir=${root_dir}/osm/   # trailing comment\n"
                'QUOTED = "notset"\n'
                "single='a b'\n"
                "windows=C:\\data\\inputs\n",
                encoding="utf-8",
            )
            values = read_env_file(env_file)
            self.assertEqual(values["root_dir"], "/data/inputs")
            self.assertEqual(values["child_dir"], "/data/inputs/osm/")
            self.assertEqual(values["QUOTED"], "notset")
            self.assertEqual(values["single"], "a b")
            self.assertEqual(values["windows"], "C:\\data\\inputs")
            self.assertEqual(read_env_file(Path(temp_dir) / "missing.env"), {})

    def test_reads_the_repo_bash_variables_file(self):
        values = read_env_file(PROJECT_ROOT / "src" / "bash_variables.env")
        self.assertEqual(values["res"], "10")
        self.assertIn("pre_clip_huc8", values["pre_clip_huc_dir"])
        self.assertIn("/osm/roads/", values["osm_roads_per_huc_dir"])
        self.assertTrue(values["input_bridge_elev_diff_alaska"].endswith("/alaska/bridge_elev_diff.vrt"))


if __name__ == "__main__":
    unittest.main()
