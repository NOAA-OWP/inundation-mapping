import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from workflows.inputs.osm_common import load_workflow_params
from workflows.shared.s3_copy import (
    build_s3_copy_step,
    build_s3_file_list,
    copy_to_fim_s3,
    preflight_fim_s3,
    resolve_bucket,
    target_prefix,
)


PARAMS = {"FIM_S3_BUCKET_NAME": "test-bucket", "FIM_S3_ROOT_INPUTS_PATH": "foss_fim/temp/inputs"}


class FakeS3:
    """Stands in for data.aws.*: records uploads and lists them back like list_objects_v2."""

    def __init__(self, folder_exists: bool = False, drop_uploads: int = 0):
        self.folder_exists = folder_exists
        self.drop_uploads = drop_uploads
        self.objects: set[str] = set()
        self.uploads: list[tuple[str, list[dict[str, str]]]] = []
        self.client = types.SimpleNamespace(get_paginator=self.get_paginator)

    def get_paginator(self, name: str):
        objects = self.objects

        class Paginator:
            def paginate(self, Bucket: str, Prefix: str):
                yield {"Contents": [{"Key": key} for key in sorted(objects) if key.startswith(Prefix)]}

        return Paginator()

    def modules(self) -> dict[str, types.ModuleType]:
        aws_module = types.ModuleType("data.aws.aws_shared_functions")
        aws_module.create_aws_client = lambda **kwargs: (True, "", self.client)
        s3_module = types.ModuleType("data.aws.s3_shared_functions")
        s3_module.does_s3_bucket_exist = lambda client, bucket: (bucket == "test-bucket", "no such bucket")
        s3_module.does_s3_folder_exist = lambda client, bucket, prefix: self.folder_exists
        s3_module.upload_by_file_list = self.upload_by_file_list
        return {"data.aws.aws_shared_functions": aws_module, "data.aws.s3_shared_functions": s3_module}

    def upload_by_file_list(self, client, bucket, file_list):
        self.uploads.append((bucket, file_list))
        kept = file_list[: len(file_list) - self.drop_uploads]
        self.objects.update(item["trg_file"] for item in kept)


class S3CopyTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.local_root = root / "inputs"
        self.output = self.local_root / "osm" / "roads" / "20261108"
        self.output.mkdir(parents=True)
        (self.output / "roads_01010001.parquet").write_bytes(b"data")
        (self.output / "roads_01010002.parquet").write_bytes(b"data")
        (self.output / "download.pbf").write_bytes(b"pbf")
        (self.output / "points.las").write_bytes(b"las")
        (self.output / "states_pbf").mkdir()
        (self.output / "states_pbf" / "delaware.txt").write_bytes(b"skip")
        self.credentials = root / "aws_credentials.env"
        self.credentials.write_text(
            "FIM_AWS_ACCESS_KEY_ID=key\nFIM_AWS_SECRET_ACCESS_KEY=secret\nFIM_AWS_REGION_NAME=us-east-1\n",
            encoding="utf-8",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_file_list_maps_keys_and_skips_pbf_las_and_states_pbf(self):
        file_list = build_s3_file_list([self.output], self.local_root, "foss_fim/temp/inputs/")
        self.assertEqual(
            [item["trg_file"] for item in file_list],
            [
                "foss_fim/temp/inputs/osm/roads/20261108/roads_01010001.parquet",
                "foss_fim/temp/inputs/osm/roads/20261108/roads_01010002.parquet",
            ],
        )
        self.assertEqual(file_list[0]["src_file"], str(self.output / "roads_01010001.parquet"))
        self.assertEqual(target_prefix(self.output, self.local_root, "root"), "root/osm/roads/20261108")

    def test_preview_writes_a_manifest_and_makes_no_s3_calls(self):
        summary = []
        manifest = Path(self.temp_dir.name) / "logs" / "s3_copy_osm.roads_per_huc.s3_copy.json"
        with patch("workflows.shared.s3_copy._aws_modules", side_effect=AssertionError("no S3 in preview")):
            self.assertTrue(
                copy_to_fim_s3(
                    [self.output], self.local_root, PARAMS, None, summary=summary, manifest_path=manifest
                )
            )
        self.assertIn("Preview only: 2 file(s)", summary[0])
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertEqual((payload["mode"], payload["file_count"]), ("preview", 2))

        summary.clear()
        self.assertTrue(copy_to_fim_s3([self.output], self.local_root, {}, None, summary=summary))
        self.assertIn("still notset", summary[0])

    def test_upload_sends_the_file_list_and_confirms_every_key(self):
        fake = FakeS3()
        summary = []
        with patch.dict("sys.modules", fake.modules()):
            ok = copy_to_fim_s3(
                [self.output], self.local_root, PARAMS, self.credentials, upload=True, summary=summary
            )
        self.assertTrue(ok)
        bucket, files = fake.uploads[0]
        self.assertEqual(bucket, "test-bucket")
        self.assertEqual(len(files), 2)
        self.assertEqual(set(files[0].keys()), {"src_file", "trg_file"})
        self.assertEqual(len(fake.objects), 2)
        self.assertIn("Uploaded 2 file(s)", summary[0])

    def test_silently_failed_uploads_fail_the_step(self):
        fake = FakeS3(drop_uploads=1)
        with patch.dict("sys.modules", fake.modules()), self.assertLogs(level="ERROR") as logs:
            self.assertFalse(
                copy_to_fim_s3([self.output], self.local_root, PARAMS, self.credentials, upload=True)
            )
        self.assertIn("1 of 2 file(s) are missing", logs.output[0])

    def test_existing_s3_folder_blocks_upload_unless_overwrite(self):
        fake = FakeS3(folder_exists=True)
        with patch.dict("sys.modules", fake.modules()):
            with self.assertLogs(level="ERROR"):
                self.assertFalse(
                    copy_to_fim_s3([self.output], self.local_root, PARAMS, self.credentials, upload=True)
                )
            self.assertEqual(fake.uploads, [])
            self.assertTrue(
                copy_to_fim_s3(
                    [self.output], self.local_root, PARAMS, self.credentials, upload=True, overwrite=True
                )
            )
        self.assertEqual(len(fake.uploads), 1)

    def test_upload_refuses_notset_settings_and_sources_outside_the_root(self):
        with self.assertLogs(level="ERROR"):
            self.assertFalse(
                copy_to_fim_s3([self.output], self.local_root, {}, self.credentials, upload=True)
            )
        outside = Path(self.temp_dir.name) / "elsewhere"
        outside.mkdir()
        with self.assertLogs(level="ERROR"):
            self.assertFalse(
                copy_to_fim_s3([outside], self.local_root, PARAMS, self.credentials, upload=True)
            )
        with self.assertLogs(level="WARNING"):
            self.assertTrue(copy_to_fim_s3([outside], self.local_root, PARAMS, None))

    def test_preflight_checks_settings_credentials_and_existing_targets(self):
        errors = preflight_fim_s3([self.output], self.local_root, {}, None)
        self.assertEqual(len(errors), 3)
        self.assertTrue(all("notset" in error or "missing" in error for error in errors))

        fake = FakeS3(folder_exists=True)
        with patch.dict("sys.modules", fake.modules()):
            errors = preflight_fim_s3([self.output], self.local_root, PARAMS, self.credentials)
            self.assertTrue(any("already exists" in error for error in errors))
            self.assertEqual(
                preflight_fim_s3([self.output], self.local_root, PARAMS, self.credentials, True), []
            )
            bad_bucket = {**PARAMS, "FIM_S3_BUCKET_NAME": "other-bucket"}
            errors = preflight_fim_s3([self.output], self.local_root, bad_bucket, self.credentials)
            self.assertTrue(any("pre-flight failed" in error for error in errors))

    def test_bucket_name_can_come_from_the_credentials_file(self):
        self.credentials.write_text("FIM_S3_BUCKET_NAME=test-bucket\n", encoding="utf-8")
        params = {"FIM_S3_BUCKET_NAME": "notset", "FIM_S3_ROOT_INPUTS_PATH": "foss_fim/inputs"}
        self.assertEqual(resolve_bucket(params, self.credentials), "test-bucket")
        self.assertEqual(resolve_bucket(PARAMS, None), "test-bucket")
        self.assertEqual(resolve_bucket({}, None), "notset")

    def test_copy_step_runs_the_copy_and_describes_it(self):
        summary = []
        step = build_s3_copy_step(
            "osm.roads_per_huc", [self.output], self.local_root, PARAMS, None, summary=summary
        )
        self.assertEqual(step.name, "osm.roads_per_huc.s3_copy")
        self.assertIn("Preview", step.description)
        self.assertTrue(step.action())
        self.assertEqual(len(summary), 1)

    def test_template_defaults(self):
        params = load_workflow_params()
        self.assertEqual(params["FIM_S3_BUCKET_NAME"], "notset")
        self.assertEqual(params["FIM_S3_ROOT_INPUTS_PATH"], "notset")
        self.assertIn("10m_NorthAlaska", params["OSM_DEM_DIFF_EXTRA_DEM_DIRS_ALASKA"])

        working_copy = Path(self.temp_dir.name) / "workflow_params.env"
        working_copy.write_text('FIM_S3_ROOT_INPUTS_PATH = "foss_fim/inputs"\n', encoding="utf-8")
        params = load_workflow_params(working_copy)
        self.assertEqual(params["FIM_S3_ROOT_INPUTS_PATH"], "foss_fim/inputs")
        self.assertIn("10m_NorthAlaska", params["OSM_DEM_DIFF_EXTRA_DEM_DIRS_ALASKA"])
        with self.assertRaises(FileNotFoundError):
            load_workflow_params(Path(self.temp_dir.name) / "missing.env")


if __name__ == "__main__":
    unittest.main()
