"""Guarded FIM S3 copy step for validated workflow outputs.

- Preview (default): log what would be copied and write the full file list to a JSON manifest. No S3 calls.
- Upload (`--upload_to_fim_s3`): create the S3 client the same way as the `workflows/deploy` tools, refuse to
  write into an S3 folder that already exists unless `--s3_overwrite` is given, upload with
  `upload_by_file_list`, then confirm every key arrived (that helper logs failed uploads without raising).

Each local file maps to `<FIM_S3_ROOT_INPUTS_PATH>/<path relative to the local inputs root>`. Files under
`states_pbf/` and `*.pbf` / `*.las` files are never copied. boto3 is only imported for uploads.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
from pathlib import Path
from typing import Iterable, Mapping

from workflows.shared.workflow_runner import Step, read_env_file


NOTSET = "notset"
S3_COPY_SUFFIX = ".s3_copy"
SKIPPED_DIR_NAMES = {"states_pbf"}
SKIPPED_SUFFIXES = {".pbf", ".las"}


def copy_step_name(name: str) -> str:
    return f"{name}{S3_COPY_SUFFIX}"


def _setting(values: Mapping[str, str], key: str) -> str:
    return (values.get(key) or NOTSET).strip().strip("/")


def _is_set(value: str) -> bool:
    return bool(value) and value.lower() != NOTSET


def resolve_bucket(params: Mapping[str, str], aws_creds_file: Path | str | None) -> str:
    """FIM_S3_BUCKET_NAME from the workflows params file, else from the AWS credentials file."""
    bucket = _setting(params, "FIM_S3_BUCKET_NAME")
    if not _is_set(bucket) and aws_creds_file and Path(aws_creds_file).is_file():
        bucket = _setting(read_env_file(aws_creds_file), "FIM_S3_BUCKET_NAME")
    return bucket


def _join_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part and part.strip("/"))


def _relative_path(path: Path, local_root: Path) -> Path:
    try:
        return Path(path).resolve().relative_to(Path(local_root).resolve())
    except ValueError as exc:
        raise ValueError(f"{path} is outside the local inputs root {local_root}") from exc


def build_s3_file_list(paths: Iterable[Path], local_root: Path, s3_root: str) -> list[dict[str, str]]:
    """Return `upload_by_file_list` entries (`src_file`, `trg_file`) for every file under `paths`."""
    file_list: dict[str, dict[str, str]] = {}
    for path in map(Path, paths):
        if not path.exists():
            raise FileNotFoundError(f"S3 copy source does not exist: {path}")
        _relative_path(path, local_root)  # every source must sit under the local inputs root
        sources = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
        for source in sources:
            relative = _relative_path(source, local_root)
            if SKIPPED_DIR_NAMES.intersection(relative.parts) or source.suffix.lower() in SKIPPED_SUFFIXES:
                continue
            key = _join_key(s3_root, relative.as_posix())
            file_list[key] = {"src_file": str(source), "trg_file": key}
    return [file_list[key] for key in sorted(file_list)]


def target_prefix(path: Path, local_root: Path, s3_root: str) -> str:
    """S3 folder for a local output folder (or for the folder that holds a local output file)."""
    path = Path(path)
    folder = path.parent if path.is_file() else path
    return _join_key(s3_root, _relative_path(folder, local_root).as_posix())


def _aws_modules():
    # Imported only for uploads, so previews and host tests do not need boto3.
    aws_functions = importlib.import_module("data.aws.aws_shared_functions")
    s3_functions = importlib.import_module("data.aws.s3_shared_functions")
    return aws_functions, s3_functions


def _connect(params: Mapping[str, str], aws_creds_file: Path | str | None):
    """Create the S3 client from the FIM_AWS_* credentials and confirm the bucket is reachable."""
    creds_path = Path(aws_creds_file) if aws_creds_file else None
    if creds_path is None or not creds_path.is_file():
        raise FileNotFoundError(f"AWS credentials file is missing: {aws_creds_file}")
    bucket = resolve_bucket(params, creds_path)
    if not _is_set(bucket):
        raise ValueError("FIM_S3_BUCKET_NAME is notset")
    credentials = read_env_file(creds_path)

    def credential(key: str) -> str | None:
        return credentials.get(key) or os.getenv(key)

    aws_functions, s3_functions = _aws_modules()
    success, message, client = aws_functions.create_aws_client(
        aws_service_type_name="s3",
        aws_access_key_id=credential("FIM_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=credential("FIM_AWS_SECRET_ACCESS_KEY"),
        aws_region=credential("FIM_AWS_REGION_NAME"),
    )
    if not success or client is None:
        raise RuntimeError(message or "Could not create the S3 client")
    success, message = s3_functions.does_s3_bucket_exist(client, bucket)
    if not success:
        raise RuntimeError(message or f"FIM S3 bucket {bucket} is not accessible")
    return client, bucket, s3_functions


def _missing_uploaded_keys(client, bucket: str, file_list: list[dict[str, str]]) -> list[str]:
    """List the target folder after the upload and return any expected keys that are not there."""
    expected = {item["trg_file"] for item in file_list}
    common = os.path.commonprefix(sorted(expected))
    prefix = common[: common.rfind("/") + 1]
    found: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        found.update(item["Key"] for item in page.get("Contents", []))
    return sorted(expected - found)


def preflight_fim_s3(
    output_paths: Iterable[Path],
    local_root: Path,
    params: Mapping[str, str],
    aws_creds_file: Path | str | None,
    overwrite: bool = False,
) -> list[str]:
    """Upload pre-flight: settings are set, credentials exist, the bucket is reachable, targets are new."""
    errors = []
    s3_root = _setting(params, "FIM_S3_ROOT_INPUTS_PATH")
    if not _is_set(s3_root):
        errors.append("FIM_S3_ROOT_INPUTS_PATH is notset in the workflows params file (-wp)")
    if not _is_set(resolve_bucket(params, aws_creds_file)):
        errors.append(
            "FIM_S3_BUCKET_NAME is notset in the workflows params file (-wp) and the AWS credentials file"
        )
    if aws_creds_file is None or not Path(aws_creds_file).is_file():
        errors.append(f"AWS credentials file is missing: {aws_creds_file}")
    if errors:
        return errors

    try:
        client, bucket, s3_functions = _connect(params, aws_creds_file)
        if not overwrite:
            for path in output_paths:
                prefix = target_prefix(path, local_root, s3_root)
                if s3_functions.does_s3_folder_exist(client, bucket, prefix):
                    errors.append(
                        f"S3 target already exists: s3://{bucket}/{prefix} (add --s3_overwrite to replace)"
                    )
    except Exception as exc:
        errors.append(f"FIM S3 pre-flight failed: {exc}")
    return errors


def _write_manifest(manifest_path: Path, mode: str, bucket: str, file_list: list[dict[str, str]]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"mode": mode, "bucket": bucket, "file_count": len(file_list), "files": file_list}
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def copy_to_fim_s3(
    paths: list[Path],
    local_root: Path,
    params: Mapping[str, str],
    aws_creds_file: Path | str | None,
    upload: bool = False,
    overwrite: bool = False,
    summary: list[str] | None = None,
    manifest_path: Path | None = None,
) -> bool:
    """Preview (default) or upload the files under `paths`. Returns False only when an upload fails."""

    def note(message: str, level: int = logging.INFO) -> None:
        logging.log(level, message)
        if summary is not None:
            summary.append(message)

    sources = ", ".join(str(path) for path in paths)
    s3_root = _setting(params, "FIM_S3_ROOT_INPUTS_PATH")
    bucket = resolve_bucket(params, aws_creds_file)
    try:
        file_list = build_s3_file_list(paths, local_root, s3_root)
    except (OSError, ValueError) as exc:
        if upload:
            note(f"FIM S3 upload stopped: could not list {sources}: {exc}", logging.ERROR)
            return False
        note(f"FIM S3 preview skipped for {sources}: {exc}", logging.WARNING)
        return True

    if manifest_path is not None:
        _write_manifest(Path(manifest_path), "upload" if upload else "preview", bucket, file_list)
    if not file_list:
        note(f"No files to copy to FIM S3 from {sources}")
        return True

    destination = f"s3://{bucket}/{s3_root}"
    file_list_note = f" (file list: {manifest_path})" if manifest_path is not None else ""
    if not upload:
        message = f"Preview only: {len(file_list)} file(s) from {sources} would go under {destination}"
        if not (_is_set(bucket) and _is_set(s3_root)):
            message += "; FIM S3 settings are still notset"
        note(message + file_list_note)
        return True

    if not (_is_set(bucket) and _is_set(s3_root)):
        note("FIM S3 upload stopped: FIM_S3_BUCKET_NAME or FIM_S3_ROOT_INPUTS_PATH is notset", logging.ERROR)
        return False
    try:
        client, bucket, s3_functions = _connect(params, aws_creds_file)
        if not overwrite:
            prefixes = sorted({target_prefix(path, local_root, s3_root) for path in paths})
            existing = [
                prefix for prefix in prefixes if s3_functions.does_s3_folder_exist(client, bucket, prefix)
            ]
            if existing:
                note(
                    f"FIM S3 upload stopped: s3://{bucket}/{existing[0]} already exists (add --s3_overwrite)",
                    logging.ERROR,
                )
                return False
        s3_functions.upload_by_file_list(client, bucket, file_list)
        missing = _missing_uploaded_keys(client, bucket, file_list)
    except Exception as exc:
        note(f"FIM S3 upload failed for {sources}: {exc}", logging.ERROR)
        return False

    if missing:
        note(
            f"FIM S3 upload incomplete: {len(missing)} of {len(file_list)} file(s) are missing, "
            f"e.g. {missing[0]}",
            logging.ERROR,
        )
        return False
    note(f"Uploaded {len(file_list)} file(s) from {sources} to {destination}{file_list_note}")
    return True


def build_s3_copy_step(
    name: str,
    paths: list[Path],
    local_root: Path,
    params: Mapping[str, str],
    aws_creds_file: Path | str | None,
    upload: bool = False,
    overwrite: bool = False,
    summary: list[str] | None = None,
    manifest_dir: Path | None = None,
) -> Step:
    """In-process step named `<name>.s3_copy` that previews or uploads `paths` after `<name>` passed."""
    step_name = copy_step_name(name)
    paths = [Path(path) for path in paths]
    manifest_path = Path(manifest_dir) / f"s3_copy_{step_name}.json" if manifest_dir is not None else None

    def copy_action() -> bool:
        return copy_to_fim_s3(
            paths,
            local_root,
            params,
            aws_creds_file,
            upload=upload,
            overwrite=overwrite,
            summary=summary,
            manifest_path=manifest_path,
        )

    mode = "Upload to" if upload else "Preview (no upload) of the"
    description = f"{mode} FIM S3 copy of {', '.join(str(path) for path in paths)}"
    return Step(name=step_name, action=copy_action, description=description)
