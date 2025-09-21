# unzip_meca_modal.py

import argparse
import io
import mimetypes
import os
import tempfile
from typing import List, Tuple

import modal

app = modal.App("biorxiv-meca-unpacker")
image = modal.Image.debian_slim().pip_install("boto3")

AWS_SECRET_NAME = "aws-prod"  # modal secret with AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION


def _make_s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        config=Config(
            retries={"max_attempts": 10, "mode": "adaptive"},
            max_pool_connections=128,
        ),
    )


def _list_meca_keys(bucket: str, prefix: str) -> List[str]:
    s3 = _make_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".meca"):
                keys.append(key)
    return keys


def _already_unpacked(s3, dest_bucket: str, out_base_prefix: str) -> bool:
    resp = s3.list_objects_v2(Bucket=dest_bucket, Prefix=out_base_prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def _derive_out_base(src_prefix: str, out_prefix: str, full_key: str) -> str:
    if full_key.startswith(src_prefix):
        rel = full_key[len(src_prefix):]
    else:
        # Fallback: replace first /meca/ -> /unpacked/
        rel = full_key.split("/meca/", 1)[-1]
    if rel.endswith(".meca"):
        rel = rel[:-5]
    return f"{out_prefix.rstrip('/')}/{rel}/"


def _upload_stream(s3, dest_bucket: str, dest_key: str, fileobj: io.BufferedReader):
    extra = {}
    ctype, _ = mimetypes.guess_type(dest_key)
    if ctype:
        extra["ContentType"] = ctype
    if extra:
        s3.upload_fileobj(Fileobj=fileobj, Bucket=dest_bucket, Key=dest_key, ExtraArgs=extra)
    else:
        s3.upload_fileobj(Fileobj=fileobj, Bucket=dest_bucket, Key=dest_key)


@app.function(
    image=image,
    secrets=[modal.Secret.from_name(AWS_SECRET_NAME)],
    timeout=900,
    retries=3,
    cpu=1.0,
    max_containers=1,
)
def process_one_meca(
    src_bucket: str,
    src_prefix: str,
    dest_bucket: str,
    out_prefix: str,
    meca_key: str,
    content_only: bool = False,
    skip_if_exists: bool = True,
) -> Tuple[str, str]:
    import zipfile
    from pathlib import PurePosixPath

    s3 = _make_s3_client()
    out_base_prefix = _derive_out_base(src_prefix, out_prefix, meca_key)

    if skip_if_exists and _already_unpacked(s3, dest_bucket, out_base_prefix):
        print(f"[SKIP] {meca_key} => {out_base_prefix}")
        return meca_key, "skipped"

    # Download to temp so zipfile has random access
    with tempfile.NamedTemporaryFile(suffix=".meca") as tmp:
        s3.download_fileobj(src_bucket, meca_key, tmp)
        tmp.flush()
        tmp.seek(0)

        # Determine optional leading folder inside the zip to strip (e.g., "<id>/...")
        with zipfile.ZipFile(tmp.name) as zf:
            names = [zi.filename for zi in zf.infolist() if not zi.is_dir()]
            strip_prefix = ""
            if names:
                first = PurePosixPath(names[0]).parts
                if first:
                    candidate = first[0].rstrip("/")
                    # Basename of the archive without .meca, e.g., "00114533-...dadad"
                    basename = os.path.basename(meca_key)[:-5]
                    if all(p.startswith(candidate + "/") or p == candidate for p in names) and candidate == basename:
                        strip_prefix = candidate + "/"

        with zipfile.ZipFile(tmp.name) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                name = info.filename
                if strip_prefix and name.startswith(strip_prefix):
                    name = name[len(strip_prefix):]

                if content_only and not (name.startswith("content/") or "/content/" in name):
                    continue

                dest_key = out_base_prefix + name
                with zf.open(info, "r") as fobj:
                    _upload_stream(s3, dest_bucket, dest_key, fobj)

    print(f"[OK]   {meca_key} => {out_base_prefix}")
    return meca_key, "ok"


@app.function(image=image, secrets=[modal.Secret.from_name(AWS_SECRET_NAME)], timeout=600, retries=2)
def list_meca(src_bucket: str, src_prefix: str) -> List[str]:
    return _list_meca_keys(src_bucket, src_prefix)


@app.local_entrypoint()
def main(
    src_bucket: str = "biorxiv-copy",
    src_prefix: str = "biorxiv/meca/",
    dest_bucket: str = "",
    out_prefix: str = "biorxiv/unpacked/",
    workers: int = 8,
    limit: int = 0,
    content_only: bool = False,
    no_skip: bool = False,
):
    if not dest_bucket:
        dest_bucket = src_bucket

    # Dial parallelism up/down (containers for process_one_meca)
    process_one_meca.update_autoscaler(max_containers=max(1, int(workers)))  # scale knob :contentReference[oaicite:1]{index=1}

    # Cloud-side listing (correct API is .remote)
    keys = list_meca.remote(src_bucket, src_prefix)  # :contentReference[oaicite:2]{index=2}
    keys = [k for k in keys if k.endswith(".meca")]
    if limit:
        keys = keys[: int(limit)]

    print(f"Found {len(keys)} .meca under s3://{src_bucket}/{src_prefix}.")
    print(f"Output base: s3://{dest_bucket}/{out_prefix} | Workers: {workers}")

    args_iter = [
        (src_bucket, src_prefix, dest_bucket, out_prefix, k, content_only, not no_skip) for k in keys
    ]

    ok = skipped = failed = 0
    for result in process_one_meca.starmap(
        args_iter, return_exceptions=True, wrap_returned_exceptions=False
    ):  # ordered results; collect exceptions instead of crashing :contentReference[oaicite:3]{index=3}
        if isinstance(result, Exception):
            failed += 1
            print(f"[ERR] {result}")
        else:
            _k, status = result
            if status == "skipped":
                skipped += 1
            else:
                ok += 1

    print(f"Done. ok={ok} skipped={skipped} failed={failed}")
