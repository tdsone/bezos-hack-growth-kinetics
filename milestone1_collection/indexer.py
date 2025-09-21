#!/usr/bin/env python3
import argparse
import concurrent.futures as futures
import fnmatch
import io
import json
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Set, Tuple, Dict
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# -----------------------------
# Defaults / ENV configuration
# -----------------------------
DEFAULT_BUCKET = os.getenv("BUCKET_NAME", "biorxiv-src-monthly")
REQUEST_PAYER = os.getenv("REQUEST_PAYER", "requester")
AWS_REGION = os.getenv("AWS_REGION")  # let boto3 infer if unset
MAX_WORKERS = int(os.getenv("INDEXER_MAX_WORKERS", "8"))
DEFAULT_OUT_PREFIX = os.getenv("OUT_PREFIX", "biorxiv/unpacked/")

# -----------------------------
# Data model
# -----------------------------
@dataclass
class IndexRow:
    doi: str
    s3_key: str
    xml_member: Optional[str] = None
    pdf_member: Optional[str] = None
    size: Optional[int] = None
    last_modified: Optional[str] = None  # ISO 8601
    etag: Optional[str] = None
    paper_prefix: Optional[str] = None  # e.g., biorxiv/unpacked/.../<uuid>/
    xml_key: Optional[str] = None       # paper_prefix + content/XXXX.xml
    pdf_key: Optional[str] = None       # paper_prefix + content/XXXX.pdf

# -----------------------------
# Helpers
# -----------------------------
DOI_RE = re.compile(
    r'<article-id[^>]*\bpub-id-type\s*=\s*"(?:doi|DOI)"[^>]*>\s*([^<\s]+)\s*</article-id>',
    flags=re.IGNORECASE,
)

def normalize_doi(doi: str) -> str:
    doi = doi.strip()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return doi.lower()

def mk_client():
    cfg = Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"})
    if AWS_REGION:
        return boto3.client("s3", region_name=AWS_REGION, config=cfg)
    return boto3.client("s3", config=cfg)

def iter_s3_mecas(bucket: str, prefix: str) -> Iterator[Dict]:
    """
    Yield minimal S3 object dicts for .meca files under a prefix.
    Dict includes: Key, Size, LastModified, ETag
    """
    s3 = mk_client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix.rstrip("/") + "/",
        RequestPayer=REQUEST_PAYER,
    ):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if key.endswith(".meca"):
                yield {
                    "Key": key,
                    "Size": obj.get("Size"),
                    "LastModified": obj.get("LastModified"),
                    "ETag": obj.get("ETag", "").strip('"'),
                }

def parse_keys_file(path: Path, bucket: str) -> List[str]:
    """
    Accepts lines that are either full presigned URLs or plain s3 keys.
    Returns a list of S3 keys.
    """
    import urllib.parse as up
    keys = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("http://") or line.startswith("https://"):
            # Extract /<bucket>/<key> from URL
            parsed = up.urlparse(line)
            p = parsed.path
            # Some URLs may be like /biorxiv-src-monthly/Current_Content/Month/uuid.meca
            if p.startswith(f"/{bucket}/"):
                key = p[len(bucket) + 2 :].lstrip("/")
                keys.append(key)
            else:
                # Could be virtual-hosted–style URLs like https://biorxiv-src-monthly.s3.amazonaws.com/...
                host = parsed.netloc
                if host.split(".")[0] == bucket:
                    key = p.lstrip("/")
                    keys.append(key)
                else:
                    # best effort: take everything after the bucket name if present
                    if f"/{bucket}/" in p:
                        key = p.split(f"/{bucket}/", 1)[1].lstrip("/")
                        keys.append(key)
                    else:
                        # not our bucket; skip
                        continue
        else:
            # assume it's an S3 key
            keys.append(line)
    # de-dup & keep original order
    seen = set()
    out = []
    for k in keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out

@contextmanager
def tmpfile(suffix=""):
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    try:
        yield Path(path)
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

def _xml_try_etree(xml_bytes: bytes) -> Optional[str]:
    """
    Try to parse with ElementTree and extract DOI. Falls back to regex.
    """
    try:
        import xml.etree.ElementTree as ET
        # Disable external entity loading implicitly (ElementTree default)
        root = ET.fromstring(xml_bytes)
        # Search for article-id with pub-id-type="doi"
        for elem in root.iter():
            if elem.tag.endswith("article-id") and elem.attrib.get("pub-id-type", "").lower() == "doi":
                text = (elem.text or "").strip()
                if text:
                    return normalize_doi(text)
    except Exception:
        pass
    # Fallback: regex
    m = DOI_RE.search(xml_bytes.decode("utf-8", "ignore"))
    return normalize_doi(m.group(1)) if m else None

def extract_doi_from_meca(local_zip: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns (doi, xml_member, pdf_member) from a local .meca zip.
    We look for content/*.xml (prefer a file named like content/<digits>.xml),
    parse it, and infer the sibling PDF if present.
    """
    with zipfile.ZipFile(local_zip, "r") as zf:
        names = zf.namelist()
        # Get candidate XMLs in content/
        xmls = [n for n in names if n.startswith("content/") and n.endswith(".xml")]
        if not xmls:
            return None, None, None
        # Prefer content/<digits>.xml if present; else first
        digit_xmls = [n for n in xmls if re.match(r"content/\d+\.xml$", n)]
        xml_name = digit_xmls[0] if digit_xmls else xmls[0]
        with zf.open(xml_name, "r") as fh:
            xml_bytes = fh.read()
        doi = _xml_try_etree(xml_bytes)
        # infer pdf member
        pdf_guess = None
        stem = Path(xml_name).stem
        candidate_pdf = f"content/{stem}.pdf"
        if candidate_pdf in names:
            pdf_guess = candidate_pdf
        return doi, xml_name, pdf_guess

def download_meca(bucket: str, key: str, dest: Path) -> None:
    s3 = mk_client()
    s3.download_file(
        bucket,
        key,
        str(dest),
        ExtraArgs={"RequestPayer": REQUEST_PAYER},
    )

def head_for_key(bucket: str, key: str) -> Dict:
    s3 = mk_client()
    try:
        resp = s3.head_object(
            Bucket=bucket,
            Key=key,
            RequestPayer=REQUEST_PAYER,
        )
        # Return minimal info
        return {
            "Size": resp.get("ContentLength"),
            "LastModified": resp.get("LastModified"),
            "ETag": (resp.get("ETag") or "").strip('"'),
        }
    except Exception:
        return {}

# -----------------------------
# Unpacked path derivation
# -----------------------------
def _derive_out_base(src_prefix: Optional[str], out_prefix: str, full_key: str) -> str:
    """
    Derive the base unpacked prefix for a given .meca key.
    Mirrors logic in scripts/unzip_meca_modal.py
    """
    if src_prefix and full_key.startswith(src_prefix):
        rel = full_key[len(src_prefix):]
    else:
        parts = full_key.split("/meca/", 1)
        rel = parts[1] if len(parts) == 2 else full_key
    if rel.endswith(".meca"):
        rel = rel[:-5]
    return f"{out_prefix.rstrip('/')}/{rel}/"

# -----------------------------
# Core worker
# -----------------------------
def process_key(bucket: str, key: str, src_prefix: Optional[str], out_prefix: str) -> Optional[IndexRow]:
    """
    Download one MECA, extract DOI & members, return IndexRow (or None if DOI not found).
    """
    # Optional head for metadata; if you already have Size/LastModified (from list), pass it instead.
    meta = head_for_key(bucket, key)
    try:
        with tmpfile(suffix=".meca") as tmp:
            download_meca(bucket, key, tmp)
            doi, xml_member, pdf_member = extract_doi_from_meca(tmp)
            if not doi:
                sys.stderr.write(f"[WARN] No DOI found in {key}\n")
                return None
            paper_prefix = _derive_out_base(src_prefix, out_prefix, key)
            xml_key = f"{paper_prefix}{xml_member}" if xml_member else None
            pdf_key = f"{paper_prefix}{pdf_member}" if pdf_member else None

            row = IndexRow(
                doi=doi,
                s3_key=key,
                xml_member=xml_member,
                pdf_member=pdf_member,
                size=meta.get("Size"),
                last_modified=meta.get("LastModified").astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                if meta.get("LastModified") else None,
                etag=meta.get("ETag"),
                paper_prefix=paper_prefix,
                xml_key=xml_key,
                pdf_key=pdf_key,
            )
            return row
    except (BotoCoreError, ClientError) as e:
        sys.stderr.write(f"[ERROR] S3 error for {key}: {e}\n")
        return None
    except zipfile.BadZipFile:
        sys.stderr.write(f"[ERROR] Corrupt zip (meca) for {key}\n")
        return None
    except Exception as e:
        sys.stderr.write(f"[ERROR] Unexpected error for {key}: {e}\n")
        return None

# -----------------------------
# JSONL I/O
# -----------------------------
def load_existing_keys(out_path: Path) -> Set[str]:
    """
    Return set of S3 keys already present in JSONL (to support resume).
    """
    if not out_path.exists():
        return set()
    seen: Set[str] = set()
    with out_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                k = obj.get("s3_key")
                if k:
                    seen.add(k)
            except Exception:
                continue
    return seen

def append_jsonl(rows: List[IndexRow], out_path: Path):
    if not rows:
        return
    with out_path.open("a", encoding="utf-8") as f:
        for r in rows:
            obj = {
                "doi": r.doi,
                "paper_prefix": r.paper_prefix,
                "xml_key": r.xml_key,
                "pdf_key": r.pdf_key,
            }
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# -----------------------------
# Main routines
# -----------------------------
def build_from_prefixes(bucket: str, prefixes: List[str], out: Path, out_prefix: str):
    already = load_existing_keys(out)
    to_process: List[Tuple[str, str, Optional[int], Optional[datetime], Optional[str]]] = []
    for prefix in prefixes:
        for obj in iter_s3_mecas(bucket, prefix):
            key = obj["Key"]
            if key in already:
                continue
            to_process.append((
                key,
                prefix,
                obj.get("Size"),
                obj.get("LastModified"),
                obj.get("ETag"),
            ))

    sys.stderr.write(f"[INFO] Found {len(to_process)} new .meca objects to process\n")

    # process concurrently
    rows: List[IndexRow] = []
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = []
        for key, _, size, lm, etag in to_process:
            fut = ex.submit(process_key, bucket, key, prefix, out_prefix)
            fut._meta = {"Size": size, "LastModified": lm, "ETag": etag}  # attach for potential enrichment
            futs.append(fut)

        for fut in futures.as_completed(futs):
            row = fut.result()
            if row:
                # If list_objects provided meta, use it (avoids extra HEAD time)
                meta = getattr(fut, "_meta", {})
                if meta.get("Size") and not row.size:
                    row.size = meta["Size"]
                if meta.get("LastModified") and not row.last_modified:
                    row.last_modified = meta["LastModified"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                if meta.get("ETag") and not row.etag:
                    row.etag = meta["ETag"]
                rows.append(row)
                if len(rows) >= 100:
                    append_jsonl(rows, out)
                    rows.clear()

    append_jsonl(rows, out)
    sys.stderr.write("[INFO] Done.\n")

def build_from_keys_file(bucket: str, keys_file: Path, out: Path, src_prefix: Optional[str], out_prefix: str):
    keys = parse_keys_file(keys_file, bucket)
    already = load_existing_keys(out)
    keys = [k for k in keys if k.endswith(".meca") and k not in already]
    sys.stderr.write(f"[INFO] Keys to process from file: {len(keys)}\n")

    rows: List[IndexRow] = []
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(process_key, bucket, k, src_prefix, out_prefix) for k in keys]
        for fut in futures.as_completed(futs):
            row = fut.result()
            if row:
                rows.append(row)
                if len(rows) >= 100:
                    append_jsonl(rows, out)
                    rows.clear()
    append_jsonl(rows, out)
    sys.stderr.write("[INFO] Done.\n")

# -----------------------------
# CLI
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Build DOI → S3 index for bioRxiv MECA archives.")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"S3 bucket (default: {DEFAULT_BUCKET})")
    ap.add_argument("--out", default="biorxiv_doi_to_s3.jsonl", help="Output JSONL path")
    ap.add_argument("--out-prefix", default=DEFAULT_OUT_PREFIX, help=f"Unpacked base prefix (default: {DEFAULT_OUT_PREFIX})")
    ap.add_argument("--src-prefix", default=None, help="Optional source prefix for .meca keys (used when --keys-file)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--prefix", action="append", help="S3 prefix to scan (can pass multiple)")
    g.add_argument("--keys-file", type=Path, help="Text file with presigned URLs or S3 keys (one per line)")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # quick sanity
    if not args.bucket:
        print("ERROR: --bucket is empty.", file=sys.stderr)
        sys.exit(2)

    if args.prefix:
        build_from_prefixes(args.bucket, args.prefix, out_path, args.out_prefix)
    else:
        build_from_keys_file(args.bucket, args.keys_file, out_path, args.src_prefix, args.out_prefix)

if __name__ == "__main__":
    main()
