#!/usr/bin/env python3
import argparse, concurrent.futures as futures, json, os, re, sys
from datetime import timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

DOI_RE = re.compile(
    r'<article-id[^>]*\bpub-id-type\s*=\s*"(?:doi|DOI)"[^>]*>\s*([^<\s]+)\s*</article-id>',
    flags=re.IGNORECASE,
)

def normalize_doi(doi: str) -> str:
    doi = doi.strip()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return doi.lower()

def mk_client(region=None):
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(retries={"max_attempts": 8, "mode": "standard"})
    )

def is_xml_key(key: str) -> bool:
    # match ".../<paper-id>/content/<something>.xml"
    return key.endswith(".xml") and "/content/" in key and not key.endswith("/")

def list_xml_keys(bucket: str, prefix: str, request_payer: str, region=None):
    s3 = mk_client(region)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, RequestPayer=request_payer):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if is_xml_key(key):
                yield key, obj.get("LastModified"), obj.get("Size")

def try_get_doi_from_bytes(b: bytes):
    # tiny, fast parse: try ET first, fall back to regex
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(b)
        for el in root.iter():
            if el.tag.endswith("article-id") and el.attrib.get("pub-id-type", "").lower() == "doi":
                t = (el.text or "").strip()
                if t:
                    return normalize_doi(t)
    except Exception:
        pass
    m = DOI_RE.search(b.decode("utf-8", "ignore"))
    return normalize_doi(m.group(1)) if m else None

def fetch_doi_fast(bucket, key, request_payer, region=None, range_bytes=131072):
    s3 = mk_client(region)
    # 1) cheap Range-GET of first ~128 KiB
    try:
        part = s3.get_object(
            Bucket=bucket, Key=key, Range=f"bytes=0-{range_bytes-1}",
            RequestPayer=request_payer
        )["Body"].read()
        doi = try_get_doi_from_bytes(part)
        if doi:
            return doi
    except Exception:
        pass
    # 2) full GET fallback
    body = s3.get_object(Bucket=bucket, Key=key, RequestPayer=request_payer)["Body"].read()
    return try_get_doi_from_bytes(body)

def infer_pdf_key(xml_key: str) -> str:
    # .../<uuid>/content/674743.xml -> .../<uuid>/content/674743.pdf
    base = xml_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    pdf_key = xml_key.rsplit("/", 1)[0] + f"/{base}.pdf"
    return pdf_key

def head_exists(bucket, key, request_payer, region=None) -> bool:
    s3 = mk_client(region)
    try:
        s3.head_object(Bucket=bucket, Key=key, RequestPayer=request_payer)
        return True
    except Exception:
        return False

def process_xml_key(bucket, key, request_payer, check_pdf=False, region=None):
    try:
        doi = fetch_doi_fast(bucket, key, request_payer, region=region)
        if not doi:
            sys.stderr.write(f"[WARN] DOI not found in {key}\n")
            return None
        # paper prefix (the folder containing /content/)
        paper_prefix = key.split("/content/")[0]
        pdf_key = infer_pdf_key(key)
        if check_pdf and not head_exists(bucket, pdf_key, request_payer, region=region):
            pdf_key = None
        return {
            "doi": doi,
            "paper_prefix": paper_prefix,   # e.g., "01ad8e08-70ca-1014-a85f-cf9dc087f2c4"
            "xml_key": key,                 # e.g., "01ad8e08-.../content/674743.xml"
            "pdf_key": pdf_key,             # e.g., "01ad8e08-.../content/674743.pdf" (or null if not found)
        }
    except (BotoCoreError, ClientError) as e:
        sys.stderr.write(f"[ERROR] S3 error {key}: {e}\n")
        return None
    except Exception as e:
        sys.stderr.write(f"[ERROR] {key}: {e}\n")
        return None

def load_seen(out_path: Path):
    if not out_path.exists():
        return set()
    seen = set()
    with out_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                seen.add(obj.get("xml_key"))
            except Exception:
                pass
    return seen

def main():
    ap = argparse.ArgumentParser("Index unzipped bioRxiv MECAs in S3 via content/*.xml only.")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="", help="Limit scan (e.g. 'September_2025/' or a shard)")
    ap.add_argument("--request-payer", default=os.getenv("REQUEST_PAYER", "requester"))
    ap.add_argument("--region", default=os.getenv("AWS_REGION"))
    ap.add_argument("--out", default="biorxiv_unzipped_index.jsonl")
    ap.add_argument("--max-workers", type=int, default=int(os.getenv("MAX_WORKERS", "32")))
    ap.add_argument("--check-pdf", action="store_true", help="HEAD the inferred PDF to confirm existence (slower)")
    args = ap.parse_args()

    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    seen = load_seen(out)
    sys.stderr.write("[INFO] Listing XML keys...\n")

    xml_keys = []
    for key, lm, size in list_xml_keys(args.bucket, args.prefix, args.request_payer, args.region):
        if key in seen:
            continue
        xml_keys.append(key)

    sys.stderr.write(f"[INFO] Found {len(xml_keys)} XML objects to process\n")

    batch = []
    with futures.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = [ex.submit(process_xml_key, args.bucket, k, args.request_payer, args.check_pdf, args.region)
                for k in xml_keys]
        for i, fut in enumerate(futs, 1):
            row = fut.result()
            if row:
                batch.append(row)
            if len(batch) >= 500:
                with out.open("a", encoding="utf-8") as f:
                    for r in batch:
                        f.write(json.dumps(r) + "\n")
                batch.clear()
            if i % 2000 == 0:
                sys.stderr.write(f"[INFO] processed {i}/{len(futs)}\n")

    if batch:
        with out.open("a", encoding="utf-8") as f:
            for r in batch:
                f.write(json.dumps(r) + "\n")

    sys.stderr.write("[INFO] Done.\n")

if __name__ == "__main__":
    from pathlib import Path
    main()
