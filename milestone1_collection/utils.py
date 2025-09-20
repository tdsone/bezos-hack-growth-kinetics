import csv
import os
from pathlib import Path
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    requests = None
    import urllib.request as urllib_request


def download_pdf_from_presigned(
    tsv_path: str,
    index: int,
    out_dir: str | None = ".",
    skip_missing: bool = True,
    timeout: int = 60,
    overwrite: bool = False,
) -> str:
    """
    Download the PDF at a given index from a presigned-URLs TSV file into the current folder.

    TSV format per row: rel_meca_path<TAB>s3_pdf_key<TAB>presigned_url

    Args:
        tsv_path: Path to 'presigned_urls.tsv'.
        index: Zero-based index among rows (skips rows without a URL if skip_missing=True).
        out_dir: Target directory (defaults to current directory ".").
        skip_missing: Skip rows with missing/invalid URLs.
        timeout: HTTP timeout (seconds).
        overwrite: If False, auto-rename to avoid clobbering existing files.

    Returns:
        Absolute path to the downloaded PDF.
    """
    tsv = Path(tsv_path)
    if not tsv.exists():
        raise FileNotFoundError(f"TSV not found: {tsv}")

    rows = []
    with tsv.open(newline="") as f:
        for fields in csv.reader(f, delimiter="\t"):
            if not fields or len(fields) < 3:
                continue
            rel, s3key, url = (fields[0].strip(), fields[1].strip(), fields[2].strip())
            if skip_missing and not url.lower().startswith("http"):
                continue
            rows.append((rel, s3key, url))

    if not rows:
        raise ValueError("No valid rows with presigned URLs found.")
    if index < 0 or index >= len(rows):
        raise IndexError(f"Index {index} out of range (0..{len(rows)-1}).")

    rel, s3key, url = rows[index]

    # Target directory (current folder by default)
    target_dir = Path(out_dir or ".")
    target_dir.mkdir(parents=True, exist_ok=True)

    # Derive filename (prefer S3 key basename; otherwise parse from URL)
    name = Path(s3key).name if s3key else Path(urlparse(url).path).name
    if not name:
        name = "paper.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"

    dest = (target_dir / name)

    if not overwrite:
        # Avoid overwriting existing files: "name (1).pdf", "name (2).pdf", ...
        base, suffix = dest.stem, dest.suffix or ".pdf"
        i = 1
        while dest.exists():
            dest = target_dir / f"{base} ({i}){suffix}"
            i += 1

    # Download
    if requests:
        r = requests.get(url, stream=True, timeout=timeout)
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code} fetching presigned URL (maybe expired).")
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(1 << 16):
                if chunk:
                    fh.write(chunk)
    else:
        try:
            with urllib_request.urlopen(url, timeout=timeout) as resp, open(dest, "wb") as fh:
                while True:
                    chunk = resp.read(1 << 16)
                    if not chunk:
                        break
                    fh.write(chunk)
        except Exception as e:
            # Clean up partial file
            try: os.remove(dest)
            except OSError: pass
            raise ValueError(f"Download failed: {e}") from e

    # Quick PDF sanity check
    with open(dest, "rb") as fh:
        magic = fh.read(5)
    if magic[:4] != b"%PDF":
        try: os.remove(dest)
        except OSError: pass
        raise ValueError("Downloaded file does not look like a PDF (URL may have expired).")

    return str(dest.resolve())


# Example:
if __name__ == "__main__":
    path = download_pdf_from_presigned("presigned_urls.tsv", out_dir="biorxiv-papers", index=0)
    print("Saved to:", path)
