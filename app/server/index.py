import modal

image = (
    modal.Image
    .debian_slim()
    .pip_install(
        "fastapi[standard]",
        "orjson",
        "boto3",
        "PyPDF2",
        "openai",
    )
)

app = modal.App("paper2dataset")

# Persistent job store for async jobs
job_store = modal.Dict.from_name("paper2dataset-jobs", create_if_missing=True)

# Persisted Volume for per-job folders
vol = modal.Volume.from_name("paper2dataset-jobs-vol", create_if_missing=True)
JOB_MOUNT = "/mnt/jobs"

# Include the DOI->S3 index file in the image (baked at build time)
MAPPING_REMOTE_PATH = "/root/biorxiv_doi_to_s3.jsonl"
image = image.add_local_file(
    "/Users/tds122/Documents/bezos-hack-growth-kinetics/milestone1_collection/scripts/biorxiv_unzipped_index.jsonl",
    remote_path=MAPPING_REMOTE_PATH,
)

# Optional AWS credentials secret name (configure in Modal dashboard)
try:
    aws_secret = modal.Secret.from_name("aws-prod")
except Exception:
    aws_secret = None

# OpenAI API key secret (should expose OPENAI_API_KEY)
try:
    openai_secret = modal.Secret.from_name("openai-secret")
except Exception:
    openai_secret = None

# Cached DOI->PDF key mapping (populated on first use in a container)
_DOI_TO_PDF_KEY_CACHE = None

def _load_doi_to_pdf_mapping() -> dict:

    import os 

    global _DOI_TO_PDF_KEY_CACHE
    if _DOI_TO_PDF_KEY_CACHE is not None and _DOI_TO_PDF_KEY_CACHE is not {}:
        return _DOI_TO_PDF_KEY_CACHE
    import json
    mapping: dict = {}
    try:
        with open(MAPPING_REMOTE_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    doi = obj.get("doi")
                    pdf_key = obj.get("pdf_key")
                    if isinstance(doi, str) and isinstance(pdf_key, str):
                        mapping[doi] = pdf_key
                except Exception:
                    continue
    except FileNotFoundError:
        raise FileNotFoundError("Couldn't find mapping file!!!")
    _DOI_TO_PDF_KEY_CACHE = mapping
    return mapping

def _normalize_doi(doi: str) -> str:
    d = (doi or "").strip()
    if d.startswith("https://doi.org/"):
        d = d[len("https://doi.org/") : ]
    elif d.startswith("http://doi.org/"):
        d = d[len("http://doi.org/") : ]
    if d.lower().startswith("doi:"):
        d = d[4:]
    return d.strip().lower()

@app.function(
    image=image,
    volumes={JOB_MOUNT: vol},
    secrets=[aws_secret] if aws_secret else [],
)
def download_pdfs_from_s3(job_id: str, dois: list[str]) -> dict:
    """Download PDFs listed by DOIs into JOB_MOUNT/<job_id>/pdfs and write a manifest.

    Returns a manifest mapping base filename (without extension) -> original DOI.
    """
    print("Running download_pdfs_from_s3...")
    import os
    import json

    pdf_dir = f"{JOB_MOUNT}/{job_id}/pdfs"
    os.makedirs(pdf_dir, exist_ok=True)

    manifest: dict[str, str] = {}
    for doi in dois:
        try:
            nd = _normalize_doi(doi)
            dest_path = download_pdf_for_doi.remote(job_id, nd)
            base = os.path.splitext(os.path.basename(dest_path))[0]
            manifest[base] = nd
        except Exception as e:
            # Record missing/failed DOIs explicitly with empty value
            manifest[f"missing::{_normalize_doi(doi)}"] = _normalize_doi(doi)
            print("\tGot an exception!")
            continue

    # Persist manifest for downstream steps
    with open(f"{JOB_MOUNT}/{job_id}/manifest.json", "w") as f:
        json.dump(manifest, f)

    # Ensure changes are persisted to the Volume for downstream readers
    vol.commit()

    return manifest

@app.function(
    image=image,
    volumes={JOB_MOUNT: vol},
    secrets=[aws_secret] if aws_secret else [],
)
def download_pdf_for_doi(job_id: str, doi: str) -> str:
    """Download the PDF for a single DOI into the job's pdfs folder.

    Returns the destination file path.
    Requires env var S3_BUCKET to be set in the container environment and AWS creds.
    """
    import os
    import boto3

    mapping = _load_doi_to_pdf_mapping()
    nd = _normalize_doi(doi)
    pdf_key = mapping.get(nd)
    if not pdf_key:
        raise FileNotFoundError(f"DOI not found in mapping or missing pdf_key: {doi}")

    bucket = "biorxiv-copy"
    if not bucket:
        raise RuntimeError("S3_BUCKET environment variable must be set")

    pdf_dir = f"{JOB_MOUNT}/{job_id}/pdfs"
    os.makedirs(pdf_dir, exist_ok=True)

    # Create a filesystem-safe filename from the DOI
    safe_doi = nd.replace("/", "_").replace(":", "_")
    dest_path = f"{pdf_dir}/{safe_doi}.pdf"

    s3 = boto3.client("s3")
    s3.download_file(bucket, pdf_key, dest_path)

    # Ensure the downloaded PDF is persisted to the shared Volume
    vol.commit()

    return dest_path

@app.function(image=image, volumes={JOB_MOUNT: vol})
def extract_plaintext_from_pdfs(job_id: str) -> None:
    """Extract text from PDFs in JOB_MOUNT/<job_id>/pdfs to JOB_MOUNT/<job_id>/plaintext.

    Code adapted from convert_PDFs_to_txts.py (copied, not imported).
    """
    print("Running extract_plaintext_from_pdfs...")
    import os
    import PyPDF2
    import time

    # Ensure we see any prior commits (e.g., downloads written by another function)
    vol.reload()

    input_folder = f"{JOB_MOUNT}/{job_id}/pdfs"
    output_folder = f"{JOB_MOUNT}/{job_id}/plaintext"
    print(f"[convert] job={job_id} input={input_folder} output={output_folder}")

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    if not os.path.isdir(input_folder):
        print(f"[convert] input folder missing: {input_folder}")
        return

    # Wait briefly for downloads to materialize in the shared volume, in case
    # upstream downloads are still committing.
    max_wait_seconds = 20
    waited = 0
    pdf_files: list[str] = []
    while waited <= max_wait_seconds:
        if os.path.isdir(input_folder):
            files = os.listdir(input_folder)
            pdf_files = [f for f in files if f.lower().endswith(".pdf")]
            if pdf_files:
                break
        time.sleep(1)
        waited += 1
        vol.reload()
    if not pdf_files:
        print("[convert] no PDFs found to convert")
        return

    preview = ", ".join(pdf_files[:5]) + (" ..." if len(pdf_files) > 5 else "")
    print(f"[convert] found {len(pdf_files)} pdf(s): {preview}")

    converted = skipped = failed = 0

    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_folder, pdf_file)
        text_file_name = os.path.splitext(pdf_file)[0] + ".txt"
        text_path = os.path.join(output_folder, text_file_name)

        if os.path.exists(text_path):
            skipped += 1
            print(f"[convert] skip existing {text_file_name}")
            continue

        try:
            with open(pdf_path, "rb") as pdf_file_obj:
                pdf_reader = PyPDF2.PdfReader(pdf_file_obj)
                num_pages = len(pdf_reader.pages)
                text_content = ""
                for page_num in range(num_pages):
                    page = pdf_reader.pages[page_num]
                    text_content += page.extract_text() or ""
                with open(text_path, "w", encoding="utf-8") as text_file:
                    text_file.write(text_content)
                converted += 1
                print(f"[convert] wrote {text_file_name} (pages={num_pages})")
        except Exception as e:
            failed += 1
            print(f"[convert] error converting '{pdf_file}': {e}")
            continue

    print(f"[convert] done. converted={converted} skipped={skipped} failed={failed}")
    # Persist plaintext outputs for readers
    vol.commit()

@app.function(image=image, volumes={JOB_MOUNT: vol}, secrets=[openai_secret] if openai_secret else [])
def filter_plaintext_for_growth(job_id: str) -> dict:
    """Scan plaintext files and return per-DOI growth rate flags.

    Code adapted from filtering2_OpenAI_sync.py (copied, not imported), using OpenAI API.

    Returns: {"results": [{"doi": str, "hasGrowthRate": bool}]}
    """
    import os
    import re
    import json
    import asyncio
    from pathlib import Path
    from openai import AsyncOpenAI

    # Ensure we see any prior commits (e.g., extracted plaintext and manifest)
    vol.reload()

    plaintext_dir = Path(f"{JOB_MOUNT}/{job_id}/plaintext")
    manifest_path = Path(f"{JOB_MOUNT}/{job_id}/manifest.json")
    if not plaintext_dir.exists():
        return {"results": []}

    # Load filename -> DOI mapping
    base_to_doi: dict[str, str] = {}
    if manifest_path.exists():
        try:
            base_to_doi = json.loads(manifest_path.read_text())
        except Exception:
            base_to_doi = {}

    def _get_main_text(file_path: Path) -> str:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                full_text = f.read()

            bibliography_headers = [
                "references", "bibliography", "works cited", "literature cited"
            ]

            bibliography_regex = re.compile(
                r"^(?:" + "|".join(re.escape(h) for h in bibliography_headers) + r")\\b",
                re.IGNORECASE | re.MULTILINE
            )

            match = bibliography_regex.search(full_text)
            if match:
                return full_text[:match.start()].strip()
            return full_text.strip()
        except Exception:
            return ""

    async def analyze_paper_for_growth_rates_advanced(file_path: Path) -> tuple[bool, str]:
        # Require OPENAI_API_KEY via Modal Secret
        if not os.environ.get("OPENAI_API_KEY"):
            return (False, "API Key Not Set")

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                full_text = f.read()
        except Exception:
            return (False, "File Read Error")

        client = AsyncOpenAI()
        prompt_text = (
            f"Analyze the following text from a scientific paper:\n\n---\n{full_text}\n\n"
            f"Does this text contain information regarding the growth rates of microorganisms? "
            f"Respond with a single word, either 'True' or 'False'. Do not add any other text, punctuation, or explanation."
        )

        try:
            response = await client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "user", "content": prompt_text}],
            )
            response_text = response.choices[0].message.content.strip().lower()

            if response_text == 'true':
                return (True, "Relevant")
            elif response_text == 'false':
                return (False, "Not Relevant - API")
            else:
                return (False, f"Unexpected API response for {file_path.name}")
        except Exception as e:
            return (False, f"API Error: {e}")

    async def filter_paper(paper_path: Path, keywords, organism_keywords) -> tuple[bool, str]:
        file_name = os.path.basename(paper_path)
        main_text = _get_main_text(paper_path)
        if not main_text:
            return (False, "Not Relevant")
        text_lower = main_text.lower()
        found_relevant_info = any(keyword in text_lower for keyword in keywords)
        if found_relevant_info:
            return await analyze_paper_for_growth_rates_advanced(paper_path)
        else:
            return (False, "Not Relevant - Simple Keyword Filter")

    async def run_all() -> list[tuple[Path, bool]]:
        keywords_list = [
            "growth rate", "doubling time", "specific growth rate", "proliferation",
            "cell division", "generation time", "biomass yield", "cell density",
            "optical density", "turbidity", "OD600", "fermentation", "culture",
            "cultivation", "bioreactor", "batch culture", "exponential phase",
            "log phase", "stationary phase", "lag phase", "growth kinetics", "growth parameters",
            "growth curve"
        ]
        organism_keywords_list = [
            "microorganism", "bacterium", "bacteria", "yeast", "e. coli", "fungus",
            "algae", "microbe", "saccharomyces", "bacillus", "prokaryote", "eukaryote",
            "fungi", "algae", "protozoa", "virus", "pathogen", "streptococcus", "staphylococcus",
            "pseudomonas", "bacillus subtilis", "aspergillus", "penicillium",
            "chlorella", "paramecium", "amoeba", "lactobacillus", "mycobacterium",
            "archaea", "cyanobacteria"
        ]

        paths = [p for p in plaintext_dir.iterdir() if p.is_file() and p.suffix.lower() == '.txt']
        tasks = [filter_paper(p, keywords_list, organism_keywords_list) for p in paths]
        if not tasks:
            return []
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return list(zip(paths, [r[0] for r in results]))

    results_pairs = asyncio.run(run_all())
    results_list = []
    for path, has in results_pairs:
        base = path.stem
        doi = base_to_doi.get(base, base)
        results_list.append({"doi": doi, "hasGrowthRate": bool(has)})

    return {"results": results_list}

@app.function(image=image, timeout=60 * 10, volumes={JOB_MOUNT: vol})
def process_filter_job(job_id: str, dois: list[str]):
    """Background worker that computes filter results and stores them in job_store."""
    try:
        # Mark job as running
        entry = job_store.get(job_id, {})
        entry["status"] = "running"
        job_store[job_id] = entry

        # Ensure job folder exists
        import os
        os.makedirs(f"{JOB_MOUNT}/{job_id}", exist_ok=True)
        vol.commit()

        download_pdfs_from_s3.remote(job_id, dois)
        extract_plaintext_from_pdfs.remote(job_id)
        final_result = filter_plaintext_for_growth.remote(job_id)

        job_store[job_id] = {
            "status": "completed",
            "result": final_result,
        }
    except Exception as exc:
        job_store[job_id] = {
            "status": "failed",
            "error": str(exc),
        }
        raise

MINUTES = 1

@app.function(image=image, volumes={JOB_MOUNT: vol})
@modal.concurrent(max_inputs=100)
@modal.asgi_app()
def fastapi_app():
    """ASGI FastAPI application with CORS enabled."""
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.gzip import GZipMiddleware
    from fastapi.responses import ORJSONResponse
    from pydantic import BaseModel
    from typing import List, Optional
    from enum import Enum
    import uuid
    import time
    import os

    app = FastAPI(title="paper2dataset", default_response_class=ORJSONResponse)

    # --- CORS CONFIGURATION ---
    allowed_origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://bezos-hack-growth-kinetics.vercel.app"
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.add_middleware(GZipMiddleware, minimum_size=1024)

    class GenerateRequest(BaseModel):
        dois: List[str]

    class FilterResult(BaseModel):
        doi: str
        hasGrowthRate: bool

    class FilterResponse(BaseModel):
        results: List[FilterResult]

    class JobStatus(str, Enum):
        queued = "queued"
        running = "running"
        completed = "completed"
        failed = "failed"

    class SubmitResponse(BaseModel):
        job_id: str
        status: JobStatus

    class ResultEnvelope(BaseModel):
        job_id: str
        status: JobStatus
        result: Optional[FilterResponse] = None
        error: Optional[str] = None

    @app.post("/submit", response_model=SubmitResponse)
    async def submit(req: GenerateRequest):
        job_id = str(uuid.uuid4())
        job_store[job_id] = {
            "status": "queued",
            "created_at": time.time(),
            "params": {"dois": req.dois},
        }
        # Create a job folder proactively for this request
        os.makedirs(f"{JOB_MOUNT}/{job_id}", exist_ok=True)
        vol.commit()
        # Fire-and-forget background execution
        process_filter_job.spawn(job_id, req.dois)
        return SubmitResponse(job_id=job_id, status=JobStatus.queued)

    @app.get("/result/{job_id}", response_model=ResultEnvelope)
    async def result(job_id: str):
        entry = job_store.get(job_id)
        if not entry:
            raise HTTPException(status_code=404, detail="Job not found")
        return ResultEnvelope(
            job_id=job_id,
            status=JobStatus(entry["status"]),
            result=entry.get("result"),
            error=entry.get("error"),
        )

    return app