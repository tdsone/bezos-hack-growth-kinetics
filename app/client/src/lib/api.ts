export type FilterResult = {
  doi: string;
  hasGrowthRate: boolean;
  datasetCsv?: string;
  datasetError?: string;
};

export type JobStatus = "queued" | "running" | "completed" | "failed";

function apiBase(): string {
  return (
    (import.meta as any)?.env?.VITE_API_BASE ??
    "https://tom-ellis-lab--paper2dataset-fastapi-app.modal.run"
  ).replace(/\/$/, "");
}

function normalizeResults(dois: string[], data: unknown): FilterResult[] {
  // Case 1: { results: Array<{ doi, hasGrowthRate }>} or [{ doi, hasGrowthRate }]
  if (
    data &&
    typeof data === "object" &&
    "results" in (data as Record<string, unknown>) &&
    Array.isArray((data as Record<string, unknown>).results)
  ) {
    const arr = (data as { results: unknown[] }).results as Array<{
      doi?: unknown;
      hasGrowthRate?: unknown;
      datasetCsv?: unknown;
      datasetError?: unknown;
    }>;
    return arr
      .filter(
        (x) =>
          typeof x?.doi === "string" && typeof x?.hasGrowthRate === "boolean"
      )
      .map((x) => ({
        doi: x.doi as string,
        hasGrowthRate: x.hasGrowthRate as boolean,
        datasetCsv:
          typeof x.datasetCsv === "string"
            ? (x.datasetCsv as string)
            : undefined,
        datasetError:
          typeof x.datasetError === "string"
            ? (x.datasetError as string)
            : undefined,
      }));
  }

  // Case 2: [{ doi, hasGrowthRate }] or []
  if (Array.isArray(data)) {
    const arr = data as Array<
      { doi?: unknown; hasGrowthRate?: unknown } | boolean
    >;
    if (arr.length === 0) {
      return [];
    }
    if (
      arr.length > 0 &&
      typeof arr[0] === "object" &&
      arr[0] !== null &&
      "doi" in (arr[0] as object)
    ) {
      return (arr as Array<{ doi: string; hasGrowthRate: boolean }>).filter(
        (x) => typeof x.doi === "string" && typeof x.hasGrowthRate === "boolean"
      );
    }
    // Case 3: [boolean, boolean, ...] in order of input
    if (arr.length === dois.length && typeof arr[0] === "boolean") {
      return dois.map((doi, idx) => ({
        doi,
        hasGrowthRate: arr[idx] as boolean,
      }));
    }
  }

  // Case 4: { [doi: string]: boolean }
  if (data && typeof data === "object" && !Array.isArray(data)) {
    const entries = Object.entries(data as Record<string, unknown>);
    if (
      entries.length > 0 &&
      entries.every(([, v]) => typeof v === "boolean")
    ) {
      return entries.map(([doi, has]) => ({
        doi,
        hasGrowthRate: has as boolean,
      }));
    }
  }

  throw new Error("Unexpected result payload from /result");
}

export async function submitDois(
  dois: string[]
): Promise<{ job_id: string; status: JobStatus }> {
  const res = await fetch(`${apiBase()}/submit`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dois }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API error ${res.status}: ${text || res.statusText}`);
  }
  return res.json();
}

export async function fetchResult(jobId: string): Promise<{
  job_id: string;
  status: JobStatus;
  result?: { results: FilterResult[] };
  error?: string;
}> {
  const res = await fetch(`${apiBase()}/result/${encodeURIComponent(jobId)}`);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API error ${res.status}: ${text || res.statusText}`);
  }
  return res.json();
}

export async function pollResult(
  jobId: string,
  opts: {
    maxWaitMs?: number;
    initialDelayMs?: number;
    maxDelayMs?: number;
  } = {}
): Promise<FilterResult[]> {
  const maxWaitMs = opts.maxWaitMs ?? 60_000;
  const maxDelayMs = opts.maxDelayMs ?? 5_000;
  let delay = opts.initialDelayMs ?? 300;
  const start = Date.now();

  while (Date.now() - start < maxWaitMs) {
    const data = await fetchResult(jobId);
    if (data.status === "completed") {
      return normalizeResults([], data.result?.results ?? []);
    }
    if (data.status === "failed") {
      throw new Error(data.error || "Job failed");
    }
    await new Promise((r) => setTimeout(r, delay));
    delay = Math.min(Math.floor(delay * 1.5), maxDelayMs);
  }
  throw new Error("Timed out waiting for job result");
}
