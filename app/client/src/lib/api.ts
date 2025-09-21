export type FilterResult = {
  doi: string;
  hasGrowthRate: boolean;
};

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
    }>;
    return arr
      .filter(
        (x) =>
          typeof x?.doi === "string" && typeof x?.hasGrowthRate === "boolean"
      )
      .map((x) => ({
        doi: x.doi as string,
        hasGrowthRate: x.hasGrowthRate as boolean,
      }));
  }

  // Case 2: [{ doi, hasGrowthRate }]
  if (Array.isArray(data)) {
    const arr = data as Array<
      { doi?: unknown; hasGrowthRate?: unknown } | boolean
    >;
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

  throw new Error("Unexpected response from /api/filter");
}

export async function filterDois(dois: string[]): Promise<FilterResult[]> {
  const base =
    (import.meta as any)?.env?.VITE_API_BASE ??
    "https://tom-ellis-lab--paper2dataset-fastapi-app-dev.modal.run";
  const url = `${String(base).replace(/\/$/, "")}/filter`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dois }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API error ${res.status}: ${text || res.statusText}`);
  }

  const data = await res.json();
  return normalizeResults(dois, data);
}
