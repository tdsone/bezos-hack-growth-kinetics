const DOI_REGEX = /10\.\d{4,9}\/[-._;()/:A-Za-z0-9]+/g;

export function extractDois(input: string): string[] {
  const matches = input.match(DOI_REGEX) || [];
  const cleaned = matches.map((m) => m.replace(/[).,;]+$/, ""));
  // Deduplicate and preserve order
  const seen = new Set<string>();
  const result: string[] = [];
  for (const doi of cleaned) {
    if (!seen.has(doi)) {
      seen.add(doi);
      result.push(doi);
    }
  }
  return result;
}

export async function readTextFromFile(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error);
    reader.readAsText(file);
  });
}
