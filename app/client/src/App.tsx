import "./App.css";
import { useState } from "react";
import DoiInput from "./components/DoiInput";
import ResultsList from "./components/ResultsList";
import { extractDois, readTextFromFile } from "./lib/doi";
import { submitDois, pollResult, type FilterResult } from "./lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "./components/ui/card";

function App() {
  const [rawInput, setRawInput] = useState("");
  const [results, setResults] = useState<FilterResult[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit() {
    setError(null);
    setResults([]);
    const dois = extractDois(rawInput);
    if (dois.length === 0) {
      setError("No DOIs found in the input.");
      return;
    }
    setIsProcessing(true);
    try {
      const { job_id } = await submitDois(dois);
      const results = await pollResult(job_id, { maxWaitMs: 10 * 60_000 });
      setResults(results);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to process DOIs");
    } finally {
      setIsProcessing(false);
    }
  }

  async function handleFile(file: File) {
    try {
      const text = await readTextFromFile(file);
      setRawInput((prev) => (prev ? prev + "\n" + text : text));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to read file");
    }
  }

  return (
    <div className="min-h-screen">
      <header className="border-b bg-white">
        <div className="mx-auto max-w-4xl px-4 py-4">
          <h1 className="text-xl font-semibold">Paper2Dataset</h1>
        </div>
      </header>

      <main className="mx-auto max-w-4xl px-4 py-6">
        <Card>
          <CardHeader>
            <CardTitle>
              Extract Growth Rate Experiment Data from any biorxiv paper
            </CardTitle>
          </CardHeader>
          <CardContent>
            <DoiInput
              value={rawInput}
              onChange={setRawInput}
              onSubmit={handleSubmit}
              onFileSelected={handleFile}
              isProcessing={isProcessing}
            />
            {error && <p className="mt-3 text-sm text-red-600">{error}</p>}
            <ResultsList results={results} />
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

export default App;
