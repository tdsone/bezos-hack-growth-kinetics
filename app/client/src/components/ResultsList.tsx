import type { FilterResult } from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Button } from "./ui/button";
import * as React from "react";

function Modal({
  open,
  onClose,
  title,
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  children: React.ReactNode;
}) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/40" onClick={onClose} />
      <div className="relative z-10 mx-4 max-w-3xl w-full rounded-lg bg-white shadow-lg border">
        <div className="flex items-center justify-between px-4 py-3 border-b">
          <div className="font-semibold text-sm">{title}</div>
          <Button variant="ghost" size="sm" onClick={onClose}>
            Close
          </Button>
        </div>
        <div className="p-4 overflow-auto max-h-[70vh] text-xs">{children}</div>
      </div>
    </div>
  );
}

type Props = {
  results: FilterResult[];
};

function CheckIcon({ className = "" }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
    >
      <path d="M20 6L9 17l-5-5" />
    </svg>
  );
}

function XIcon({ className = "" }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
    >
      <path d="M18 6L6 18M6 6l12 12" />
    </svg>
  );
}

export default function ResultsList({ results }: Props) {
  const [openForDoi, setOpenForDoi] = React.useState<string | null>(null);

  function downloadCsv(doi: string, csv: string) {
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${doi.replaceAll("/", "_")}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }
  if (!results.length) return null;

  return (
    <>
      <Card className="mt-6">
        <CardHeader>
          <CardTitle>Results</CardTitle>
        </CardHeader>
        <CardContent>
          <ul className="divide-y divide-gray-200">
            {results.map((r) => (
              <li key={r.doi} className="flex items-start gap-3 py-3">
                {r.hasGrowthRate ? (
                  <CheckIcon className="h-5 w-5 text-green-600 mt-0.5" />
                ) : (
                  <XIcon className="h-5 w-5 text-red-600 mt-0.5" />
                )}
                <div className="flex-1">
                  <div className="text-sm font-medium break-all">{r.doi}</div>
                  <div
                    className={`text-xs ${
                      r.hasGrowthRate ? "text-green-700" : "text-red-700"
                    }`}
                  >
                    {r.hasGrowthRate
                      ? "Contains growth rate data"
                      : "Does not contain growth rate data"}
                  </div>
                  {r.hasGrowthRate && (
                    <div className="mt-2 flex items-center gap-2">
                      <Button
                        size="sm"
                        onClick={() => r.datasetCsv && setOpenForDoi(r.doi)}
                        disabled={!r.datasetCsv}
                        title={
                          r.datasetError ||
                          (!r.datasetCsv ? "Dataset not available" : "")
                        }
                      >
                        View Dataset
                      </Button>
                      {r.datasetCsv && (
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => downloadCsv(r.doi, r.datasetCsv!)}
                        >
                          Download CSV
                        </Button>
                      )}
                      {r.datasetError && (
                        <span className="text-xs text-red-600">
                          {r.datasetError}
                        </span>
                      )}
                    </div>
                  )}
                </div>
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>
      {results.map((r) => (
        <Modal
          key={`modal-${r.doi}`}
          open={openForDoi === r.doi}
          onClose={() => setOpenForDoi(null)}
          title={`Dataset for ${r.doi}`}
        >
          {r.datasetCsv ? (
            <pre className="whitespace-pre-wrap break-words">
              {r.datasetCsv}
            </pre>
          ) : (
            <div className="text-sm text-gray-600">No dataset available.</div>
          )}
        </Modal>
      ))}
    </>
  );
}
