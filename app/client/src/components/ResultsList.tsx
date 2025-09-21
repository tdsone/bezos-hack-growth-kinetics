import type { FilterResult } from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";

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
  if (!results.length) return null;

  return (
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
              </div>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}
