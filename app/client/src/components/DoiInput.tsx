import { useRef } from "react";
import { Textarea } from "./ui/textarea";
import { Button } from "./ui/button";

type Props = {
  value: string;
  onChange: (text: string) => void;
  onSubmit: () => void;
  onFileSelected: (file: File) => void;
  isProcessing?: boolean;
};

export default function DoiInput({
  value,
  onChange,
  onSubmit,
  onFileSelected,
  isProcessing,
}: Props) {
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  return (
    <div className="space-y-3">
      <label className="block text-sm font-medium">Enter DOIs or links</label>
      <Textarea
        className="h-40"
        placeholder="One per line, or paste text containing DOIs"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      <div className="flex items-center gap-3">
        <Button type="button" onClick={onSubmit} disabled={isProcessing}>
          {isProcessing ? (
            <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-white border-t-transparent" />
          ) : null}
          {isProcessing ? " Processing" : "Process"}
        </Button>

        <Button
          type="button"
          variant="outline"
          onClick={() => fileInputRef.current?.click()}
        >
          Upload .txt
        </Button>
        <input
          ref={fileInputRef}
          type="file"
          accept=".txt"
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) onFileSelected(f);
            e.currentTarget.value = "";
          }}
        />
      </div>
    </div>
  );
}
