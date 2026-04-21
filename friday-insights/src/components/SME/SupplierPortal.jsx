import { useMemo, useRef, useState } from "react";
import Papa from "papaparse";

const REQUIRED_COLUMNS = ["item_name", "current_stock", "minimum_stock_level", "unit_price"];

function normalizeHeader(value) {
  return String(value || "").trim().toLowerCase().replace(/\s+/g, "_");
}

export default function SupplierPortal() {
  const [file, setFile] = useState(null);
  const [businessId, setBusinessId] = useState("SME-001");
  const [rows, setRows] = useState([]);
  const [headers, setHeaders] = useState([]);
  const [error, setError] = useState("");
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const inputRef = useRef(null);

  const missingColumns = useMemo(() => {
    const normalized = new Set(headers.map(normalizeHeader));
    return REQUIRED_COLUMNS.filter((col) => !normalized.has(col));
  }, [headers]);

  const parseFile = (selectedFile) => {
    setError("");
    setResult(null);
    setFile(selectedFile);
    Papa.parse(selectedFile, {
      header: true,
      skipEmptyLines: true,
      complete: (parsed) => {
        const parsedHeaders = (parsed.meta.fields || []).map((field) => String(field));
        setHeaders(parsedHeaders);
        setRows(Array.isArray(parsed.data) ? parsed.data.slice(0, 20) : []);
      },
      error: (err) => {
        setError(err?.message || "Unable to parse CSV file");
      },
    });
  };

  const onDrop = (event) => {
    event.preventDefault();
    const dropped = event.dataTransfer.files?.[0];
    if (dropped) parseFile(dropped);
  };

  const onUpload = async () => {
    if (!file) {
      setError("Select a CSV file first");
      return;
    }
    if (missingColumns.length > 0) {
      setError(`Missing required columns: ${missingColumns.join(", ")}`);
      return;
    }

    setUploading(true);
    setError("");
    setResult(null);
    try {
      const form = new FormData();
      form.append("csv_file", file);
      form.append("business_id", businessId.trim() || "SME-001");

      const response = await fetch("/api/v1/suppliers/bulk-upload", {
        method: "POST",
        body: form,
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.detail || `Upload failed (${response.status})`);
      }
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "CSV upload failed");
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="min-h-screen px-4 py-6 md:px-8 md:py-10 bg-slate-100 text-slate-900 dark:bg-slate-950 dark:text-white">
      <div className="mx-auto max-w-6xl space-y-6">
        <section className="rounded-3xl border border-slate-300 bg-white/90 p-6 dark:border-cyan-400/25 dark:bg-slate-900/80">
          <p className="text-xs uppercase tracking-[0.18em] text-cyan-700 dark:text-cyan-300">Supplier Portal</p>
          <h1 className="mt-2 text-3xl md:text-4xl font-black">Bulk Catalog Upload</h1>
          <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
            Drag and drop your supplier CSV, review the mapped preview, then push it to inventory in one click.
          </p>

          <div className="mt-4 flex gap-3 items-center">
            <label className="text-sm font-semibold">Business ID</label>
            <input
              value={businessId}
              onChange={(event) => setBusinessId(event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 bg-white text-slate-900 dark:border-slate-700 dark:bg-slate-950 dark:text-white"
            />
          </div>
        </section>

        <section
          onDragOver={(event) => event.preventDefault()}
          onDrop={onDrop}
          onClick={() => inputRef.current?.click()}
          className="rounded-3xl border-2 border-dashed border-cyan-400/50 bg-cyan-50 p-8 text-center cursor-pointer dark:bg-cyan-500/10"
        >
          <input
            ref={inputRef}
            type="file"
            accept=".csv,text/csv"
            className="hidden"
            onChange={(event) => {
              const selected = event.target.files?.[0];
              if (selected) parseFile(selected);
            }}
          />
          <p className="text-lg font-semibold">Drop CSV here or click to select</p>
          {file ? <p className="mt-2 text-sm">Selected: {file.name}</p> : null}
        </section>

        {headers.length > 0 ? (
          <section className="rounded-3xl border border-slate-300 bg-white p-5 dark:border-slate-700 dark:bg-slate-900/80">
            <h2 className="text-xl font-black mb-3">Preview ({rows.length} rows)</h2>
            {missingColumns.length > 0 ? (
              <p className="mb-3 text-rose-600 dark:text-rose-300">Missing columns: {missingColumns.join(", ")}</p>
            ) : (
              <p className="mb-3 text-emerald-700 dark:text-emerald-300">All required columns detected.</p>
            )}
            <div className="overflow-auto rounded-xl border border-slate-300 dark:border-slate-700">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-100 dark:bg-slate-800/80">
                  <tr>
                    {headers.map((header) => (
                      <th key={header} className="px-3 py-2 text-left font-semibold whitespace-nowrap">{header}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, index) => (
                    <tr key={index} className="border-t border-slate-200 dark:border-slate-800">
                      {headers.map((header) => (
                        <td key={`${index}-${header}`} className="px-3 py-2 whitespace-nowrap">
                          {String(row[header] ?? "")}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <button
              type="button"
              onClick={() => void onUpload()}
              disabled={uploading || missingColumns.length > 0}
              className="mt-4 rounded-xl border border-emerald-400/40 bg-emerald-600 px-4 py-2 font-semibold text-white disabled:opacity-60"
            >
              {uploading ? "Uploading..." : "Confirm Upload"}
            </button>
          </section>
        ) : null}

        {error ? <p className="text-sm text-rose-600 dark:text-rose-300">{error}</p> : null}
        {result ? (
          <p className="text-sm text-emerald-700 dark:text-emerald-300">
            Uploaded {result.inserted_rows} rows to {result.business_id}
          </p>
        ) : null}
      </div>
    </div>
  );
}
