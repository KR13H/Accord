import { useMemo, useState } from "react";
import apiClient from "../../api/client";

export default function InvoiceUploader() {
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  const totalTax = useMemo(() => {
    if (!result) return "0.00";
    const cgst = Number(result.cgst || 0);
    const sgst = Number(result.sgst || 0);
    const igst = Number(result.igst || 0);
    return (cgst + sgst + igst).toFixed(2);
  }, [result]);

  const processFile = async (file) => {
    setError("");
    setUploading(true);
    setResult(null);
    try {
      const form = new FormData();
      form.append("file", file);
      const res = await apiClient.post("/invoices/parse", form, {
        headers: { "Content-Type": "multipart/form-data" },
      });
      setResult(res.data.extracted || null);
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || "Upload failed");
    } finally {
      setUploading(false);
    }
  };

  const onDrop = async (event) => {
    event.preventDefault();
    setDragActive(false);
    const file = event.dataTransfer?.files?.[0];
    if (file) await processFile(file);
  };

  return (
    <section className="rounded-2xl border border-cyan-400/20 bg-slate-950/70 p-4 sm:p-5 space-y-4">
      <h2 className="text-lg font-semibold text-cyan-100">AI Invoice Parser</h2>

      <div
        onDragOver={(e) => { e.preventDefault(); setDragActive(true); }}
        onDragLeave={() => setDragActive(false)}
        onDrop={onDrop}
        className={`rounded-xl border-2 border-dashed p-8 text-center transition ${dragActive ? "border-cyan-400 bg-cyan-500/10" : "border-slate-600 bg-slate-900/60"}`}
      >
        <p className="text-slate-200">Drag and drop invoice file here</p>
        <p className="text-xs text-slate-400 mt-1">or click to select</p>
        <label className="inline-block mt-3 rounded-lg px-3 py-2 bg-slate-800 border border-slate-600 cursor-pointer">
          Choose File
          <input type="file" className="hidden" onChange={(e) => e.target.files?.[0] && processFile(e.target.files[0])} />
        </label>
      </div>

      {uploading ? <div className="text-cyan-300">Local AI extracting GST data...</div> : null}
      {error ? <div className="text-red-300 text-sm">{error}</div> : null}

      {result ? (
        <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-4 space-y-1 text-sm">
          <div><span className="text-slate-300">Vendor:</span> {result.vendor_name}</div>
          <div><span className="text-slate-300">GSTIN:</span> {result.gstin}</div>
          <div><span className="text-slate-300">Total Tax:</span> ₹{Number(totalTax).toLocaleString("en-IN")}</div>
          <div><span className="text-slate-300">Total:</span> ₹{Number(result.total || 0).toLocaleString("en-IN")}</div>
        </div>
      ) : null}
    </section>
  );
}
