import { useState } from "react";
import { useParams } from "react-router-dom";
import axios from "axios";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "/api/v1";

export default function VendorPortal() {
  const { vendorLinkId = "" } = useParams();
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);

  const uploadInvoice = async () => {
    if (!vendorLinkId.trim()) {
      setError("Invalid vendor link.");
      return;
    }
    if (!file) {
      setError("Please select an invoice file.");
      return;
    }

    setUploading(true);
    setError("");
    setResult(null);

    try {
      const form = new FormData();
      form.append("file", file);

      const response = await axios.post(
        `${API_BASE_URL}/vendor/upload/${encodeURIComponent(vendorLinkId)}`,
        form,
        { headers: { "Content-Type": "multipart/form-data" }, timeout: 15000 }
      );
      setResult(response.data);
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || "Upload failed");
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="min-h-[calc(100vh-9rem)] flex items-center justify-center px-4 py-10">
      <section className="w-full max-w-xl rounded-2xl border border-cyan-400/30 bg-slate-950/80 p-6 sm:p-8 space-y-5 shadow-[0_18px_70px_rgba(6,182,212,0.12)]">
        <div className="space-y-2">
          <p className="text-xs uppercase tracking-[0.22em] text-cyan-300/80">Accord Vendor Portal</p>
          <h1 className="text-2xl font-semibold text-cyan-100">Upload Invoice</h1>
          <p className="text-sm text-slate-300">
            Submit your invoice for verification. Your upload is automatically marked as pending approval.
          </p>
        </div>

        <div className="space-y-3">
          <label className="block text-sm text-slate-200">Invoice file</label>
          <input
            type="file"
            onChange={(e) => setFile(e.target.files?.[0] || null)}
            className="block w-full rounded-lg border border-slate-600 bg-slate-900/80 text-slate-200 file:mr-4 file:rounded-md file:border-0 file:bg-cyan-500/20 file:px-3 file:py-2 file:text-cyan-100"
          />
          <button
            type="button"
            onClick={uploadInvoice}
            disabled={uploading}
            className="w-full rounded-xl border border-cyan-400/35 bg-cyan-500/20 px-4 py-2.5 text-cyan-100 font-semibold disabled:opacity-60"
          >
            {uploading ? "Uploading..." : "Submit Invoice"}
          </button>
        </div>

        {error ? <div className="rounded-lg border border-red-400/40 bg-red-500/10 px-3 py-2 text-sm text-red-200">{error}</div> : null}

        {result ? (
          <div className="rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100 space-y-1">
            <div>Submission ID: {result.submission_id}</div>
            <div>Status: {result.approval_status}</div>
            <div>Vendor: {result?.extracted?.vendor_name || "Unknown"}</div>
            <div>GSTIN: {result?.extracted?.gstin || "Unknown"}</div>
            <div>Total: INR {Number(result?.extracted?.total || 0).toLocaleString("en-IN")}</div>
          </div>
        ) : null}
      </section>
    </div>
  );
}
