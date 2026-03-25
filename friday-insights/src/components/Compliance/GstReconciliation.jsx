import { useEffect, useState } from "react";
import apiClient from "../../api/client";

export default function GstReconciliation() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await apiClient.get("/compliance/gst-reconciliation/mock");
      setRows(res.data.discrepant_flags || []);
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || "Failed to fetch reconciliation results");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  return (
    <section className="rounded-2xl border border-cyan-400/20 bg-slate-950/70 p-4 sm:p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-cyan-100">GST vs GSTR-2B Reconciliation</h2>
        <button type="button" onClick={load} className="text-xs px-3 py-1.5 rounded-lg border border-cyan-500/40">Refresh</button>
      </div>

      {error ? <div className="text-red-300 text-sm">{error}</div> : null}
      {loading ? <div className="text-slate-300 text-sm">Loading...</div> : null}

      <div className="overflow-auto rounded-xl border border-slate-800">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-900 text-slate-300">
            <tr>
              <th className="p-2 text-left">Our Ledger (Internal)</th>
              <th className="p-2 text-left">GSTR-2B (Government)</th>
              <th className="p-2 text-right">Tax Delta</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && !loading ? (
              <tr><td colSpan={3} className="p-3 text-slate-400">No discrepancies flagged</td></tr>
            ) : (
              rows.map((row, idx) => (
                <tr key={`${row.gstin}-${idx}`} className="border-t border-slate-800 bg-red-500/10">
                  <td className="p-2">
                    <div className="font-medium">{row.invoice_number_internal}</div>
                    <div className="text-xs text-slate-400">{row.gstin}</div>
                    <div className="text-xs text-slate-400">Tax: ₹{Number(row.internal_total_tax || 0).toLocaleString("en-IN")}</div>
                  </td>
                  <td className="p-2">
                    <div className="font-medium">{row.invoice_number_gstr}</div>
                    <div className="text-xs text-slate-400">Tax: ₹{Number(row.gstr_total_tax || 0).toLocaleString("en-IN")}</div>
                    <div className="text-xs text-slate-400">Confidence: {row.match_confidence}%</div>
                  </td>
                  <td className="p-2 text-right font-semibold text-red-300">₹{Number(row.tax_delta || 0).toLocaleString("en-IN")}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
