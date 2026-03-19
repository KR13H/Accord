import { useEffect, useMemo, useState } from "react";
import { BarChart3, FileText, Loader2, RefreshCw, ShieldCheck, Users } from "lucide-react";

function formatDate(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString("en-IN", { dateStyle: "medium", timeStyle: "short" });
}

export default function ReportsCenter() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [exportHistory, setExportHistory] = useState([]);
  const [marketingRows, setMarketingRows] = useState([]);
  const [forensic, setForensic] = useState(null);
  const [reversalSummary, setReversalSummary] = useState(null);

  const refreshReports = async () => {
    setLoading(true);
    setError("");
    try {
      const [historyRes, marketingRes, forensicRes, reversalRes] = await Promise.all([
        fetch("/api/v1/reports/export-history?limit=40"),
        fetch("/api/v1/reports/marketing-signups?limit=150"),
        fetch("/api/v1/insights/forensic-audit?limit=150", {
          method: "POST",
          headers: {
            "X-Role": "admin",
            "X-Admin-Id": "101",
          },
        }),
        fetch("/api/v1/journal/reversal-summary/recent?hours=72&include_filed=false"),
      ]);

      const [historyData, marketingData, forensicData, reversalData] = await Promise.all([
        historyRes.json(),
        marketingRes.json(),
        forensicRes.json(),
        reversalRes.json(),
      ]);

      if (!historyRes.ok) {
        throw new Error(historyData?.detail || `Export history failed (${historyRes.status})`);
      }
      if (!marketingRes.ok) {
        throw new Error(marketingData?.detail || `Marketing report failed (${marketingRes.status})`);
      }
      if (!forensicRes.ok) {
        throw new Error(forensicData?.detail || `Forensic report failed (${forensicRes.status})`);
      }
      if (!reversalRes.ok) {
        throw new Error(reversalData?.detail || `Reversal report failed (${reversalRes.status})`);
      }

      setExportHistory(Array.isArray(historyData) ? historyData : []);
      setMarketingRows(Array.isArray(marketingData?.rows) ? marketingData.rows : []);
      setForensic(forensicData);
      setReversalSummary(reversalData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load reports");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refreshReports();
  }, []);

  const totals = useMemo(() => {
    return {
      exports: exportHistory.length,
      leads: marketingRows.length,
      atRisk: Number(reversalSummary?.summary?.at_risk_invoice_count || 0),
      riskScore: Number(forensic?.risk_score || 0),
    };
  }, [exportHistory, marketingRows, reversalSummary, forensic]);

  return (
    <main className="mx-auto max-w-7xl px-4 sm:px-6 py-12 space-y-6">
      <section className="rounded-3xl border border-cyan-500/25 bg-slate-950/70 p-6 sm:p-8">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.24em] text-cyan-200">Accord Report Center</p>
            <h1 className="mt-2 text-3xl font-black tracking-tight text-slate-100">Compliance, Filing, and Growth Reports</h1>
          </div>
          <button
            onClick={() => {
              void refreshReports();
            }}
            disabled={loading}
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm font-semibold text-slate-100 hover:bg-slate-800 disabled:opacity-60"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            Refresh Reports
          </button>
        </div>
        {error ? <p className="mt-4 text-sm text-red-300">{error}</p> : null}
      </section>

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <article className="rounded-2xl border border-cyan-500/30 bg-cyan-900/20 p-4">
          <p className="text-xs text-cyan-200">Total Exports</p>
          <p className="mt-1 text-2xl font-black text-cyan-100">{totals.exports}</p>
        </article>
        <article className="rounded-2xl border border-emerald-500/30 bg-emerald-900/20 p-4">
          <p className="text-xs text-emerald-200">Marketing Leads</p>
          <p className="mt-1 text-2xl font-black text-emerald-100">{totals.leads}</p>
        </article>
        <article className="rounded-2xl border border-amber-500/30 bg-amber-900/20 p-4">
          <p className="text-xs text-amber-200">Invoices At Risk</p>
          <p className="mt-1 text-2xl font-black text-amber-100">{totals.atRisk}</p>
        </article>
        <article className="rounded-2xl border border-violet-500/30 bg-violet-900/20 p-4">
          <p className="text-xs text-violet-200">Forensic Risk Score</p>
          <p className="mt-1 text-2xl font-black text-violet-100">{Math.round(totals.riskScore)}</p>
        </article>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <FileText className="w-4 h-4 text-cyan-300" />
            <h2 className="font-semibold text-slate-100">Filing Export History</h2>
          </div>
          <div className="mt-3 space-y-2 max-h-80 overflow-y-auto">
            {exportHistory.slice(0, 20).map((row) => (
              <div key={row.id} className="rounded-lg border border-slate-800 bg-slate-900/70 p-3 text-xs">
                <div className="flex items-center justify-between gap-3">
                  <span className="font-semibold text-cyan-200">{row.report_type}</span>
                  <span className="text-slate-400">{row.status}</span>
                </div>
                <p className="mt-1 text-slate-300">{row.period_from} to {row.period_to}</p>
                <p className="text-slate-500">Generated: {formatDate(row.generated_at)}</p>
              </div>
            ))}
            {!loading && exportHistory.length === 0 ? <p className="text-xs text-slate-400">No export records found.</p> : null}
          </div>
        </article>

        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <Users className="w-4 h-4 text-emerald-300" />
            <h2 className="font-semibold text-slate-100">Marketing Signup Leads</h2>
          </div>
          <div className="mt-3 space-y-2 max-h-80 overflow-y-auto">
            {marketingRows.slice(0, 20).map((row) => (
              <div key={row.id} className="rounded-lg border border-slate-800 bg-slate-900/70 p-3 text-xs">
                <div className="flex items-center justify-between gap-2">
                  <span className="font-semibold text-emerald-200">{row.name}</span>
                  <span className="text-slate-400">{row.provider}</span>
                </div>
                <p className="mt-1 text-slate-300">{row.email}</p>
                <p className="text-slate-500">Updated: {formatDate(row.updated_at)}</p>
              </div>
            ))}
            {!loading && marketingRows.length === 0 ? <p className="text-xs text-slate-400">No marketing leads captured yet.</p> : null}
          </div>
        </article>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <ShieldCheck className="w-4 h-4 text-amber-300" />
            <h2 className="font-semibold text-slate-100">Reversal Snapshot</h2>
          </div>
          <div className="mt-3 text-sm text-slate-300 space-y-1">
            <p>At-risk invoices: {reversalSummary?.summary?.at_risk_invoice_count ?? 0}</p>
            <p>Immediate reversal risk: {reversalSummary?.summary?.immediate_reversal_risk ?? "0.00"}</p>
            <p>Generated at: {formatDate(reversalSummary?.generated_at)}</p>
          </div>
        </article>

        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <BarChart3 className="w-4 h-4 text-violet-300" />
            <h2 className="font-semibold text-slate-100">Forensic Summary</h2>
          </div>
          <div className="mt-3 text-sm text-slate-300 space-y-1">
            <p>Risk score: {Math.round(Number(forensic?.risk_score || 0))}</p>
            <p>Flagged entries: {Array.isArray(forensic?.flagged_entries) ? forensic.flagged_entries.length : 0}</p>
            <p>Model: {forensic?.model || "-"}</p>
          </div>
        </article>
      </section>
    </main>
  );
}
